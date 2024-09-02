import json
import socket
import logging
import time
import ssl
from threading import Thread
import sys

# set to true on debug environment only
DEBUG = True

# default connection properties
DEFAULT_XAPI_ADDRESS = 'xapi.xtb.com'
DEFAULT_XAPI_PORT = 5124
DEFAULT_XAPI_STREAMING_PORT = 5125

# wrapper name and version
WRAPPER_NAME = 'python'
WRAPPER_VERSION = '2.5.0'

# API inter-command timeout (in ms)
API_SEND_TIMEOUT = 100

# max connection tries
API_MAX_CONN_TRIES = 3

# logger properties
logger = logging.getLogger("jsonSocket")
FORMAT = '[%(asctime)s][%(funcName)s:%(lineno)d] %(message)s'
logging.basicConfig(format=FORMAT)

if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.CRITICAL)


class TransactionSide:
    BUY = 0
    SELL = 1
    BUY_LIMIT = 2
    SELL_LIMIT = 3
    BUY_STOP = 4
    SELL_STOP = 5


class TransactionType:
    ORDER_OPEN = 0
    ORDER_CLOSE = 2
    ORDER_MODIFY = 3
    ORDER_DELETE = 4


class JsonSocket:
    def __init__(self, address, port, encrypt=False):
        self._ssl = encrypt
        if not self._ssl:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            context = ssl.create_default_context()
            self.socket = context.wrap_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), server_hostname=address)
        self.conn = self.socket
        self._timeout = None
        self._address = address
        self._port = port
        self._decoder = json.JSONDecoder()
        self._receivedData = ''

    def connect(self):
        for _ in range(API_MAX_CONN_TRIES):
            try:
                self.socket.connect((self._address, self._port))
            except socket.error as msg:
                logger.error("SockThread Error: %s", msg)
                time.sleep(0.25)
                continue
            logger.info("Socket connected")
            return True
        return False

    def _sendObj(self, obj):
        msg = json.dumps(obj)
        self._waitingSend(msg)

    def _waitingSend(self, msg):
        if self.socket:
            sent = 0
            msg = msg.encode('utf-8')
            while sent < len(msg):
                sent += self.conn.send(msg[sent:])
                logger.info('Sent: %s', msg)
                time.sleep(API_SEND_TIMEOUT / 1000)

    def _read(self, bytesSize=4096):
        if not self.socket:
            raise RuntimeError("Socket connection broken")
        while True:
            char = self.conn.recv(bytesSize).decode()
            self._receivedData += char
            try:
                resp, size = self._decoder.raw_decode(self._receivedData)
                if size == len(self._receivedData):
                    self._receivedData = ''
                    break
                elif size < len(self._receivedData):
                    self._receivedData = self._receivedData[size:].strip()
                    break
            except ValueError:
                continue
        logger.info('Received: %s', resp)
        return resp

    def _readObj(self):
        return self._read()

    def close(self):
        logger.debug("Closing socket")
        self._closeSocket()
        if self.socket is not self.conn:
            logger.debug("Closing connection socket")
            self._closeConnection()

    def _closeSocket(self):
        self.socket.close()

    def _closeConnection(self):
        self.conn.close()

    def _get_timeout(self):
        return self._timeout

    def _set_timeout(self, timeout):
        self._timeout = timeout
        self.socket.settimeout(timeout)

    def _get_address(self):
        return self._address

    def _set_address(self, address):
        pass

    def _get_port(self):
        return self._port

    def _set_port(self, port):
        pass

    def _get_encrypt(self):
        return self._ssl

    def _set_encrypt(self, encrypt):
        pass

    timeout = property(_get_timeout, _set_timeout, doc='Get/set the socket timeout')
    address = property(_get_address, _set_address, doc='Read only property socket address')
    port = property(_get_port, _set_port, doc='Read only property socket port')
    encrypt = property(_get_encrypt, _set_encrypt, doc='Read only property socket port')


class APIClient(JsonSocket):
    def __init__(self, address=DEFAULT_XAPI_ADDRESS, port=DEFAULT_XAPI_PORT, encrypt=True):
        super().__init__(address, port, encrypt)
        if not self.connect():
            raise Exception("Cannot connect to {}:{} after {} retries".format(address, port, API_MAX_CONN_TRIES))

    def readCredentials(self, file):
        try:
            with open(file) as f:
                data = json.load(f)
            return data['userId'], data['password']
        except (FileNotFoundError, TypeError):
            print("Missing json file!")
            sys.exit(1)

    def execute(self, dictionary):
        self._sendObj(dictionary)
        return self._readObj()

    def disconnect(self):
        self.close()

    def commandExecute(self, commandName, arguments=None):
        return self.execute(baseCommand(commandName, arguments))


class APIStreamClient(JsonSocket):
    def __init__(self, address=DEFAULT_XAPI_ADDRESS, port=DEFAULT_XAPI_STREAMING_PORT, encrypt=True, ssId=None,
                 tickFun=None, tradeFun=None, balanceFun=None, tradeStatusFun=None, profitFun=None, newsFun=None):
        super().__init__(address, port, encrypt)
        self._ssId = ssId
        self._tickFun = tickFun
        self._tradeFun = tradeFun
        self._balanceFun = balanceFun
        self._tradeStatusFun = tradeStatusFun
        self._profitFun = profitFun
        self._newsFun = newsFun

        if not self.connect():
            raise Exception("Cannot connect to streaming on {}:{} after {} retries".format(address, port, API_MAX_CONN_TRIES))

        self._running = True
        self._t = Thread(target=self._readStream)
        self._t.daemon = True
        self._t.start()

    def _readStream(self):
        while self._running:
            msg = self._readObj()
            logger.info("Stream received: %s", msg)
            print(msg)
            if msg["command"] == 'tickPrices':
                self._tickFun(msg)
            elif msg["command"] == 'trade':
                self._tradeFun(msg)
            elif msg["command"] == "balance":
                self._balanceFun(msg)
            elif msg["command"] == "tradeStatus":
                self._tradeStatusFun(msg)
            elif msg["command"] == "profit":
                self._profitFun(msg)
            elif msg["command"] == "news":
                self._newsFun(msg)

    def disconnect(self):
        self._running = False
        self._t.join()
        self.close()

    def execute(self, dictionary):
        self._sendObj(dictionary)

    def subscribePrice(self, symbol):
        self.execute(dict(command='getTickPrices', symbol=symbol, streamSessionId=self._ssId))

    def subscribePrices(self, symbols):
        for symbolX in symbols:
            self.subscribePrice(symbolX)

    def subscribeTrades(self):
        self.execute(dict(command='getTrades', streamSessionId=self._ssId))

    def subscribeBalance(self):
        self.execute(dict(command='getBalance', streamSessionId=self._ssId))

    def subscribeTradeStatus(self):
        self.execute(dict(command='getTradeStatus', streamSessionId=self._ssId))

    def subscribeProfits(self):
        self.execute(dict(command='getProfits', streamSessionId=self._ssId))

    def subscribeNews(self):
        self.execute(dict(command='getNews', streamSessionId=self._ssId))

    def unsubscribePrice(self, symbol):
        self.execute(dict(command='stopTickPrices', symbol=symbol, streamSessionId=self._ssId))

    def unsubscribePrices(self, symbols):
        for symbolX in symbols:
            self.unsubscribePrice(symbolX)

    def unsubscribeTrades(self):
        self.execute(dict(command='stopTrades', streamSessionId=self._ssId))

    def unsubscribeBalance(self):
        self.execute(dict(command='stopBalance', streamSessionId=self._ssId))

    def unsubscribeTradeStatus(self):
        self.execute(dict(command='stopTradeStatus', streamSessionId=self._ssId))

    def unsubscribeProfits(self):
        self.execute(dict(command='stopProfits', streamSessionId=self._ssId))

    def unsubscribeNews(self):
        self.execute(dict(command='stopNews', streamSessionId=self._ssId))


# Command templates
def baseCommand(commandName, arguments=None):
    if arguments == None:
        arguments = dict()
    return dict([('command', commandName), ('arguments', arguments)])

CHART_RANGE_INFO_RECORD = {
    'end': float,
    'period': int,
    'start': float,
    'symbol': str
    
    
}
class command_api_execute:
    get_candles_history = 'getChartRangeRequest'

def api_candles_recorder(candle_message):
    return {'data':{
            'ts': int(candle_message['ctm']),
            'open' : float(candle_message['open']),
            'high' : float(candle_message['high']),
            'low' : float(candle_message['low']),
            'close' : float(candle_message['close']),
            'quoteId': int(candle_message['quoteId']),
            'volume' : float(candle_message['vol'])
            }
        }
def xtb_convert_time_period(period):
    period_mapping = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '30m': 30,
        '1h': 60,
        '4h': 240,
        '1d': 1440,
        '1w': 10080,
        '1M': 43200
    }
    return period_mapping.get(period, "Invalid period")
def get_candles_history(    client_api,
                            start_time,
                            end_time,
                            interval='1m',
                            limit = 0,
                            base_asset = '', 
                            quote_asset = 'USD',):
        # if base_asset == '':
        #     symbol = self.symbol_exchange
        # else:
        symbol = f'{base_asset}{quote_asset}'
        candles_dict = {}
        candles_dict['data'] = []
        info = dict(CHART_RANGE_INFO_RECORD)
        info['start'] = start_time
        info['end'] = end_time
        info['symbol'] = symbol
        info['period'] = xtb_convert_time_period(interval)
        info['ticks'] = limit

        # info = json.dumps(info)
        print('339----',info)

        data = client_api.commandExecute(commandName = command_api_execute.get_candles_history,
                                        arguments = {'info':info})
        print(data)
        time.sleep(5)
        for item in data['returnData']['rateInfos']:
            candle = api_candles_recorder(item)
            candles_dict['data'].append(candle['data'])
        candles_dict['ts'] = int(time.time()*1000)
        return candles_dict
        

def main():
    # Cấu hình
    address = 'xapi.xtb.com'
    port = 5124
    encrypt = True

    # Khởi tạo APIClient
    try:
        client = APIClient(address=address, port=port, encrypt=encrypt)
        
        # getCandle =  {
        #                     "start": 1724288400000,
        #                     "period": 5,
        #                     "end": 1724202000000,
        #                     "symbol": "AAPL.US"
        #                 }
                    
        
        # print("APIClient connected successfully.", candle)
    except Exception as e:
        print(f"Failed to connect APIClient: {e}")
        return

    # Đọc thông tin đăng nhập từ file (thay đổi theo đường dẫn file của bạn)
    credentials_file = './data_provider/private_infomations/my_credentials.json'
    try:
        userId, password = client.readCredentials(credentials_file)
        print(f"UserId: {userId}, Password: {password}")
    except Exception as e:
        print(f"Failed to read credentials: {e}")
        return

    # Gửi lệnh đăng nhập (cần thay đổi tùy vào API)
    try:
        login_command = {
            'command': 'login',
            "arguments": {
            'userId': userId,
            'password': password,
            'wrapper': WRAPPER_NAME,
            'wrapperVersion': WRAPPER_VERSION
            }
        }
        response = client.execute(login_command)
        print("Login response:", response)
        candle = get_candles_history(client_api = client,
                                     start_time = 1724236743000,
                                     end_time =1724409543000, 
                                     base_asset = 'EUR', quote_asset = 'USD', 
                                     interval = '1h', limit= 0)
        # candle = client.commandExecute(commandName ='getChartRangeRequest', 
        #                                 arguments = { 'info':CHART_RANGE_INFO_RECORD }
        #                                 )
        # print("APIClient connected successfully.", candle['returnData']['rateInfos'])
        # print(type(candle['returnData']['rateInfos']))
        print(len(candle))
    except Exception as e:
        print(f"Failed to execute login command: {e}")
        return

    # # Khởi tạo APIStreamClient
    # try:
    #     print('327 ----',response)
    #     time.sleep(3)
    #     stream_client = APIStreamClient(
    #         address=address,
    #         port=DEFAULT_XAPI_STREAMING_PORT,
    #         encrypt=encrypt,
    #         ssId = response.get('streamSessionId'),  # Thay đổi với session ID của bạn
    #         tickFun=lambda msg: print("Tick data received:", msg),
    #         tradeFun=lambda msg: print("Trade data received:", msg),
    #         balanceFun=lambda msg: print("Balance data received:", msg),
    #         tradeStatusFun=lambda msg: print("Trade status data received:", msg),
    #         profitFun=lambda msg: print("Profit data received:", msg),
    #         newsFun=lambda msg: print("News data received:", msg)
    #     )
    #     print("APIStreamClient connected successfully.")
    # except Exception as e:
    #     print(f"Failed to connect APIStreamClient: {e}")
    #     return

    # # Đăng ký nhận dữ liệu
    # try:
    #     stream_client.subscribePrice("EURUSD")
    #     print("Subscribed to EURUSD tick prices.")
    # except Exception as e:
    #     print(f"Failed to subscribe to tick prices: {e}")

    # # Giữ cho chương trình chạy để nhận dữ liệu
    # try:
    #     print("Press Ctrl+C to exit...")
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     print("Interrupted by user.")
    # finally:
    #     # Ngắt kết nối
    #     client.disconnect()
    #     stream_client.disconnect()
    #     print("Disconnected from server.")

if __name__ == "__main__":
    main()
