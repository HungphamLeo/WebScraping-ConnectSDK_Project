import json
import socket
import time
import ssl
from threading import Thread
import sys
from ..utils import *
from logger import setup_logger_global
connection_logger_name = os.path.abspath(__file__)
logger_streaming = setup_logger_global(connection_logger_name, connection_logger_name + '.log')

import socket
import ssl
import json
import time
import logging
from threading import Thread

# Cấu hình logging
logger_streaming = logging.getLogger(__name__)
API_MAX_CONN_TRIES = 5
API_SEND_TIMEOUT = 1000
DEFAULT_XAPI_ADDRESS = "localhost"
DEFAULT_XAPI_PORT = 8080
DEFUALT_XAPI_STREAMING_PORT = 8081

class JsonSocket(object):
    def __init__(self, address, port, encrypt=False):
        self._ssl = encrypt
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        if self._ssl:
            context = ssl.create_default_context()
            self.socket = context.wrap_socket(self.socket, server_hostname=address)

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
                logger_streaming.error("SockThread Error: %s" % msg)
                time.sleep(0.5)
                continue
            logger_streaming.info("Socket connected")
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
                logger_streaming.info('Sent: ' + str(msg))
                time.sleep(API_SEND_TIMEOUT / 1000)

    def _read(self, bytesSize=4096):
        if not self.socket:
            raise RuntimeError("socket connection broken")
        while True:
            char = self.conn.recv(bytesSize).decode()
            self._receivedData += char
            try:
                (resp, size) = self._decoder.raw_decode(self._receivedData)
                if size == len(self._receivedData):
                    self._receivedData = ''
                    break
                elif size < len(self._receivedData):
                    self._receivedData = self._receivedData[size:].strip()
                    break
            except ValueError as e:
                continue
        logger_streaming.info('Received: ' + str(resp))
        return resp

    def _readObj(self):
        msg = self._read()
        return msg

    def close(self):
        logger_streaming.debug("Closing socket")
        self._closeSocket()
        if self.socket is not self.conn:
            logger_streaming.debug("Closing connection socket")
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
    address = property(_get_address, _set_address, doc='read only property socket address')
    port = property(_get_port, _set_port, doc='read only property socket port')
    encrypt = property(_get_encrypt, _set_encrypt, doc='read only property socket port')


class APIClient(JsonSocket):
    def __init__(self, address=DEFAULT_XAPI_ADDRESS, port=DEFAULT_XAPI_PORT, encrypt=True):
        super(APIClient, self).__init__(address, port, encrypt)
        if not self.connect():
            raise Exception("Cannot connect to " + address + ":" +
                            str(port) + " after " + str(API_MAX_CONN_TRIES) + " retries")

    def readCredentials(self, file):
        try:
            with open(file) as f:
                data = json.load(f)
            return data['userId'], data['password']
        except (FileNotFoundError, TypeError) as e:
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
    def __init__(self, address=DEFAULT_XAPI_ADDRESS, port=DEFUALT_XAPI_STREAMING_PORT, encrypt=True, ssId=None
                ):
        super(APIStreamClient, self).__init__(address, port, encrypt)
        self._ssId = ssId
        

        if not self.connect():
            raise Exception("Cannot connect to streaming on " + address + ":" +
                            str(port) + " after " + str(API_MAX_CONN_TRIES) + " retries")

        self._running = True
        self._t = Thread(target=self._readStream, args=())
        self._t.setDaemon(True)
        self._t.start()

    def _readStream(self):
        while self._running:
            self._readObj()
            time.sleep(0.15)

    def disconnect(self):
        self._running = False
        self._t.join()
        self.close()

    def execute(self, dictionary):
        self._sendObj(dictionary)

    def subscribePrice(self, symbol):
        self.execute(dict(command=cmd_stream_execute.GET_TICK_PRICE, symbol=symbol, streamSessionId=self._ssId))

    def subscribePrices(self, symbols):
        for symbolX in symbols:
            self.subscribePrice(symbolX)

    def subscribeTrades(self):
        self.execute(dict(command=cmd_stream_execute.GET_TRADE, streamSessionId=self._ssId))

    def subscribeBalance(self):
        self.execute(dict(command=cmd_stream_execute.GET_BALANCE, streamSessionId=self._ssId))

    def subscribeTradeStatus(self):
        self.execute(dict(command=cmd_stream_execute.GET_TRADE_STATUS, streamSessionId=self._ssId))

    def subscribeProfits(self):
        self.execute(dict(command=cmd_stream_execute.GET_PROFIT, streamSessionId=self._ssId))

    def subscribeNews(self):
        self.execute(dict(command=cmd_stream_execute.GET_NEWS, streamSessionId=self._ssId))

    def unsubscribePrice(self, symbol):
        self.execute(dict(command=cmd_stream_execute.STOP_TICK_PRICES, symbol=symbol, streamSessionId=self._ssId))

    def unsubscribePrices(self, symbols):
        for symbolX in symbols:
            self.unsubscribePrice(symbolX)

    def unsubscribeTrades(self):
        self.execute(dict(command=cmd_stream_execute.STOP_TRADE, streamSessionId=self._ssId))

    def unsubscribeBalance(self):
        self.execute(dict(command=cmd_stream_execute.STOP_BALANCE, streamSessionId=self._ssId))

    def unsubscribeTradeStatus(self):
        self.execute(dict(command=cmd_stream_execute.STOP_TRADE_STATUS, streamSessionId=self._ssId))

    def unsubscribeProfits(self):
        self.execute(dict(command=cmd_stream_execute.STOP_PROFIT, streamSessionId=self._ssId))

    def unsubscribeNews(self):
        self.execute(dict(command=cmd_stream_execute.STOP_NEWS, streamSessionId=self._ssId))

def baseCommand(commandName, arguments=None):
    if arguments is None:
        arguments = dict()
    return dict([('command', commandName), ('arguments', arguments)])


def loginCommand(userId, password, appName=''):
    return baseCommand('login', dict(userId=userId, password=password, appName=appName))


