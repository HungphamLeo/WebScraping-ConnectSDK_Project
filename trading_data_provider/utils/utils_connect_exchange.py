import os
import sys
import time
sys.path.append(os.getcwd())
from dotenv import load_dotenv
load_dotenv()
mode = os.getenv('MODE')

HOST = 'xapi.xtb.com'
DEMO_XAPI_PORT = 5124
DEMO_XAPI_STREAMING_PORT = 5125
REAL_XPAI_PORT = 5112
REAL_XAPI_STREAMING_PORT = 5113

if mode in ['staging', 'production']:
    DEFAULT_XAPI_ADDRESS = HOST
    DEFAULT_XAPI_PORT = REAL_XPAI_PORT
    DEFAULT_XAPI_STREAMING_PORT = REAL_XAPI_STREAMING_PORT
else:
    DEFAULT_XAPI_ADDRESS = HOST
    DEFAULT_XAPI_PORT = DEMO_XAPI_PORT
    DEFAULT_XAPI_STREAMING_PORT = DEMO_XAPI_STREAMING_PORT

WRAPPER_NAME = 'python'
WRAPPER_VERSION = '2.5.0'
API_SEND_TIMEOUT = 100
API_MAX_CONN_TRIES = 5
INTERVAL = ['1m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d', '1w', '1M']


class cmd:
    TICK_PRICE = "tickPrices"
    TRADE = "trade"
    BALANCE = "balance"
    TRADE_STATUS = "tradeStatus"
    PROFIT = "profit"
    NEWS = "news"
    CANDLES = "candles"

class exchange_name:
    XTB = 'XTB'
    #another exchanges: SSI, EXNESS, etc

class cmd_stream_execute:
    """
    This class contains the commands used to interact with the exchange
    """
    GET_TICK_PRICE = "getTickPrices"
    GET_CANDLES = "getCandles"
    GET_TRADE = "getTrades"
    GET_BALANCE = "getBalance"
    GET_TRADE_STATUS = "getTradeStatus"
    GET_PROFIT = "getProfits"
    GET_NEWS = "getNews"
    STOP_TICK_PRICES = "stopTickPrices"
    STOP_CANDLES = "stopCandles"
    STOP_TRADE = "stopTrades"
    STOP_BALANCE = "stopBalance"
    STOP_TRADE_STATUS = "stopTradeStatus"
    STOP_PROFIT = "stopProfits"
    STOP_NEWS = "stopNews"

def xtb_convert_time_period(period, type = 1):
    if type == 1 :
        period_mapping_1 = {
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
        period = period_mapping_1.get(period, "Invalid period")
    else:
        period_mapping_2 = {
            1: '1m',
            5: '5m',
            15: '15m',
            30: '30m',
            60: '1h',
            240: '4h',
            1440: '1d',
            10080: '1w',
            43200: '1M'
        }
        period = period_mapping_2.get(period, "Invalid period")
    return period

def get_start_time_from_interval(interval, limit=200):
        """
        Retrieves the start time and period for a given interval and limit.

        Args:
            interval (str): The time interval. Can be '1m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d', '1w', '1M'.
            limit (int): The maximum number of intervals. Defaults to 200.

        Returns:
            tuple: A tuple containing the start time (int) and period (int).
        """
        time_multipliers = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '8h': 28800,
            '12h': 43200,
            '1d': 86400,
            '1w': 604800,
            '1M': 2592000
        }
        period = time_multipliers.get(interval, 60) * 1000 - 1
        start_time = int(time.time()) - period * limit
        return start_time, period

def get_end_time_candle(interval, start_time_candle):
        """
        Retrieves the start time and period for a given interval and limit.

        Args:
            interval (str): The time interval. Can be '1m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d', '1w', '1M'.
            limit (int): The maximum number of intervals. Defaults to 200.

        Returns:
            tuple: A tuple containing the start time (int) and period (int).
        """
        time_multipliers = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '8h': 28800,
            '12h': 43200,
            '1d': 86400,
            '1w': 604800,
            '1M': 2592000
        }
        return int(start_time_candle + time_multipliers.get(interval, 60))

def aggregate_candles(candle_list):
    open_price = candle_list[0][1]
    close_price = candle_list[-1][4]
    high_price = max(candle[2] for candle in candle_list)
    low_price = min(candle[3] for candle in candle_list)
    quoteId = candle_list[0]['quoteId']
    total_volume = sum(candle[6] for candle in candle_list)
    start_time = candle_list[0][0]
    end_time = candle_list[-1][-1]
    aggregated_candle = [
        start_time,
        open_price,
        high_price,
        low_price,
        close_price,
        quoteId,
        total_volume,
        end_time
    ]
    
    return aggregated_candle



class quoteId:
    FIXED = 1
    FLOAT = 2
    DEPTH = 3
    CROSS = 4

class requestSatus:
    ERROR = 1
    PENDING = 1
    ACCEPTED = 3
    REJECTED = 4


class TransactionSide:
    BUY = 0
    SELL = 1
    BUY_LIMIT = 2
    SELL_LIMIT = 3
    BUY_STOP = 4
    SELL_STOP = 5
    BALANCE = 6
    CREDIT = 7


class TransactionType:
    ORDER_OPEN = 0
    ORDER_PENDING = 1
    ORDER_CLOSE = 2
    ORDER_MODIFY = 3
    ORDER_DELETE = 4

def TransactionTypeConvert(type):
    if type == 0:
        return TransactionType.ORDER_OPEN
    elif type == 1:
        return TransactionType.ORDER_PENDING
    elif type == 2:
        return TransactionType.ORDER_CLOSE
    elif type == 3:
        return TransactionType.ORDER_MODIFY
    elif type == 4:
        return TransactionType.ORDER_DELETE
    else:
        return None
    
def TransactionSideConvert(side):
    if side == 0:
        return TransactionSide.BUY
    elif side == 1:
        return TransactionSide.SELL
    elif side == 2:
        return TransactionSide.BUY_LIMIT
    elif side == 3:
        return TransactionSide.SELL_LIMIT
    elif side == 4:
        return TransactionSide.BUY_STOP
    elif side == 5:
        return TransactionSide.SELL_STOP
    elif side == 6:
        return TransactionSide.BALANCE
    elif side == 7:
        return TransactionSide.CREDIT
    else:
        return None
    
def calculate_last_price(ticker, price_type):
        """
        Calculate the last price of an asset based on the best bid and best ask prices.

        Parameters:
        ticker (dict): The ticker data
        price_type (str): The type of price to calculate. Can be 'AMP' or 'WAMP'

        Returns:
        float: The calculated last price
        """
        bid_price = float(ticker['bestBid'])
        ask_price = float(ticker['bestAsk'])
        bid_volume = float(ticker['bidSz'])
        ask_volume = float(ticker['askSz'])

        if price_type == 'AMP':
            return (bid_price + ask_price) / 2
        elif price_type == 'WAMP': 
            return ((bid_price * bid_volume) + (ask_price * ask_volume)) / (bid_volume + ask_volume)
def requestSatusConvert(status):
    if status == 1:
        return requestSatus.ERROR
    elif status == 2:
        return requestSatus.PENDING
    elif status == 3:
        return requestSatus.ACCEPTED
    elif status == 4:
        return requestSatus.REJECTED
    else:
        return None