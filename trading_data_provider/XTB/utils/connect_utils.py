import os
import sys
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


class quoteId:
    FIXED = 1
    FLOAT = 2
    DEPTH = 3
    CROSS = 4

class requestSatus:
    ERROR = 1
    PENDING = 2
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
    match type:
        case 0:
            return TransactionType.ORDER_OPEN
        case 1:
            return TransactionType.ORDER_PENDING
        case 2:
            return TransactionType.ORDER_CLOSE
        case 3:
            return TransactionType.ORDER_MODIFY
        case 4:
            return TransactionType.ORDER_DELETE
        case _:
            return None
    
def TransactionSideConvert(side):
    match side:
        case 0:
            return TransactionSide.BUY
        case 1:
            return TransactionSide.SELL
        case 2:
            return TransactionSide.BUY_LIMIT
        case 3:
            return TransactionSide.SELL_LIMIT
        case 4:
            return TransactionSide.BUY_STOP
        case 5:
            return TransactionSide.SELL_STOP
        case 6:
            return TransactionSide.BALANCE
        case 7:
            return TransactionSide.CREDIT
        case _:
            return None
    

