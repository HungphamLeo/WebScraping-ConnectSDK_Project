import time
from dataclasses import dataclass
from abc import ABC, abstractmethod
from ..utils import TransactionTypeConvert,TransactionSideConvert, get_end_time_candle

class CommandAPI:
    GET_ALL_SYMBOLS = 'getAllSymbols'
    GET_SYMBOL = 'getSymbol'
    GET_TICKER = 'getTickPrices'
    GET_OPEN_ORDERS = 'getTrades'
    GET_TRADE_HISTORY = 'getTradesHistory'
    GET_ORDER_DETAILS = 'getTradeRecords'
    GET_ORDER_STATUS = 'tradeTransactionStatus'
    PLACE_ORDER = 'tradeTransaction'
    GET_CALENDARS = 'getCalendars'
    GET_LAST_CANDLES = 'getChartLastRequest'
    GET_CANDLES_HISTORY = 'getChartRangeRequest'
    GET_FEES = 'getCommissionDef'
    GET_ACCOUNT_INFOS = 'getCurrentUserData'
    GET_MARGIN_LEVELS = 'getMarginLevel'
    GET_MARGIN_TRADE = 'getMarginTrade'
    GET_NEWS = 'getNews'
    GET_PROFIT = 'getProfitCalculation'
    GET_SERVER_TIME = 'getServerTime'


# Dataclasses for various API records
@dataclass
class ChartLastInfoRecord:
    period: int
    start: float
    symbol: str

@dataclass
class ChartRangeInfoRecord:
    end: float
    period: int
    start: float
    symbol: str
    ticks: int = 0

@dataclass
class TradeTransInfo:
    cmd: int
    expiration: float
    offset: int
    order: int
    price: float
    sl: float
    symbol: str
    tp: float
    type: int
    volume: float


@dataclass
class GetLastCandlesRecord:
    info: dict

@dataclass
class GetCandlesHistoryRecord:
    info: dict

@dataclass
class GetFeesRecord:
    symbol: str
    volume: float

@dataclass
class GetMarginTrade:
    symbol: str
    volume: float

@dataclass
class GetNewsRecord:
    end: int
    start: int

@dataclass
class GetProfitRecord:
    closePrice: float
    openPrice: float
    cmd: int
    symbol: str
    volume: float

@dataclass
class GetSymbolRecord:
    symbol: str

@dataclass
class GetTickerRecord:
    level: int
    symbol: list
    timestamp: int

@dataclass
class GetOrderDetailsRecord:
    orders: list

@dataclass
class GetOpenOrdersRecord:
    openedOnly: bool

@dataclass
class GetTradeHistoryRecord:
    end: int
    start: int

@dataclass
class PlaceOrderRecord:
    tradeTransInfo: TradeTransInfo

class APIRecorder(ABC):
    @abstractmethod
    def record(self, message):
        pass

class APISymbolRecorder(APIRecorder):
    def record(self, message):
        return {
            'data': {
                'ts': message['time'],
                'symbol': message['symbol'],
                'category': message['categoryName'],
                'askPr': message['ask'],
                'bidPr': message['bid'],
                'bestBid': message['bidPr'],
                'bestAsk': message['askPr'],
                'contractSize': message['contractSize'],
                'baseAsset': message['currency'],
                'highest_24h_price': message['high'],
                'lowest_24h_price': message['low'],
                'spread': message['spreadRaw'],
                'leverage': message['leverage'],
                'minQty': message['lotMin'],
                'maxQty': message['lotMax'],
                'tickSize': message['tickSize'],
                'tickPrice': message['tickValue'],
                'priceScale': message['precision'],
                'qtyScale': message['precision'],
            }
        }


class APICalendarRecorder(APIRecorder):
    def record(self, message):
        return {
            'data': {
                'ts': message['time'],
                'wait_news_time': message['previous'],
                'official_news': message['current'] if message['current'] != '' else False,
                'country': message['country'],
                'title': message['title'],
                'body': message['body'],
            }
        }


class APICandlesRecorder(APIRecorder):
    def __init__(self, interval):
        self.interval = interval

    def record(self, candle_message):
        return [
            int(candle_message['ctm']),
            float(candle_message['open']),
            float(candle_message['high']),
            float(candle_message['low']),
            float(candle_message['close']),
            int(candle_message['quoteId']),
            float(candle_message['vol']),
            int(get_end_time_candle(self.interval, candle_message['ctm']))
        ]


class APIMarginLevelRecorder(APIRecorder):
    def record(self, message):
        message = message['returnData']
        return {
            'data': {
                'ts': message['time'],
                'symbol': message['symbol'],
                'level': message['level'],
                'marginBalance': message['marginBalance'],
                'marginFrozen': message['marginFrozen'],
                'marginFree': message['marginFree'],
            }
        }

class APINewsRecorder(APIRecorder):
    def record(self, message):
        return {
            'data': {
                'ts': int(time.time() * 1000),
                'title': str(message['title']),
                'info': str(message['body'])
            }
        }


class APIProfitRecorder(APIRecorder):
    def record(self, message):
        message = message['returnData']
        return {
            'data': {
                'ts': int(message['time']),
                'profit': float(message['profit']),
            }
        }

class APIOrderRecorder(APIRecorder):
    def record(self, message):
        return {
            'ts': int(time.time() * 1000),
            'open_time': int(message['open_time']),
            'open_price': float(message['open_price']),
            'close_time': int(message['close_time']) if message['type'] == 0 else 0,
            'close_price': float(message['close_price']),
            'symbol': str(message['symbol']),
            'side': TransactionSideConvert(message['cmd']),
            'orderId': float(message['order']),
            'transactionId': float(message['order2']),
            'status': TransactionTypeConvert(message['type']),
            'fee': float(message['commission']),
            'position': float(message['position']),
            'profit': float(message['profit']) if message['type'] in [0, 2] else 0,
        }


class APIPlaceOrderRecorder(APIRecorder):
    def record(self, message):
        message = message['returnData']
        return {
            'data': {
                'ts': int(time.time() * 1000),
                'orderId': float(message['order'])
            }
        }

class APIError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.timestamp = int(time.time() * 1000)

class APIRecorderFacade:
    def __init__(self):
        self.recorders = {
            "symbol": APISymbolRecorder(),
            "calendar": APICalendarRecorder(),
            "candles": APICandlesRecorder(interval=1),  # Cần điều chỉnh interval
            "margin_level": APIMarginLevelRecorder(),
            "news": APINewsRecorder(),
            "profit": APIProfitRecorder(),
            "order": APIOrderRecorder(),
            "place_order": APIPlaceOrderRecorder(),
        }

    def record(self, message_type, message):
        if message_type in self.recorders:
            return self.recorders[message_type].record(message)
        else:
            raise APIError(f"Unknown message type: {message_type}")
