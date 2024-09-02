
import time
from ..utils import (TransactionTypeConvert, 
                    TransactionSideConvert, 
                    requestSatusConvert,
                    calculate_last_price,
                    get_end_time_candle)

class command_api_execute:
    get_all_symbols = 'getAllSymbols'
    get_symbols = 'getSymbol'
    get_ticker = 'getTickPrices'
    get_open_orders = 'getTrades'
    get_trade_history = 'getTradesHistory'
    get_order_details = 'getTradeRecords'
    get_order_status = 'tradeTransactionStatus'
    place_order = 'tradeTransaction'
    get_calendars = 'getCalendars'
    get_last_candles = 'getChartLastRequest'
    get_candles_history = 'getChartRangeRequest'
    get_fees = 'getCommissionDef'
    get_account_infos = 'getCurrentUserData'
    get_margin_levels = 'getMarginLevel'
    get_margin_trade = 'getMarginTrade'
    get_news = 'getNews'
    get_profit = 'getProfitCalculation'
    get_server_time = 'getServerTime'

CHART_LAST_INFO_RECORD = {
    'period': int,
    'start': float,
    'symbol': str
}
CHART_RANGE_INFO_RECORD = {
    'end': float,
    'period': int,
    'start': float,
    'symbol': str,
    'ticks': 0
}
TRADE_TRANS_INFO = {
    "cmd": int,
	"expiration": float,
	"offset": int,
	"order": int,
	"price": float,
	"sl": float,
	"symbol": str,
	"tp": float,
	"type": int,
	"volume": float
}
class get_last_candles_record:
    info :dict
    
class get_candles_history_record:
    info: dict
    

class get_fees_record:
    symbol: str
    volume: float

class get_margin_trade:
    symbol: str
    volume: float

class get_news_record:
    end: int
    start: int

class get_profit_record:
    closePrice: float
    openPrice: float
    cmd: int
    symbol: str
    volume: float

class get_symbol_record:
    symbol: str

class get_ticker_record:
    level: int 
    symbol: list
    timestamp: int

class get_order_details_record:
    orders: list

class get_open_orders_record:
    openedOnly : bool

class get_trade_history_record:
    end: int
    start: int


class place_order_record:
    tradeTransInfo = dict(TRADE_TRANS_INFO)


def api_symbol_recorder(message):
    return {'data': {
                'ts': message['time'],
                'symbol': message['symbol'],
                'category': message['categoryName'],
                'askPr': message['ask'],
                'bidPr': message['bid'],
                'bestBid': message['bidPr'],
                'bestAsk': message['askPr'],
                'contractSize': message['contractSize'],
                'baseAsset': message['curency'],
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

def api_calendar_recorder(message):
    return {'data': {
                'ts': message['time'],
                'wait_news_time': message['previous'],
                'official_news': message['current'] if message['current'] != '' else False,
                'country': message['country'],
                'title': message['title'],
                'body': message['body'],
                }
            }

def api_candles_recorder(interval, candle_message):
    return [
            int(candle_message['ctm']),
            float(candle_message['open']),
            float(candle_message['high']),
            float(candle_message['low']),
            float(candle_message['close']),
            int(candle_message['quoteId']),
            float(candle_message['vol']),
            int(get_end_time_candle(interval,candle_message['ctm']))
        ]



def api_margin_level_recorder(message):
    message = message['returnData']
    return {'data': {
                'ts': message['time'],
                'symbol': message['symbol'],
                'level': message['level'],
                'marginBalance': message['marginBalance'],
                'marginFrozen': message['marginFrozen'],
                'marginFree': message['marginFree'],
            }
    }
def api_margin_trade_recorder(message):
    message = message['returnData']
    return {'data':
            {
                'ts': int(time.time()*1000),
                'margin': float(message['margin']),
            }
        }

def api_news_recorder(message):
    return {'data':
            {
                'ts': int(time.time()*1000),
                'title':str(message['title']),
                'info':str(message['body'])
                
            }
        }
def api_profit_recorder(message):
    message = message['returnData']
    return {'data':
            {
                'ts': int(message['time']),
                'profit': float(message['profit']),
            }
        }

def api_order_recorder(message):
    return {
                'ts': int(time.time()*1000),
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
    

def api_place_order_recorder(message):
    message = message['returnData']
    return {'data':{
                'ts': int(time.time()*1000),
                'orderId': float(message['order'])
            }
    }

def api_order_status_recorder(message):
    message = message['returnData']
    return {'data':{
                'ts': int(message['time']),
                'orderId': float(message['order']),
                'bidPr': float(message['bid']),
                'askPr': float(message['ask']),
                'status': requestSatusConvert(message['requestStatus']),
            }
    }

def api_ticker_recorder(ticker_message):
    tick = {
                'symbol': str(ticker_message.get('symbol')),
                'askPr': float(ticker_message.get('ask')),
                'bidPr': float(ticker_message.get('bid')),
                'bestBid': float(ticker_message.get('bestBid')),
                'bestAsk': float(ticker_message.get('bestAsk')),
                'bidSz': float(ticker_message.get('bidVolume')),
                'askSz': float(ticker_message.get('askVolume')),
                'highest_24h_price': float(ticker_message.get('high')),
                'lowest_24h_price': float(ticker_message.get('low')),
                'spread': float(ticker_message.get('spreadRaw')),
                'ts': int(ticker_message.get('timestamp'))
                }
            
    lastPr = calculate_last_price(tick['data'], 'AMP')
    tick['lastPr'] = float(lastPr)
    tick['last'] = float(lastPr)
    return tick