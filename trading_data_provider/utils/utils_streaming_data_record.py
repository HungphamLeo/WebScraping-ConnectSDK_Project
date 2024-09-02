import time
from ..utils import TransactionTypeConvert,TransactionSideConvert,requestSatusConvert,calculate_last_price, get_end_time_candle

def streaming_candle_recorder(candle_message):   
    #             'start_time_candle': int(candle_message['ctm']),
    #             'open' : float(candle_message['open']),
    #             'high' : float(candle_message['high']),
    #             'low' : float(candle_message['low']),
    #             'close' : float(candle_message['close']),
    #             'quoteId': int(candle_message['quoteId']),
    #             'volume' : float(candle_message['vol']),
    #             'end_time_candle': int(get_end_time_candle('1m',candle_message['ctm']))
    return [
            int(candle_message['ctm']),
            float(candle_message['open']),
            float(candle_message['high']),
            float(candle_message['low']),
            float(candle_message['close']),
            int(candle_message['quoteId']),
            float(candle_message['vol']),
            int(get_end_time_candle('1m',candle_message['ctm']))
        ]
    
   
def streaming_ticker_recorder(ticker_message):
    """
    This function records the current state of the ticker
    :param ticker_message: the message from the streaming API
    :return: a dictionary containing the current state of the ticker
    """
    tick = {'data': {
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
            }
    lastPr = calculate_last_price(tick['data'], 'AMP')
    tick['data']['lastPr'] = float(lastPr)
    tick['data']['last'] = float(lastPr)
    return tick

def streaming_balance_recorder (message):
    return {'data': {
        'balance': float(message.get('balance')),
        'credit': float(message.get('credit')),
        'equity': float(message.get('equity')),
        'margin': float(message.get('margin')),
        'marginFee': float(message.get('marginFee')),
        'marginLevel': float(message.get('marginLevel')),
    }
}

def streaming_news_recorder(news_message):
    return {'data':{
                'ts': int(news_message['time']),
                'title': str(news_message['title']),
                'info': str(news_message['body']),
            }
    }

def streaming_profits_recorder(message):
    return {'data':{
                'ts': int(time.time()*1000),
                'orderId': float(message['order']),
                'transactionId': float(message['order2']),
                'position': float(message['position']),
                'profit': float(message['profit']),
            }
    }

def streaming_order_recorder(message):
    return {'data':{
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
    }

def streaming_order_status_recorder(message):
    return {'data':{
                'ts': int(time.time()*1000),
                'order': float(message['order']),
                'lastPr': float(message['price']),
                'status': requestSatusConvert(message['requestStatus'])
                }
            }