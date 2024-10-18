import time
from . import TransactionTypeConvert, TransactionSideConvert, requestSatusConvert, calculate_last_price, get_end_time_candle

class CandleRecorder:
    @staticmethod
    def record(candle_message):
        return [
            int(candle_message['ctm']),
            float(candle_message['open']),
            float(candle_message['high']),
            float(candle_message['low']),
            float(candle_message['close']),
            int(candle_message['quoteId']),
            float(candle_message['vol']),
            int(get_end_time_candle('1m', candle_message['ctm']))
        ]

class TickerRecorder:
    @staticmethod
    def record(ticker_message):
        tick_data = {
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
        last_price = calculate_last_price(tick_data, 'AMP')
        tick_data['lastPr'] = float(last_price)
        tick_data['last'] = float(last_price)
        return {'data': tick_data}

class BalanceRecorder:
    @staticmethod
    def record(message):
        return {'data': {
            'balance': float(message.get('balance')),
            'credit': float(message.get('credit')),
            'equity': float(message.get('equity')),
            'margin': float(message.get('margin')),
            'marginFee': float(message.get('marginFee')),
            'marginLevel': float(message.get('marginLevel')),
        }}

class NewsRecorder:
    @staticmethod
    def record(news_message):
        return {'data': {
            'ts': int(news_message['time']),
            'title': str(news_message['title']),
            'info': str(news_message['body']),
        }}

class ProfitsRecorder:
    @staticmethod
    def record(message):
        return {'data': {
            'ts': int(time.time() * 1000),
            'orderId': float(message['order']),
            'transactionId': float(message['order2']),
            'position': float(message['position']),
            'profit': float(message['profit']),
        }}

class OrderRecorder:
    @staticmethod
    def record(message):
        return {'data': {
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
        }}

class OrderStatusRecorder:
    @staticmethod
    def record(message):
        return {'data': {
            'ts': int(time.time() * 1000),
            'order': float(message['order']),
            'lastPr': float(message['price']),
            'status': requestSatusConvert(message['requestStatus'])
        }}

# Usage Example
def process_streaming_message(message_type, message =''):
    recorders = {
        'candle': CandleRecorder,
        'ticker': TickerRecorder,
        'balance': BalanceRecorder,
        'news': NewsRecorder,
        'profit': ProfitsRecorder,
        'order': OrderRecorder,
        'order_status': OrderStatusRecorder
    }
    
    if message_type in recorders:
        return recorders[message_type].record(message)
    else:
        raise ValueError("Invalid message type")
