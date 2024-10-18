import os
import json
from redis import Redis
from socket_port import APIStreamClient, APIClient, process_streaming_message, loginCommand
from private.http_data import http_data_provider
from ...utils import handle_error_code, cmd, xtb_convert_time_period
from logger import setup_logger_global
r = Redis(host='localhost', port=6379, decode_responses=True)

class xtb_streaming_data_provider(http_data_provider):
    def __init__(self, base_asset, quote_asset, api_key, secret_key):
        super().__init__(base_asset, quote_asset, api_key, secret_key)
        self.r = r
        self.bids = {}
        self.asks = {}
        self.candles = {}
        self.balances = {}
        self.tick_price = {}
        self.channel = []
    
    def add_channel(self, channel):
        if channel not in self.channel:
            self.channel.append(channel)
    
    def update_data(self):
        self.client_subscribe()
    
    def exit_handle(self):
        self.client_unsubscribe()
    
    def handle_data(self, msg):
        try:
            if msg is None:
                self.logger_base_connect.error("msg is None")
                return
            msg = json.loads(msg)
            message = msg['data']
            if msg["command"] == cmd.TICK_PRICE:
                tick_price = process_streaming_message("tick_price", message)
                self.r.set(f'{self.symbol_redis}_XTB_ticker', json.dumps(tick_price))
            elif msg["command"] == cmd.TRADE:
                order_details = process_streaming_message("order", message)
                self.r.set(f'XTB_order_details', json.dumps(order_details))
            elif msg["command"] == cmd.BALANCE:
                balance = process_streaming_message("balance", message)
                self.r.set(f'XTB_balance', json.dumps(balance))
            elif msg["command"] == cmd.TRADE_STATUS:
                trade_status = process_streaming_message("trade_status", message)
                self.r.set(f'XTB_trade_status', json.dumps(trade_status))
            elif msg["command"] == cmd.PROFIT:
                profit = process_streaming_message("profit", message)
                self.r.set(f'XTB_profit', json.dumps(profit))
            elif msg["command"] == cmd.NEWS:    
                news = process_streaming_message("news", message)
                self.r.set(f'XTB_news', json.dumps(news))
            elif msg["command"] == cmd.CANDLES:
                candle_latest = process_streaming_message("candle", message)
                if len(self.candles.keys()) == 0:
                    self.candles = candle_latest[0]
                else:
                    for key, candle_list in self.candles.items():
                        if key == '1m' and (int(candle_latest[0]) > int(candle_list[0][-1])):
                            candle_list[0] == candle_latest
                            candle_list.pop(-1)
                        else:
                            if (int(candle_latest[0]) > int(candle_list[0][-1])):
                                interval_to_get_candle = xtb_convert_time_period(key,1)
                                candles_list_update = self.get_candles(f'{self.base_asset}{self.quote_asset}', interval_to_get_candle, 200)
                                self.candles[key] = candles_list_update
                        self.r.set(f'XTB_candles_{key}', json.dumps(self.candles))                           
            else:
                self.logger_base_connect.info("Unknown command: " + msg["command"])
        except Exception as e:
            handle_error_code(msg['errorCode'])
 
    
    def client_subscribe(self):
        symbol = f'{self.base_asset}{self.quote_asset}'
        for channel in self.channel:
            if channel == 'ticker':
                self.client_stream.subscribePrice(symbol)
            elif channel == 'balance':
                self.client_stream.subscribeBalance()
            elif channel == 'order_status':
                self.client_stream.subscribeTradeStatus()
            elif channel == 'news':
                self.client_stream.subscribeNews()
            elif channel == 'profits':
                self.client_stream.subscribeProfits()
            elif channel[:-5] == 'klines':
                interval_key = channel.split('_')[1]
                interval_to_get_candle = xtb_convert_time_period(interval_key,1)
                candles_list = get_candles(symbol, interval_to_get_candle, 1000)
                self.candles[interval_key] = candles_list
                self.client_stream.subscribePrice(symbol)

    def client_unsubscribe(self):
        symbol = f'{self.base_asset}{self.quote_asset}'
        for channel in self.channel:
            if channel == 'ticker':
                self.client_stream.unsubscribePrice(symbol)
            elif channel == 'balance':
                self.client_stream.unsubscribeBalance()
            elif channel == 'order_status':
                self.client_stream.unsubscribeTradeStatus()
            elif channel == 'news':
                self.client_stream.unsubscribeNews()
            elif channel == 'profits':
                self.client_stream.unsubscribeProfits()
            elif channel == 'candle':
                self.client_stream.unsubscribePrice(symbol)

    

        

