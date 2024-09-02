import time
import os
import json
import threading
import redis
from xtb_connection import xstreaming_data_provider
r = redis.Redis(host='localhost', port=6379, decode_responses=True)


exchange_dict = {
}
#           "xtb": ["ticker", "kline_1m", "kline_5m", "kline_1h"],

data_global = {}
try:
    json_config = './config.json'
    json_key = './private_infomations/my_credentials.json'
    data_input = {}
    if os.path.exists(json_config):
        with open(json_config, 'r', encoding='utf-8') as f:
            data_input = json.load(f)
            f.close()
    if os.path.exists(json_key):
        with open(json_key, 'r', encoding='utf-8') as f:
            account_key = json.load(f)
            f.close()
        exchanges_key = [item for item in account_key]

    if len(data_input)>0:
        for symbol_raw in data_input.keys():
            symbol = symbol_raw.split('-')[0]
            quote = symbol_raw.split('-')[1]
            for exchange in data_input[symbol_raw].keys():
                symbol_exchange = f'{symbol_raw}_{exchange}'
                if exchange == "xtb" and exchange in exchanges_key:
                    api_key = account_key[exchange]['api_key']
                    secret_key = account_key[exchange]['secret_key']
                    data_global[symbol_exchange] = xstreaming_data_provider(
                                                                            api_key= api_key,
                                                                            secret_key= secret_key,
                                                                            r = r, 
                                                                            symbol=symbol, 
                                                                            quote= quote)
                for channel in data_input[symbol_raw][exchange]:
                    data_global[symbol_exchange].add_channel(channel)
                threading.Thread(target=data_global[symbol_exchange].update_data).start()

        while True:
            print('running')
            time.sleep(10)

except Exception as e:
    print(e, e.__traceback__.tb_lineno )
