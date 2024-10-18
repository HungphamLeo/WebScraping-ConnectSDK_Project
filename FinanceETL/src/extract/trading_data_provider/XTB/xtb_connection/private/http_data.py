import os
from redis import Redis
from dataclasses import asdict
from socket_port import APIStreamClient, APIClient, process_streaming_message, loginCommand
from ...utils import *
from logger import setup_logger_global
r = Redis(host='localhost', port=6379, decode_responses=True)

class http_data_provider:
    def __init__(self, api_key, secret_key, base_asset = '', quote_asset =''):
        self._client = APIClient()
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.symbol_redis = f'{base_asset}_{quote_asset}'
        loginResponse = self._client.execute(
            loginCommand(apiKey=api_key, secretKey=secret_key)
        )
        connection_logger_name = os.path.abspath(__file__)
        self.logger_base_connect = setup_logger_global(connection_logger_name, connection_logger_name + '.log')
        self.logger_base_connect.info(str(loginResponse))
        if (loginResponse['status'] == False):
            handle_error_code(loginResponse['errorCode'])
            return
        ssid = loginResponse['streamSessionId']
        self.client_stream = APIStreamClient(ssId=ssid, 
                                handle=process_streaming_message)
        self.r = r
        self.format_output = APIRecorderFacade()


    def get_candles(self, symbol, interval, limit, start_time, end_time):
        try:
            candles_list = []
            candles = self._client.commandExecute(
                commandName = CommandAPI.GET_CANDLES_HISTORY,
                arguments = {
                        "info": asdict(ChartRangeInfoRecord(
                            symbol = symbol,
                            period = interval,
                            ticks = limit,
                            start = start_time,
                            end = end_time
                            )
                        )
                    }
            )
            data = candles['returnData']['rateInfos']
            for candle in data:
                candle_format = self.format_output.record('candles',candle)
                candles_list.append(candle_format)
            return candles_list
        except Exception as e:
            self.logger_base_connect.error(e)
        pass

    def get_calendar(self):
        pass

    def get_all_symbols(self):
        pass