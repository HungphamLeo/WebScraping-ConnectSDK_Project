import requests
import pandas as pd
import os
from .utils_research import URL_STANDARD, STOCK_API_FUNCTION, stock_candles_record
from logger import setup_logger_global

class StockAPIData:
    def __init__(self, api_key, base_asset, quote_asset):
        self.api_key = api_key
        self.symbol_exchange = f'{base_asset}.{quote_asset}'
        self.exchange_name = quote_asset
        current_file = os.path.abspath(__file__)
        connection_logger_name = os.path.basename(current_file)
        self.logger_stock = setup_logger_global(connection_logger_name, connection_logger_name + '.log')

    
    def _fetch_data(self, params):
        """
        Fetches data from the API endpoint specified by URL_STANDARD with the given parameters.

        Args:
            params (dict): A dictionary of parameters to be passed to the API request.

        Returns:
            dict: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            response = requests.get(URL_STANDARD, params=params)
            if response.status_code == 200:
                re = response.json()
                return re
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")

    def get_intraday_time_series(self, interval, base_asset = '', quote_asset = '', 
                                 function=STOCK_API_FUNCTION.TIME_SERIES_INTRADAY,
                                 adjusted=False, extended_hours=False, month=None, 
                                 outputsize="compact", datatype="json", result_type = 'table'):
        """
        Fetches intraday time series data for a given equity symbol using the Alpha Vantage API.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'

            params = {
                "function": function,
                "symbol": symbol,
                "interval": interval,
                "apikey": self.api_key,
                "adjusted": "true" if adjusted else "false",
                "extended_hours": "true" if extended_hours else "false",
                "outputsize": outputsize,
                "datatype": datatype,
            }
            if month:
                params["month"] = month
            re = self._fetch_data(params)
            data = stock_candles_record(re,result_type)
            return data
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")

    def get_daily_time_series(self, base_asset = '', quote_asset = '', 
                              function=STOCK_API_FUNCTION.TIME_SERIES_DAILY, 
                              outputsize="compact", datatype="json", result_type = 'table'):
        """
        Fetches daily time series data for a given equity symbol using the Alpha Vantage API.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'

            params = {
                "function": function,
                "symbol": symbol,
                "apikey": self.api_key,
                "outputsize": outputsize,
                "datatype": datatype,
            }
            re = self._fetch_data(params)
            data = stock_candles_record(re,result_type)
            return data
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")
            
    def get_daily_adjusted_time_series(self,  base_asset = '', quote_asset = '', 
                              function=STOCK_API_FUNCTION.TIME_SERIES_DAILY_ADJUSTED, 
                              outputsize="compact", datatype="json", result_type = 'table'):
        """
        Fetches the daily adjusted time series data for a given equity symbol using the Alpha Vantage API.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": function,
                "symbol": symbol,
                "apikey": self.api_key,
                "outputsize": outputsize,
                "datatype": datatype,
            }
            re = self._fetch_data(params)
            data = stock_candles_record(re,result_type)
            return data
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")

    def get_weekly_time_series(self, base_asset = '', quote_asset = '', 
                              function=STOCK_API_FUNCTION.TIME_SERIES_WEEKLY, 
                              datatype="json", result_type = 'table'):
        """
        Fetches the weekly time series data for a given equity symbol using the Alpha Vantage API.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": function,
                "symbol": symbol,
                "apikey": self.api_key,
                "datatype": datatype,
            }
            re = self._fetch_data(params)
            data = stock_candles_record(re,result_type)
            return data
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")

    def get_weekly_adjusted_time_series(self, base_asset = '', quote_asset = '', 
                                        function=STOCK_API_FUNCTION.TIME_SERIES_WEEKLY_ADJUSTED,
                                        datatype="json", result_type = 'table'):
        """
        Fetches the weekly adjusted time series data for a given equity symbol using the Alpha Vantage API.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": function,
                "symbol": symbol,
                "apikey": self.api_key,
                "datatype": datatype,
            }
            re = self._fetch_data(params)
            data = stock_candles_record(re,result_type)
            return data
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")

    def get_monthly_time_series(self, base_asset = '', quote_asset = '', 
                                function=STOCK_API_FUNCTION.TIME_SERIES_MONTHLY, 
                                datatype="json", result_type = 'table'):
        """
        Fetches the monthly time series data for a given equity symbol using the Alpha Vantage API.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": function,
                "symbol": symbol,
                "apikey": self.api_key,
                "datatype": datatype,
            }
            re = self._fetch_data(params)
            data = stock_candles_record(re,result_type)
            return data
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")

    def get_monthly_adjusted_time_series(self, base_asset = '', quote_asset = '', 
                                        function=STOCK_API_FUNCTION.TIME_SERIES_MONTHLY_ADJUSTED, 
                                        datatype="json", result_type = 'table'):
        """
        Fetches the monthly adjusted time series data for a given equity symbol using the Alpha Vantage API.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": function,
                "symbol": symbol,
                "apikey": self.api_key,
                "datatype": datatype,
            }
            re = self._fetch_data(params)
            data = stock_candles_record(re,result_type)
            return data
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")

    def get_ticker(self, base_asset = '', quote_asset = '',
                    function=STOCK_API_FUNCTION.GLOBAL_QUOTE,  
                    datatype="json", result_type = 'table'):
        """
        Fetches the latest price and volume information for a given ticker symbol using the Alpha Vantage API.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            quote_dict = {}
            params = {
                "function": function,
                "symbol": symbol,
                "apikey": self.api_key,
                "datatype": datatype,
            }
            re = self._fetch_data(params)
            temp_dict = re.values()
            quote_dict['symbol'] = temp_dict['01. symbol']
            quote_dict['open_price'] = float(temp_dict['02. open'])
            quote_dict['high_price'] = float(temp_dict['03. high'])
            quote_dict['low_price'] = float(temp_dict['04. low'])
            quote_dict['close_price'] = float(temp_dict['05. price'])
            quote_dict['volume'] = float(temp_dict['06. volume'])
            quote_dict['last_trade_date'] = temp_dict['07. latest trading day']
            quote_dict['previous_close'] = float(temp_dict['08. previous close'])
            quote_dict['change_price'] = float(temp_dict['09. change'])
            quote_dict['change_percent'] = float(temp_dict['10. change percent'])
            if result_type == 'table':
                table = pd.DataFrame.from_dict(dict, orient='index')
                return table
            else:
                return quote_dict
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")

    def search_symbol(self, keywords, function=STOCK_API_FUNCTION.SYMBOL_SEARCH, datatype="json"):
        """
        Searches for stock symbols or companies using the Alpha Vantage API.
        """
        try:
            params = {
                "function": function,
                "keywords": keywords,
                "apikey": self.api_key,
                "datatype": datatype,
            }
            re = self._fetch_data(params)
            return re
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")
    

    def get_historical_options(self, base_asset = '', quote_asset = '', date=None, datatype='json', result_type = 'table'):
        """
        Fetches the full historical options chain for a specific symbol on a specific date.
        
        Parameters:
            symbol (str): The name of the equity (e.g., 'IBM').
            date (str, optional): The date for which to retrieve the data in 'YYYY-MM-DD' format.
                                If not specified, data for the previous trading session is returned.
            datatype (str, optional): The format in which to receive the data. Options are 'json' or 'csv'.
                                    Defaults to 'json'.
            apikey (str): Your Alpha Vantage API key.
            
        Returns:
            dict or str: The options data in JSON format if datatype='json', otherwise a CSV string.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            
            params = {
                'function': function,
                'symbol': symbol,
                'datatype': datatype,
                'apikey': self.api_key
            }
            
            if date:
                params['date'] = date
            re = self._fetch_data(params)
            if result_type == 'table':
                table = pd.DataFrame.from_dict(re, orient='index')
                return table
            else:
                return re
        except Exception as e:
            self.logger_stock.error(f"API request failed {e}")



