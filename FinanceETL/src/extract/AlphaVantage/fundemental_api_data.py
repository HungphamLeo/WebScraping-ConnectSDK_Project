from .utils import URL_STANDARD, STOCK_FUNDEMENTAL_FUNCTION
from .base_abstract import AlphaVantageBase

class FundementalStockData(AlphaVantageBase):
    def __init__(self, api_key, base_asset, quote_asset):
        
        super().__init__(api_key, base_asset, quote_asset)
        """
        Initializes a FundementalStockData object.

        Parameters:
        api_key (str): The API key for authentication.
        base_asset (str): The base asset of the stock symbol.
        quote_asset (str): The quote asset of the stock symbol.

        Returns:
        None
        """

    def get_fundamental_stock_data(self, base_asset = '', quote_asset = ''):
        """
        Retrieves fundamental stock data from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.

        Returns:
            dict: The JSON response from the API containing the fundamental stock data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.OVERVIEW,
                "symbol": symbol,
                "apikey": self.api_key
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_fundamental_etf_data(self, base_asset = '', quote_asset = ''):
        """
        Retrieves fundamental ETF data from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.

        Returns:
            dict: The JSON response from the API containing the fundamental ETF data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.ETF_PROFILE,
                "symbol": symbol,
                "apikey": self.api_key
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")

    def get_fundamental_corporate_splits(self, base_asset = '', quote_asset = ''):
        """
        Retrieves fundamental corporate splits data from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.

        Returns:
            dict: The JSON response from the API containing the fundamental corporate splits data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.SPLITS,
                "symbol": symbol,
                "apikey": self.api_key
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")


    def get_fundamental_income_statement(self, base_asset = '', quote_asset = ''):
        """
        Retrieves fundamental income statement data from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.

        Returns:
            dict: The JSON response from the API containing the fundamental income statement data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.INCOME_STATEMENT,
                "symbol": symbol,
                "apikey": self.api_key
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_fundamental_balance_sheet(self, base_asset = '', quote_asset = ''):
        """
        Retrieves fundamental balance sheet data from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.

        Returns:
            dict: The JSON response from the API containing the fundamental balance sheet data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.INCOME_STATEMENT,
                "symbol": symbol,
                "apikey": self.api_key
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_fundamental_cash_flow(self, base_asset = '', quote_asset = ''):
        """
        Retrieves fundamental cash flow data from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.

        Returns:
            dict: The JSON response from the API containing the fundamental cash flow data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.CASH_FLOW,
                "symbol": symbol,
                "apikey": self.api_key
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_fundamental_earning(self, base_asset = '', quote_asset = ''):
        """
        Retrieves fundamental earning data from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.

        Returns:
            dict: The JSON response from the API containing the fundamental earning data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.EARNINGS,
                "symbol": symbol,
                "apikey": self.api_key
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_fundamental_listing_status(self, base_asset = '', quote_asset = '', date = '', state = ''):
        """
        Retrieves the listing status of a stock from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.
            date (str): The date for which to retrieve the listing status. Defaults to an empty string.
            state (str): The state for which to retrieve the listing status. Defaults to an empty string.

        Returns:
            str: The CSV response from the API containing the listing status data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.LISTS_OR_DELISTS,
                "symbol": symbol,
                "apikey": self.api_key
            }
            if date:
                params['date'] = date
            if state:
                params['state'] = state

            data = self._fetch_data_csv(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_fundamental_earning_calendar(self, base_asset = '', quote_asset = '', horizon = STOCK_FUNDEMENTAL_FUNCTION.QUARTER):
        """
        Retrieves the earning calendar data for a given stock symbol from the API.

        Args:
            base_asset (str): The base asset symbol. Defaults to an empty string.
            quote_asset (str): The quote asset symbol. Defaults to an empty string.
            horizon (str): The horizon for which to retrieve the earning calendar data. Defaults to STOCK_FUNDEMENTAL_FUNCTION.QUARTER.

        Returns:
            str: The CSV response from the API containing the earning calendar data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            symbol = self.symbol_exchange
            if base_asset == '':
                symbol = self.symbol_exchange
            else:
                symbol = f'{base_asset}.{quote_asset}'
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.EARNINGS_CALENDAR,
                "symbol": symbol,
                "apikey": self.api_key,
                "horizon": horizon
            }
            data = self._fetch_data_csv(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_fundamental_ipo_calendar(self):
        """
        Retrieves the IPO calendar data from the API.

        Args:
            None

        Returns:
            str: The CSV response from the API containing the IPO calendar data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": STOCK_FUNDEMENTAL_FUNCTION.IPO_CALENDAR,
                "apikey": self.api_key,
            }
            data = self._fetch_data_csv(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    

    
    



