from .utils import URL_STANDARD, ECONOMICS_FUNCTION ,ECONOMICS_INTERVAL, TREASURY_YIELD_TIME
from .base_abstract import AlphaVantageBase

class EconomicsData(AlphaVantageBase):
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
    
    def get_real_gdp_per_capital(self, interval = ECONOMICS_INTERVAL.ANUAL_PAYROLL, datatype='json'):
        """
        Fetches the real GDP per capital data from the API endpoint.

        Args:
            interval (ECONOMICS_INTERVAL): The interval of the data. Defaults to ECONOMICS_INTERVAL.ANUAL_PAYROLL.
            datatype (str): The format of the data. It can be 'json' or 'csv'.Defaults to 'json'. 

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.REAL_GDP,
                "apikey": self.api_key,
                'interval':interval,
                'datatype':datatype
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_treasury_yield(self, interval = TREASURY_YIELD_TIME.DAILY, 
                           maturity =TREASURY_YIELD_TIME.MATUARITY_2Y, 
                           datatype='json'):
        """
        Fetches the treasury yield data from the API endpoint.

        Args:
            interval (TREASURY_YIELD_TIME): The interval of the data. Defaults to TREASURY_YIELD_TIME.DAILY.
            maturity (TREASURY_YIELD_TIME): The maturity of the treasury yield. Defaults to TREASURY_YIELD_TIME.MATUARITY_2Y.
            datatype (str): The format of the data. It can be 'json' or 'csv'. Defaults to 'json'.

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.TREASURY_YIELD,
                "apikey": self.api_key,
                'datatype':datatype,
                'interval':interval,
                'maturity':maturity
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_federal_fund_rate(self, interval = TREASURY_YIELD_TIME.DAILY, datatype='json'):
        """
        Fetches the federal fund rate data from the API endpoint.

        Args:
            interval (TREASURY_YIELD_TIME): The interval of the data. Defaults to TREASURY_YIELD_TIME.DAILY.
            datatype (str): The format of the data. It can be 'json' or 'csv'. Defaults to 'json'.

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.FEDERAL_FUNDS_RATE,
                "apikey": self.api_key,
                'interval':interval,
                'datatype':datatype
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")

    def get_cpi(self, interval = TREASURY_YIELD_TIME.SEMIANNUAL, datatype='json'):
        """
        Fetches the Consumer Price Index (CPI) data from the API endpoint.

        Args:
            interval (TREASURY_YIELD_TIME): The interval of the data. Defaults to TREASURY_YIELD_TIME.SEMIANNUAL.
            datatype (str): The format of the data. It can be 'json' or 'csv'. Defaults to 'json'.

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.CPI,
                "apikey": self.api_key,
                'interval':interval,
                'datatype':datatype
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_inflation(self,  datatype='json'):
        """
        Fetches the inflation data from the API endpoint.

        Args:
            datatype (str): The format of the data. It can be 'json' or 'csv'. Defaults to 'json'.

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.INFLATION,
                "apikey": self.api_key,
                'datatype':datatype
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_retails_sale(self,  datatype='json'):
        """
        Fetches the retail sales data from the API endpoint.

        Args:
            datatype (str): The format of the data. It can be 'json' or 'csv'. Defaults to 'json'.

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.RETAIL_SALES,
                "apikey": self.api_key,
                'datatype':datatype
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_durable_goods(self,  datatype='json'):
        """
        Fetches the durable goods data from the API endpoint.

        Args:
            datatype (str): The format of the data. It can be 'json' or 'csv'. Defaults to 'json'.

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.DURABLES,
                "apikey": self.api_key,
                'datatype':datatype
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_unemployment_rate(self,  datatype='json'):
        """
        Fetches the unemployment rate data from the API endpoint.

        Args:
            datatype (str): The format of the data. It can be 'json' or 'csv'. Defaults to 'json'.

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.UNEMPLOYMENT,
                "apikey": self.api_key,
                'datatype':datatype
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    
    def get_nonfarm_payroll(self,  datatype='json'):
        """
        Fetches the non-farm payroll data from the API endpoint.

        Args:
            datatype (str): The format of the data. It can be 'json' or 'csv'. Defaults to 'json'.

        Returns:
            dict or None: The JSON response from the API, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            params = {
                "function": ECONOMICS_FUNCTION.NONFARM_PAYROLL,
                "apikey": self.api_key,
                'datatype':datatype
            }
            data = self._fetch_data(params)
            return data
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed {e}")
    

    
