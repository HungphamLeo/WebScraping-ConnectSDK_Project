import requests
import csv
import os
from .utils import URL_STANDARD
from logger import setup_logger_global
from abc import ABC

class AlphaVantageBase(ABC):
    def __init__(self, api_key, base_asset, quote_asset):
        """
        Initializes a AlphaVantageData object.

        Parameters:
        api_key (str): The API key for authentication.
        base_asset (str): The base asset of the stock symbol.
        quote_asset (str): The quote asset of the stock symbol.

        Returns:
        None
        """
        self.api_key = api_key
        self.symbol_exchange = f'{base_asset}.{quote_asset}'
        self.exchange_name = quote_asset
        current_file = os.path.abspath(__file__)
        connection_logger_name = os.path.basename(current_file)
        self.logger_alphavantage = setup_logger_global(connection_logger_name, connection_logger_name + '.log')

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
                return response.json()
            else:
                self.logger_alphavantage.error(f"Request failed with status code: {response.status_code}")
                return None
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed: {e}")
            return None

    def _fetch_data_csv(self, params):
        """
        Fetches data from the API endpoint specified by URL_STANDARD with the given parameters and returns it in CSV format.

        Args:
            params (dict): A dictionary of parameters to be passed to the API request.

        Returns:
            list: A list of lists containing the CSV data, or None if the request fails.

        Raises:
            Exception: If the API request fails for any reason.
        """
        try:
            response = requests.get(URL_STANDARD, params=params)
            if response.status_code == 200:
                decoded_content = response.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                return list(cr)
            else:
                self.logger_alphavantage.error(f"Request failed with status code: {response.status_code}")
                return None
        except Exception as e:
            self.logger_alphavantage.error(f"API request failed: {e}")
            return None
