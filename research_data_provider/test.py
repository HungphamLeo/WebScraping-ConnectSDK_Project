# import pandas as pd
# import requests
# URL_STANDARD = 'https://www.alphavantage.co/query'
# url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'
# r = requests.get(url)
# re = r.json()

# def stock_candles_record(data, type_data = 'dict'):
#     dict = {}
#     info_data_key = list(data.keys())[0]
#     second_data_key = list(data.keys())[1]
#     info_data = data[info_data_key]
#     time_series = data[second_data_key]
#     symbol = info_data['2. Symbol']
#     interval = info_data['4. Interval']

#     for keys, values in time_series.items():
#         temp_dict = {}
#         temp_dict['symbol'] = symbol
#         temp_dict['interval'] = interval
#         temp_dict['date_time'] = keys
#         temp_dict['date'] = keys.split(' ')[0]
#         temp_dict['hour'] = keys.split(' ')[1]
#         temp_dict['open_price'] = values['1. open']
#         temp_dict['high_price'] = values['2. high']
#         temp_dict['low_price'] = values['3. low']
#         temp_dict['close_price'] = values['4. close']
#         temp_dict['volume'] = values['5. volume']
#         dict[keys] = temp_dict
#     if type_data == 'table':
#         table = pd.DataFrame.from_dict(dict, orient='index')
#         return table
#     else:
#         return dict

# data = stock_candles_record(data=re, type_data = 'table')
# print(data)
        
import re

class StockExchangeInventory:
    def __init__(self):
        self.exchanges = {
            "US": [],
            "UK": [],
            "Canada": [],
            "Germany": [],
            "India": [],
            "China": [],
        }
        
    def add_exchange(self, url):
        # Extract the symbol from the URL
        match = re.search(r'symbol=([A-Z0-9.]+)', url)
        if match:
            symbol = match.group(1)
            # Determine the exchange based on the symbol suffix
            if ".LON" in symbol:
                self.exchanges["UK"].append(symbol)
            elif ".TRT" in symbol or ".TRV" in symbol:
                self.exchanges["Canada"].append(symbol)
            elif ".DEX" in symbol:
                self.exchanges["Germany"].append(symbol)
            elif ".BSE" in symbol:
                self.exchanges["India"].append(symbol)
            elif ".SHH" in symbol or ".SHZ" in symbol:
                self.exchanges["China"].append(symbol)
            else:
                self.exchanges["US"].append(symbol)
    
    def display_exchanges(self):
        for country, symbols in self.exchanges.items():
            print(f"Sàn giao dịch ở {country}:")
            for symbol in symbols:
                print(f"  - {symbol}")

# Khởi tạo class
inventory = StockExchangeInventory()

# Thêm các URL mẫu vào class
urls = [
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo",
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&outputsize=full&apikey=demo",
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSCO.LON&outputsize=full&apikey=demo",
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SHOP.TRT&outputsize=full&apikey=demo",
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=GPV.TRV&outputsize=full&apikey=demo",
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MBG.DEX&outputsize=full&apikey=demo",
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=RELIANCE.BSE&outputsize=full&apikey=demo",
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=600104.SHH&outputsize=full&apikey=demo",
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=000002.SHZ&outputsize=full&apikey=demo"
]

# Thêm tất cả các URL vào inventory
for url in urls:
    inventory.add_exchange(url)

# Hiển thị các sàn giao dịch và mã chứng khoán
inventory.display_exchanges()
