import pandas as pd 

URL_STANDARD = 'https://www.alphavantage.co/query'

class STOCK_API_FUNCTION:
    TIME_SERIES_INTRADAY = 'TIME_SERIES_INTRADAY'
    TIME_SERIES_DAILY = 'TIME_SERIES_DAILY'
    TIME_SERIES_DAILY_ADJUSTED = 'TIME_SERIES_DAILY_ADJUSTED'
    TIME_SERIES_WEEKLY = 'TIME_SERIES_WEEKLY'
    TIME_SERIES_WEEKLY_ADJUSTED = 'TIME_SERIES_WEEKLY_ADJUSTED'
    TIME_SERIES_MONTHLY = 'TIME_SERIES_MONTHLY'
    TIME_SERIES_MONTHLY_ADJUSTED = 'TIME_SERIES_MONTHLY_ADJUSTED'
    GLOBAL_QUOTE = 'GLOBAL_QUOTE'
    SYMBOL_SEARCH = 'SYMBOL_SEARCH'
    HISTORICAL_OPTIONS = 'HISTORICAL_OPTIONS'

class STOCK_FUNDEMENTAL_FUNCTION:
    OVERVIEW = 'OVERVIEW'
    ETF_PROFILE = 'ETF_PROFILE'
    SPLITS = 'SPLITS'
    INCOME_STATEMENT = 'INCOME_STATEMENT'
    BALANCE_SHEET = 'BALANCE_SHEET'
    CASH_FLOW = 'CASH_FLOW'
    EARNINGS = 'EARNINGS'
    LISTS_OR_DELISTS = 'LISTING_STATUS'
    EARNINGS_CALENDAR = 'EARNINGS_CALENDAR'
    IPO_CALENDAR ='IPO_CALENDAR'
    QUARTER = '3month'
    HALF_YEAR = '6month'
    YEAR = '12month'

class ECONOMICS_INTERVAL:
    ANUAL_PAYROLL = 'annual'
    QUARTERLY_PAYROLL = 'quarterly'

class TREASURY_YIELD_TIME:
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'
    SEMIANNUAL =  'semiannual'
    MATUARITY_1MONTH = '1month'
    MATUARITY_3MONTH = '3month'
    MATUARITY_2Y = '2year'
    MATUARITY_5Y = '5year'
    MATUARITY_7Y = '7year'
    MATUARITY_10Y = '10year'
    MATUARITY_30Y = '30year'


class ECONOMICS_FUNCTION:
    REAL_GDP = 'REAL_GDP'
    REAL_GDP_PER_CAPITA = 'REAL_GDP_PER_CAPITA'
    TREASURY_YIELD = 'TREASURY_YIELD'
    FEDERAL_FUNDS_RATE = 'FEDERAL_FUNDS_RATE'
    CPI = 'CPI'
    INFLATION = 'INFLATION'
    RETAIL_SALES = 'RETAIL_SALES'
    DURABLES = 'DURABLES'
    UNEMPLOYMENT = 'UNEMPLOYMENT'
    NONFARM_PAYROLL = 'NONFARM_PAYROLL'
    
class quote_exchange:
    US = ''
    UK = '.LON'
    CANADA = '.TRT'
    GERMANY = '.XETRA'
    INDIA = '.BSE'
    SHENZEN_CHINA = '.SHZ'
    SHANGHAI_CHINA = '.SHH'


def stock_candles_record(data, type_data = 'dict'):
    dict = {}
    info_data_key = list(data.keys())[0]
    second_data_key = list(data.keys())[1]
    info_data = data[info_data_key]
    time_series = data[second_data_key]
    symbol = info_data['2. Symbol']
    if '4. Interval' in info_data:
        interval = info_data['4. Interval']
    else:
        interval = info_data['Time Zone']
    for keys, values in time_series.items():
        temp_dict = {}
        temp_dict['symbol'] = symbol
        temp_dict['interval'] = interval
        temp_dict['date_time'] = keys
        temp_dict['date'] = keys.split(' ')[0]
        temp_dict['hour'] = keys.split(' ')[1]
        temp_dict['open_price'] = float(values['1. open'])
        temp_dict['high_price'] = float(values['2. high'])
        temp_dict['low_price'] = float(values['3. low'])
        temp_dict['close_price'] = float(values['4. close'])
        temp_dict['volume'] = float(values['5. volume'])
        dict[keys] = temp_dict
    if type_data == 'table':
        table = pd.DataFrame.from_dict(dict, orient='index')
        return table
    else:
        return dict
    

