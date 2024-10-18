import time

def xtb_convert_time_period(period, type = 1):
    """
    1: convert from config interval to xtb interval
    2: convert from xtb interval to config interval
    """
    match type:
        case 1:
            period_mapping_1 = {
                '1m': 1,
                '5m': 5,
                '15m': 15,
                '30m': 30,
                '1h': 60,
                '4h': 240,
                '1d': 1440,
                '1w': 10080,
                '1M': 43200
            }
            period = period_mapping_1.get(period, "Invalid period")
        case 2:
            period_mapping_2 = {
                1: '1m',
                5: '5m',
                15: '15m',
                30: '30m',
                60: '1h',
                240: '4h',
                1440: '1d',
                10080: '1w',
                43200: '1M'
            }
            period = period_mapping_2.get(period, "Invalid period")
    return period

def get_start_time_from_interval(interval, limit=200):
        """
        Retrieves the start time and period for a given interval and limit.

        Args:
            interval (str): The time interval. Can be '1m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d', '1w', '1M'.
            limit (int): The maximum number of intervals. Defaults to 200.

        Returns:
            tuple: A tuple containing the start time (int) and period (int).
        """
        time_multipliers = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '8h': 28800,
            '12h': 43200,
            '1d': 86400,
            '1w': 604800,
            '1M': 2592000
        }
        period = time_multipliers.get(interval, 60) * 1000 - 1
        start_time = int(time.time()) - period * limit
        return start_time, period

def get_end_time_candle(interval, start_time_candle):
        """
        Retrieves the start time and period for a given interval and limit.

        Args:
            interval (str): The time interval. Can be '1m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d', '1w', '1M'.
            limit (int): The maximum number of intervals. Defaults to 200.

        Returns:
            tuple: A tuple containing the start time (int) and period (int).
        """
        time_multipliers = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '8h': 28800,
            '12h': 43200,
            '1d': 86400,
            '1w': 604800,
            '1M': 2592000
        }
        return int(start_time_candle + time_multipliers.get(interval, 60))
