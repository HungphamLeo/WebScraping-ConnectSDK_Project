
def calculate_last_price(ticker, price_type):
    """
    Calculate the last price of an asset based on the best bid and best ask prices.

    Parameters:
    ticker (dict): The ticker data
    price_type (str): The type of price to calculate. Can be 'AMP' or 'WAMP'

    Returns:
    float: The calculated last price
    """
    bid_price = float(ticker['bestBid'])
    ask_price = float(ticker['bestAsk'])
    bid_volume = float(ticker['bidSz'])
    ask_volume = float(ticker['askSz'])

    match price_type:
        case 'AMP':
            last_price = (bid_price + ask_price) / 2
        case 'WAMP':
            total_volume = bid_volume + ask_volume
            last_price = (bid_price * ask_volume + ask_price * bid_volume) / total_volume
        case _:
            raise ValueError("Invalid price type")

    return last_price

def aggregate_candles(candle_list):
    open_price = candle_list[0][1]
    close_price = candle_list[-1][4]
    high_price = max(candle[2] for candle in candle_list)
    low_price = min(candle[3] for candle in candle_list)
    quoteId = candle_list[0]['quoteId']
    total_volume = sum(candle[6] for candle in candle_list)
    start_time = candle_list[0][0]
    end_time = candle_list[-1][-1]
    aggregated_candle = [
        start_time,
        open_price,
        high_price,
        low_price,
        close_price,
        quoteId,
        total_volume,
        end_time
    ]
    
    return aggregated_candle