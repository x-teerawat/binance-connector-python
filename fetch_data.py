import pandas as pd
from clients.spot.examples.rest_api.Market.klines import klines

def fetch_data(
        symbol,
        start_time,
        end_time
):
    ### ------------------------------
    ### Fetch data
    ### ------------------------------

    data = klines(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
    )

    ### ------------------------------
    ### Convert list to DataFrame
    ### ------------------------------

    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "n_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]

    prepared_data = pd.DataFrame(data, columns=columns)

    ### Rename column
    prepared_data = prepared_data.rename(columns={'open_time': 'date'})

    ### Drop unreferred data
    prepared_data = prepared_data.drop(columns=['close_time', 'ignore'])

    ### Convert types
    prepared_data["date"] = pd.to_datetime(prepared_data["date"], unit="ms")

    ### Correct type of data
    price_cols = ["open", "high", "low", "close", "volume",
                "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]
    prepared_data[price_cols] = prepared_data[price_cols].astype(float)
    prepared_data["n_trades"] = prepared_data["n_trades"].astype(int)
            
    # print(f"prepared_data: \n{prepared_data}")

    return prepared_data

if __name__ == '__main__':
    prepared_data = fetch_data(
        symbol='BTCUSDT',
        start_time="2025-01-01",
        end_time="2025-12-31",
    )