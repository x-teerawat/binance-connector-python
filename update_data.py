import pandas as pd
from fetch_data import fetch_data
from datetime import datetime, timezone, timedelta
from google.cloud import storage
from io import StringIO


def update_data(symbol):
    ### ------------------------------
    ### Load previous data
    ### ------------------------------

    previous_data = pd.read_csv(
        filepath_or_buffer=f"gs://cryptocurrency-prices/1d/{symbol}.csv",
    )
    previous_data['date'] = pd.to_datetime(previous_data['date'])

    print(f"previous_data: \n{previous_data}")
    print()
    print("-"*30)
    print()

    ### ------------------------------
    ### Fetch new data
    ### ------------------------------

    today_date = datetime.now(timezone.utc)
    yesterday_date = today_date - timedelta(days=1)
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    # print(f"today_date: \n{today_date}")
    # print(f"yesterday_date: \n{yesterday_date}")
    print(f"yesterday_date_str: \n{yesterday_date_str}")
    print()
    print("-"*30)
    print()

    new_data = fetch_data(
        symbol=symbol,
        start_time=yesterday_date_str,
        end_time=yesterday_date_str,
    )

    print(f"new_data: \n{new_data}")
    print()
    print("-"*30)
    print()

    ### ------------------------------
    ### Concat
    ### ------------------------------

    prepared_data = pd.concat([previous_data, new_data], axis=0)
    prepared_data = prepared_data.reset_index(drop=True)
    print(f"prepared_data: \n{prepared_data}")
    print()
    print("-"*30)
    print()

    ### ------------------------------
    ### Update prepared data to GCS
    ### ------------------------------

    client = storage.Client()
    bucket = client.bucket("cryptocurrency-prices")
    blob = bucket.blob(f"1d/{symbol}.csv")

    # DataFrame -> CSV in memory
    buffer = StringIO()
    prepared_data.to_csv(buffer, index=False)
    buffer.seek(0)

    # upload to GCS
    blob.upload_from_string(
        buffer.getvalue(),
        content_type="text/csv"
    )

if __name__ == "__main__":
    symbols = [
        'BTCUSDT',
        'ETHUSDT',
        'BNBUSDT',

    ]
    for symbol in symbols:
        update_data(symbol)