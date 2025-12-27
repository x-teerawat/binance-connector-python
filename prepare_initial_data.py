import pandas as pd
from clients.spot.examples.rest_api.Market.klines import klines
from fetch_data import fetch_data
from datetime import datetime, timezone

from google.cloud import storage
from io import StringIO

def prepare_initial_data(symbol):
    years = [i for i in range(2010, 2025+1)]

    list_prepared_data = []

    for year in years:
        prepared_data = fetch_data(
            symbol=symbol,
            start_time=f"{year}-01-01",
            end_time=f"{year}-12-31",
        )

        list_prepared_data.append(prepared_data)

    prepared_initial_data = pd.concat(list_prepared_data, axis=0)
    prepared_initial_data = prepared_initial_data.reset_index(drop=True)
    prepared_initial_data = prepared_initial_data.iloc[:-1]
    
    return prepared_initial_data

def update_data_to_gcs(
        symbol,
        prepared_initial_data,
        bucket_name,
):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"1d/{symbol}.csv")

    # DataFrame -> CSV in memory
    buffer = StringIO()
    prepared_initial_data.to_csv(buffer, index=False)
    buffer.seek(0)

    # upload to GCS
    blob.upload_from_string(
        buffer.getvalue(),
        content_type="text/csv"
    )

if __name__ == '__main__':
    # symbol = 'BTCUSDT'
    # symbol = 'ETHUSDT'
    symbol = 'BNBUSDT'
    bucket_name = 'cryptocurrency-prices'

    prepared_initial_data = prepare_initial_data(symbol=symbol)
    prepared_initial_data

    update_data_to_gcs(
        symbol=symbol,
        prepared_initial_data=prepared_initial_data,
        bucket_name=bucket_name
    )