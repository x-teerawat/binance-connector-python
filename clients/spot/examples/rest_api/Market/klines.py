import os
import logging
import pandas as pd


from binance_sdk_spot.spot import Spot, ConfigurationRestAPI, SPOT_REST_API_PROD_URL
from binance_sdk_spot.rest_api.models import KlinesIntervalEnum


# Configure logging
logging.basicConfig(level=logging.INFO)

# Create configuration for the REST API
configuration_rest_api = ConfigurationRestAPI(
    api_key=os.getenv("API_KEY", ""),
    api_secret=os.getenv("API_SECRET", ""),
    base_path=os.getenv("BASE_PATH", SPOT_REST_API_PROD_URL),
)

# Initialize Spot client
client = Spot(config_rest_api=configuration_rest_api)


def klines(symbol, start_time, end_time):
    try:
        start_time_ms = int(
            pd.Timestamp(start_time, tz="UTC").timestamp() * 1000
        )
        end_time_ms = int(
            pd.Timestamp(end_time, tz="UTC").timestamp() * 1000
        )
        
        response = client.rest_api.klines(
            symbol=symbol,
            interval=KlinesIntervalEnum["INTERVAL_1d"].value,
            start_time=start_time_ms,
            end_time=end_time_ms,
            # limit=1000,
        )

        rate_limits = response.rate_limits
        logging.info(f"klines() rate limits: {rate_limits}")

        data = response.data()

        logging.info(f"klines() response: {data}")

        return data
    
    except Exception as e:
        logging.error(f"klines() error: {e}")


if __name__ == "__main__":
    klines()
