import requests
import time
from typing import Dict
from src.utils.logger import logger


class AlphaVantageClient:
    """
    Production-grade client for fetching stock data from Alpha Vantage API.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def get_daily_stock_data(self, symbol: str) -> Dict:
        """
        Fetch daily stock data for a given symbol.
        """

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "compact"
        }

        max_retries = 3
        backoff_factor = 2

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Fetching daily stock data for {symbol}, attempt {attempt}")

                response = requests.get(self.BASE_URL, params=params, timeout=10)
                response.raise_for_status()

                data = response.json()

                # Handle API errors
                if "Error Message" in data:
                    raise ValueError(f"API returned error: {data['Error Message']}")

                # Handle rate limiting properly (FIXED)
                if "Note" in data:
                    logger.warning("API rate limit hit. Sleeping 60 seconds...")
                    time.sleep(60)
                    continue

                logger.info(f"Successfully fetched data for {symbol}")
                return data

            except (requests.RequestException, ValueError) as e:
                logger.warning(f"Attempt {attempt} failed: {e}")

                if attempt < max_retries:
                    sleep_time = backoff_factor ** (attempt - 1)
                    logger.info(f"Retrying after {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to fetch data for {symbol} after {max_retries} attempts")
                    raise