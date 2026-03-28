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

        Returns:
            Dict: Raw JSON response from API.

        Raises:
            RuntimeError: If all retries fail due to rate limiting or API errors.
            ValueError: If API returns an error message or unexpected structure.
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

                # Handle rate limiting (both cases)
                if "Note" in data or "Information" in data:
                    logger.warning(f"API rate limit or info message hit for {symbol}. Sleeping 60 seconds...")
                    time.sleep(60)
                    continue  # retry after sleep

                # Validate expected structure
                if "Time Series (Daily)" not in data:
                    raise ValueError(f"Unexpected API response structure for {symbol}: {data}")

                logger.info(f"Successfully fetched data for {symbol}")
                return data

            except (requests.RequestException, ValueError) as e:
                logger.warning(f"Attempt {attempt} failed for {symbol}: {e}")

                if attempt < max_retries:
                    sleep_time = backoff_factor ** (attempt - 1)
                    logger.info(f"Retrying after {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to fetch data for {symbol} after {max_retries} attempts")
                    raise  # re-raise the last exception

        # If we exit the loop without returning (e.g., all attempts hit rate limit and never broke),
        # raise a clear error.
        raise RuntimeError(f"Max retries exceeded for {symbol} due to rate limiting or persistent API errors")