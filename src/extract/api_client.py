# src/extract/api_client.py

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
        """
        Initialize the Alpha Vantage client.

        Args:
            api_key (str): Alpha Vantage API key
        """
        self.api_key = api_key

    def get_daily_stock_data(self, symbol: str) -> Dict:
        """
        Fetch daily stock data for a given symbol.

        Args:
            symbol (str): Stock ticker symbol (e.g., "AAPL")

        Returns:
            dict: JSON response from Alpha Vantage API

        Raises:
            Exception: If request fails after retries
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

                if "Error Message" in data:
                    raise ValueError(f"API returned error: {data['Error Message']}")
                if "Note" in data:
                    raise RuntimeError(f"API notice/limit hit: {data['Note']}")

                logger.info(f"Successfully fetched data for {symbol}")
                return data

            except (requests.RequestException, ValueError, RuntimeError) as e:
                logger.warning(f"Attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    sleep_time = backoff_factor ** (attempt - 1)
                    logger.info(f"Retrying after {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to fetch data for {symbol} after {max_retries} attempts")
                    raise