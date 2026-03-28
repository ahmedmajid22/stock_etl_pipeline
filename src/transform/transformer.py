import pandas as pd
from src.utils.logger import logger
from typing import Dict


class StockDataTransformer:
    """
    Production-grade transformer for stock data from Alpha Vantage.
    Converts raw API JSON into a clean, analytics-ready pandas DataFrame.
    """

    def transform(self, raw_data: Dict, symbol: str) -> pd.DataFrame:
        logger.info(f"Starting transformation for symbol: {symbol}")

        # Validate input
        if not raw_data or not isinstance(raw_data, dict):
            raise ValueError("Invalid raw_data: must be a non-empty dictionary")

        try:
            time_series = raw_data["Time Series (Daily)"]
        except KeyError:
            raise ValueError("Invalid data format: 'Time Series (Daily)' key not found")

        if not time_series:
            raise ValueError("Empty time series data received from API")

        # Convert nested dict → DataFrame
        df = pd.DataFrame.from_dict(time_series, orient="index")

        # Rename columns
        df = df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume"
        })

        # Add symbol
        df["symbol"] = symbol

        # Reset index → date
        df = df.reset_index().rename(columns={"index": "date"})

        # Convert types safely
        before = len(df)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

        # Drop bad rows
        df = df.dropna()
        dropped = before - len(df)
        if dropped > 0:
            logger.warning(f"Dropped {dropped} rows due to conversion errors or NaNs")

        # Convert final types
        df["volume"] = df["volume"].astype(int)

        # Sort
        df = df.sort_values(by="date").reset_index(drop=True)

        logger.info(f"Transformation complete for {symbol}, records: {len(df)}")

        return df