# src/transform/transformer.py

import pandas as pd
from src.utils.logger import logger
from typing import Dict

class StockDataTransformer:
    """
    Production-grade transformer for stock data from Alpha Vantage.
    Converts raw API JSON into a clean, analytics-ready pandas DataFrame.
    """

    def transform(self, raw_data: Dict, symbol: str) -> pd.DataFrame:
        """
        Transform raw Alpha Vantage daily stock data into a clean DataFrame.

        Args:
            raw_data (dict): Raw JSON from Alpha Vantage API
            symbol (str): Stock ticker symbol

        Returns:
            pd.DataFrame: Cleaned DataFrame with columns: date, symbol, open, high, low, close, volume

        Raises:
            ValueError: If the input data format is invalid
        """
        logger.info(f"Starting transformation for symbol: {symbol}")

        try:
            time_series = raw_data["Time Series (Daily)"]
        except KeyError:
            raise ValueError("Invalid data format: 'Time Series (Daily)' key not found")

        # Convert nested dict to DataFrame
        df = pd.DataFrame.from_dict(time_series, orient="index")
        df = df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume"
        })

        # Add symbol column
        df["symbol"] = symbol

        # Reset index and rename to 'date'
        df = df.reset_index().rename(columns={"index": "date"})
        df["date"] = pd.to_datetime(df["date"])

        # Convert data types
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        df["volume"] = df["volume"].astype(int)

        # Sort by date ascending
        df = df.sort_values(by="date").reset_index(drop=True)

        # Drop missing values
        df = df.dropna()

        logger.info(f"Transformation complete for {symbol}, records processed: {len(df)}")

        return df