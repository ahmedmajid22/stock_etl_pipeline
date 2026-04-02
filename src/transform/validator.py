import pandas as pd
from datetime import date as date_type
from src.utils.logger import logger
from typing import List


class StockDataValidator:
    """
    Production-grade validator for stock market data.

    Ensures that DataFrames from Alpha Vantage:
    - Have required columns
    - Have correct data types
    - Do not contain nulls or duplicates
    - Meet logical consistency rules for stock prices
    - Validate price and volume constraints
    """

    REQUIRED_COLUMNS: List[str] = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean stock market DataFrame.

        Args:
            df (pd.DataFrame): Input raw DataFrame

        Returns:
            pd.DataFrame: Cleaned and validated DataFrame

        Raises:
            ValueError: If required columns are missing
        """
        df = df.copy()

        logger.info(f"Starting validation: {len(df)} rows")

        # Check required columns
        missing_cols = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Drop nulls
        before_nulls = len(df)
        df = df.dropna()
        dropped_nulls = before_nulls - len(df)
        if dropped_nulls > 0:
            logger.info(f"Dropped {dropped_nulls} rows due to null values")

        # Remove duplicates
        before_dupes = len(df)
        df = df.drop_duplicates()
        dropped_dupes = before_dupes - len(df)
        if dropped_dupes > 0:
            logger.info(f"Dropped {dropped_dupes} duplicate rows")

        # Normalize date column — handle both datetime.date objects and strings/Timestamps
        # The transformer already produces datetime.date objects; this is a safety net
        if len(df) > 0 and not isinstance(df['date'].iloc[0], date_type):
            df['date'] = pd.to_datetime(df['date'], utc=True).dt.date

        # Convert numeric columns safely
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove any rows that became null after conversion
        before_numeric = len(df)
        df = df.dropna(subset=numeric_cols)
        dropped_numeric = before_numeric - len(df)
        if dropped_numeric > 0:
            logger.info(f"Dropped {dropped_numeric} rows due to numeric conversion errors")

        # Price validation — ensure prices are positive
        before_positive = len(df)
        df = df[(df["open"] > 0) & (df["close"] > 0) & (df["high"] > 0) & (df["low"] > 0)]
        dropped_positive = before_positive - len(df)
        if dropped_positive > 0:
            logger.info(f"Dropped {dropped_positive} rows with non-positive prices")

        # Volume validation — volume should be non-negative
        before_volume = len(df)
        df = df[df["volume"] >= 0]
        dropped_volume = before_volume - len(df)
        if dropped_volume > 0:
            logger.info(f"Dropped {dropped_volume} rows with negative volume")

        # Logical consistency (high >= low, high >= open/close, low <= open/close)
        logical_mask = (
            (df['high'] >= df['low']) &
            (df['high'] >= df['open']) &
            (df['high'] >= df['close']) &
            (df['low'] <= df['open']) &
            (df['low'] <= df['close'])
        )
        before_logic = len(df)
        df = df[logical_mask]
        dropped_logic = before_logic - len(df)
        if dropped_logic > 0:
            logger.info(f"Dropped {dropped_logic} rows violating price consistency rules")

        # Final type enforcement
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
        df['volume'] = df['volume'].astype(int)

        total_removed = (
            dropped_nulls + dropped_dupes + dropped_numeric
            + dropped_positive + dropped_volume + dropped_logic
        )
        logger.info(
            f"Validation complete. Rows before: {before_nulls}, after: {len(df)}, removed: {total_removed}"
        )

        return df