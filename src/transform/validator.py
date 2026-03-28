import pandas as pd
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
        # Work on a copy to avoid mutating the original DataFrame
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

        # Convert data types
        df['date'] = pd.to_datetime(df['date'])
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
        df['volume'] = df['volume'].astype(int)

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

        total_removed = dropped_nulls + dropped_dupes + dropped_logic
        logger.info(
            f"Validation complete. Rows before: {before_nulls}, after: {len(df)}, removed: {total_removed}"
        )

        return df