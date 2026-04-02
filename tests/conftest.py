import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text
from src.load.database import DatabaseLoader


@pytest.fixture
def sample_av_response():
    """Raw Alpha Vantage API response loaded from file."""
    data_path = Path(__file__).parent / "data" / "sample_av_response.json"
    with open(data_path) as f:
        return json.load(f)


@pytest.fixture
def rate_limit_response():
    return {
        "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute."
    }


@pytest.fixture
def error_response():
    return {
        "Error Message": "Invalid API call. Please retry or visit the documentation."
    }


@pytest.fixture
def valid_df():
    """
    Clean, valid OHLCV DataFrame matching what the transformer produces.
    Uses datetime.date objects for the date column — NOT pd.Timestamp —
    because that is what transformer.py outputs after .dt.date conversion.
    """
    return pd.DataFrame(
        {
            "date": [
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
            ],
            "symbol": ["AAPL"] * 4,
            "open": [187.15, 184.22, 182.15, 181.99],
            "high": [188.44, 185.88, 183.09, 182.76],
            "low": [183.88, 183.43, 180.93, 180.17],
            "close": [185.85, 184.25, 181.91, 181.18],
            "volume": [128256700, 58414460, 71983700, 62983920],
        }
    )


@pytest.fixture
def df_with_bad_row(valid_df):
    """DataFrame with one row where high < low (invalid)."""
    df = valid_df.copy()
    df.loc[0, "high"] = 100.0
    df.loc[0, "low"] = 200.0
    return df


@pytest.fixture
def df_with_nulls(valid_df):
    df = valid_df.copy()
    df.loc[1, "close"] = None
    return df


@pytest.fixture
def df_missing_column(valid_df):
    return valid_df.drop(columns=["volume"])


@pytest.fixture(scope="function")
def pg_loader():
    """
    Creates a DatabaseLoader pointed at a real PostgreSQL test database.
    Requires a running PostgreSQL instance.
    Set TEST_DATABASE_URL env var or use the default localhost connection.
    Cleans up all test data after each test.
    """
    import os

    db_url = os.getenv(
        "TEST_DATABASE_URL", "postgresql://postgres:@localhost:5432/stock_db_test"
    )
    loader = DatabaseLoader(db_url)
    yield loader
    with loader.engine.begin() as conn:
        conn.execute(text("DELETE FROM data_quality_log"))
        conn.execute(text("DELETE FROM stock_prices"))
        conn.execute(text("DELETE FROM stocks"))
