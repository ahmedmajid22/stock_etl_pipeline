import json
from pathlib import Path

import pandas as pd
import pytest


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
    """Clean, valid OHLCV DataFrame matching what the transformer produces."""
    return pd.DataFrame({
        "date":   pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]),
        "symbol": ["AAPL"] * 4,
        "open":   [187.15, 184.22, 182.15, 181.99],
        "high":   [188.44, 185.88, 183.09, 182.76],
        "low":    [183.88, 183.43, 180.93, 180.17],
        "close":  [185.85, 184.25, 181.91, 181.18],
        "volume": [128256700, 58414460, 71983700, 62983920],
    })


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