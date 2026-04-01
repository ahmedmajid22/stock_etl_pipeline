import pytest
import pandas as pd
from datetime import date

from src.transform.transformer import StockDataTransformer


@pytest.fixture
def transformer():
    return StockDataTransformer()


def test_transform_happy_path(transformer, sample_av_response):
    df = transformer.transform(sample_av_response, "AAPL")
    assert len(df) == 4
    assert set(df.columns) == {"date", "open", "high", "low", "close", "volume", "symbol"}
    assert (df["symbol"] == "AAPL").all()


def test_transform_output_is_sorted_by_date(transformer, sample_av_response):
    df = transformer.transform(sample_av_response, "AAPL")
    assert df["date"].is_monotonic_increasing


def test_transform_correct_types(transformer, sample_av_response):
    df = transformer.transform(sample_av_response, "AAPL")
    # Date column is now object dtype containing datetime.date objects
    assert df["date"].dtype == object
    assert isinstance(df["date"].iloc[0], date)
    assert pd.api.types.is_float_dtype(df["open"])
    assert pd.api.types.is_integer_dtype(df["volume"])


def test_transform_raises_on_empty_time_series(transformer):
    with pytest.raises(ValueError, match="Empty time series"):
        transformer.transform({"Time Series (Daily)": {}}, "AAPL")


def test_transform_raises_on_missing_key(transformer):
    with pytest.raises(ValueError, match="'Time Series"):
        transformer.transform({"wrong_key": {}}, "AAPL")


def test_transform_raises_on_invalid_input(transformer):
    with pytest.raises(ValueError, match="Invalid raw_data"):
        transformer.transform(None, "AAPL")


def test_transform_drops_rows_with_non_numeric_prices(transformer):
    bad = {
        "Time Series (Daily)": {
            "2024-01-02": {
                "1. open": "not_a_number",
                "2. high": "188.44",
                "3. low": "183.88",
                "4. close": "185.85",
                "5. volume": "128256700",
            }
        }
    }
    df = transformer.transform(bad, "AAPL")
    assert len(df) == 0