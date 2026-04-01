import pytest
import pandas as pd
from datetime import date

from src.transform.validator import StockDataValidator


@pytest.fixture
def validator():
    return StockDataValidator()


def test_validate_passes_clean_data(validator, valid_df):
    result = validator.validate(valid_df)
    assert len(result) == 4


def test_validate_drops_high_less_than_low(validator, df_with_bad_row):
    result = validator.validate(df_with_bad_row)
    assert len(result) == 3  # 1 bad row dropped


def test_validate_drops_null_rows(validator, df_with_nulls):
    result = validator.validate(df_with_nulls)
    assert len(result) == 3  # row with null dropped


def test_validate_raises_on_missing_column(validator, df_missing_column):
    with pytest.raises(ValueError, match="Missing required columns"):
        validator.validate(df_missing_column)


def test_validate_drops_zero_price(validator, valid_df):
    valid_df.loc[0, "open"] = 0.0
    result = validator.validate(valid_df)
    assert len(result) == 3


def test_validate_drops_negative_volume(validator, valid_df):
    valid_df.loc[0, "volume"] = -1
    result = validator.validate(valid_df)
    assert len(result) == 3


def test_validate_drops_duplicates(validator, valid_df):
    df_with_dupe = pd.concat([valid_df, valid_df.iloc[[0]]], ignore_index=True)
    result = validator.validate(df_with_dupe)
    assert len(result) == 4


def test_validate_output_types(validator, valid_df):
    result = validator.validate(valid_df)
    # Date column is now object dtype containing datetime.date objects
    assert result["date"].dtype == object
    assert isinstance(result["date"].iloc[0], date)
    assert pd.api.types.is_float_dtype(result["close"])
    assert pd.api.types.is_integer_dtype(result["volume"])