"""
Real database integration tests.

These tests require a running PostgreSQL instance.
Set TEST_DATABASE_URL env var, or use the default localhost connection.

Run with:
    pytest tests/integration/test_database.py -v -m integration
"""
import pytest
import pandas as pd
from datetime import date
from sqlalchemy import text

from src.load.database import DatabaseLoader


pytestmark = pytest.mark.integration  # skip by default unless -m integration


@pytest.fixture
def sample_df():
    """4 rows of clean OHLCV data matching what the transformer produces."""
    return pd.DataFrame({
        "date":   [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)],
        "symbol": ["AAPL"] * 4,
        "open":   [187.15, 184.22, 182.15, 181.99],
        "high":   [188.44, 185.88, 183.09, 182.76],
        "low":    [183.88, 183.43, 180.93, 180.17],
        "close":  [185.85, 184.25, 181.91, 181.18],
        "volume": [128256700, 58414460, 71983700, 62983920],
    })


class TestUpsertDataframe:

    def test_upsert_writes_correct_row_count(self, pg_loader, sample_df):
        """Basic write: 4 rows in → 4 rows in stock_prices."""
        pg_loader.upsert_dataframe(sample_df, "AAPL")

        with pg_loader.engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM stock_prices sp JOIN stocks s ON sp.stock_id = s.id WHERE s.symbol = 'AAPL'")
            ).scalar()

        assert count == 4

    def test_upsert_is_idempotent(self, pg_loader, sample_df):
        """Running upsert twice must not create duplicate rows."""
        pg_loader.upsert_dataframe(sample_df, "AAPL")
        pg_loader.upsert_dataframe(sample_df, "AAPL")  # second run

        with pg_loader.engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM stock_prices sp JOIN stocks s ON sp.stock_id = s.id WHERE s.symbol = 'AAPL'")
            ).scalar()

        assert count == 4, "Upsert should not duplicate rows on re-run"

    def test_upsert_updates_price_on_conflict(self, pg_loader, sample_df):
        """If the same (stock_id, date) is upserted with a new close price, the row updates."""
        pg_loader.upsert_dataframe(sample_df, "AAPL")

        # Modify close price for the first row
        updated_df = sample_df.copy()
        updated_df.loc[0, "close"] = 999.99
        pg_loader.upsert_dataframe(updated_df, "AAPL")

        with pg_loader.engine.connect() as conn:
            close_val = conn.execute(
                text("SELECT close FROM stock_prices sp JOIN stocks s ON sp.stock_id = s.id WHERE s.symbol='AAPL' AND sp.date='2024-01-02'")
            ).scalar()

        assert float(close_val) == pytest.approx(999.99, abs=0.001)

    def test_empty_dataframe_does_not_raise(self, pg_loader):
        """Empty DataFrame should be a no-op, not an error."""
        empty_df = pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])
        pg_loader.upsert_dataframe(empty_df, "AAPL")  # should not raise

    def test_stock_dimension_row_created(self, pg_loader, sample_df):
        """Upserting prices for a new symbol should create the stocks row."""
        pg_loader.upsert_dataframe(sample_df, "NVDA")

        with pg_loader.engine.connect() as conn:
            symbol = conn.execute(
                text("SELECT symbol FROM stocks WHERE symbol = 'NVDA'")
            ).scalar()

        assert symbol == "NVDA"

    def test_multiple_symbols_isolated(self, pg_loader, sample_df):
        """Rows for AAPL and MSFT must not interfere with each other."""
        msft_df = sample_df.copy()
        msft_df["symbol"] = "MSFT"

        pg_loader.upsert_dataframe(sample_df, "AAPL")
        pg_loader.upsert_dataframe(msft_df, "MSFT")

        with pg_loader.engine.connect() as conn:
            aapl_count = conn.execute(text("SELECT COUNT(*) FROM stock_prices sp JOIN stocks s ON sp.stock_id=s.id WHERE s.symbol='AAPL'")).scalar()
            msft_count = conn.execute(text("SELECT COUNT(*) FROM stock_prices sp JOIN stocks s ON sp.stock_id=s.id WHERE s.symbol='MSFT'")).scalar()

        assert aapl_count == 4
        assert msft_count == 4


class TestDataQualityLog:

    def test_log_quality_metrics_inserts_row(self, pg_loader):
        """log_quality_metrics must insert exactly one row per call."""
        pg_loader.log_quality_metrics(
            symbol="AAPL",
            run_id="test-run-001",
            rows_raw=100,
            rows_transformed=98,
            rows_validated=97,
            rows_loaded=97,
            api_latency_ms=450,
            db_latency_ms=120,
            pipeline_duration_s=3.7,
            status="success",
        )

        with pg_loader.engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM data_quality_log WHERE run_id = 'test-run-001'")
            ).scalar()

        assert count == 1

    def test_log_quality_metrics_captures_failure(self, pg_loader):
        """Failed pipeline runs should be recorded with status=failed and error_message."""
        pg_loader.log_quality_metrics(
            symbol="TSLA",
            run_id="test-run-fail-001",
            rows_raw=0,
            rows_transformed=0,
            rows_validated=0,
            rows_loaded=0,
            api_latency_ms=0,
            db_latency_ms=0,
            pipeline_duration_s=0.5,
            status="failed",
            error_message="Connection timeout",
        )

        with pg_loader.engine.connect() as conn:
            row = conn.execute(
                text("SELECT status, error_message FROM data_quality_log WHERE run_id = 'test-run-fail-001'")
            ).fetchone()

        assert row.status == "failed"
        assert "timeout" in row.error_message.lower()


class TestGetLatestDate:

    def test_returns_none_for_unknown_symbol(self, pg_loader):
        result = pg_loader.get_latest_date("UNKNOWN_XYZ")
        assert result is None

    def test_returns_max_date(self, pg_loader, sample_df):
        pg_loader.upsert_dataframe(sample_df, "AAPL")
        latest = pg_loader.get_latest_date("AAPL")
        assert latest == date(2024, 1, 5)