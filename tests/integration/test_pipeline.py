import pandas as pd
from unittest.mock import MagicMock

from src.main import main
from src.extract.api_client import AlphaVantageClient
from src.load.database import DatabaseLoader


# Sample mocked API response
MOCK_API_RESPONSE = {
    "Time Series (Daily)": {
        "2024-01-03": {
            "1. open": "100",
            "2. high": "110",
            "3. low": "95",
            "4. close": "105",
            "5. volume": "1000000",
        },
        "2024-01-02": {
            "1. open": "98",
            "2. high": "102",
            "3. low": "97",
            "4. close": "100",
            "5. volume": "800000",
        },
    }
}


def test_pipeline_integration(monkeypatch):
    """
    End-to-end pipeline test with mocked API, staging, and DB.
    """

    # -----------------------------
    # Mock AlphaVantageClient
    # -----------------------------
    mock_client = MagicMock(spec=AlphaVantageClient)
    mock_client.get_daily_stock_data.return_value = MOCK_API_RESPONSE

    # -----------------------------
    # Mock DatabaseLoader
    # -----------------------------
    mock_loader = MagicMock(spec=DatabaseLoader)
    mock_loader.get_latest_date.return_value = None
    mock_loader.upsert_dataframe.return_value = None

    # -----------------------------
    # Patch dependencies inside main
    # -----------------------------
    monkeypatch.setattr("src.main.AlphaVantageClient", lambda api_key: mock_client)

    monkeypatch.setattr("src.main.DatabaseLoader", lambda conn: mock_loader)

    # -----------------------------
    # Mock write_stage to avoid filesystem writes
    # -----------------------------
    monkeypatch.setattr(
        "src.main.write_stage", lambda df, symbol, run_id: "/tmp/fake_path.parquet"
    )

    # -----------------------------
    # Run pipeline
    # -----------------------------
    main("AAPL")

    # -----------------------------
    # Assertions
    # -----------------------------
    assert mock_client.get_daily_stock_data.called
    assert mock_loader.upsert_dataframe.called

    # Verify dataframe passed to loader
    args, kwargs = mock_loader.upsert_dataframe.call_args
    df_passed = args[0]

    assert isinstance(df_passed, pd.DataFrame)
    assert not df_passed.empty
    assert "date" in df_passed.columns
