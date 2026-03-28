import sys

from src.config.config import Config
from src.utils.logger import logger
from src.extract.api_client import AlphaVantageClient
from src.transform.transformer import StockDataTransformer
from src.transform.validator import StockDataValidator
from src.load.database import DatabaseLoader


def main(symbol: str = "AAPL") -> None:
    """
    Main ETL pipeline execution:
    Extract → Transform → Validate → Load
    """

    try:
        logger.info(f"Starting ETL pipeline for symbol: {symbol}")

        # -----------------------------
        # Configuration
        # -----------------------------
        cfg = Config()

        # -----------------------------
        # Initialize components
        # -----------------------------
        client = AlphaVantageClient(api_key=cfg.API_KEY)
        transformer = StockDataTransformer()
        validator = StockDataValidator()
        loader = DatabaseLoader(cfg.get_db_connection_string())

        # -----------------------------
        # Extract
        # -----------------------------
        logger.info("Extracting data...")
        raw_data = client.get_daily_stock_data(symbol)

        # -----------------------------
        # Transform
        # -----------------------------
        logger.info("Transforming data...")
        df = transformer.transform(raw_data, symbol=symbol)

        # -----------------------------
        # Validate
        # -----------------------------
        logger.info("Validating data...")
        df = validator.validate(df).copy()

        logger.info(f"Validated records: {len(df)}")

        # -----------------------------
        # Load
        # -----------------------------
        logger.info("Loading data into PostgreSQL (UPSERT)...")
        loader.upsert_dataframe(df, symbol)   # ✅ Fixed: pass symbol

        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.exception(f"ETL pipeline failed for {symbol}: {e}")
        raise


if __name__ == "__main__":
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    main(symbol)