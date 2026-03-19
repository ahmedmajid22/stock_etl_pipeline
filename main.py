# main.py

from src.config.config import Config
from src.utils.logger import logger
from src.extract.api_client import AlphaVantageClient
from src.transform.transformer import StockDataTransformer
from src.transform.validator import StockDataValidator
from src.load.database import DatabaseLoader

def main():
    try:
        # Initialize configuration
        cfg = Config()

        # Initialize pipeline components
        client = AlphaVantageClient(api_key=cfg.API_KEY)
        transformer = StockDataTransformer()
        validator = StockDataValidator()
        loader = DatabaseLoader(cfg.get_db_connection_string())

        # -----------------------------
        # Extract
        # -----------------------------
        logger.info("Starting data extraction from Alpha Vantage API...")
        raw_data = client.get_daily_stock_data("AAPL")
        logger.info("Data extraction completed successfully.")

        # -----------------------------
        # Transform
        # -----------------------------
        logger.info("Starting data transformation...")
        df = transformer.transform(raw_data, symbol="AAPL")
        logger.info(f"Data transformation completed. Records: {len(df)}")

        # -----------------------------
        # Validate
        # -----------------------------
        logger.info("Starting data validation...")
        df = validator.validate(df)
        logger.info(f"Data validation successful. Records after validation: {len(df)}")

        # -----------------------------
        # Load
        # -----------------------------
        logger.info("Starting data load into PostgreSQL...")
        loader.load_dataframe(df, table_name="stock_prices")
        logger.info("ETL Pipeline completed successfully!")

    except Exception as e:
        logger.exception(f"ETL Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()