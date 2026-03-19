from src.config.config import Config
from src.utils.logger import logger
from src.extract.api_client import AlphaVantageClient

def main():
    try:
        cfg = Config()

        client = AlphaVantageClient(api_key=cfg.API_KEY)

        data = client.get_daily_stock_data("AAPL")

        logger.info("API data fetched successfully.")
        logger.info(f"Sample response keys: {list(data.keys())}")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()