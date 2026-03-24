import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import String, Float, BigInteger, Date
from sqlalchemy.exc import SQLAlchemyError
from src.utils.logger import logger


class DatabaseLoader:
    def __init__(self, db_connection_string: str) -> None:
        self.engine = create_engine(db_connection_string)
        self.metadata = MetaData()

        # Define table schema (IMPORTANT)
        self.table = Table(
            "stock_prices",
            self.metadata,
            Column("date", Date, primary_key=True),
            Column("symbol", String, primary_key=True),
            Column("open", Float),
            Column("high", Float),
            Column("low", Float),
            Column("close", Float),
            Column("volume", BigInteger),
        )

        # Create table if not exists
        self.metadata.create_all(self.engine)

        logger.info("Database initialized successfully")

    def upsert_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        try:
            with self.engine.begin() as conn:

                records = df.to_dict(orient="records")

                stmt = insert(self.table).values(records)

                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol", "date"],
                    set_={
                        "open": stmt.excluded.open,
                        "high": stmt.excluded.high,
                        "low": stmt.excluded.low,
                        "close": stmt.excluded.close,
                        "volume": stmt.excluded.volume,
                    },
                )

                conn.execute(stmt)

            logger.info(f"Upsert successful: {len(df)} records")

        except SQLAlchemyError as e:
            logger.exception(f"Database error: {e}")
            raise