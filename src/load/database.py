import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import String, Float, BigInteger, Date, Integer
from sqlalchemy.exc import SQLAlchemyError

from src.utils.logger import logger


class DatabaseLoader:
    def __init__(self, db_connection_string: str) -> None:
        self.engine = create_engine(db_connection_string)
        self.metadata = MetaData()

        # Dimension table
        self.stocks = Table(
            "stocks",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("symbol", String(10), unique=True, nullable=False),
        )

        # Fact table
        self.stock_prices = Table(
            "stock_prices",
            self.metadata,
            Column("stock_id", Integer, primary_key=True),
            Column("date", Date, primary_key=True),
            Column("open", Float),
            Column("high", Float),
            Column("low", Float),
            Column("close", Float),
            Column("volume", BigInteger),
        )

        # Ensure tables exist
        self.metadata.create_all(self.engine)

        logger.info("Database schema initialized")

    def _get_or_create_stock_id(self, conn, symbol: str) -> int:
        """
        Ensure stock exists in dimension table and return its ID.
        Uses atomic UPSERT to avoid race conditions.
        """
        # Try to insert – if symbol already exists, ON CONFLICT does nothing
        insert_stmt = insert(self.stocks).values(symbol=symbol).on_conflict_do_nothing(
            index_elements=["symbol"]
        ).returning(self.stocks.c.id)

        result = conn.execute(insert_stmt)
        stock_id_row = result.fetchone()

        if stock_id_row:
            stock_id = stock_id_row[0]
            logger.info(f"Inserted new stock: {symbol} with id {stock_id}")
            return stock_id

        # If insert did nothing, symbol already exists – fetch existing id
        stmt = select(self.stocks.c.id).where(self.stocks.c.symbol == symbol)
        stock_id = conn.execute(stmt).scalar_one()
        logger.info(f"Found existing stock: {symbol} with id {stock_id}")
        return stock_id

    def upsert_dataframe(self, df: pd.DataFrame, symbol: str) -> None:
        """
        Insert stock data using stock_id (dimension-based design).
        """
        try:
            with self.engine.begin() as conn:

                # Get or create stock_id
                stock_id = self._get_or_create_stock_id(conn, symbol)

                # Prepare data
                records = df.copy()
                records["stock_id"] = stock_id

                # Safely drop the 'symbol' column if it exists
                records = records.drop(columns=["symbol"], errors='ignore')

                data = records.to_dict(orient="records")

                # UPSERT into fact table
                stmt = insert(self.stock_prices).values(data)

                stmt = stmt.on_conflict_do_update(
                    index_elements=["stock_id", "date"],
                    set_={
                        "open": stmt.excluded.open,
                        "high": stmt.excluded.high,
                        "low": stmt.excluded.low,
                        "close": stmt.excluded.close,
                        "volume": stmt.excluded.volume,
                    },
                )

                conn.execute(stmt)

            logger.info(f"Upsert completed for {symbol} ({len(df)} rows)")

        except SQLAlchemyError as e:
            logger.exception(f"Database error: {e}")
            raise