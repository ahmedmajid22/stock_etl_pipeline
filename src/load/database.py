import pandas as pd
import time
from typing import List, Dict, Any
from sqlalchemy import create_engine, MetaData, Table, Column, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import String, Float, BigInteger, Date, Integer
from sqlalchemy.exc import SQLAlchemyError, OperationalError, InterfaceError
from sqlalchemy.pool import QueuePool

from src.utils.logger import logger


class DatabaseLoader:
    """
    Handles database operations for the stock ETL pipeline.
    - Connection pooling for scalability
    - Batch UPSERT with chunking
    - Retry logic for transient failures
    - Latency logging
    """

    def __init__(self, db_connection_string: str, batch_size: int = 1000) -> None:
        """
        Args:
            db_connection_string: PostgreSQL connection string.
            batch_size: Number of rows to insert per batch.
        """
        self.batch_size = batch_size
        self.engine = create_engine(
            db_connection_string,
            poolclass=QueuePool,
            pool_size=10,               # number of connections to keep open
            max_overflow=20,            # extra connections allowed
            pool_pre_ping=True,         # verify connections before using
            pool_recycle=3600,          # recycle after 1 hour
            echo=False,                 # disable SQL logging in production
        )
        self.metadata = MetaData()

        # Dimension table reference
        self.stocks = Table(
            "stocks",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("symbol", String(10), unique=True, nullable=False),
        )

        # Fact table reference
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

        logger.info("DatabaseLoader initialized (tables assumed to exist)")

    def _get_or_create_stock_id(self, conn, symbol: str) -> int:
        """Atomic upsert for stock dimension table."""
        insert_stmt = (
            insert(self.stocks)
            .values(symbol=symbol)
            .on_conflict_do_nothing(index_elements=["symbol"])
            .returning(self.stocks.c.id)
        )
        result = conn.execute(insert_stmt)
        stock_id_row = result.fetchone()
        if stock_id_row:
            stock_id = stock_id_row[0]
            logger.info(f"Inserted new stock: {symbol} with id {stock_id}")
            return stock_id

        # Already exists – fetch the id
        stmt = select(self.stocks.c.id).where(self.stocks.c.symbol == symbol)
        stock_id = conn.execute(stmt).scalar_one()
        logger.info(f"Found existing stock: {symbol} with id {stock_id}")
        return stock_id

    def _execute_with_retry(self, operation, max_retries: int = 3):
        """
        Execute a database operation with retry logic for transient failures.
        """
        retry_delay = 1  # start with 1 second
        for attempt in range(1, max_retries + 1):
            try:
                return operation()
            except (OperationalError, InterfaceError) as e:
                # These are usually transient (network, connection, deadlock)
                if attempt == max_retries:
                    logger.error(f"Database operation failed after {max_retries} attempts")
                    raise
                logger.warning(f"Transient DB error (attempt {attempt}): {e}. Retrying in {retry_delay}s")
                time.sleep(retry_delay)
                retry_delay *= 2  # exponential backoff
            except SQLAlchemyError as e:
                # Other SQL errors are not retried (they indicate data/logic issues)
                logger.exception(f"Non‑retriable database error: {e}")
                raise

    def upsert_dataframe(self, df: pd.DataFrame, symbol: str) -> None:
        """
        Insert stock data using stock_id (dimension-based design).
        Performs batch UPSERT to manage memory and improve performance.
        """
        # Early return if empty
        if df.empty:
            logger.info(f"No data to upsert for {symbol}")
            return

        start_time = time.time()
        total_rows = len(df)

        # Get stock_id once
        with self.engine.connect() as conn:
            stock_id = self._get_or_create_stock_id(conn, symbol)

        # Prepare data (drop symbol column, add stock_id)
        records = df.copy()
        records["stock_id"] = stock_id
        records = records.drop(columns=["symbol"], errors="ignore")

        # Convert to list of dicts for chunking
        data = records.to_dict(orient="records")

        # Chunk the data
        for i in range(0, len(data), self.batch_size):
            chunk = data[i : i + self.batch_size]
            self._upsert_chunk(chunk, symbol)

        elapsed = time.time() - start_time
        logger.info(
            f"Upsert completed for {symbol} ({total_rows} rows) in {elapsed:.2f} seconds"
        )

    def _upsert_chunk(self, chunk: List[Dict[str, Any]], symbol: str) -> None:
        """
        Execute UPSERT for a single chunk of rows.
        """
        def do_upsert():
            with self.engine.begin() as conn:
                stmt = insert(self.stock_prices).values(chunk)
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

        self._execute_with_retry(do_upsert)