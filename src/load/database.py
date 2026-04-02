import time
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, select, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.pool import QueuePool
from sqlalchemy.types import BigInteger, Date, Integer, Numeric, String, Float, Text

from src.utils.logger import logger


class DatabaseLoader:
    def __init__(self, db_connection_string: str, batch_size: int = 1000) -> None:
        self.batch_size = batch_size
        self.engine = create_engine(
            db_connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
        )
        self.metadata = MetaData()

        self.stocks = Table(
            "stocks",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("symbol", String(10), unique=True, nullable=False),
        )
        self.stock_prices = Table(
            "stock_prices",
            self.metadata,
            Column("stock_id", Integer, primary_key=True),
            Column("date", Date, primary_key=True),
            Column("open", Numeric(10, 4)),
            Column("high", Numeric(10, 4)),
            Column("low", Numeric(10, 4)),
            Column("close", Numeric(10, 4)),
            Column("volume", BigInteger),
        )
        self.data_quality_log = Table(
            "data_quality_log",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("symbol", String(10)),
            Column("run_id", Text),
            Column("run_date", Date),
            Column("rows_raw", Integer),
            Column("rows_transformed", Integer),
            Column("rows_validated", Integer),
            Column("rows_loaded", Integer),
            Column("rows_dropped", Integer),
            Column("api_latency_ms", Integer),
            Column("db_latency_ms", Integer),
            Column("pipeline_duration_s", Float),
            Column("status", String(20)),
            Column("error_message", Text),
        )
        self.ensure_tables_exist()
        logger.info("DatabaseLoader initialized")

    def ensure_tables_exist(self):
        try:
            # SA 2.0: pass engine directly to create_all
            self.metadata.create_all(self.engine)
            logger.info("All tables ensured to exist")
        except SQLAlchemyError as e:
            logger.error(f"Failed to ensure tables exist: {e}")
            raise

    def get_latest_date(self, symbol: str) -> Optional[date]:
        try:
            # SA 2.0: use engine.connect() as context manager, execute() returns CursorResult
            with self.engine.connect() as conn:
                stock_id = conn.execute(
                    select(self.stocks.c.id).where(self.stocks.c.symbol == symbol)
                ).scalar()

                if stock_id is None:
                    return None

                latest = conn.execute(
                    select(func.max(self.stock_prices.c.date)).where(
                        self.stock_prices.c.stock_id == stock_id
                    )
                ).scalar()

                if latest:
                    logger.info(f"Latest date in DB for {symbol}: {latest}")
                return latest

        except SQLAlchemyError as e:
            logger.error(f"Error fetching latest date for {symbol}: {e}")
            return None

    def _get_or_create_stock_id(self, conn, symbol: str) -> int:
        # SA 2.0: insert().returning() still works, but fetchone() returns a Row
        result = conn.execute(
            insert(self.stocks)
            .values(symbol=symbol)
            .on_conflict_do_nothing(index_elements=["symbol"])
            .returning(self.stocks.c.id)
        )
        row = result.fetchone()
        if row:
            logger.info(f"Inserted new stock: {symbol} → id {row[0]}")
            return row[0]

        # SA 2.0: scalar_one() still supported
        stock_id = conn.execute(
            select(self.stocks.c.id).where(self.stocks.c.symbol == symbol)
        ).scalar_one()
        logger.info(f"Existing stock: {symbol} → id {stock_id}")
        return stock_id

    def _execute_with_retry(self, operation, max_retries: int = 3):
        delay = 1
        for attempt in range(1, max_retries + 1):
            try:
                return operation()
            except (OperationalError, InterfaceError) as e:
                if attempt == max_retries:
                    logger.error(f"DB operation failed after {max_retries} attempts")
                    raise
                logger.warning(
                    f"Transient DB error (attempt {attempt}): {e}. Retry in {delay}s"
                )
                time.sleep(delay)
                delay *= 2
            except SQLAlchemyError as e:
                logger.exception(f"Non-retriable DB error: {e}")
                raise

    def upsert_dataframe(self, df: pd.DataFrame, symbol: str) -> None:
        if df.empty:
            logger.info(f"No data to upsert for {symbol}")
            return

        start = time.time()
        total = len(df)

        # SA 2.0: engine.begin() still works as a context manager
        with self.engine.begin() as conn:
            stock_id = self._get_or_create_stock_id(conn, symbol)

        records = df.copy()
        records["stock_id"] = stock_id
        records = records.drop(columns=["symbol"], errors="ignore")
        data = records.to_dict(orient="records")

        for i in range(0, len(data), self.batch_size):
            self._upsert_chunk(data[i : i + self.batch_size])

        elapsed = time.time() - start
        logger.info(f"Upsert complete for {symbol}: {total} rows in {elapsed:.2f}s")

    def _upsert_chunk(self, chunk: List[Dict[str, Any]]) -> None:
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

    def log_quality_metrics(
        self,
        symbol,
        run_id,
        rows_raw,
        rows_transformed,
        rows_validated,
        rows_loaded,
        api_latency_ms,
        db_latency_ms,
        pipeline_duration_s,
        status="success",
        error_message=None,
    ) -> None:
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    insert(self.data_quality_log).values(
                        symbol=symbol,
                        run_id=run_id,
                        run_date=datetime.now(timezone.utc).date(),
                        rows_raw=rows_raw,
                        rows_transformed=rows_transformed,
                        rows_validated=rows_validated,
                        rows_loaded=rows_loaded,
                        rows_dropped=rows_raw - rows_loaded,
                        api_latency_ms=api_latency_ms,
                        db_latency_ms=db_latency_ms,
                        pipeline_duration_s=pipeline_duration_s,
                        status=status,
                        error_message=error_message,
                    )
                )
            logger.info(f"Quality metrics logged for {symbol} run {run_id}")
        except SQLAlchemyError as e:
            logger.warning(f"Failed to log quality metrics for {symbol}: {e}")
