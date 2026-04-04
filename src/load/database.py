import time
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, select, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.pool import QueuePool
from sqlalchemy.types import (
    BigInteger,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    Text,
)

from src.utils.logger import logger


class DatabaseLoader:
    """
    Handles all database writes for the stock ETL pipeline.

    Key design decisions:
    - Idempotent UPSERT on (stock_id, date) — safe to re-run at any time
    - Batched writes (default 1 000 rows/batch) to bound memory usage
    - Exponential-backoff retry on transient connection errors
    - ensure_tables_exist() uses checkfirst=True so it is safe in production
      (it will not drop or alter existing tables/constraints managed by init_db.sql)
    """

    def __init__(self, db_connection_string: str, batch_size: int = 1000) -> None:
        self.batch_size = batch_size
        self.engine = create_engine(
            db_connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,  # test connections before use (handles stale sockets)
            pool_recycle=3600,  # recycle connections every hour
            echo=False,
        )
        self.metadata = MetaData()

        # ── Table definitions ─────────────────────────────────────────────────
        # These mirror init_db.sql so that SQLAlchemy can generate correct INSERT/
        # SELECT statements.  create_all() is only used in integration tests (with
        # checkfirst=True) — production schema is always managed by init_db.sql.
        # Note: SQLAlchemy Table definitions intentionally omit CHECK constraints
        # and partial indexes — those are DDL concerns owned by init_db.sql.

        self.stocks = Table(
            "stocks",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("symbol", String(10), unique=True, nullable=False),
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=text("now()"),
            ),
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
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=text("now()"),
            ),
            Column(
                "updated_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=text("now()"),
            ),
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
            Column(
                "logged_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=text("now()"),
            ),
        )

        self.ensure_tables_exist()
        logger.info("DatabaseLoader initialized")

    def ensure_tables_exist(self) -> None:
        """
        Create tables if they do not already exist.

        Uses checkfirst=True so this is a no-op in production (where init_db.sql
        has already created the full schema with constraints and indexes).
        In integration tests running against a fresh DB it creates the minimal
        schema needed for the tests to pass.
        """
        try:
            self.metadata.create_all(self.engine, checkfirst=True)
            logger.info("All tables verified / created")
        except SQLAlchemyError as e:
            logger.error(f"Failed to ensure tables exist: {e}")
            raise

    def get_latest_date(self, symbol: str) -> Optional[date]:
        """Return the most recent date already loaded for *symbol*, or None."""
        try:
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
        """
        Upsert into stocks dimension table and return the row id.

        Uses INSERT … ON CONFLICT DO NOTHING returning id.  If the row already
        existed the RETURNING clause yields nothing, so we fall back to a SELECT.
        This two-step pattern is safe under the (low) concurrency of this pipeline.
        """
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

        stock_id = conn.execute(
            select(self.stocks.c.id).where(self.stocks.c.symbol == symbol)
        ).scalar_one()
        logger.info(f"Existing stock: {symbol} → id {stock_id}")
        return stock_id

    def _execute_with_retry(self, operation, max_retries: int = 3):
        """Run *operation* with exponential-backoff retry on transient DB errors."""
        delay = 1
        for attempt in range(1, max_retries + 1):
            try:
                return operation()
            except (OperationalError, InterfaceError) as e:
                if attempt == max_retries:
                    logger.error(f"DB operation failed after {max_retries} attempts")
                    raise
                logger.warning(
                    f"Transient DB error (attempt {attempt}/{max_retries}): {e}. "
                    f"Retrying in {delay}s…"
                )
                time.sleep(delay)
                delay *= 2
            except SQLAlchemyError as e:
                # Non-retriable (constraint violation, bad SQL, etc.) — fail fast
                logger.exception(f"Non-retriable DB error: {e}")
                raise

    def upsert_dataframe(self, df: pd.DataFrame, symbol: str) -> None:
        """Batch-upsert all rows in *df* for *symbol* into stock_prices."""
        if df.empty:
            logger.info(f"No data to upsert for {symbol}")
            return

        start = time.time()
        total = len(df)

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
        """Execute a single batch UPSERT."""

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
                        # updated_at is handled by the DB trigger (trg_stock_prices_updated_at)
                    },
                )
                conn.execute(stmt)

        self._execute_with_retry(do_upsert)

    def log_quality_metrics(
        self,
        symbol: str,
        run_id: str,
        rows_raw: int,
        rows_transformed: int,
        rows_validated: int,
        rows_loaded: int,
        api_latency_ms: int,
        db_latency_ms: int,
        pipeline_duration_s: float,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> None:
        """
        Insert one audit row into data_quality_log for this pipeline run.

        rows_dropped is defined as (rows_validated - rows_loaded): the number of
        records that passed all quality checks but were not written to the DB
        (e.g. already present from a previous run, or filtered by incremental logic).
        This is intentionally different from (rows_raw - rows_loaded) which would
        conflate intentional transform drops with genuine load failures.
        """
        # FIX: previous formula was `max(0, rows_raw - rows_loaded)` which counted
        # every row removed during transform/validate as "dropped at load" — semantically
        # incorrect. rows_dropped should only reflect data lost *after* validation.
        rows_dropped = max(0, rows_validated - rows_loaded)

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
                        rows_dropped=rows_dropped,
                        api_latency_ms=api_latency_ms,
                        db_latency_ms=db_latency_ms,
                        pipeline_duration_s=pipeline_duration_s,
                        status=status,
                        error_message=error_message,
                    )
                )
            logger.info(
                f"Quality metrics logged for {symbol} run {run_id} — status={status}"
            )
        except SQLAlchemyError as e:
            # Metric logging failure must not crash the pipeline
            logger.warning(f"Failed to log quality metrics for {symbol}: {e}")
