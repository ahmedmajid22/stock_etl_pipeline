import sys
import uuid
import os
from datetime import datetime, timezone

import pandas as pd

from src.config.config import Config
from src.extract.api_client import AlphaVantageClient
from src.load.database import DatabaseLoader
from src.storage.staging import write_stage
from src.transform.transformer import StockDataTransformer
from src.transform.validator import StockDataValidator
from src.utils.logger import logger


def _push_metrics(
    symbol,
    rows_loaded,
    api_latency_ms,
    db_latency_ms,
    pipeline_duration_s,
    status,
):
    """Push metrics to Prometheus Pushgateway."""
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        # Use localhost when running outside Docker, internal hostname inside Docker
        default_gateway = (
            "pushgateway:9091" if os.path.exists("/opt/airflow") else "localhost:9091"
        )
        gateway = os.getenv("PUSHGATEWAY_URL", default_gateway)

        registry = CollectorRegistry()

        Gauge("etl_rows_loaded", "Rows loaded", ["symbol"], registry=registry) \
            .labels(symbol=symbol).set(rows_loaded)

        Gauge("etl_api_latency_ms", "API latency ms", ["symbol"], registry=registry) \
            .labels(symbol=symbol).set(api_latency_ms)

        Gauge("etl_db_latency_ms", "DB latency ms", ["symbol"], registry=registry) \
            .labels(symbol=symbol).set(db_latency_ms)

        Gauge("etl_duration_s", "Pipeline duration seconds", ["symbol"], registry=registry) \
            .labels(symbol=symbol).set(pipeline_duration_s)

        Gauge("etl_success", "Pipeline success (1/0)", ["symbol"], registry=registry) \
            .labels(symbol=symbol).set(1 if status == "success" else 0)

        push_to_gateway(gateway, job="stock_etl", registry=registry)
        logger.info(f"Metrics pushed to Pushgateway at {gateway} for {symbol}")

    except Exception as e:
        logger.warning(f"Metrics push failed (gateway may not be running): {e}")


def main(symbol: str = "AAPL") -> None:
    """
    Local / standalone pipeline entry point.
    Extract → Transform → Validate → Stage → Load
    """

    pipeline_start = datetime.now(timezone.utc)
    run_id = f"{pipeline_start.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"

    rows_raw = 0
    rows_transformed = 0
    rows_validated = 0
    rows_loaded = 0
    api_latency_ms = 0
    db_latency_ms = 0
    status = "success"
    error_message = None

    cfg = Config()
    loader = DatabaseLoader(cfg.get_db_connection_string())

    try:
        logger.info(f"{'=' * 60}")
        logger.info(f"ETL start | symbol={symbol} | run_id={run_id}")
        logger.info(f"{'=' * 60}")

        client = AlphaVantageClient(api_key=cfg.API_KEY)
        transformer = StockDataTransformer()
        validator = StockDataValidator()

        # ── Extract ─────────────────────────────────────────────
        logger.info("EXTRACT: fetching from Alpha Vantage")
        t0 = datetime.now(timezone.utc)
        raw_data = client.get_daily_stock_data(symbol)
        api_latency_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)

        rows_raw = len(raw_data.get("Time Series (Daily)", {}))
        logger.info(f"EXTRACT complete: rows_raw={rows_raw} api_latency_ms={api_latency_ms}")

        # ── Transform ───────────────────────────────────────────
        logger.info("TRANSFORM: converting to DataFrame")
        df = transformer.transform(raw_data, symbol=symbol)
        rows_transformed = len(df)
        logger.info(f"TRANSFORM complete: rows_transformed={rows_transformed}")

        # ── Validate ────────────────────────────────────────────
        logger.info("VALIDATE: running quality checks")
        df = validator.validate(df).copy()
        rows_validated = len(df)
        logger.info(f"VALIDATE complete: rows_validated={rows_validated}")

        if rows_validated == 0:
            logger.warning(f"No valid records for {symbol}. Skipping load.")
            return

        # ── Incremental ─────────────────────────────────────────
        latest_db_date = loader.get_latest_date(symbol)

        if latest_db_date:
            before = len(df)
            df = df[df["date"] > latest_db_date]
            logger.info(
                f"INCREMENTAL: {len(df)} new rows (skipped {before - len(df)} already in DB)"
            )

            if df.empty:
                logger.info(f"No new data for {symbol}. Pipeline complete.")
                return

        # ── Stage ───────────────────────────────────────────────
        logger.info("STAGE: writing Parquet")
        staging_path = write_stage(df, symbol, run_id)
        logger.info(f"STAGE complete: path={staging_path}")

        # ── Load ────────────────────────────────────────────────
        logger.info("LOAD: upserting to PostgreSQL")
        t0 = datetime.now(timezone.utc)
        loader.upsert_dataframe(df, symbol)
        db_latency_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)

        rows_loaded = len(df)
        logger.info(f"LOAD complete: rows_loaded={rows_loaded} db_latency_ms={db_latency_ms}")

    except Exception as e:
        status = "failed"
        error_message = str(e)
        logger.exception(f"ETL failed for {symbol}: {e}")
        raise

    finally:
        pipeline_duration_s = (datetime.now(timezone.utc) - pipeline_start).total_seconds()

        loader.log_quality_metrics(
            symbol=symbol,
            run_id=run_id,
            rows_raw=rows_raw,
            rows_transformed=rows_transformed,
            rows_validated=rows_validated,
            rows_loaded=rows_loaded,
            api_latency_ms=api_latency_ms,
            db_latency_ms=db_latency_ms,
            pipeline_duration_s=pipeline_duration_s,
            status=status,
            error_message=error_message,
        )

        _push_metrics(
            symbol,
            rows_loaded,
            api_latency_ms,
            db_latency_ms,
            pipeline_duration_s,
            status,
        )

        logger.info(
            f"{'=' * 60}\n"
            f"ETL {'DONE' if status == 'success' else 'FAILED'} | symbol={symbol} | "
            f"loaded={rows_loaded} | duration={pipeline_duration_s:.2f}s\n"
            f"{'=' * 60}"
        )


if __name__ == "__main__":
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    main(symbol)