-- =============================================================================
-- Stock ETL — PostgreSQL schema
-- =============================================================================

-- ── Dimension table ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stocks (
    id         SERIAL      PRIMARY KEY,
    symbol     VARCHAR(10) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Fact table ────────────────────────────────────────────────────────────────
-- NUMERIC(10,4) for financial precision (avoids float rounding errors).
-- PRIMARY KEY (stock_id, date) ensures idempotent upserts via ON CONFLICT.
CREATE TABLE IF NOT EXISTS stock_prices (
    stock_id    INTEGER         NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date        DATE            NOT NULL,
    open        NUMERIC(10, 4),
    high        NUMERIC(10, 4),
    low         NUMERIC(10, 4),
    close       NUMERIC(10, 4),
    volume      BIGINT,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_id, date)
);

-- Main lookup index
CREATE INDEX IF NOT EXISTS idx_stock_prices_stock_date
    ON stock_prices (stock_id, date DESC);

-- Recent-data partial index
-- NOTE: must use a constant date literal — CURRENT_DATE is not IMMUTABLE and
-- cannot appear in an index predicate.
CREATE INDEX IF NOT EXISTS idx_stock_prices_recent
    ON stock_prices (date DESC)
    WHERE date >= '2000-01-01'::date;

-- ── Data quality / observability log ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_quality_log (
    id                  SERIAL       PRIMARY KEY,
    symbol              VARCHAR(10)  NOT NULL,
    -- run_id links back to the Airflow DagRun run_id for full traceability
    run_id              TEXT         NOT NULL DEFAULT 'unknown',
    run_date            DATE         NOT NULL,
    rows_raw            INTEGER,
    rows_transformed    INTEGER,
    rows_validated      INTEGER,
    rows_loaded         INTEGER,
    rows_dropped        INTEGER,
    api_latency_ms      INTEGER,
    db_latency_ms       INTEGER,
    pipeline_duration_s FLOAT,
    status              VARCHAR(20)  NOT NULL DEFAULT 'success'
                            CHECK (status IN ('success', 'failure', 'partial')),
    error_message       TEXT,
    logged_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dql_symbol_date
    ON data_quality_log (symbol, run_date DESC);

-- ── updated_at trigger ───────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_stock_prices_updated_at ON stock_prices;

CREATE TRIGGER trg_stock_prices_updated_at
    BEFORE UPDATE ON stock_prices
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ── Upsert helper view ───────────────────────────────────────────────────────
-- The application layer should use:
--
--   INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
--   VALUES (...)
--   ON CONFLICT (stock_id, date) DO UPDATE SET
--     open       = EXCLUDED.open,
--     high       = EXCLUDED.high,
--     low        = EXCLUDED.low,
--     close      = EXCLUDED.close,
--     volume     = EXCLUDED.volume,
--     updated_at = now();
--
-- This guarantees idempotent re-runs when a DAG is retried after partial failure.
