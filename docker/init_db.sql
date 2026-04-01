-- Dimension table
CREATE TABLE IF NOT EXISTS stocks (
    id         SERIAL      PRIMARY KEY,
    symbol     VARCHAR(10) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fact table — NUMERIC(10,4) replaces FLOAT for financial precision
CREATE TABLE IF NOT EXISTS stock_prices (
    stock_id    INTEGER         NOT NULL,
    date        DATE            NOT NULL,
    open        NUMERIC(10, 4),
    high        NUMERIC(10, 4),
    low         NUMERIC(10, 4),
    close       NUMERIC(10, 4),
    volume      BIGINT,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_id, date),
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

-- Primary analytics covering index: "all prices for symbol X between date A and B"
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date
    ON stock_prices (stock_id, date DESC);

-- Partial index for the hot path — last 90 days (dashboards, recent queries)
CREATE INDEX IF NOT EXISTS idx_stock_prices_recent
    ON stock_prices (date DESC)
    WHERE date >= CURRENT_DATE - INTERVAL '90 days';

-- Data quality audit trail — one row per pipeline run per symbol
CREATE TABLE IF NOT EXISTS data_quality_log (
    id                  SERIAL       PRIMARY KEY,
    symbol              VARCHAR(10)  NOT NULL,
    run_id              TEXT,
    run_date            DATE         NOT NULL,
    rows_raw            INTEGER,
    rows_transformed    INTEGER,
    rows_validated      INTEGER,
    rows_loaded         INTEGER,
    rows_dropped        INTEGER,
    api_latency_ms      INTEGER,
    db_latency_ms       INTEGER,
    pipeline_duration_s FLOAT,
    status              VARCHAR(20)  NOT NULL DEFAULT 'success',
    error_message       TEXT,
    logged_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Auto-update updated_at on every UPDATE to stock_prices
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
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();