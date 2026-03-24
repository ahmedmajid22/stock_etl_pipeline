-- Create database if not exists
SELECT 'CREATE DATABASE stock_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'stock_db')\gexec

-- Connect to the database
\c stock_db;

-- Create table if not exists
CREATE TABLE IF NOT EXISTS stock_prices (
    date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (symbol, date)
);