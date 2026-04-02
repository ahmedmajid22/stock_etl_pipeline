# 📈 Stock Market ETL Pipeline

> A production-grade data engineering pipeline that ingests daily OHLCV stock data from Alpha Vantage, transforms and validates it, stages it to Parquet, and loads it into PostgreSQL — orchestrated by Apache Airflow with full observability via Prometheus and Grafana.

[![CI](https://github.com/ahmedmajid22/stock_etl_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ahmedmajid22/stock_etl_pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-336791)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

---

## Architecture

```
Alpha Vantage API
       │
       ▼
  ┌─────────────┐     Raw JSON      ┌──────────────────┐
  │   Extract   │ ─────────────────▶│  data/raw/       │
  └─────────────┘                   └──────────────────┘
       │
       ▼
  ┌─────────────┐    Parquet file   ┌──────────────────┐
  │  Transform  │ ─────────────────▶│  data/staging/   │
  └─────────────┘                   └──────────────────┘
       │
       ▼
  ┌─────────────┐
  │  Validate   │  (price rules, nulls, duplicates, volume checks)
  └─────────────┘
       │
       ▼
  ┌─────────────┐
  │    Stage    │  (confirms Parquet on disk — replay safe)
  └─────────────┘
       │
       ▼
  ┌─────────────┐    UPSERT         ┌──────────────────┐
  │    Load     │ ─────────────────▶│   PostgreSQL     │
  └─────────────┘                   └──────────────────┘
                                            │
                                            ▼
                                    ┌──────────────────┐
                                    │  Prometheus  +   │
                                    │    Grafana       │
                                    └──────────────────┘
```

**4 symbols run in parallel:** `AAPL · MSFT · GOOG · TSLA`  
Each symbol runs its own independent 5-task chain. Concurrent API calls are capped at 3 via an Airflow pool.

---

## Tech Stack

| Tool | Role | Why I chose it |
|---|---|---|
| **Apache Airflow 2.8** | Orchestration | Industry-standard, DAG-as-code, built-in retry/alerting |
| **PostgreSQL 13** | Data store | ACID compliance, `NUMERIC(10,4)` for financial precision |
| **Celery + Redis** | Distributed execution | Scales workers horizontally, decouples scheduling from execution |
| **pandas + PyArrow** | Transform & staging | Columnar Parquet staging enables fast replay without DB round-trips |
| **SQLAlchemy 1.4** | DB abstraction | Connection pooling, dialect-agnostic UPSERT |
| **Prometheus + Grafana** | Observability | Push-based metrics per symbol: rows loaded, latency, duration |
| **Docker Compose** | Local infra | One-command spin-up of all 8 services |
| **loguru** | Logging | Structured, rotating, thread-safe — works cleanly with Celery workers |
| **pytest + pytest-cov** | Testing | Unit + integration coverage with CI enforcement |

---

## Key Design Decisions

| Decision | What I did | Why |
|---|---|---|
| **Financial precision** | `NUMERIC(10,4)` not `FLOAT` | Floating-point rounding errors are unacceptable for stock prices |
| **XCom carries paths, not DataFrames** | Tasks push file paths via XCom | DataFrames in XCom serialise to the Airflow DB — a known anti-pattern that blows up at scale |
| **Staging layer** | Raw JSON + Parquet on disk before DB load | Enables full pipeline replay without re-hitting the API; decouples extraction from loading |
| **UPSERT on `(stock_id, date)`** | PostgreSQL `ON CONFLICT DO UPDATE` | Idempotent — any task can be re-run safely with no duplicate data |
| **Circuit breaker** | `CLOSED → OPEN → HALF_OPEN` state machine | Stops hammering a failing API; recovers automatically after a cooldown window |
| **API concurrency cap** | Airflow pool `alpha_vantage_pool = 3` | Alpha Vantage free tier caps at 5 req/min; pool prevents 4 parallel tasks from hitting it simultaneously |
| **Audit log** | `data_quality_log` table per run | Every pipeline run writes row counts, latencies, and status — queryable quality history |
| **Partial DB index** | `idx_stock_prices_recent WHERE date >= now() - 90 days` | Dashboard queries for recent data are the hot path; partial index keeps them fast as the table grows |
| **Prometheus `honor_labels`** | Set in `prometheus.yml` scrape config | Prevents Prometheus from overwriting the `symbol` label pushed by Pushgateway, ensuring per-symbol metrics are queryable in Grafana |
| **Environment-aware Pushgateway URL** | Defaults to `localhost:9091` locally, `pushgateway:9091` inside Docker | Allows the pipeline to push metrics correctly whether run as `python -m src.main` or via Airflow workers |

---

## Quickstart

### Prerequisites
- Docker + Docker Compose
- Python 3.11+ (for running tests or the standalone pipeline locally)

### 1. Clone and configure

```bash
git clone https://github.com/ahmedmajid22/stock_etl_pipeline.git
cd stock_etl_pipeline
cp .env.example .env
# Edit .env and add your Alpha Vantage API key
```

### 2. Generate a Fernet key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output into .env as FERNET_KEY
```

### 3. Start all services

```bash
cd docker
docker compose up -d
```

This starts: PostgreSQL, Redis, Airflow webserver, Airflow scheduler, Airflow worker, Airflow init, Prometheus, Grafana, and Pushgateway.

### 4. Wait for init, then open the UI

```bash
docker compose logs -f airflow-init  # wait for "completed successfully"
```

| Service | URL | Default credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Pushgateway | http://localhost:9091 | — |

### 5. Trigger the pipeline

**Via Airflow UI:** enable and trigger `stock_market_etl_v2`

**Standalone (local, no Airflow needed):**
```bash
# Run from the project root, not the docker/ directory
PUSHGATEWAY_URL=localhost:9091 python -m src.main AAPL
PUSHGATEWAY_URL=localhost:9091 python -m src.main MSFT
PUSHGATEWAY_URL=localhost:9091 python -m src.main GOOG
PUSHGATEWAY_URL=localhost:9091 python -m src.main TSLA
```

> **Note:** `PUSHGATEWAY_URL=localhost:9091` is required when running locally so metrics reach the Pushgateway container. Inside Airflow workers the URL defaults correctly to `pushgateway:9091`.

### 6. View the Grafana dashboard

1. Go to `http://localhost:3000` and log in (admin / admin)
2. Navigate to **Dashboards → Stock ETL Pipeline**
3. Set the time range to **Last 1 hour** to see freshly pushed metrics
4. All 5 panels will populate: rows loaded, pipeline success, API latency, DB latency, and pipeline duration — broken down by symbol

---

## Running Tests

```bash
# Install dependencies
pip install -r docker/requirements.txt

# Run unit tests (no external services needed)
pytest tests/unit -v

# Run with coverage report
pytest tests/unit -v --cov=src --cov-report=term-missing

# Run all tests including integration
pytest -v
```

Expected output:
```
tests/unit/test_api_client.py::test_successful_fetch                PASSED
tests/unit/test_api_client.py::test_raises_rate_limit_error         PASSED
tests/unit/test_api_client.py::test_circuit_opens_after_threshold   PASSED
tests/unit/test_api_client.py::test_circuit_blocks_calls_when_open  PASSED
tests/unit/test_transformer.py::test_transform_happy_path           PASSED
tests/unit/test_transformer.py::test_transform_output_is_sorted     PASSED
tests/unit/test_validator.py::test_validate_passes_clean_data       PASSED
tests/unit/test_validator.py::test_validate_drops_high_less_than_low PASSED
...
```

---

## Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   └── stock_etl_dag.py       # DAG definition — 5 tasks × 4 symbols
│   └── logs/
├── docker/
│   ├── docker-compose.yml          # 8 services: Airflow, Postgres, Redis, observability
│   ├── init_db.sql                 # Schema: stocks, stock_prices, data_quality_log
│   ├── prometheus.yml              # Scrape config with honor_labels for Pushgateway
│   ├── grafana/
│   │   ├── dashboards/
│   │   │   └── stock_etl.json     # Pre-built Stock ETL Pipeline dashboard
│   │   └── provisioning/
│   │       ├── dashboards/
│   │       │   └── dashboard.yml  # Auto-loads dashboards from disk
│   │       └── datasources/
│   │           └── prometheus.yml # Auto-configures Prometheus datasource
│   └── requirements.txt
├── src/
│   ├── config/config.py            # Validated env-var config with warnings
│   ├── extract/api_client.py       # Alpha Vantage client + circuit breaker
│   ├── transform/
│   │   ├── transformer.py          # Raw JSON → clean DataFrame
│   │   └── validator.py            # Price rules, nulls, duplicates, volume
│   ├── load/database.py            # Batch UPSERT + audit logging
│   ├── storage/staging.py          # Raw JSON + Parquet staging layer
│   └── main.py                     # Standalone entry point (no Airflow needed)
├── tests/
│   ├── unit/
│   │   ├── test_api_client.py
│   │   ├── test_transformer.py
│   │   ├── test_validator.py
│   │   └── test_config.py
│   ├── integration/
│   │   ├── test_pipeline.py        # End-to-end with mocked API + DB
│   │   └── test_database.py        # Real DB upsert and audit log tests
│   └── data/
│       └── sample_av_response.json
├── .env.example
└── pytest.ini
```

---

## Database Schema

```sql
-- Dimension table
stocks (id, symbol, created_at)

-- Fact table — NUMERIC(10,4) for financial precision
stock_prices (stock_id, date, open, high, low, close, volume, created_at, updated_at)
  PRIMARY KEY (stock_id, date)
  INDEX: (stock_id, date DESC)
  PARTIAL INDEX: (date DESC) WHERE date >= now() - 90 days

-- Audit trail — one row per pipeline run per symbol
data_quality_log (symbol, run_id, run_date, rows_raw, rows_transformed,
                  rows_validated, rows_loaded, rows_dropped,
                  api_latency_ms, db_latency_ms, pipeline_duration_s,
                  status, error_message)
```

---

## Observability

Pipeline metrics are pushed to Prometheus via Pushgateway after every run:

| Metric | Description |
|---|---|
| `etl_rows_loaded` | Rows written to PostgreSQL per symbol |
| `etl_api_latency_ms` | Alpha Vantage API response time |
| `etl_db_latency_ms` | PostgreSQL UPSERT duration |
| `etl_duration_s` | Total pipeline wall-clock time |
| `etl_success` | 1 = success, 0 = failure |

The Grafana dashboard at `http://localhost:3000` is **automatically provisioned** on first startup — no manual setup required. It displays all 5 metrics broken down by symbol (AAPL, MSFT, GOOG, TSLA) with a 30-second auto-refresh.

Slack alerts are supported via an Airflow connection — add a `slack_webhook` connection (HTTP type, webhook URL in the host field) to enable failure notifications.

---

## Circuit Breaker

The Alpha Vantage client implements a three-state circuit breaker:

```
CLOSED ──(5 failures)──▶ OPEN ──(60s cooldown)──▶ HALF_OPEN
  ▲                                                     │
  └────────────────(success)───────────────────────────┘
```

- **CLOSED**: normal operation
- **OPEN**: all calls blocked immediately, no API hits
- **HALF_OPEN**: allows up to 3 test calls; resets on success, reopens on failure

Airflow's own retry mechanism (`retries=2, retry_delay=5min`) handles transient failures at the task level.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

```bash
API_KEY=your_alpha_vantage_key     # https://www.alphavantage.co/support/#api-key
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stocks
DB_USER=airflow
DB_PASSWORD=your_password
FERNET_KEY=your_generated_fernet_key
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:your_password@localhost/stocks
AIRFLOW_UID=50000
```

The `PUSHGATEWAY_URL` environment variable is optional. When running locally it defaults to `localhost:9091`; inside Docker it defaults to `pushgateway:9091`. Override it only if you have a non-standard setup.

---

## Troubleshooting

**Grafana won't start / crashes on provisioning:**
Grafana 12 changed how datasource UIDs are resolved during provisioning. Ensure `docker/grafana/provisioning/datasources/prometheus.yml` includes `orgId: 1` and `editable: true`. If the issue persists after editing the file, run `docker compose down -v && docker compose up -d` to wipe the Grafana volume and force a clean provisioning.

**Metrics not appearing in Grafana panels:**
Ensure `docker/prometheus.yml` has `honor_labels: true` under the pushgateway scrape config. Without it, Prometheus strips the `symbol` label from pushed metrics and per-symbol Grafana queries return no data. After editing the file run `docker compose restart prometheus`.

**Running standalone pipeline — metrics not reaching Grafana:**
Always prefix the command with `PUSHGATEWAY_URL=localhost:9091` when running outside Docker. The hostname `pushgateway` only resolves inside the Docker network.

**Airflow DAG not visible:**
Wait for `airflow-init` to complete (`docker compose logs -f airflow-init`), then refresh the Airflow UI. The DAG file is mounted from `airflow/dags/` so changes take effect without a container restart.

---

## What I Would Add Next

- **dbt models** — 30-day moving average, daily returns, and volatility calculations on top of `stock_prices`; separates analytical transformation logic from ingestion
- **S3 / MinIO staging** — replace local Parquet files with object storage for durability and horizontal scaling
- **pre-commit hooks** — `black`, `ruff`, and `detect-secrets` to enforce style and catch accidental credential commits before they hit git history
- **Expanded symbol list** — parameterise the DAG so symbols are configurable via Airflow Variables without a code change
- **dbt tests** — data freshness checks and referential integrity assertions at the warehouse layer
- **Airflow DAG audit metrics** — push per-task latency and row counts from the DAG's `load_task` to Pushgateway so the Grafana dashboard reflects Airflow-triggered runs, not just standalone ones