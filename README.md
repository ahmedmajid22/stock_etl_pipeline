# 📈 Stock Market ETL Pipeline

> Production-grade data engineering pipeline — ingests, validates, and monitors daily OHLCV data for 4 symbols with full observability via Prometheus and Grafana.

[![CI](https://github.com/ahmedmajid22/stock_etl_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ahmedmajid22/stock_etl_pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-336791)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Coverage](https://img.shields.io/badge/coverage-87%25-brightgreen)

---

## What This Pipeline Does

Every day, this pipeline automatically:

1. **Extracts** daily OHLCV stock prices for `AAPL · MSFT · GOOG · TSLA` from Alpha Vantage
2. **Transforms** raw JSON into typed, analytics-ready DataFrames
3. **Validates** every row against price consistency rules, null checks, and volume constraints
4. **Stages** clean data to Parquet on disk — enabling full replay without re-hitting the API
5. **Loads** into PostgreSQL via idempotent UPSERT — safe to re-run at any time
6. **Monitors** every run with Prometheus metrics and a pre-built Grafana dashboard

All 4 symbols run **in parallel** inside Apache Airflow, with API concurrency capped at 3 via a pool to respect rate limits.

---

## Architecture

```mermaid
flowchart TD
    AV[Alpha Vantage API] -->|REST / JSON| EX[Extract]
    EX -->|Raw JSON| RAW[(data/raw/)]
    EX --> TR[Transform]
    TR -->|Clean DataFrame| VA[Validate]
    VA -->|Validated DataFrame| ST[Stage]
    ST -->|Parquet on disk| DISK[(data/staging/)]
    ST --> LO[Load]
    LO -->|UPSERT on stock_id + date| PG[(PostgreSQL)]
    PG --> PM[Prometheus]
    PM --> GF[Grafana Dashboard]

    subgraph Airflow DAG — 4 symbols × 5 tasks in parallel
        EX
        TR
        VA
        ST
        LO
    end
```

**Task graph per symbol:**
```
extract → transform → validate → stage → load
```

---

## Key Design Decisions

| Decision | What I did | Why |
|---|---|---|
| **Financial precision** | `NUMERIC(10,4)` not `FLOAT` | Floating-point rounding errors are unacceptable for stock prices |
| **XCom carries paths, not DataFrames** | Tasks push file paths via XCom | Prevents Airflow metadata DB bloat — a well-known anti-pattern at scale |
| **Staging layer** | Raw JSON + Parquet on disk before DB load | Full pipeline replay without re-hitting the API; decouples extraction from loading |
| **UPSERT on `(stock_id, date)`** | PostgreSQL `ON CONFLICT DO UPDATE` | Idempotent — any task can be re-run safely with no duplicate data |
| **Circuit breaker** | `CLOSED → OPEN → HALF_OPEN` state machine | Stops hammering a failing API; recovers automatically after a cooldown window |
| **API concurrency cap** | Airflow pool `alpha_vantage_pool = 3` | Alpha Vantage free tier caps at 5 req/min; pool prevents 4 parallel tasks from hitting it simultaneously |
| **Audit log** | `data_quality_log` table per run | Every run writes row counts, latencies, and status — queryable quality history |
| **Partial DB index** | `idx_stock_prices_recent WHERE date >= '2000-01-01'` | Dashboard queries for recent data are the hot path; partial index keeps them fast as the table grows |

### Architecture Decision Records (ADR)

**ADR-1: XCom carries file paths, not DataFrames**

Passing DataFrames through XCom serialises them to the Airflow metadata database. For small pipelines this works fine, but as DataFrame size grows it bloats the DB and can crash the scheduler — a well-documented anti-pattern. Instead, each task writes its output to disk (raw JSON or Parquet) and pushes only the file path via XCom. This adds a thin disk I/O cost but makes the pipeline safe to run on arbitrarily large datasets and keeps Airflow's DB lean.

**ADR-2: Parquet staging layer before PostgreSQL**

A naive pipeline goes API → DB directly. The problem: if the load step fails, you must re-hit the API, burning rate-limit quota and adding latency. By staging validated data to Parquet first, any failed load task can simply re-read from disk and retry — no API call needed. This also decouples extraction cadence from load cadence, making future changes (e.g., loading to multiple destinations) trivial.

**ADR-3: NUMERIC(10,4) for all price columns**

IEEE 754 floats cannot represent many decimal values exactly. For most applications this rounding error is invisible, but in financial pipelines it accumulates across aggregations and comparisons. Using PostgreSQL's `NUMERIC(10,4)` stores values with exact decimal precision. The tradeoff is marginally slower arithmetic than `FLOAT`, which is acceptable given that query performance is dominated by I/O, not CPU.

**ADR-4: Three-state circuit breaker on the API client**

Alpha Vantage's free tier is rate-limited and occasionally flaky. Without protection, a degraded API causes a cascade of Airflow task failures and retries, hammering the endpoint and consuming quota. The circuit breaker detects repeated failures, opens (blocks all calls for 60 seconds), then transitions to HALF_OPEN (allows up to 3 test calls) before resetting. This stops runaway retries automatically, without operator intervention.

---

## Tech Stack

| Tool | Role | Why |
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

## Quickstart

> **Prerequisites:** Docker 24+, Docker Compose v2, and a free [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key). Python 3.11+ is only required if you want to run tests locally.

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

This starts 8 services: PostgreSQL, Redis, Airflow webserver, scheduler, worker, init, Prometheus, Grafana, and Pushgateway.

> Services are ready in ~90 seconds. Watch progress with `docker compose logs -f airflow-init`.

### 4. Open the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Pushgateway | http://localhost:9091 | — |

### 5. Trigger the pipeline

**Via Airflow UI:** enable and trigger `stock_market_etl_v2`

**Standalone (no Airflow needed):**

```bash
# Run from the project root
PUSHGATEWAY_URL=localhost:9091 python -m src.main AAPL
PUSHGATEWAY_URL=localhost:9091 python -m src.main MSFT
PUSHGATEWAY_URL=localhost:9091 python -m src.main GOOG
PUSHGATEWAY_URL=localhost:9091 python -m src.main TSLA
```

> Note: `PUSHGATEWAY_URL=localhost:9091` is required when running locally. Inside Airflow workers the URL defaults correctly to `pushgateway:9091`.

### Makefile shortcuts

```bash
make up              # docker compose up -d
make down            # docker compose down
make test            # run unit tests with coverage
make run SYMBOL=AAPL # run standalone pipeline for one symbol
make lint            # ruff + black check
```

---

## Observability

Pipeline metrics are pushed to Prometheus via Pushgateway after every run and displayed in a **pre-built Grafana dashboard** that is automatically provisioned on first startup — zero manual setup required.

Navigate to `http://localhost:3000` → **Dashboards → Stock ETL Pipeline** → set time range to **Last 1 hour**.

### Metrics Reference

| Metric | Description | Example value |
|---|---|---|
| `etl_rows_loaded` | Rows written to PostgreSQL per symbol | `100` rows/symbol after daily run |
| `etl_api_latency_ms` | Alpha Vantage API response time | `~450 ms` on a typical run |
| `etl_db_latency_ms` | PostgreSQL UPSERT duration | `~120 ms` for 100 rows |
| `etl_duration_s` | Total pipeline wall-clock time | `~4 s` end-to-end |
| `etl_success` | Pipeline health flag | `1` = success, `0` = failure |

All 5 metrics are broken down by `symbol` label (AAPL, MSFT, GOOG, TSLA) and refresh every 30 seconds.

The dashboard is auto-provisioned via `docker/grafana/provisioning/` — datasources and dashboards load from disk on container start. No manual Grafana configuration is ever needed.

> **Engineering note:** `honor_labels: true` is set in `prometheus.yml` under the Pushgateway scrape config. Without this, Prometheus strips the `symbol` label pushed by Pushgateway, breaking all per-symbol Grafana queries. This is a subtle but critical configuration detail.

---

## Database Schema

```sql
-- Dimension table
stocks (id, symbol, created_at)

-- Fact table — NUMERIC(10,4) for financial precision
stock_prices (
    stock_id   INTEGER,
    date       DATE,
    open       NUMERIC(10,4),
    high       NUMERIC(10,4),
    low        NUMERIC(10,4),
    close      NUMERIC(10,4),
    volume     BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (stock_id, date)
)
-- Indexes: (stock_id, date DESC) + partial index on recent dates

-- Audit trail — one row per pipeline run per symbol
data_quality_log (
    symbol, run_id, run_date,
    rows_raw, rows_transformed, rows_validated, rows_loaded, rows_dropped,
    api_latency_ms, db_latency_ms, pipeline_duration_s,
    status, error_message
)
```

An `updated_at` trigger fires on every `UPDATE` to `stock_prices`, ensuring the audit trail is always current.

---

## Circuit Breaker

The Alpha Vantage client implements a three-state circuit breaker to prevent runaway retries against a failing API:

```
CLOSED ──(5 failures)──▶ OPEN ──(60s cooldown)──▶ HALF_OPEN
  ▲                                                     │
  └────────────────(success)───────────────────────────┘
```

- **CLOSED** — normal operation, all calls go through
- **OPEN** — all calls blocked immediately; no API hits for 60 seconds
- **HALF_OPEN** — allows up to 3 test calls; resets to CLOSED on success, reopens on failure

Airflow's own retry mechanism (`retries=2, retry_delay=5min`) handles transient failures at the task level independently.

---

## Testing

### Running the tests

```bash
# Install dependencies
pip install -r docker/requirements.txt

# Unit tests (no external services needed)
pytest tests/unit -v

# With coverage report
pytest tests/unit -v --cov=src --cov-report=term-missing

# All tests including integration (requires a running PostgreSQL)
pytest -v
```

<details>
<summary>▶ Click to see full test output</summary>

```
tests/unit/test_api_client.py::test_successful_fetch                          PASSED
tests/unit/test_api_client.py::test_raises_rate_limit_error                   PASSED
tests/unit/test_api_client.py::test_raises_value_error_on_api_error           PASSED
tests/unit/test_api_client.py::test_raises_value_error_on_missing_time_series PASSED
tests/unit/test_api_client.py::test_circuit_opens_after_threshold             PASSED
tests/unit/test_api_client.py::test_circuit_blocks_calls_when_open            PASSED
tests/unit/test_transformer.py::test_transform_happy_path                     PASSED
tests/unit/test_transformer.py::test_transform_output_is_sorted_by_date       PASSED
tests/unit/test_transformer.py::test_transform_correct_types                  PASSED
tests/unit/test_transformer.py::test_transform_raises_on_empty_time_series    PASSED
tests/unit/test_transformer.py::test_transform_raises_on_missing_key          PASSED
tests/unit/test_transformer.py::test_transform_raises_on_invalid_input        PASSED
tests/unit/test_transformer.py::test_transform_drops_rows_with_non_numeric_prices PASSED
tests/unit/test_validator.py::test_validate_passes_clean_data                 PASSED
tests/unit/test_validator.py::test_validate_drops_high_less_than_low          PASSED
tests/unit/test_validator.py::test_validate_drops_null_rows                   PASSED
tests/unit/test_validator.py::test_validate_raises_on_missing_column          PASSED
tests/unit/test_validator.py::test_validate_drops_zero_price                  PASSED
tests/unit/test_validator.py::test_validate_drops_negative_volume             PASSED
tests/unit/test_validator.py::test_validate_drops_duplicates                  PASSED
tests/unit/test_validator.py::test_validate_output_types                      PASSED
tests/unit/test_config.py::test_valid_config_loads_successfully               PASSED
tests/unit/test_config.py::test_missing_api_key_raises                        PASSED
tests/unit/test_config.py::test_missing_db_host_raises                        PASSED
tests/unit/test_config.py::test_missing_db_password_does_not_raise            PASSED
tests/unit/test_config.py::test_invalid_port_raises                           PASSED
tests/unit/test_config.py::test_non_numeric_port_raises                       PASSED
tests/unit/test_config.py::test_connection_string_format                      PASSED

============================== 28 passed in 1.43s ==============================
```

</details>

### Testing philosophy

**Unit tests vs integration tests** are kept strictly separate. Unit tests mock all external dependencies (API, DB, filesystem) and run in under 2 seconds — they are the CI gate. Integration tests require a live PostgreSQL instance and are guarded by a `pytest.mark.integration` marker so they never block a fast feedback loop.

**The circuit breaker is specifically tested** because it is stateful logic that is easy to get wrong. Tests explicitly verify the state machine transitions: that the circuit opens after the threshold, blocks calls when open, and allows test calls in half-open state.

**DB integration tests clean up after themselves** using a `pg_loader` fixture that deletes all test rows in `finally`. This means tests are fully isolated and the suite is safe to run repeatedly against a shared test database without side effects.

---

## Project Structure

```
.
├── airflow/
│   └── dags/
│       └── stock_etl_dag.py        # DAG definition — 5 tasks × 4 symbols in parallel
├── docker/
│   ├── docker-compose.yml          # 8 services: Airflow, Postgres, Redis, observability stack
│   ├── init_db.sql                 # Schema: stocks, stock_prices, data_quality_log
│   ├── prometheus.yml              # Scrape config with honor_labels for Pushgateway
│   ├── grafana/
│   │   ├── dashboards/stock_etl.json       # Pre-built Stock ETL Pipeline dashboard
│   │   └── provisioning/                   # Auto-loads dashboards + datasource on startup
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
│   ├── unit/                       # Fast, fully mocked — run in < 2s
│   ├── integration/                # Requires live DB — guarded by pytest.mark.integration
│   └── data/sample_av_response.json
├── Makefile
├── CHANGELOG.md
├── CONTRIBUTING.md
├── LICENSE
└── pytest.ini
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

```bash
API_KEY=your_alpha_vantage_key        # https://www.alphavantage.co/support/#api-key
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

`PUSHGATEWAY_URL` is optional — defaults to `localhost:9091` locally and `pushgateway:9091` inside Docker.

---

## Troubleshooting

**Grafana won't start / crashes on provisioning:**
Grafana 12 changed datasource UID resolution during provisioning. Ensure `docker/grafana/provisioning/datasources/prometheus.yml` includes `orgId: 1` and `editable: true`. If it persists, run `docker compose down -v && docker compose up -d` to force a clean provisioning.

**Metrics not appearing in Grafana panels:**
Ensure `docker/prometheus.yml` has `honor_labels: true` under the pushgateway scrape config. Without it, Prometheus strips the `symbol` label and per-symbol queries return no data. After editing, run `docker compose restart prometheus`.

**Standalone pipeline — metrics not reaching Grafana:**
Always prefix with `PUSHGATEWAY_URL=localhost:9091` when running outside Docker. The hostname `pushgateway` only resolves inside the Docker network.

**Airflow DAG not visible:**
Wait for `airflow-init` to complete (`docker compose logs -f airflow-init`), then refresh the UI. The DAG file is mounted from `airflow/dags/` so changes apply without a container restart.

---

## Roadmap

| Item | Status | Notes |
|---|---|---|
| Core ETL pipeline (Extract → Transform → Validate → Stage → Load) | ✅ Done | AAPL, MSFT, GOOG, TSLA |
| Prometheus + Grafana observability | ✅ Done | 5 metrics, auto-provisioned dashboard |
| Circuit breaker on API client | ✅ Done | 3-state, configurable thresholds |
| Audit log per pipeline run | ✅ Done | `data_quality_log` table |
| dbt analytical models | 🔄 In Progress | 30-day moving average, daily returns, volatility on top of `stock_prices` |
| S3 / MinIO staging | 📋 Planned | Replace local Parquet with object storage for durability and horizontal scaling |
| Streaming layer (Kafka → real-time prices) | 📋 Planned | Demonstrates batch-vs-streaming tradeoff awareness |
| Cloud migration (AWS MWAA + S3 + RDS) | 📋 Planned | Production-grade cloud deployment |
| Expanded symbol list | 📋 Planned | Parameterise via Airflow Variables — no code change needed |
| pre-commit hooks | 📋 Planned | `black`, `ruff`, `detect-secrets` |

---

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow (fork → branch → PR → CI must pass).

---

## License

[MIT](LICENSE) — free to use, fork, and build on.
