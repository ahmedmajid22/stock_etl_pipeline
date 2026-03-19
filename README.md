# 📈 Stock ETL Pipeline: Production-Grade Financial Data Engineering

[![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/database-PostgreSQL-336791.svg)](https://www.postgresql.org/)
[![Pandas](https://img.shields.io/badge/library-pandas-150458.svg)](https://pandas.pydata.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📖 Overview
This repository contains a **production-ready ETL (Extract, Transform, Load) pipeline** designed to ingest, process, and store financial market data. Built with Python, it automates the flow of data from the **Alpha Vantage API** into a **PostgreSQL** data warehouse, ensuring high data integrity through rigorous validation layers.

### 💡 Why This Project Matters
In the financial sector, data accuracy is non-negotiable. This pipeline isn't just a script; it's a modular system implementing **Senior Data Engineering** principles:
* **Observability:** Integrated logging via `loguru`.
* **Security:** Decoupled credentials using environment variables.
* **Scalability:** Modular architecture designed to support future Airflow orchestration.
* **Resilience:** Built-in error handling for API and Database connectivity.

---

## 🏗 System Architecture



1.  **Extraction:** Securely fetches time-series JSON data from Alpha Vantage.
2.  **Transformation:** Normalizes nested JSON into flattened, typed DataFrames.
3.  **Validation:** Executes data quality checks (Null handling, Type enforcement).
4.  **Loading:** Performs optimized batch inserts into a PostgreSQL relational schema.

---

## 🛠 Tech Stack
* **Language:** Python 3.13+
* **Processing:** `pandas` (Vectorized transformations)
* **Database:** `PostgreSQL` + `SQLAlchemy` (ORM & Connection pooling)
* **Logging:** `loguru` (Structured, rotation-capable logs)
* **Config:** `python-dotenv` (Twelve-Factor App methodology)

---

## 📂 Project Structure
```text
stock_etl_pipeline/
├── src/
│   ├── extract/      # API Client & Request logic
│   ├── transform/    # Business logic & Data cleaning
│   ├── load/         # PostgreSQL Loading modules
│   ├── config/       # Environment & Global settings
│   └── utils/        # Shared Logger & Helpers
├── airflow/          # DAG definitions (Future scaling)
├── docker/           # Containerization setup
├── logs/             # Persistent execution history
├── main.py           # Orchestration entry point
└── requirements.txt  # Project dependencies
```

-----

## 🚀 Getting Started

### 1\. Environment Setup

```bash
# Clone the repository
git clone [https://github.com/ahmedmajid22/stock_etl_pipeline.git](https://github.com/ahmedmajid22/stock_etl_pipeline.git)
cd stock_etl_pipeline

# Setup Virtual Environment
python3 -m venv venv
source venv/bin/activate

# Install Dependencies
pip install -r requirements.txt
```

### 2\. Configuration

Create a `.env` file in the root directory:

```env
ALPHA_VANTAGE_API_KEY=your_key_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stock_db
DB_USER=postgres
DB_PASSWORD=your_password
```

### 3\. Initialize Database

```sql
-- Run in your PostgreSQL terminal
CREATE DATABASE stock_db;
```

### 4\. Execute Pipeline

```bash
python main.py
```

-----

## 📊 Pipeline Output

### Database Preview (`stock_prices` table):

| date       | symbol | open   | high   | low    | close  | volume   |
| :--------- | :----- | :----- | :----- | :----- | :----- | :------- |
| 2026-03-19 | AAPL   | 170.12 | 172.00 | 169.50 | 171.45 | 48230000 |
| 2026-03-18 | AAPL   | 168.50 | 170.30 | 167.90 | 169.95 | 39560000 |

### Logging Output:

```text
2026-03-19 | INFO | Fetching data for AAPL...
2026-03-19 | INFO | Transformation complete: 100 records processed.
2026-03-19 | INFO | Successfully loaded 100 records into 'stock_prices'.
```

-----

## 🛤 Roadmap & Future Enhancements

  - [ ] **Incremental Loading:** Implement high-watermark tracking to fetch only new records.
  - [ ] **Dockerization:** Wrap the entire environment in a Docker Compose file.
  - [ ] **Data Quality Suite:** Integrate `Great Expectations` for advanced schema validation.
  - [ ] **CI/CD:** GitHub Actions to automate unit tests on every push.

-----

**Maintained by [ahmedmajid22](https://www.google.com/search?q=https://github.com/ahmedmajid22)**