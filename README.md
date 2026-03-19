# Stock Data ETL Pipeline

## Overview

This project is a production-style ETL pipeline that extracts stock market data from the Alpha Vantage API, transforms and validates it, and loads it into a PostgreSQL database.

The goal of this project is to demonstrate real-world data engineering practices including:

- API data ingestion
- Data transformation using pandas
- Data validation and cleaning
- Database loading with SQLAlchemy
- Logging and error handling

---

## Architecture

The pipeline follows a simple ETL flow:

Extract → Transform → Validate → Load

- Extract: Fetch data from Alpha Vantage API
- Transform: Clean and structure the data
- Validate: Ensure data quality and consistency
- Load: Store data into PostgreSQL

---

## Project Structure

```
src/
├── config/        # Environment configuration
├── extract/       # API data extraction
├── transform/     # Data transformation and validation
├── load/          # Database loading
├── utils/         # Logging utilities
```

---

## Requirements

- Python 3.9+
- PostgreSQL database

---

## Installation

1. Clone the repository:

```
git clone https://github.com/your-username/stock-etl-pipeline.git
cd stock-etl-pipeline
```

2. Create virtual environment:

```
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:

```
pip install -r requirements.txt
```

---

## Environment Variables

Create a `.env` file in the root directory:

```
API_KEY=your_alpha_vantage_api_key

DB_HOST=localhost
DB_PORT=5432
DB_NAME=stock_db
DB_USER=postgres
DB_PASSWORD=postgres
```

---

## How to Run

```
python main.py
```

---

## Features

- Retry mechanism for API calls
- Structured logging using Loguru
- Data validation rules for financial data
- Clean modular architecture
- PostgreSQL integration
- Production-like error handling

---

## Future Improvements

- Add Apache Airflow for orchestration
- Support multiple stock symbols
- Add data visualization layer
- Containerize with Docker
- Add unit and integration tests
- Implement data warehouse (Star schema)
