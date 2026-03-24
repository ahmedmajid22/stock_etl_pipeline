# 📈 Stock Market ETL Pipeline (Production-Ready)

## 🚀 Overview

This project is a **production-style ETL pipeline** that extracts stock market data from the Alpha Vantage API, processes it, and loads it into a PostgreSQL database using **Apache Airflow for orchestration** and **Docker for containerization**.

The project demonstrates real-world **Data Engineering practices** including pipeline orchestration, modular architecture, data validation, and containerized deployment.

---

## 🏗️ Architecture

The pipeline follows a modern ETL workflow:

**Extract** → **Transform** → **Validate** → **Load** → **Orchestrate**

### 🔹 Flow Description

* **Extract**: Fetches daily stock data from Alpha Vantage API.
* **Transform**: Cleans and structures raw JSON into a pandas DataFrame.
* **Validate**: Applies data quality checks (nulls, types, financial logic).
* **Load**: Stores clean data into PostgreSQL.
* **Orchestrate**: Managed and scheduled using Apache Airflow.

---

## ⚙️ Tech Stack

* **Python** (Core Logic)
* **Pandas** (Data Transformation)
* **PostgreSQL** (Data Storage)
* **SQLAlchemy** (ORM/Database Connection)
* **Apache Airflow** (Orchestration)
* **Docker & Docker Compose** (Containerization)
* **Loguru** (Advanced Logging)

---

## 📁 Project Structure

```text
airflow/
├── dags/                 # Airflow DAG definitions
├── logs/                 # Airflow logs
├── plugins/              # Custom Airflow plugins
docker/
├── docker-compose.yml    # Multi-container setup
├── Dockerfile            # Custom Airflow image
├── init_db.sql           # Database initialization script
├── requirements.txt      # Python dependencies
src/
├── config/               # Environment configuration
├── extract/              # API client logic
├── transform/            # Transformation & validation logic
├── load/                 # Database loading logic
├── utils/                # Logging and helper utilities
├── main.py               # ETL pipeline entry point
tests/                    # Unit and integration tests
.env                      # Environment variables (API keys, DB creds)