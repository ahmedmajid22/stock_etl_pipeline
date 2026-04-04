FROM apache/airflow:2.8.1-python3.11

# ── system deps ──────────────────────────────────────────────────────────────
# psycopg2-binary ships its own libpq, so we do NOT need libpq-dev or gcc.
# The only reason to stay root is to write the constraints file, which we do
# in one layer then immediately drop back to the airflow user.
USER root

# Keep the image lean: install nothing — psycopg2-binary, pyarrow, etc. are
# all pure-wheel or ship their own native libs.  If you ever switch to
# psycopg2 (non-binary) you will need libpq-dev and gcc here.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ── Python deps ───────────────────────────────────────────────────────────────
# Pin against Airflow's own constraints file so transitive deps are
# reproducible.  The URL must match the Airflow version exactly.
ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt /requirements.txt

# Split into two layers:
#  1. production deps (cached aggressively)
#  2. nothing else — test deps belong only in CI
RUN pip install --no-cache-dir \
      --constraint "${CONSTRAINTS_URL}" \
      -r /requirements.txt
