import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
UTC = timezone.utc
import uuid
from typing import Optional, Union
import glob
import json
import os

# ─────────────────────────────────────────────
# Environment-aware base path (Docker vs local)
# ─────────────────────────────────────────────

if os.path.exists("/opt/airflow"):
    _BASE = Path("/opt/airflow/data")
else:
    _BASE = Path(__file__).resolve().parents[2] / "data"

STAGING_ROOT = _BASE / "staging"
RAW_STAGING_ROOT = _BASE / "raw"


# ─────────────────────────────────────────────
# Raw JSON staging (NEW)
# ─────────────────────────────────────────────

def write_raw_stage(raw_data: dict, symbol: str, run_id: str) -> Path:
    """
    Save raw API JSON response to disk.

    Structure:
    data/raw/symbol=AAPL/<timestamp>_<run_id>.json
    """
    partition_path = RAW_STAGING_ROOT / f"symbol={symbol}"
    partition_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    file_path = partition_path / f"{timestamp}_{run_id[:8]}.json"

    with open(file_path, "w") as f:
        json.dump(raw_data, f)

    return file_path


def read_raw_stage(path: Union[str, Path]) -> dict:
    """
    Read a raw JSON staging file by path.
    """
    with open(path, "r") as f:
        return json.load(f)


# ─────────────────────────────────────────────
# Parquet staging (existing logic)
# ─────────────────────────────────────────────

def write_stage(df: pd.DataFrame, symbol: str, run_id: str) -> Path:
    """
    Save dataframe to a parquet file in staging with partitioned structure.
    
    Structure: data/staging/symbol=AAPL/year=2025/month=11/day=07/uuid.parquet
    """
    if not df.empty and 'date' in df.columns:
        latest_date = df['date'].max()
        if hasattr(latest_date, 'to_pydatetime'):
            latest_date = latest_date.to_pydatetime()
    else:
        latest_date = datetime.now(UTC)

    year = latest_date.year
    month = latest_date.month
    day = latest_date.day

    partition_path = (
        STAGING_ROOT
        / f"symbol={symbol}"
        / f"year={year}"
        / f"month={month:02d}"
        / f"day={day:02d}"
    )
    partition_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{uuid.uuid4().hex[:8]}.parquet"
    file_path = partition_path / filename

    df.to_parquet(file_path, index=False)

    return file_path


def read_stage(symbol: str, run_id: str) -> pd.DataFrame:
    search_pattern = STAGING_ROOT / f"symbol={symbol}" / "**" / f"*{run_id}*.parquet"
    files = list(glob.glob(str(search_pattern), recursive=True))

    if files:
        latest_file = max(files, key=lambda x: Path(x).stat().st_ctime)
        return pd.read_parquet(latest_file)

    old_path = STAGING_ROOT / symbol / f"{run_id}.parquet"
    if old_path.exists():
        return pd.read_parquet(old_path)

    raise FileNotFoundError(f"No staging file found for symbol={symbol}, run_id={run_id}")


def read_latest_stage(symbol: str) -> pd.DataFrame:
    search_pattern = STAGING_ROOT / f"symbol={symbol}" / "**" / "*.parquet"
    files = list(glob.glob(str(search_pattern), recursive=True))

    if not files:
        old_pattern = STAGING_ROOT / symbol / "*.parquet"
        files = list(glob.glob(str(old_pattern), recursive=True))

    if not files:
        raise FileNotFoundError(f"No staging files found for symbol={symbol}")

    latest_file = max(files, key=lambda x: Path(x).stat().st_ctime)
    return pd.read_parquet(latest_file)


def read_stage_by_date(symbol: str, year: int, month: int, day: int) -> pd.DataFrame:
    partition_path = (
        STAGING_ROOT
        / f"symbol={symbol}"
        / f"year={year}"
        / f"month={month:02d}"
        / f"day={day:02d}"
    )

    if not partition_path.exists():
        raise FileNotFoundError(
            f"No staging data found for {symbol} on {year}-{month:02d}-{day:02d}"
        )

    all_files = list(partition_path.glob("*.parquet"))
    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {partition_path}")

    dfs = [pd.read_parquet(file) for file in all_files]

    return pd.concat(dfs, ignore_index=True)