import os
from typing import Optional

from dotenv import load_dotenv

from src.utils.logger import logger

# FIX: _REQUIRED_VARS and _OPTIONAL_VARS were defined but never referenced anywhere
# in the class — dead code. Removed to avoid misleading readers into thinking the
# validation logic iterates over these sets (it doesn't; each field is validated
# explicitly). If you want to drive validation from these sets in the future,
# that refactor is straightforward.


class Config:
    """
    Validated application configuration loaded from environment variables.

    Required variables (raises EnvironmentError if missing or empty):
        API_KEY, DB_HOST, DB_NAME, DB_USER

    Optional variables (logs a warning if missing, falls back to empty string / default):
        DB_PASSWORD  — passwordless local Postgres is a valid setup
        DB_PORT      — defaults to 5432

    Usage:
        cfg = Config()
        conn = cfg.get_db_connection_string()
    """

    def __init__(self) -> None:
        # Only load .env when running outside Airflow (Airflow injects env vars directly)
        if not os.getenv("AIRFLOW_HOME") and os.path.exists(".env"):
            load_dotenv()
            logger.info("Loaded environment from .env file")

        self._api_key: Optional[str] = os.getenv("API_KEY")
        self._db_host: Optional[str] = os.getenv("DB_HOST")
        self._db_port_raw: Optional[str] = os.getenv("DB_PORT")
        self._db_name: Optional[str] = os.getenv("DB_NAME")
        self._db_user: Optional[str] = os.getenv("DB_USER")
        self._db_password: Optional[str] = os.getenv("DB_PASSWORD")

        # Required — raises EnvironmentError immediately if missing or blank
        self.API_KEY: str = self._require("API_KEY", self._api_key)
        self.DB_HOST: str = self._require("DB_HOST", self._db_host)
        self.DB_NAME: str = self._require("DB_NAME", self._db_name)
        self.DB_USER: str = self._require("DB_USER", self._db_user)

        # Optional — logs a warning, does not raise
        self.DB_PASSWORD: str = self._warn_if_missing("DB_PASSWORD", self._db_password)

        self.DB_PORT: int = self._validate_port(self._db_port_raw)
        self._validate_api_key_format()

        logger.info("Configuration validated successfully")

    # ── Private helpers ────────────────────────────────────────────────────

    def _require(self, name: str, value: Optional[str]) -> str:
        """Raise immediately if a required environment variable is missing or empty."""
        if not value or not value.strip():
            raise EnvironmentError(
                f"Required environment variable '{name}' is not set. "
                f"Check your .env file or container environment."
            )
        return value.strip()

    def _warn_if_missing(self, name: str, value: Optional[str]) -> str:
        """Log a warning for optional variables, but do not raise."""
        if not value:
            logger.warning(
                f"Optional environment variable '{name}' is not set. "
                f"This is fine for passwordless local Postgres, but verify in production."
            )
            return ""
        return value

    def _validate_port(self, port_str: Optional[str]) -> int:
        """Parse and range-check DB_PORT; default to 5432 if not set."""
        if not port_str or port_str.strip() == "":
            logger.warning("DB_PORT not set, defaulting to 5432")
            return 5432
        try:
            port = int(port_str.strip())
            if port < 1 or port > 65535:
                raise ValueError(f"Port {port} out of range 1–65535")
            return port
        except ValueError:
            raise EnvironmentError(
                f"Invalid DB_PORT: '{port_str}' — must be an integer between 1 and 65535"
            )

    def _validate_api_key_format(self) -> None:
        """Warn if the API key looks suspiciously short (likely a placeholder)."""
        if len(self.API_KEY) < 10:
            logger.warning(
                f"API_KEY is only {len(self.API_KEY)} characters — "
                f"is it correct? Alpha Vantage keys are typically 16 characters."
            )

    # ── Public API ─────────────────────────────────────────────────────────

    def get_db_connection_string(self) -> str:
        """Return a SQLAlchemy-compatible PostgreSQL connection string."""
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    def __repr__(self) -> str:
        """Safe repr — never exposes API_KEY or DB_PASSWORD."""
        return (
            f"Config(host={self.DB_HOST}, port={self.DB_PORT}, "
            f"db={self.DB_NAME}, user={self.DB_USER})"
        )
