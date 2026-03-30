import os
from typing import Optional
from dotenv import load_dotenv
from src.utils.logger import logger


class Config:
    """
    Production‑grade configuration manager with type safety and validation.
    """

    def __init__(self) -> None:
        # Only load .env if we're not inside Airflow (to avoid overriding Docker env)
        # and if the file exists.
        if not os.getenv("AIRFLOW_HOME") and os.path.exists(".env"):
            load_dotenv()
            logger.info("Loaded environment from .env file")

        # Fetch raw values
        self._api_key: Optional[str] = os.getenv("API_KEY")
        self._db_host: Optional[str] = os.getenv("DB_HOST")
        self._db_port_raw: Optional[str] = os.getenv("DB_PORT")
        self._db_name: Optional[str] = os.getenv("DB_NAME")
        self._db_user: Optional[str] = os.getenv("DB_USER")
        self._db_password: Optional[str] = os.getenv("DB_PASSWORD")

        # Type conversion and validation (now non‑strict for DAG parsing)
        self.API_KEY = self._validate_non_empty("API_KEY", self._api_key)
        self.DB_HOST = self._validate_non_empty("DB_HOST", self._db_host)
        self.DB_PORT = self._validate_port(self._db_port_raw)
        self.DB_NAME = self._validate_non_empty("DB_NAME", self._db_name)
        self.DB_USER = self._validate_non_empty("DB_USER", self._db_user)
        self.DB_PASSWORD = self._validate_non_empty("DB_PASSWORD", self._db_password)

        # Optional: add format checks (e.g., API key length)
        self._validate_api_key_format()

        logger.info("Configuration validated successfully")

    def _validate_non_empty(self, name: str, value: Optional[str]) -> str:
        """
        Ensure the variable is not None or empty.
        If missing, log a warning and return empty string.
        This allows DAG parsing to succeed even if environment variables are not set.
        """
        if not value:
            logger.warning(f"Environment variable {name} not set, using empty value")
            return ""
        return value

    def _validate_port(self, port_str: Optional[str]) -> int:
        """
        Convert DB_PORT to int and validate range.
        If missing, default to 5432.
        """
        if not port_str or port_str.strip() == "":
            logger.warning("DB_PORT not set, defaulting to 5432")
            return 5432
        try:
            port = int(port_str)
            if port < 1 or port > 65535:
                raise ValueError
            return port
        except ValueError:
            logger.error(f"Invalid DB_PORT: {port_str} (must be an integer between 1 and 65535)")
            raise EnvironmentError(f"Invalid DB_PORT: {port_str} (must be an integer between 1 and 65535)")

    def _validate_api_key_format(self) -> None:
        """
        Basic sanity check for API key (e.g., length).
        Skip if API_KEY is empty.
        """
        if self.API_KEY and len(self.API_KEY) < 10:
            logger.warning(f"API_KEY seems too short: {self.API_KEY} (length {len(self.API_KEY)})")

    def get_db_connection_string(self) -> str:
        """
        Generate PostgreSQL connection string.
        """
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )