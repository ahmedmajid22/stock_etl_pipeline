import os
from typing import Optional
from dotenv import load_dotenv
from src.utils.logger import logger

# Variables that must be set — pipeline cannot run without them
_REQUIRED_VARS = {"API_KEY", "DB_HOST", "DB_NAME", "DB_USER"}

# Variables that are optional (e.g. passwordless local postgres is valid)
_OPTIONAL_VARS = {"DB_PASSWORD"}


class Config:
    def __init__(self) -> None:
        if not os.getenv("AIRFLOW_HOME") and os.path.exists(".env"):
            load_dotenv()
            logger.info("Loaded environment from .env file")

        self._api_key: Optional[str] = os.getenv("API_KEY")
        self._db_host: Optional[str] = os.getenv("DB_HOST")
        self._db_port_raw: Optional[str] = os.getenv("DB_PORT")
        self._db_name: Optional[str] = os.getenv("DB_NAME")
        self._db_user: Optional[str] = os.getenv("DB_USER")
        self._db_password: Optional[str] = os.getenv("DB_PASSWORD")

        # Required — raises EnvironmentError immediately if missing
        self.API_KEY = self._require("API_KEY", self._api_key)
        self.DB_HOST = self._require("DB_HOST", self._db_host)
        self.DB_NAME = self._require("DB_NAME", self._db_name)
        self.DB_USER = self._require("DB_USER", self._db_user)

        # Optional — logs a warning, does not raise
        self.DB_PASSWORD = self._warn_if_missing("DB_PASSWORD", self._db_password)

        self.DB_PORT = self._validate_port(self._db_port_raw)
        self._validate_api_key_format()
        logger.info("Configuration validated successfully")

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
            logger.warning(f"Optional environment variable '{name}' is not set.")
            return ""
        return value

    def _validate_port(self, port_str: Optional[str]) -> int:
        if not port_str or port_str.strip() == "":
            logger.warning("DB_PORT not set, defaulting to 5432")
            return 5432
        try:
            port = int(port_str)
            if port < 1 or port > 65535:
                raise ValueError
            return port
        except ValueError:
            raise EnvironmentError(
                f"Invalid DB_PORT: '{port_str}' — must be an integer between 1 and 65535"
            )

    def _validate_api_key_format(self) -> None:
        if len(self.API_KEY) < 10:
            logger.warning(
                f"API_KEY seems too short (length {len(self.API_KEY)}) — is it correct?"
            )

    def get_db_connection_string(self) -> str:
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
