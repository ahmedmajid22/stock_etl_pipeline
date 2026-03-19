# src/config/config.py

from dotenv import load_dotenv
import os

class Config:
    """
    Configuration class for the Stock ETL Pipeline.

    Loads environment variables from the .env file and provides
    database connection string for PostgreSQL.
    """

    def __init__(self) -> None:
        """Initialize configuration by loading environment variables."""
        load_dotenv()

        self.API_KEY: str = os.getenv("API_KEY")
        self.DB_HOST: str = os.getenv("DB_HOST")
        self.DB_PORT: str = os.getenv("DB_PORT")
        self.DB_NAME: str = os.getenv("DB_NAME")
        self.DB_USER: str = os.getenv("DB_USER")
        self.DB_PASSWORD: str = os.getenv("DB_PASSWORD")

        self._validate_env()

    def _validate_env(self) -> None:
        """Validate that all required environment variables are set."""
        missing_vars = []
        for var_name, value in {
            "API_KEY": self.API_KEY,
            "DB_HOST": self.DB_HOST,
            "DB_PORT": self.DB_PORT,
            "DB_NAME": self.DB_NAME,
            "DB_USER": self.DB_USER,
            "DB_PASSWORD": self.DB_PASSWORD
        }.items():
            if not value:
                missing_vars.append(var_name)

        if missing_vars:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

    def get_db_connection_string(self) -> str:
        """
        Generate PostgreSQL connection string.

        Returns:
            str: PostgreSQL connection string in the format:
                 postgresql://user:password@host:port/dbname
        """
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"