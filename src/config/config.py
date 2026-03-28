from dotenv import load_dotenv
import os


class Config:
    """
    Configuration class for the Stock ETL Pipeline.
    """

    def __init__(self) -> None:
        """
        Initialize configuration by loading environment variables.
        Load .env if it exists in the current directory.
        """

        # Load .env if present (works both locally and in Docker if file is mounted)
        if os.path.exists(".env"):
            load_dotenv()

        self.API_KEY: str = os.getenv("API_KEY")
        self.DB_HOST: str = os.getenv("DB_HOST")
        self.DB_PORT: str = os.getenv("DB_PORT")
        self.DB_NAME: str = os.getenv("DB_NAME")
        self.DB_USER: str = os.getenv("DB_USER")
        self.DB_PASSWORD: str = os.getenv("DB_PASSWORD")

        self._validate_env()

    def _validate_env(self) -> None:
        """Validate required environment variables."""
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
        """
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )