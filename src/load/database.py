# src/load/database.py

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from src.utils.logger import logger
from typing import Optional

class DatabaseLoader:
    """
    Production-grade loader for inserting pandas DataFrames into PostgreSQL.
    """

    def __init__(self, db_connection_string: str) -> None:
        """
        Initialize the DatabaseLoader.

        Args:
            db_connection_string (str): SQLAlchemy-compatible PostgreSQL connection string
        """
        self.db_connection_string = db_connection_string
        try:
            self.engine = create_engine(self.db_connection_string)
            logger.info("Database engine created successfully.")
        except SQLAlchemyError as e:
            logger.exception(f"Failed to create database engine: {e}")
            raise

    def load_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> None:
        """
        Load a pandas DataFrame into a PostgreSQL table.

        Args:
            df (pd.DataFrame): DataFrame to insert
            table_name (str): Target table name in PostgreSQL
            if_exists (str): What to do if table exists ('append', 'replace', 'fail')

        Raises:
            SQLAlchemyError: If insert fails
        """
        try:
            logger.info(f"Loading {len(df)} records into table '{table_name}'...")
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method="multi"  # batch insert for performance
            )
            logger.info(f"Successfully loaded {len(df)} records into '{table_name}'.")
        except SQLAlchemyError as e:
            logger.exception(f"Failed to load DataFrame into '{table_name}': {e}")
            raise