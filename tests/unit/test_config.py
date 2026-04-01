import pytest
import os
from unittest.mock import patch


class TestConfig:

    def _make_env(self, overrides=None):
        base = {
            "API_KEY": "valid_api_key_1234",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_NAME": "stock_db",
            "DB_USER": "postgres",
            "DB_PASSWORD": "secret",
        }
        if overrides:
            base.update(overrides)
        return base

    def test_valid_config_loads_successfully(self):
        with patch.dict(os.environ, self._make_env(), clear=True):
            from src.config.config import Config
            cfg = Config()
            assert cfg.API_KEY == "valid_api_key_1234"
            assert cfg.DB_PORT == 5432

    def test_missing_api_key_raises(self):
        env = self._make_env({"API_KEY": ""})
        with patch.dict(os.environ, env, clear=True):
            from src.config.config import Config
            with pytest.raises(EnvironmentError, match="API_KEY"):
                Config()

    def test_missing_db_host_raises(self):
        env = self._make_env({"DB_HOST": ""})
        with patch.dict(os.environ, env, clear=True):
            from src.config.config import Config
            with pytest.raises(EnvironmentError, match="DB_HOST"):
                Config()

    def test_missing_db_password_does_not_raise(self):
        """DB_PASSWORD is optional — passwordless local Postgres is valid."""
        env = self._make_env({"DB_PASSWORD": ""})
        with patch.dict(os.environ, env, clear=True):
            from src.config.config import Config
            cfg = Config()  # must not raise
            assert cfg.DB_PASSWORD == ""

    def test_invalid_port_raises(self):
        env = self._make_env({"DB_PORT": "99999"})
        with patch.dict(os.environ, env, clear=True):
            from src.config.config import Config
            with pytest.raises(EnvironmentError, match="DB_PORT"):
                Config()

    def test_non_numeric_port_raises(self):
        env = self._make_env({"DB_PORT": "abc"})
        with patch.dict(os.environ, env, clear=True):
            from src.config.config import Config
            with pytest.raises(EnvironmentError, match="DB_PORT"):
                Config()

    def test_connection_string_format(self):
        with patch.dict(os.environ, self._make_env(), clear=True):
            from src.config.config import Config
            cfg = Config()
            conn = cfg.get_db_connection_string()
            assert conn == "postgresql://postgres:secret@localhost:5432/stock_db"