from unittest.mock import MagicMock, patch

import pytest

from src.extract.api_client import AlphaVantageClient, CircuitState, RateLimitError


@pytest.fixture
def client():
    return AlphaVantageClient(api_key="test_key_1234567890")


def _mock_response(json_data):
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


def test_successful_fetch(client, sample_av_response):
    with patch.object(
        client.session, "get", return_value=_mock_response(sample_av_response)
    ):
        result = client.get_daily_stock_data("AAPL")
    assert "Time Series (Daily)" in result
    assert client.state == CircuitState.CLOSED


def test_raises_rate_limit_error(client, rate_limit_response):
    with patch.object(
        client.session, "get", return_value=_mock_response(rate_limit_response)
    ):
        with pytest.raises(RateLimitError):
            client.get_daily_stock_data("AAPL")


def test_raises_value_error_on_api_error(client, error_response):
    with patch.object(
        client.session, "get", return_value=_mock_response(error_response)
    ):
        # Updated to match actual error message from api_client.py
        with pytest.raises(ValueError, match="API returned error"):
            client.get_daily_stock_data("AAPL")


def test_raises_value_error_on_missing_time_series(client):
    with patch.object(
        client.session, "get", return_value=_mock_response({"unexpected": "data"})
    ):
        # Updated to match actual error message from api_client.py
        with pytest.raises(ValueError, match="Unexpected API response structure"):
            client.get_daily_stock_data("AAPL")


def test_circuit_opens_after_threshold(client, rate_limit_response):
    client.failure_threshold = 2
    for _ in range(3):
        try:
            with patch.object(
                client.session, "get", return_value=_mock_response(rate_limit_response)
            ):
                client.get_daily_stock_data("AAPL")
        except (RateLimitError, RuntimeError):
            pass
    assert client.state == CircuitState.OPEN


def test_circuit_blocks_calls_when_open(client):
    client.state = CircuitState.OPEN
    client.last_failure_time = __import__("time").time()

    with pytest.raises(RuntimeError, match="Circuit breaker OPEN"):
        client.get_daily_stock_data("AAPL")
