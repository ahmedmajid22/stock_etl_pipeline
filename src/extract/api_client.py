import requests
import time
from typing import Dict, Optional
from enum import Enum

from src.utils.logger import logger


class CircuitState(Enum):
    CLOSED = "closed"  # normal operation
    OPEN = "open"  # failing, no calls allowed
    HALF_OPEN = "half_open"  # testing after cooldown


class RateLimitError(Exception):
    """Raised when API rate limit is hit."""

    pass


class AlphaVantageClient:
    """
    Production-grade client with circuit breaker and non‑blocking retries.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(
        self,
        api_key: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_calls: int = 3,
    ):
        self.api_key = api_key
        self.session = requests.Session()

        # Circuit breaker parameters
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0

    def _check_circuit(self) -> bool:
        """Returns True if call is allowed."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            # Check if recovery timeout has elapsed
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                logger.info("Circuit breaker entering HALF_OPEN state")
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                return True
            logger.debug("Circuit breaker OPEN, call blocked")
            return False

        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls < self.half_open_max_calls:
                self.half_open_calls += 1
                return True
            logger.debug("Circuit breaker HALF_OPEN, call limit reached")
            return False

        return False

    def _record_success(self):
        """Reset circuit after successful call."""
        if self.state != CircuitState.CLOSED:
            logger.info("Circuit breaker reset to CLOSED after successful call")
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.half_open_calls = 0

    def _record_failure(self):
        """Increment failure count and possibly open circuit."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if (
            self.state == CircuitState.CLOSED
            and self.failure_count >= self.failure_threshold
        ):
            logger.warning(f"Circuit breaker OPEN after {self.failure_count} failures")
            self.state = CircuitState.OPEN
        elif self.state == CircuitState.HALF_OPEN:
            logger.warning("Circuit breaker HALF_OPEN call failed, returning to OPEN")
            self.state = CircuitState.OPEN

    def get_daily_stock_data(self, symbol: str) -> Dict:
        """
        Fetch daily stock data with circuit breaker and non‑blocking retries.
        Raises appropriate exceptions (RateLimitError, RuntimeError, ValueError)
        to let Airflow handle retries.
        """
        # Circuit breaker check
        if not self._check_circuit():
            raise RuntimeError(f"Circuit breaker OPEN for {symbol}")

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "compact",
        }

        max_retries = 1

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"Fetching daily stock data for {symbol}, attempt {attempt}"
                )

                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=10,
                )
                response.raise_for_status()

                data = response.json()

                # Handle API errors
                if "Error Message" in data:
                    raise ValueError(f"API returned error: {data['Error Message']}")

                # Rate limiting – raise a special exception so Airflow can retry
                if "Note" in data or "Information" in data:
                    logger.warning(f"Rate limit hit for {symbol}")
                    raise RateLimitError(
                        f"Alpha Vantage rate limit exceeded for {symbol}"
                    )

                # Validate expected structure
                if "Time Series (Daily)" not in data:
                    raise ValueError(
                        f"Unexpected API response structure for {symbol}: {data}"
                    )

                # Success
                self._record_success()
                logger.info(f"Successfully fetched data for {symbol}")
                return data

            except RateLimitError as e:
                # Rate limit is transient – let caller retry
                logger.warning(f"Rate limit error (attempt {attempt}): {e}")
                if attempt == max_retries:
                    self._record_failure()
                    raise  # re‑raise for Airflow
                # No sleep here – Airflow will retry after its delay
                # We just break out to the next attempt
                continue

            except requests.Timeout as e:
                # Transient network timeout
                logger.warning(f"Timeout error (attempt {attempt}): {e}")
                if attempt == max_retries:
                    self._record_failure()
                    raise RuntimeError(
                        f"Max retries exceeded for {symbol} due to timeouts"
                    )
                continue

            except (requests.RequestException, ValueError) as e:
                # Non‑transient errors (bad request, invalid data)
                logger.warning(f"Request error (attempt {attempt}): {e}")
                if attempt == max_retries:
                    self._record_failure()
                    raise
                continue

        # Should never reach here, but safeguard
        self._record_failure()
        raise RuntimeError(f"Max retries exceeded for {symbol}")
