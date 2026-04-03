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
    Production-grade client with circuit breaker and non-blocking retries.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(
        self,
        api_key: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_calls: int = 3,
        max_retries: int = 3,
    ):
        self.api_key = api_key
        self.session = requests.Session()

        # Circuit breaker parameters
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        # How many attempts per call before giving up and letting Airflow retry
        self.max_retries = max_retries

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
        Fetch daily stock data with circuit breaker and non-blocking retries.

        Attempts up to self.max_retries times for transient errors (timeouts,
        request exceptions). Rate limit errors and API errors fail immediately
        on the final attempt so Airflow can handle task-level retries.

        Raises:
            RuntimeError: Circuit breaker is OPEN, or max retries exceeded.
            RateLimitError: Alpha Vantage rate limit hit (let Airflow retry).
            ValueError: Invalid API response structure or API-level error.
        """
        if not self._check_circuit():
            raise RuntimeError(f"Circuit breaker OPEN for {symbol}")

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "compact",
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    f"Fetching daily stock data for {symbol}, attempt {attempt}/{self.max_retries}"
                )

                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=10,
                )
                response.raise_for_status()

                data = response.json()

                # Handle API-level error messages
                if "Error Message" in data:
                    raise ValueError(f"API returned error: {data['Error Message']}")

                # Rate limiting — raise immediately; Airflow handles task-level retry
                if "Note" in data or "Information" in data:
                    logger.warning(f"Rate limit hit for {symbol} on attempt {attempt}")
                    self._record_failure()
                    raise RateLimitError(
                        f"Alpha Vantage rate limit exceeded for {symbol}"
                    )

                # Validate expected structure
                if "Time Series (Daily)" not in data:
                    raise ValueError(
                        f"Unexpected API response structure for {symbol}: {data}"
                    )

                # Success — reset circuit breaker
                self._record_success()
                logger.info(f"Successfully fetched data for {symbol}")
                return data

            except (RateLimitError, ValueError):
                # Non-transient — do not retry in this loop; re-raise immediately
                raise

            except requests.Timeout as e:
                logger.warning(f"Timeout on attempt {attempt}/{self.max_retries}: {e}")
                if attempt == self.max_retries:
                    self._record_failure()
                    raise RuntimeError(
                        f"Max retries ({self.max_retries}) exceeded for {symbol} due to timeouts"
                    ) from e
                continue

            except requests.RequestException as e:
                logger.warning(
                    f"Request error on attempt {attempt}/{self.max_retries}: {e}"
                )
                if attempt == self.max_retries:
                    self._record_failure()
                    raise
                continue

        # Safety net — should never be reached
        self._record_failure()
        raise RuntimeError(f"Max retries ({self.max_retries}) exceeded for {symbol}")
