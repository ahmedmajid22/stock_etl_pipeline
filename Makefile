.PHONY: up down test lint run format install-hooks

up:
	cd docker && docker compose up -d

down:
	cd docker && docker compose down

# FIX: was `pytest tests/unit` which is correct locally, but ci.yml was running
# `pytest src/tests/` (wrong path). Both now agree on `tests/unit`.
test:
	pytest tests/unit -v --cov=src --cov-report=term-missing

# Run integration tests separately (requires a live PostgreSQL instance)
test-integration:
	pytest tests/integration -v -m integration

# Run the full test suite (unit + integration)
test-all:
	pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	ruff check src/ tests/ airflow/dags/ && black --check src/ tests/ airflow/dags/

format:
	black src/ tests/ airflow/dags/ && ruff check --fix src/ tests/ airflow/dags/

# FIX: SYMBOL has a default so `make run` doesn't silently fail with a confusing error
run:
	PUSHGATEWAY_URL=localhost:9091 python -m src.main $(or $(SYMBOL),AAPL)

install-hooks:
	pip install pre-commit detect-secrets
	# FIX: `detect-secrets scan > .secrets.baseline` truncates and overwrites the
	# baseline — losing any existing allowlist comments (# pragma: allowlist secret).
	# The correct workflow:
	#   - First install: create the baseline from scratch
	#   - Subsequent updates: use `--update` to preserve allowlist comments
	@if [ -f .secrets.baseline ]; then \
		echo "Updating existing baseline (preserving allowlist comments)..."; \
		detect-secrets scan --update .secrets.baseline; \
	else \
		echo "Creating new baseline..."; \
		detect-secrets scan > .secrets.baseline; \
	fi
	pre-commit install
	@echo "Pre-commit hooks installed. Run 'make lint' to verify."
