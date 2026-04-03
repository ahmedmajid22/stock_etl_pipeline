.PHONY: up down test lint run format install-hooks

up:
	cd docker && docker compose up -d

down:
	cd docker && docker compose down

test:
	pytest tests/unit -v --cov=src --cov-report=term-missing

lint:
	ruff check src/ tests/ && black --check src/ tests/

format:
	black src/ tests/ && ruff check --fix src/ tests/

run:
	PUSHGATEWAY_URL=localhost:9091 python -m src.main $(SYMBOL)

install-hooks:
	pip install pre-commit detect-secrets
	detect-secrets scan > .secrets.baseline
	pre-commit install
