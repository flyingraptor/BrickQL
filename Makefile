# brickQL – development task runner
# Usage: make <target>   (assumes .venv is active or use `make venv` first)

VENV     := .venv
PYTHON   := $(VENV)/bin/python
PIP      := $(VENV)/bin/pip
PYTEST   := $(VENV)/bin/pytest
RUFF     := $(VENV)/bin/ruff
MYPY     := $(VENV)/bin/mypy

.PHONY: help venv install lint fmt typecheck test test-unit test-integration clean docker-postgres-clean

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

venv:  ## Create the virtual environment
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip

install: venv  ## Install the package + all dev dependencies
	$(PIP) install -e ".[dev]"

lint:  ## Run ruff linter
	$(RUFF) check brickql/ tests/

fmt:  ## Auto-format with ruff
	$(RUFF) format brickql/ tests/
	$(RUFF) check --fix brickql/ tests/

typecheck:  ## Run mypy static type checker
	$(MYPY) brickql/

test-unit:  ## Run unit tests (no DB required)
	$(PYTEST) tests/ -m "not integration and not postgres" -v

test-integration-sqlite:  ## Run SQLite integration tests (in-memory, no Docker)
	$(PYTEST) tests/integration/test_sqlite.py -v -m integration

docker-postgres-clean:  ## Remove Postgres container and volume (use if docker-compose fails with 'ContainerConfig')
	-docker-compose rm -sf postgres 2>/dev/null || true
	-docker rm -f bricksql_postgres 2>/dev/null || true
	-docker volume rm bricksql_pgdata 2>/dev/null || true
	@echo "Postgres container and volume removed. Run 'make test-integration-postgres' to start fresh."

test-integration-postgres:  ## Run PostgreSQL integration tests (requires Docker Compose)
	@echo "Starting Postgres via Docker Compose..."
	docker-compose up -d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 5
	brickQL_PG_DSN="host=localhost port=5432 dbname=brickql user=brickql password=brickql" \
		$(PYTEST) tests/integration/test_postgres.py -v -m postgres

test:  ## Run all tests except Postgres integration
	$(PYTEST) tests/ -m "not postgres" -v

ci:  ## Full CI pipeline: lint + typecheck + unit + sqlite integration
	$(MAKE) lint
	$(MAKE) typecheck
	$(MAKE) test

clean:  ## Remove venv, caches, and build artifacts
	rm -rf $(VENV) dist build *.egg-info .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
