# brickQL – development task runner
# Usage: make <target>   (assumes .venv is active or use `make venv` first)

VENV     := .venv
PYTHON   := $(VENV)/bin/python
PIP      := $(VENV)/bin/pip
PYTEST   := $(VENV)/bin/pytest
RUFF     := $(VENV)/bin/ruff
MYPY     := $(VENV)/bin/mypy

PG_CONTAINER  := brickql_postgres
PG_IMAGE      := postgres:16-alpine
PG_DSN        := host=localhost port=5432 dbname=brickql user=brickql password=brickql

MY_CONTAINER  := brickql_mysql
MY_IMAGE      := mysql:8.4
MY_DSN        := mysql://brickql:brickql@localhost:3306/brickql

.PHONY: help venv install lint fmt typecheck test test-unit test-integration clean docker-postgres-clean docker-mysql-clean

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
	$(PYTEST) tests/ -m "not integration and not postgres and not mysql" -v

test-integration-sqlite:  ## Run SQLite integration tests (in-memory, no Docker)
	$(PYTEST) tests/integration/test_sqlite.py -v -m integration

docker-postgres-clean:  ## Remove Postgres container and volume
	-docker rm -f $(PG_CONTAINER) 2>/dev/null || true
	-docker volume rm brickql_pgdata 2>/dev/null || true
	@echo "Postgres container and volume removed."

docker-mysql-clean:  ## Remove MySQL container and volume
	-docker rm -f $(MY_CONTAINER) 2>/dev/null || true
	-docker volume rm brickql_mydata 2>/dev/null || true
	@echo "MySQL container and volume removed."

test-integration-mysql:  ## Run MySQL integration tests (starts and stops Docker automatically)
	@echo "Starting MySQL container..."
	docker run -d --rm \
		--name $(MY_CONTAINER) \
		-e MYSQL_DATABASE=brickql \
		-e MYSQL_USER=brickql \
		-e MYSQL_PASSWORD=brickql \
		-e MYSQL_ROOT_PASSWORD=root \
		-p 3306:3306 \
		$(MY_IMAGE)
	@echo "Waiting for MySQL to be ready..."
	@until docker exec $(MY_CONTAINER) mysqladmin ping -u brickql -pbrickql --silent 2>/dev/null; do sleep 2; done
	BRICKQL_MYSQL_DSN="$(MY_DSN)" \
		$(PYTEST) tests/integration/test_mysql.py -v -m mysql; \
	EXIT=$$?; \
	docker stop $(MY_CONTAINER); \
	exit $$EXIT

test-integration-postgres:  ## Run PostgreSQL integration tests (starts and stops Docker automatically)
	@echo "Starting Postgres container..."
	docker run -d --rm \
		--name $(PG_CONTAINER) \
		-e POSTGRES_DB=brickql \
		-e POSTGRES_USER=brickql \
		-e POSTGRES_PASSWORD=brickql \
		-p 5432:5432 \
		$(PG_IMAGE)
	@echo "Waiting for Postgres to be ready..."
	@until docker exec $(PG_CONTAINER) pg_isready -U brickql -d brickql -q; do sleep 1; done
	BRICKQL_PG_DSN="$(PG_DSN)" \
		$(PYTEST) tests/integration/test_postgres.py -v -m postgres; \
	EXIT=$$?; \
	docker stop $(PG_CONTAINER); \
	exit $$EXIT

test:  ## Run all tests (unit + SQLite + PostgreSQL + MySQL integration)
	$(MAKE) docker-postgres-clean
	$(MAKE) docker-mysql-clean
	$(PYTEST) tests/ -m "not postgres and not mysql" -v
	$(MAKE) test-integration-postgres
	$(MAKE) test-integration-mysql

ci:  ## Full CI pipeline: lint + typecheck + all tests
	$(MAKE) lint
	$(MAKE) typecheck
	$(MAKE) test

clean:  ## Remove venv, caches, and build artifacts
	rm -rf $(VENV) dist build *.egg-info .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
