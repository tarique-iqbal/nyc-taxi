.DEFAULT_GOAL := help
PYTHON        := python3
PIP           := pip3

# Docker
.PHONY: up up-build down restart logs ps

up:          # Start all infrastructure containers
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5

up-build:   # Build images and start all containers
	docker compose up --build

down:        # Stop and remove all containers
	docker compose down

restart:     # Restart all containers
	docker compose restart

logs:        # Tail logs for all containers
	docker compose logs -f

ps:          # Show container status
	docker compose ps

# Setup
.PHONY: install bootstrap topics schema data

install:     # Install Python dependencies
	$(PIP) install -e ".[dev]"

bootstrap:   # Full bootstrap: up + topics + schema + data
	bash scripts/bootstrap.sh

topics:      # Create Kafka topics
	bash scripts/create_topics.sh

schema:      # Apply ClickHouse schema and migrations
	bash scripts/apply_schema.sh

data:        # Download NYC taxi data
	bash scripts/download_data.sh

# Run
.PHONY: producer consumer health

producer:    # Runs producer entrypoint -- also serves /metrics on 9100 (from .env)
	$(PYTHON) -m etl.entrypoints.producer

consumer:    # Runs consumer entrypoint -- also serves /metrics on 9101 (from .env)
	$(PYTHON) -m etl.entrypoints.consumer

health:      # Run health check server
	$(PYTHON) -m etl.entrypoints.health_server

# Testing
.PHONY: test test-unit test-integration test-cov

test:        # Run all tests
	pytest

test-unit:   # Run unit tests only
	pytest tests/unit/

test-integration: # Run integration tests only
	pytest tests/integration/

test-cov:    # Run tests with coverage report
	pytest --cov=etl --cov-report=html
	@echo "Coverage report: htmlcov/index.html"

# Code Quality
.PHONY: lint format typecheck check

lint:        # Lint with ruff
	ruff check etl/ tests/

format:      # Format with ruff
	ruff format etl/ tests/

typecheck:   # Type-check with mypy
	mypy etl/

check: lint typecheck # Run lint + typecheck

# Operations
.PHONY: lag replay smoke

lag:         # Check Kafka consumer group lag
	bash scripts/check_kafka_lag.sh

replay:      # Replay DLQ records
	bash scripts/replay_dlq.sh

smoke:       # Run smoke test
	bash scripts/smoke_test.sh

# Help
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
