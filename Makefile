.PHONY: help install test test-cov test-integration lint typecheck format format-check up down wheel-smoke clean build publish-test publish release ci setup

PACKAGE := src/dewey

help:
	@echo "Dewey 🦆 — Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install       Install with dev dependencies (uv sync)"
	@echo ""
	@echo "Infrastructure (Postgres + Redis for the test suite):"
	@echo "  make up            Start containers and wait until healthy"
	@echo "  make down          Stop containers and drop their volumes"
	@echo ""
	@echo "Development:"
	@echo "  make test          Run tests"
	@echo "  make test-cov      Run tests with coverage report"
	@echo "  make lint          Run linting checks"
	@echo "  make typecheck     Run basedpyright type checks"
	@echo "  make format        Format code with ruff"
	@echo "  make format-check  Check formatting without writing"
	@echo "  make test-integration  Run the suite against the compose containers"
	@echo "  make wheel-smoke   Build a wheel and exercise it in a clean venv"
	@echo ""
	@echo "Building & Publishing:"
	@echo "  make clean         Remove build artifacts"
	@echo "  make build         Build distribution packages"
	@echo "  make release       Tag and push (triggers PyPI publish)"
	@echo "  make publish-test  Publish to TestPyPI"
	@echo "  make publish       Publish to PyPI (manual)"

install:
	uv sync --all-extras

# Compose ports are offset so they cannot collide with a local Postgres/Redis.
COMPOSE_DB := postgresql://postgres:postgres@localhost:55440/dewey_test
COMPOSE_DB_ASYNC := postgresql+asyncpg://postgres:postgres@localhost:55440/dewey_test
COMPOSE_REDIS := redis://localhost:56390/0

up:
	docker compose up -d --wait

down:
	docker compose down -v

test:
	uv run pytest

test-integration: up
	DEWEY_TEST_DATABASE_URL=$(COMPOSE_DB) \
	DEWEY_TEST_DATABASE_URL_ASYNC=$(COMPOSE_DB_ASYNC) \
	DEWEY_TEST_REDIS_URL=$(COMPOSE_REDIS) \
	PGHOST=localhost PGPORT=55440 \
	uv run pytest

wheel-smoke: build
	DEWEY_TEST_DATABASE_URL=$(COMPOSE_DB) \
	DEWEY_TEST_REDIS_URL=$(COMPOSE_REDIS) \
	./scripts/wheel_smoke.sh

test-cov:
	uv run pytest --cov=dewey --cov-report=term-missing --cov-report=html

lint:
	uv run ruff check $(PACKAGE) tests

typecheck:
	uv run basedpyright

format:
	uv run ruff check --fix $(PACKAGE) tests
	uv run ruff format $(PACKAGE) tests

format-check:
	uv run ruff format --check $(PACKAGE) tests

clean:
	rm -rf build/ dist/ *.egg-info htmlcov/ .pytest_cache/ .pyright/ .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	uv build
	@ls -lh dist/

publish-test: build
	uv publish --index testpypi

publish: build
	@echo "Publishing to PyPI. Ctrl+C to cancel."
	@read -p "Press Enter to continue..."
	uv publish

release: _check-clean _check-branch test build
	@VERSION=$$(grep 'version = ' pyproject.toml | head -1 | cut -d'"' -f2) && \
	echo "Ready to release v$$VERSION" && \
	read -p "Continue? [y/N] " confirm && \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		git tag "v$$VERSION" && \
		git push && git push --tags && \
		echo "Released v$$VERSION!"; \
	else \
		echo "Aborted."; \
	fi

_check-clean:
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "Working directory not clean."; \
		git status --short; \
		exit 1; \
	fi

_check-branch:
	@BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	if [ "$$BRANCH" != "main" ]; then \
		echo "Not on main branch (currently on $$BRANCH)"; \
		exit 1; \
	fi

ci: lint typecheck test
	@echo "CI checks passed!"

setup: install test
	@echo "Setup complete!"
