.PHONY: help install test test-cov lint typecheck format clean build publish-test publish release ci setup

PACKAGE := src/dewey

help:
	@echo "Dewey 🦆 — Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install       Install with dev dependencies (uv sync)"
	@echo ""
	@echo "Development:"
	@echo "  make test          Run tests"
	@echo "  make test-cov      Run tests with coverage report"
	@echo "  make lint          Run linting checks"
	@echo "  make typecheck     Run basedpyright type checks"
	@echo "  make format        Format code with ruff"
	@echo ""
	@echo "Building & Publishing:"
	@echo "  make clean         Remove build artifacts"
	@echo "  make build         Build distribution packages"
	@echo "  make release       Tag and push (triggers PyPI publish)"
	@echo "  make publish-test  Publish to TestPyPI"
	@echo "  make publish       Publish to PyPI (manual)"

install:
	uv sync --all-extras

test:
	uv run pytest

test-cov:
	uv run pytest --cov=dewey --cov-report=term-missing --cov-report=html

lint:
	uv run ruff check $(PACKAGE) tests

typecheck:
	uv run basedpyright

format:
	uv run ruff check --fix $(PACKAGE) tests
	uv run ruff format $(PACKAGE) tests

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
