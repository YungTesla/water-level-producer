.PHONY: run check lint typecheck install

install:
	uv sync --extra dev

run:
	uv run python -m src.main

check: lint typecheck

lint:
	uv run ruff check src/

typecheck:
	uv run pyright src/
