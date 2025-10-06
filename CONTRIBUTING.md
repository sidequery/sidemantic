# Contributing

Sidemantic is primarily developed by Sidequery Corporation. We're not actively seeking external contributions at this time.

If you find bugs, please open an issue with reproduction steps.

## Development Setup

```bash
uv sync --all-extras
uv run pytest
```

## Running Tests

```bash
uv run pytest -v
```

## Code Style

We use `ruff` for linting and formatting:

```bash
uv run ruff check .
uv run ruff format .
```
