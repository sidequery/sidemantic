# Database Adapter Tests

## Running Tests

### DuckDB Tests (default)
```bash
pytest tests/db/test_duckdb_adapter.py -v
pytest tests/db/test_semantic_layer_adapters.py -v
```

### PostgreSQL Integration Tests

PostgreSQL tests are marked with `@pytest.mark.integration` and skipped by default. They require a running Postgres instance and the `postgres` extra dependencies.

**Using Docker Compose (recommended):**
```bash
# Start Postgres and run integration tests
docker compose up test --build --abort-on-container-exit

# Or run tests locally against dockerized Postgres
docker compose up -d postgres
POSTGRES_TEST=1 uv run --extra postgres pytest -m integration -v
```

**Manual setup:**
```bash
# Install postgres dependencies
uv sync --extra postgres

# Set up Postgres (adjust connection details as needed)
export POSTGRES_TEST=1
export POSTGRES_URL="postgres://test:test@localhost:5432/sidemantic_test"

# Run integration tests only
uv run pytest -m integration -v
```

**Note:** Normal `pytest` runs will skip integration tests automatically. Use `-m integration` to run them explicitly.

## Test Coverage

- **test_duckdb_adapter.py**: Tests for DuckDB adapter implementation
- **test_postgres_adapter.py**: Basic Postgres adapter tests (mostly ImportError checks)
- **test_postgres_integration.py**: Full integration tests against real Postgres database
- **test_semantic_layer_adapters.py**: Tests for SemanticLayer integration with different adapters
