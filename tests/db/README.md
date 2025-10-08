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
POSTGRES_TEST=1 uv run --extra postgres pytest -m integration tests/db/test_postgres_integration.py -v
```

**Manual setup:**
```bash
# Install postgres dependencies
uv sync --extra postgres

# Set up Postgres (adjust connection details as needed)
export POSTGRES_TEST=1
export POSTGRES_URL="postgres://test:test@localhost:5432/sidemantic_test"

# Run integration tests only
uv run pytest -m integration tests/db/test_postgres_integration.py -v
```

### BigQuery Integration Tests

BigQuery tests use the BigQuery emulator and are marked with `@pytest.mark.integration`. They require the `bigquery` extra dependencies.

**Using Docker Compose (recommended):**
```bash
# Start BigQuery emulator and run integration tests
docker compose up test --build --abort-on-container-exit

# Or run tests locally against dockerized emulator
docker compose up -d bigquery
BIGQUERY_TEST=1 BIGQUERY_EMULATOR_HOST=localhost:9050 uv run --extra bigquery pytest -m integration tests/db/test_bigquery_integration.py -v
```

**Manual setup:**
```bash
# Install bigquery dependencies
uv sync --extra bigquery

# Set up BigQuery emulator (adjust as needed)
export BIGQUERY_TEST=1
export BIGQUERY_EMULATOR_HOST=localhost:9050
export BIGQUERY_PROJECT=test-project
export BIGQUERY_DATASET=test_dataset

# Run integration tests only
uv run pytest -m integration tests/db/test_bigquery_integration.py -v
```

**Note:** Normal `pytest` runs will skip integration tests automatically. Use `-m integration` to run them explicitly.

## Test Coverage

- **test_duckdb_adapter.py**: Tests for DuckDB adapter implementation
- **test_postgres_adapter.py**: Basic Postgres adapter tests (import checks, no connection required)
- **test_postgres_integration.py**: Full integration tests against real Postgres database (10 tests)
- **test_bigquery_adapter.py**: Basic BigQuery adapter tests (import checks, URL parsing)
- **test_bigquery_integration.py**: Full integration tests against BigQuery emulator (10 tests)
- **test_semantic_layer_adapters.py**: Tests for SemanticLayer integration with different adapters
