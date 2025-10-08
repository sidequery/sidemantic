"""Integration tests for BigQuery adapter against emulator.

Run with: docker compose up -d bigquery && pytest -m integration tests/db/test_bigquery_integration.py -v
"""

import os

import pytest

# Mark all tests in this module as integration tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("BIGQUERY_TEST") != "1",
        reason="Set BIGQUERY_TEST=1 and run docker compose up -d bigquery to run BigQuery integration tests",
    ),
]

# Use environment variable for URL (emulator endpoint)
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "test-project")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "test_dataset")


@pytest.fixture(scope="module")
def bigquery_adapter():
    """Create BigQuery adapter connected to emulator."""
    from sidemantic.db.bigquery import BigQueryAdapter

    # For emulator, we need to set BIGQUERY_EMULATOR_HOST
    emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")
    os.environ["BIGQUERY_EMULATOR_HOST"] = emulator_host

    adapter = BigQueryAdapter(project_id=BIGQUERY_PROJECT, dataset_id=BIGQUERY_DATASET)
    yield adapter
    adapter.close()


def test_bigquery_adapter_execute(bigquery_adapter):
    """Test basic query execution."""
    result = bigquery_adapter.execute("SELECT 1 as x, 2 as y")
    row = result.fetchone()
    assert row == (1, 2)


def test_bigquery_adapter_aggregations(bigquery_adapter):
    """Test aggregations in queries."""
    result = bigquery_adapter.execute("""
        SELECT
            SUM(x) as total,
            AVG(x) as average,
            MAX(x) as maximum,
            MIN(x) as minimum,
            COUNT(*) as count
        FROM (SELECT 1 as x UNION ALL SELECT 2 UNION ALL SELECT 3)
    """)
    row = result.fetchone()
    cols = [desc[0] for desc in result.description]
    row_dict = dict(zip(cols, row))

    assert row_dict["total"] == 6
    assert row_dict["average"] == 2.0
    assert row_dict["maximum"] == 3
    assert row_dict["minimum"] == 1
    assert row_dict["count"] == 3
