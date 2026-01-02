import os

import pytest
from typer.testing import CliRunner

from sidemantic.cli import app

runner = CliRunner()

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("SNOWFLAKE_TEST") != "1",
        reason="Set SNOWFLAKE_TEST=1 to run Snowflake integration tests with fakesnow",
    ),
]


def _setup_snowflake_table():
    import snowflake.connector

    conn = snowflake.connector.connect(
        account="test",
        user="test",
        password="test",
        warehouse="TEST_WH",
        database="LOCKERS_AGGREGATION__DEV",
        schema="METRICS",
    )
    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS LOCKERS_AGGREGATION__DEV")
    cur.execute("CREATE SCHEMA IF NOT EXISTS LOCKERS_AGGREGATION__DEV.METRICS")
    cur.execute(
        """
        CREATE OR REPLACE TABLE LOCKERS_AGGREGATION__DEV.METRICS.dim_location (
            sk_location_id INTEGER,
            source_location_id VARCHAR
        )
        """
    )
    cur.execute(
        """
        INSERT INTO LOCKERS_AGGREGATION__DEV.METRICS.dim_location VALUES
        (1, 'A'),
        (2, 'B'),
        (3, 'B')
        """
    )
    conn.close()


def test_cli_query_uses_snowflake_connection(tmp_path):
    snowflake = pytest.importorskip("snowflake.connector")
    assert snowflake  # silence lint

    import fakesnow

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    model_file = models_dir / "location.yml"
    model_file.write_text(
        """
models:
  - name: location
    table: LOCKERS_AGGREGATION__DEV.METRICS.dim_location
    primary_key: sk_location_id
    dimensions:
      - name: sk_location_id
        type: categorical
        sql: '"SK_LOCATION_ID"'
      - name: source_location_id
        type: categorical
        sql: '"SOURCE_LOCATION_ID"'
    metrics:
      - name: count
        agg: count_distinct
        sql: source_location_id
"""
    )

    config_file = tmp_path / "sidemantic.yaml"
    config_file.write_text(
        """
models_dir: ./models
connection:
  type: snowflake
  account: test
  username: test
  password: test
  database: LOCKERS_AGGREGATION__DEV
  schema: METRICS
  warehouse: TEST_WH
"""
    )

    with fakesnow.patch():
        _setup_snowflake_table()
        result = runner.invoke(
            app,
            [
                "--config",
                str(config_file),
                "query",
                "SELECT location.count FROM location",
                "--models",
                str(models_dir),
            ],
        )

    assert result.exit_code == 0
    assert "count" in result.stdout.lower()
    assert "2" in result.stdout
