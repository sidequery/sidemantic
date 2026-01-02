import os

import pytest
from typer.testing import CliRunner

from sidemantic.cli import app

runner = CliRunner()


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("SPARK_TEST") != "1",
        reason="Set SPARK_TEST=1 and run docker compose up -d spark to run Spark integration tests",
    ),
]


def test_cli_query_uses_spark_config(tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    model_file = models_dir / "orders.yml"
    model_file.write_text(
        """
models:
  - name: orders
    table: (SELECT 1 as order_id, 100.0 as amount UNION ALL SELECT 2, 200.0)
    primary_key: order_id
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount
"""
    )

    host = os.getenv("SPARK_HOST", "localhost")
    port = os.getenv("SPARK_PORT", "10000")
    password = os.getenv("SPARK_PASSWORD", "spark")

    config_file = tmp_path / "sidemantic.yaml"
    config_file.write_text(
        f"""
models_dir: ./models
connection:
  type: spark
  host: {host}
  port: {port}
  database: default
  username: default
  password: {password}
"""
    )

    result = runner.invoke(
        app,
        [
            "--config",
            str(config_file),
            "query",
            "SELECT orders.total_revenue FROM orders",
            "--models",
            str(models_dir),
        ],
    )

    assert result.exit_code == 0
    assert "total_revenue" in result.stdout.lower()
    assert "300" in result.stdout
