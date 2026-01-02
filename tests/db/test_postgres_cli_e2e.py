import os

import pytest
from typer.testing import CliRunner

from sidemantic.cli import app
from sidemantic.db.postgres import PostgreSQLAdapter

runner = CliRunner()

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("POSTGRES_TEST") != "1",
        reason="Set POSTGRES_TEST=1 to run Postgres integration tests",
    ),
]


def _postgres_adapter():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5433"))
    database = os.getenv("POSTGRES_DB", "sidemantic_test")
    username = os.getenv("POSTGRES_USER", "test")
    password = os.getenv("POSTGRES_PASSWORD", "test")
    return PostgreSQLAdapter(host=host, port=port, database=database, user=username, password=password)


def test_cli_query_uses_postgres_config(tmp_path):
    adapter = _postgres_adapter()
    try:
        adapter.execute("DROP TABLE IF EXISTS orders_cli_e2e")
        adapter.execute("CREATE TABLE orders_cli_e2e (order_id INT, amount NUMERIC)")
        adapter.execute("INSERT INTO orders_cli_e2e VALUES (1, 100.0), (2, 200.0)")

        models_dir = tmp_path / "models"
        models_dir.mkdir()

        model_file = models_dir / "orders.yml"
        model_file.write_text(
            """
models:
  - name: orders
    table: orders_cli_e2e
    primary_key: order_id
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount
"""
        )

        config_file = tmp_path / "sidemantic.yaml"
        config_file.write_text(
            f"""
models_dir: ./models
connection:
  type: postgres
  host: {os.getenv("POSTGRES_HOST", "localhost")}
  port: {os.getenv("POSTGRES_PORT", "5433")}
  database: {os.getenv("POSTGRES_DB", "sidemantic_test")}
  username: {os.getenv("POSTGRES_USER", "test")}
  password: {os.getenv("POSTGRES_PASSWORD", "test")}
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
    finally:
        adapter.execute("DROP TABLE IF EXISTS orders_cli_e2e")
        adapter.close()

    assert result.exit_code == 0
    assert "total_revenue" in result.stdout.lower()
    assert "300" in result.stdout
