"""Tests for CLI command wiring."""

from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app
from sidemantic.config import SidemanticConfig
from tests.optional_dep_stubs import ensure_fake_mcp, ensure_fake_riffq

runner = CliRunner()


def _write_min_model(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        sql: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
"""
    )


def _write_orders_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
    conn.execute("INSERT INTO orders VALUES (1, 'completed'), (2, 'pending')")
    conn.close()


def test_version_option_prints_version():
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert "sidemantic " in result.stdout


def test_info_prints_model_summary(tmp_path):
    _write_min_model(tmp_path)

    result = runner.invoke(app, ["info", str(tmp_path)])

    assert result.exit_code == 0
    assert "Semantic Layer:" in result.stdout
    assert "orders" in result.stdout
    assert "Dimensions: 1" in result.stdout
    assert "Metrics: 1" in result.stdout


def test_query_dry_run_emits_sql(tmp_path):
    _write_min_model(tmp_path)

    result = runner.invoke(
        app, ["query", "SELECT order_count, status FROM orders", "--models", str(tmp_path), "--dry-run"]
    )

    assert result.exit_code == 0
    assert "select" in result.stdout.lower()
    assert "count" in result.stdout.lower()


def test_query_writes_csv_using_autodetected_data_db(tmp_path):
    models_dir = tmp_path / "models"
    _write_min_model(models_dir)
    data_dir = models_dir / "data"
    data_dir.mkdir()
    _write_orders_db(data_dir / "warehouse.db")
    output_path = tmp_path / "results.csv"

    result = runner.invoke(
        app,
        [
            "query",
            "SELECT status, order_count FROM orders ORDER BY status",
            "--models",
            str(models_dir),
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    output = output_path.read_text()
    assert "status,order_count" in output
    assert "completed,1" in output
    assert "pending,1" in output


def test_workbench_calls_runner(monkeypatch, tmp_path):
    pytest.importorskip("textual")
    called = {}

    def fake_run_workbench(directory, demo_mode=False, connection=None):
        called["directory"] = directory
        called["demo_mode"] = demo_mode
        called["connection"] = connection

    monkeypatch.setattr("sidemantic.workbench.run_workbench", fake_run_workbench)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["workbench", str(tmp_path)])

    assert result.exit_code == 0
    assert called["directory"] == tmp_path
    assert called["demo_mode"] is False
    assert called["connection"] is None


def test_validate_calls_runner(monkeypatch, tmp_path):
    pytest.importorskip("textual")
    called = {}

    def fake_run_validation(directory, verbose=False):
        called["directory"] = directory
        called["verbose"] = verbose

    monkeypatch.setattr("sidemantic.workbench.run_validation", fake_run_validation)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["validate", str(tmp_path), "--verbose"])

    assert result.exit_code == 0
    assert called["directory"] == tmp_path
    assert called["verbose"] is True


def test_lsp_command_calls_main(monkeypatch):
    called = {"count": 0}

    def fake_main():
        called["count"] += 1

    monkeypatch.setattr("sidemantic.lsp.main", fake_main)

    result = runner.invoke(app, ["lsp"])

    assert result.exit_code == 0
    assert called["count"] == 1


def test_serve_calls_start_server(monkeypatch, tmp_path):
    ensure_fake_riffq()
    called = {}

    def fake_start_server(layer, host, port, username, password):
        called["layer"] = layer
        called["host"] = host
        called["port"] = port
        called["username"] = username
        called["password"] = password

    monkeypatch.setattr("sidemantic.server.server.start_server", fake_start_server)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["serve", str(tmp_path), "--port", "5544", "--username", "u", "--password", "p"])

    assert result.exit_code == 0
    assert called["port"] == 5544
    assert called["username"] == "u"
    assert called["password"] == "p"


def test_serve_uses_loaded_config_defaults(monkeypatch, tmp_path):
    ensure_fake_riffq()
    called = {}
    models_dir = tmp_path / "models"
    _write_min_model(models_dir)
    config_path = tmp_path / "sidemantic.yaml"
    config_path.write_text(
        f"""
models_dir: {models_dir}
connection:
  type: duckdb
  path: ":memory:"
pg_server:
  host: 0.0.0.0
  port: 5545
  username: config-user
  password: config-pass
"""
    )

    def fake_start_server(layer, host, port, username, password):
        called["layer"] = layer
        called["host"] = host
        called["port"] = port
        called["username"] = username
        called["password"] = password

    monkeypatch.setattr("sidemantic.server.server.start_server", fake_start_server)

    cli_module._loaded_config = None
    result = runner.invoke(app, ["--config", str(config_path), "serve"])

    assert result.exit_code == 0
    assert called["host"] == "0.0.0.0"
    assert called["port"] == 5545
    assert called["username"] == "config-user"
    assert called["password"] == "config-pass"
    assert called["layer"].connection_string == "duckdb:///:memory:"
    assert "Loaded config from:" in result.stderr


def test_api_serve_calls_start_server(monkeypatch, tmp_path):
    pytest.importorskip("fastapi")
    called = {}

    def fake_start_api_server(layer, host, port, auth_token, cors_origins, max_request_body_bytes):
        called["layer"] = layer
        called["host"] = host
        called["port"] = port
        called["auth_token"] = auth_token
        called["cors_origins"] = cors_origins
        called["max_request_body_bytes"] = max_request_body_bytes

    monkeypatch.setattr("sidemantic.api_server.start_api_server", fake_start_api_server)

    _write_min_model(tmp_path)
    result = runner.invoke(
        app,
        [
            "api-serve",
            str(tmp_path),
            "--port",
            "4410",
            "--auth-token",
            "secret",
            "--cors-origin",
            "https://app.example.com",
            "--max-request-body-bytes",
            "2048",
        ],
    )

    assert result.exit_code == 0
    assert called["host"] == "127.0.0.1"
    assert called["port"] == 4410
    assert called["auth_token"] == "secret"
    assert called["cors_origins"] == ["https://app.example.com"]
    assert called["max_request_body_bytes"] == 2048


def test_tree_alias_calls_workbench(monkeypatch, tmp_path):
    pytest.importorskip("textual")
    called = {}

    def fake_run_workbench(directory):
        called["directory"] = directory

    monkeypatch.setattr("sidemantic.workbench.run_workbench", fake_run_workbench)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["tree", str(tmp_path)])

    assert result.exit_code == 0
    assert called["directory"] == tmp_path


def test_cli_source_uses_public_adapter():
    """cli.py should not reference ._adapter anywhere."""
    import os

    cli_path = os.path.join(os.path.dirname(__file__), "..", "sidemantic", "cli.py")
    with open(cli_path) as f:
        content = f.read()

    assert "._adapter" not in content, "CLI should use .adapter, not ._adapter"


def test_mcp_serve_calls_initialize(monkeypatch, tmp_path):
    ensure_fake_mcp()
    called = {}

    def fake_initialize_layer(directory, db_path=None, connection=None, init_sql=None):
        called["directory"] = directory
        called["db_path"] = db_path
        called["connection"] = connection
        called["init_sql"] = init_sql

    def fake_run(*args, **kwargs):
        called["run"] = True

    monkeypatch.setattr("sidemantic.mcp_server.initialize_layer", fake_initialize_layer)
    monkeypatch.setattr("sidemantic.mcp_server.mcp.run", fake_run)

    tmp_path.mkdir(parents=True, exist_ok=True)
    result = runner.invoke(app, ["mcp-serve", str(tmp_path)])

    assert result.exit_code == 0
    assert called["directory"] == str(tmp_path)
    assert called["db_path"] is None
    assert called.get("run") is True


def test_mcp_serve_apps_implies_http_and_uses_config(monkeypatch, tmp_path):
    ensure_fake_mcp()
    called = {}
    models_dir = tmp_path / "models"
    _write_min_model(models_dir)
    config_path = tmp_path / "sidemantic.yaml"
    config_path.write_text(
        """
models_dir: .
connection:
  type: duckdb
  path: ":memory:"
  init_sql:
    - SELECT 42
"""
    )

    def fake_initialize_layer(directory, db_path=None, connection=None, init_sql=None):
        called["directory"] = directory
        called["db_path"] = db_path
        called["connection"] = connection
        called["init_sql"] = init_sql

    def fake_run(*args, **kwargs):
        called["transport"] = kwargs["transport"]

    monkeypatch.setattr("sidemantic.mcp_server.initialize_layer", fake_initialize_layer)
    monkeypatch.setattr("sidemantic.mcp_server.mcp.run", fake_run)

    import sidemantic.mcp_server as mcp_mod

    mcp_mod._apps_enabled = False
    cli_module._loaded_config = None
    result = runner.invoke(
        app, ["--config", str(config_path), "mcp-serve", str(models_dir), "--apps", "--port", "4201"]
    )

    assert result.exit_code == 0
    assert called["directory"] == str(models_dir)
    assert called["connection"] == "duckdb:///:memory:"
    assert called["init_sql"] == ["SELECT 42"]
    assert called["transport"] == "streamable-http"
    assert mcp_mod._apps_enabled is True
    assert "Note: --apps implies HTTP transport" in result.stderr


def test_query_uses_loaded_config_init_sql(monkeypatch, tmp_path):
    models_dir = tmp_path / "models"
    _write_min_model(models_dir)
    cli_module._loaded_config = SidemanticConfig.model_validate(
        {
            "models_dir": str(models_dir),
            "connection": {"type": "duckdb", "path": ":memory:", "init_sql": ["select 7"]},
        }
    )
    captured = {}

    class FakeResult:
        description = [("order_count",), ("status",)]

        def fetchall(self):
            return [(2, "completed")]

    class FakeLayer:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs
            self.graph = type("Graph", (), {"models": {"orders": object()}})()

        def sql(self, sql):
            captured["sql"] = sql
            return FakeResult()

    def fake_load_from_directory(layer, directory):
        captured["directory"] = directory

    monkeypatch.setattr("sidemantic.cli.SemanticLayer", FakeLayer)
    monkeypatch.setattr("sidemantic.cli.load_from_directory", fake_load_from_directory)

    result = runner.invoke(app, ["query", "SELECT order_count, status FROM orders", "--models", str(models_dir)])

    assert result.exit_code == 0
    assert captured["kwargs"]["connection"] == "duckdb:///:memory:"
    assert captured["kwargs"]["init_sql"] == ["select 7"]
    assert captured["directory"] == str(models_dir)
    assert "order_count,status" in result.stdout
    assert "2,completed" in result.stdout
