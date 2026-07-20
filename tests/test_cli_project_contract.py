"""Behavioral contract for project-oriented CLI defaults.

These tests intentionally exercise commands from a project root without repeating
model, database, and dashboard paths.  New commands should consume the same
project context rather than adding their own discovery rules.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_cli_config():
    cli_module._loaded_config = None
    yield
    cli_module._loaded_config = None


@pytest.fixture
def project(tmp_path: Path) -> Path:
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text(
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
""".lstrip()
    )

    data = tmp_path / "data"
    data.mkdir()
    db_path = data / "warehouse.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
    connection.execute("INSERT INTO orders VALUES (1, 'complete'), (2, 'pending')")
    connection.close()

    (tmp_path / "sidemantic.yaml").write_text(
        """
models_dir: models
connection:
  type: duckdb
  path: data/warehouse.duckdb
""".lstrip()
    )
    return tmp_path


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        (["info"], "orders"),
        (["validate"], "Validation Passed"),
        (["gen", "types", "--no-yaml"], '"orders"'),
        (["dashboard", "types"], "orders.order_count"),
    ],
)
def test_model_commands_honor_project_models_dir(
    monkeypatch: pytest.MonkeyPatch,
    project: Path,
    arguments: list[str],
    expected: str,
):
    monkeypatch.chdir(project)

    result = runner.invoke(app, arguments)

    assert result.exit_code == 0, result.output
    assert expected in result.output


def test_query_uses_project_models_and_connection(monkeypatch: pytest.MonkeyPatch, project: Path):
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["query", "SELECT order_count FROM orders"])

    assert result.exit_code == 0, result.output
    assert "order_count" in result.output
    assert "2" in result.output


def test_migrate_query_path_is_relative_to_selected_project(
    monkeypatch: pytest.MonkeyPatch,
    project: Path,
    tmp_path: Path,
):
    queries = project / "queries"
    queries.mkdir()
    query = queries / "orders.sql"
    query.write_text("SELECT * FROM orders")
    outside = tmp_path / "outside"
    outside.mkdir()
    captured: dict[str, object] = {}

    def fake_migrator(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, "migrator", fake_migrator)
    monkeypatch.chdir(outside)

    result = runner.invoke(
        app,
        ["--project", str(project), "migrate", "generate", "queries/orders.sql"],
    )

    assert result.exit_code == 0, result.output
    assert captured["queries"] == query
    assert captured["generate_models"] == project


def test_migrate_query_path_is_relative_to_config_selected_project(
    monkeypatch: pytest.MonkeyPatch,
    project: Path,
    tmp_path: Path,
):
    queries = project / "queries"
    queries.mkdir()
    query = queries / "orders.sql"
    query.write_text("SELECT * FROM orders")
    outside = tmp_path / "outside"
    outside.mkdir()
    captured: dict[str, object] = {}

    def fake_migrator(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, "migrator", fake_migrator)
    monkeypatch.chdir(outside)

    result = runner.invoke(
        app,
        ["--config", str(project / "sidemantic.yaml"), "migrate", "generate", "queries/orders.sql"],
    )

    assert result.exit_code == 0, result.output
    assert captured["queries"] == query
    assert captured["generate_models"] == project


def test_missing_migrate_query_path_is_a_cli_parameter_error(
    monkeypatch: pytest.MonkeyPatch,
    project: Path,
    tmp_path: Path,
):
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.chdir(outside)

    result = runner.invoke(
        app,
        ["--project", str(project), "migrate", "generate", "queries/missing.sql"],
    )

    assert result.exit_code != 0
    assert "Invalid value for QUERIES" in result.output
    assert "queries/" in result.output
    assert ".sql" in result.output
    assert "Traceback" not in result.output


def test_explicit_malformed_config_is_fatal(monkeypatch: pytest.MonkeyPatch, project: Path):
    malformed = project / "broken.yaml"
    malformed.write_text("models_dir: [\n")
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["--config", str(malformed), "info"])

    assert result.exit_code != 0
    assert "config" in result.output.lower()
    assert "warning" not in result.output.lower()
    assert "orders" not in result.output


@pytest.mark.parametrize("debug", [False, True])
def test_connection_and_db_overrides_are_mutually_exclusive(
    monkeypatch: pytest.MonkeyPatch,
    project: Path,
    debug: bool,
):
    monkeypatch.chdir(project)

    result = runner.invoke(
        app,
        [
            "query",
            "SELECT order_count FROM orders",
            "--connection",
            "duckdb:///:memory:",
            "--db",
            str(project / "data" / "warehouse.duckdb"),
            *(["--debug"] if debug else []),
        ],
    )

    assert result.exit_code == 2
    assert "--connection" in result.output
    assert "--db" in result.output
    assert "together" in result.output.lower() or "exclusive" in result.output.lower()
    assert "Traceback" not in result.output


def test_database_discovery_rejects_ambiguous_candidates(
    monkeypatch: pytest.MonkeyPatch,
    project: Path,
):
    config = project / "sidemantic.yaml"
    config.write_text("models_dir: models\n")
    existing = project / "data" / "warehouse.duckdb"
    existing.rename(project / "data" / "alpha.db")
    (project / "data" / "beta.duckdb").touch()
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["query", "SELECT order_count FROM orders"])

    assert result.exit_code != 0
    assert "ambiguous" in result.output.lower() or "multiple" in result.output.lower()
    assert "alpha.db" in result.output
    assert "beta.duckdb" in result.output


@pytest.mark.parametrize("command", ["rewrite", "explain"])
def test_compile_only_commands_do_not_require_unambiguous_database_discovery(
    monkeypatch: pytest.MonkeyPatch,
    project: Path,
    command: str,
):
    (project / "sidemantic.yaml").write_text("models_dir: models\n")
    existing = project / "data" / "warehouse.duckdb"
    existing.rename(project / "data" / "alpha.db")
    (project / "data" / "beta.duckdb").touch()
    monkeypatch.chdir(project)

    result = runner.invoke(app, [command, "SELECT order_count FROM orders"])

    assert result.exit_code == 0, result.output
    assert "Multiple databases found" not in result.output


def test_dashboard_serve_discovers_default_spec_and_project(
    monkeypatch: pytest.MonkeyPatch,
    project: Path,
):
    pytest.importorskip("fastapi")
    (project / "dashboard.yaml").write_text(
        """
schema: sidemantic.dashboard.v1
title: Orders dashboard
tabs:
  - id: overview
    charts:
      - id: orders_by_status
        type: bar
        query:
          metrics: [orders.order_count]
          dimensions: [orders.status]
        encoding:
          x: orders.status
          y: orders.order_count
""".lstrip()
    )
    static_dir = project / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<!doctype html>")
    called: dict[str, object] = {}

    def fake_start_api_server(layer, **kwargs):
        called["layer"] = layer
        called.update(kwargs)

    monkeypatch.setattr("sidemantic.api_server.ui_static_dir", lambda: static_dir)
    monkeypatch.setattr("sidemantic.api_server.start_api_server", fake_start_api_server)
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["dashboard", "serve"])

    assert result.exit_code == 0, result.output
    assert called["dashboard"].title == "Orders dashboard"
    assert called["port"] == 4400


def test_dashboard_has_one_product_entry_point():
    dashboard_help = runner.invoke(app, ["dashboard", "--help"])
    serve_help = runner.invoke(app, ["dashboard", "serve", "--help"])
    api_help = runner.invoke(app, ["api-serve", "--help"])

    assert dashboard_help.exit_code == 0, dashboard_help.output
    assert serve_help.exit_code == 0, serve_help.output
    assert "serve" in dashboard_help.output
    assert "--dashboard" not in api_help.output


@pytest.mark.parametrize(
    "arguments",
    [
        ["info"],
        ["preagg", "recommend", "--queries", "empty.sql"],
    ],
)
def test_successful_early_exit_is_not_reported_as_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
):
    (tmp_path / "empty.sql").write_text("")
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, arguments)

    assert result.exit_code == 0, result.output
    assert "Error: 0" not in result.output
    assert "Traceback" not in result.output
