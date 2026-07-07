"""Tests for CLI command wiring."""

import builtins
import json
import os
import sys
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app
from sidemantic.config import SidemanticConfig
from tests.optional_dep_stubs import ensure_fake_mcp, ensure_fake_riffq

runner = CliRunner()


@pytest.fixture(autouse=True)
def _clear_engine_env(monkeypatch):
    cli_module._loaded_config = None
    for name in (
        "SIDEMANTIC_RS_SQL_GENERATOR",
        "SIDEMANTIC_RS_QUERY_VALIDATION",
        "SIDEMANTIC_RS_REWRITER",
        "SIDEMANTIC_RS_SQL_GENERATOR_VERIFY",
        "SIDEMANTIC_RS_NO_FALLBACK",
    ):
        monkeypatch.delenv(name, raising=False)
    yield
    cli_module._loaded_config = None


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


def test_info_fails_on_detected_parse_error(tmp_path):
    _write_min_model(tmp_path)
    (tmp_path / "bad.yml").write_text(
        """
models:
  - name: broken
    table: [
"""
    )

    result = runner.invoke(app, ["info", str(tmp_path)])

    assert result.exit_code == 1
    assert "Could not parse" in result.output
    assert "bad.yml" in result.output


def test_query_dry_run_emits_sql(tmp_path):
    _write_min_model(tmp_path)

    result = runner.invoke(
        app, ["query", "SELECT order_count, status FROM orders", "--models", str(tmp_path), "--dry-run"]
    )

    assert result.exit_code == 0
    assert "select" in result.stdout.lower()
    assert "count" in result.stdout.lower()


def test_explain_sql_outputs_planner_json(tmp_path):
    _write_min_model(tmp_path)

    result = runner.invoke(
        app,
        [
            "explain-sql",
            "SELECT * FROM (SELECT order_count, status FROM orders) sq WHERE status = 'completed'",
            "--models",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["chosen_plan"] == "direct_semantic"
    assert payload["pushed_filters"] == ["orders.status = 'completed'"]
    assert "safe_filter_pushdown" in payload["applied_rules"]
    assert "rewritten_sql" in payload


def test_query_engine_rust_sets_rewriter_env(monkeypatch, tmp_path):
    _write_min_model(tmp_path)
    captured = {}
    monkeypatch.setenv("SIDEMANTIC_RS_SQL_GENERATOR", "0")
    monkeypatch.setenv("SIDEMANTIC_RS_QUERY_VALIDATION", "0")
    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "0")
    monkeypatch.setenv("SIDEMANTIC_RS_NO_FALLBACK", "0")

    class FakeRewriter:
        def __init__(self, *_args, **_kwargs):
            pass

        def rewrite(self, _sql):
            captured["rewriter"] = os.environ.get("SIDEMANTIC_RS_REWRITER")
            captured["no_fallback"] = os.environ.get("SIDEMANTIC_RS_NO_FALLBACK")
            return "select 1"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.QueryRewriter", FakeRewriter)

    result = runner.invoke(
        app,
        [
            "query",
            "SELECT order_count FROM orders",
            "--models",
            str(tmp_path),
            "--dry-run",
            "--engine",
            "rust",
            "--fallback",
        ],
    )

    assert result.exit_code == 0
    assert captured == {"rewriter": "1", "no_fallback": "0"}


def test_query_uses_config_runtime_engine(monkeypatch, tmp_path):
    _write_min_model(tmp_path)
    captured = {}
    cli_module._loaded_config = SidemanticConfig.model_validate(
        {
            "models_dir": str(tmp_path),
            "runtime": {"engine": "rust", "fallback": True},
        }
    )

    class FakeRewriter:
        def __init__(self, *_args, **_kwargs):
            pass

        def rewrite(self, _sql):
            captured["rewriter"] = os.environ.get("SIDEMANTIC_RS_REWRITER")
            captured["no_fallback"] = os.environ.get("SIDEMANTIC_RS_NO_FALLBACK")
            return "select 1"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.QueryRewriter", FakeRewriter)

    result = runner.invoke(
        app,
        ["query", "SELECT order_count FROM orders", "--models", str(tmp_path), "--dry-run"],
    )

    assert result.exit_code == 0
    assert captured == {"rewriter": "1", "no_fallback": "0"}


def test_rewrite_engine_rust_sets_rewriter_env(monkeypatch, tmp_path):
    _write_min_model(tmp_path)
    captured = {}

    class FakeRewriter:
        def __init__(self, *_args, **_kwargs):
            pass

        def rewrite(self, _sql):
            captured["rewriter"] = os.environ.get("SIDEMANTIC_RS_REWRITER")
            captured["no_fallback"] = os.environ.get("SIDEMANTIC_RS_NO_FALLBACK")
            return "select 1"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.QueryRewriter", FakeRewriter)

    result = runner.invoke(
        app,
        [
            "rewrite",
            "SELECT order_count FROM orders",
            "--models",
            str(tmp_path),
            "--engine",
            "rust",
            "--fallback",
        ],
    )

    assert result.exit_code == 0
    assert "select 1" in result.stdout
    assert captured == {"rewriter": "1", "no_fallback": "0"}


def test_export_native_writes_versioned_yaml(tmp_path):
    _write_min_model(tmp_path)
    output_path = tmp_path / "native.yml"

    result = runner.invoke(app, ["export-native", str(tmp_path), "--output", str(output_path)])

    assert result.exit_code == 0
    output = output_path.read_text()
    assert "version: 1" in output
    assert "name: orders" in output


def test_export_native_validate_rust_calls_bridge(monkeypatch, tmp_path):
    _write_min_model(tmp_path)
    output_path = tmp_path / "native.yml"
    captured = {}

    def fake_load_graph_from_yaml_with_rust(yaml_content):
        captured["yaml"] = yaml_content
        return object()

    monkeypatch.setattr("sidemantic.rust_bridge.load_graph_from_yaml_with_rust", fake_load_graph_from_yaml_with_rust)

    result = runner.invoke(
        app,
        ["export-native", str(tmp_path), "--output", str(output_path), "--validate-rust"],
    )

    assert result.exit_code == 0
    assert "version: 1" in captured["yaml"]


def test_export_native_file_input_loads_parent_directory_context(tmp_path):
    source_dir = tmp_path / "models"
    source_dir.mkdir()
    (source_dir / "orders.yml").write_text(
        """
models:
  - name: orders
    table: orders
"""
    )
    (source_dir / "customers.yml").write_text(
        """
models:
  - name: customers
    table: customers
"""
    )
    output_path = tmp_path / "native.yml"

    result = runner.invoke(app, ["export-native", str(source_dir / "orders.yml"), "--output", str(output_path)])

    assert result.exit_code == 0
    output = output_path.read_text()
    assert "name: orders" in output
    assert "name: customers" in output


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


def test_workbench_missing_extra_prints_install_hint(monkeypatch, tmp_path):
    from sidemantic.workbench import WorkbenchDependencyError

    def fake_run_workbench(directory, demo_mode=False, connection=None):
        raise WorkbenchDependencyError(
            "Missing optional dependency for `sidemantic workbench`: textual. "
            "Install the workbench extra or run it with uvx, for example: "
            "`uvx --from 'sidemantic[workbench]' sidemantic workbench --demo`."
        )

    monkeypatch.setattr("sidemantic.workbench.run_workbench", fake_run_workbench)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["workbench", str(tmp_path)])

    assert result.exit_code == 1
    assert "Missing optional dependency" in result.output
    assert "uvx --from 'sidemantic[workbench]' sidemantic workbench --demo" in result.output


def test_validate_python_runs_without_workbench_extra(monkeypatch, tmp_path):
    for module_name in list(sys.modules):
        if module_name == "sidemantic.workbench" or module_name.startswith("sidemantic.workbench."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    real_import = builtins.__import__

    def blocked_workbench_import(name, *args, **kwargs):
        if name == "sidemantic.workbench" or name.startswith("sidemantic.workbench."):
            raise ImportError("simulated missing workbench extra")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_workbench_import)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["validate", str(tmp_path), "--engine", "python", "--verbose"])

    assert result.exit_code == 0
    assert "Validation Results:" in result.output
    assert "Loaded 1 models" in result.output
    assert "Validation Passed" in result.output


def test_validate_python_fails_on_validation_errors(tmp_path):
    (tmp_path / "models.yml").write_text(
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
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
"""
    )

    result = runner.invoke(app, ["validate", str(tmp_path), "--engine", "python"])

    assert result.exit_code == 1
    assert "relationship to 'customers' which doesn't exist" in result.output
    assert "Validation Failed" in result.output


def test_validate_engine_rust_uses_rust_loader(monkeypatch, tmp_path):
    _write_min_model(tmp_path)
    called = {}

    class FakeGraph:
        models = {"orders": object()}

    def fake_load_graph_from_directory_with_rust(directory):
        called["directory"] = directory
        return FakeGraph()

    monkeypatch.setattr(
        "sidemantic.rust_bridge.load_graph_from_directory_with_rust",
        fake_load_graph_from_directory_with_rust,
    )

    result = runner.invoke(app, ["validate", str(tmp_path), "--engine", "rust", "--verbose"])

    assert result.exit_code == 0
    assert called["directory"] == tmp_path
    assert "Validated 1 models with Rust" in result.stdout
    assert "orders" in result.stdout


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


def test_serve_missing_extra_prints_install_hint(monkeypatch, tmp_path):
    for module_name in list(sys.modules):
        if module_name == "sidemantic.server.server" or module_name.startswith("sidemantic.server."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    real_import = builtins.__import__

    def blocked_server_import(name, *args, **kwargs):
        if name == "sidemantic.server.server":
            raise ImportError("simulated missing serve extra")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_server_import)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["serve", str(tmp_path)])

    assert result.exit_code == 1
    assert "requires the optional serve dependencies" in result.output
    assert "sidemantic[serve]" in result.output


def test_serve_rejects_partial_auth(monkeypatch, tmp_path):
    ensure_fake_riffq()

    def fake_start_server(*args, **kwargs):
        raise AssertionError("start_server should not be called with partial auth config")

    monkeypatch.setattr("sidemantic.server.server.start_server", fake_start_server)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["serve", str(tmp_path), "--username", "u"])

    assert result.exit_code == 1
    assert "both --username and --password" in result.output


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


def test_chart_serve_uses_crossfilter_dashboard(monkeypatch, tmp_path):
    called = {}
    output_dir = tmp_path / "chart-assets"
    db_path = tmp_path / "orders.db"
    _write_min_model(tmp_path)
    _write_orders_db(db_path)

    def fake_serve(self, output_dir_arg, *, host, port):
        called["dashboard"] = self
        called["output_dir"] = output_dir_arg
        called["host"] = host
        called["port"] = port

    monkeypatch.setattr("sidemantic.viz.CrossfilterDashboard.serve", fake_serve)

    result = runner.invoke(
        app,
        [
            "chart",
            "orders.order_count",
            "--by",
            "orders.status",
            "--renderer",
            "crossfilter",
            "--serve",
            "--interaction-preaggregations",
            "--models",
            str(tmp_path),
            "--db",
            str(db_path),
            "--output-dir",
            str(output_dir),
            "--host",
            "0.0.0.0",
            "--port",
            "9001",
        ],
    )

    assert result.exit_code == 0
    assert called["output_dir"] == output_dir
    assert called["host"] == "0.0.0.0"
    assert called["port"] == 9001
    assert called["dashboard"].tabs[0].session.interaction_preaggregations is True


def test_chart_interaction_preaggregations_require_serve(tmp_path):
    _write_min_model(tmp_path)

    result = runner.invoke(
        app,
        [
            "chart",
            "orders.order_count",
            "--renderer",
            "crossfilter",
            "--interaction-preaggregations",
            "--models",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "--interaction-preaggregations requires --serve" in result.output


def test_api_serve_calls_start_server(monkeypatch, tmp_path):
    pytest.importorskip("fastapi")
    called = {}

    def fake_start_api_server(
        layer,
        host,
        port,
        auth_token,
        cors_origins,
        max_request_body_bytes,
        serve_ui=True,
        result_cache_mb=0,
        result_cache_ttl=60.0,
    ):
        called["layer"] = layer
        called["host"] = host
        called["port"] = port
        called["auth_token"] = auth_token
        called["cors_origins"] = cors_origins
        called["max_request_body_bytes"] = max_request_body_bytes
        called["serve_ui"] = serve_ui
        called["result_cache_mb"] = result_cache_mb
        called["result_cache_ttl"] = result_cache_ttl

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


def test_docker_entrypoint_does_not_use_eval():
    entrypoint_path = Path(__file__).resolve().parent.parent / "docker-entrypoint.sh"
    content = entrypoint_path.read_text()

    assert "eval " not in content


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
