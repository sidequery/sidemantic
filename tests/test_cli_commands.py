"""Tests for CLI command wiring."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from sidemantic.cli import app

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
    pytest.importorskip("riffq")
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


def test_serve_rejects_partial_auth(monkeypatch, tmp_path):
    pytest.importorskip("riffq")

    def fake_start_server(*args, **kwargs):
        raise AssertionError("start_server should not be called with partial auth config")

    monkeypatch.setattr("sidemantic.server.server.start_server", fake_start_server)

    _write_min_model(tmp_path)
    result = runner.invoke(app, ["serve", str(tmp_path), "--username", "u"])

    assert result.exit_code == 1
    assert "both --username and --password" in result.output


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


def test_cli_source_uses_public_adapter():
    """cli.py should not reference ._adapter anywhere."""
    import os

    cli_path = os.path.join(os.path.dirname(__file__), "..", "sidemantic", "cli.py")
    with open(cli_path) as f:
        content = f.read()

    assert "._adapter" not in content, "CLI should use .adapter, not ._adapter"


def test_mcp_serve_calls_initialize(monkeypatch, tmp_path):
    pytest.importorskip("mcp")
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
