"""CLI engine selection is explicit and does not leak into later invocations."""

import os

import pytest
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app
from sidemantic.semantic_handoff import RustBackendUnavailableError


@pytest.fixture
def models(tmp_path, monkeypatch):
    monkeypatch.setattr("sidemantic.core.semantic_layer.get_rust_module", lambda: object())
    monkeypatch.setattr(cli_module, "_loaded_config", None)
    monkeypatch.setattr(cli_module, "_project_context", None)
    path = tmp_path / "orders.yml"
    path.write_text(
        "models:\n- name: orders\n  table: orders\n  metrics:\n  - name: revenue\n    agg: sum\n    sql: amount\n"
    )
    return path


@pytest.mark.parametrize("command", [["rewrite"], ["query", "--dry-run"]])
def test_explicit_cli_engine_is_forwarded_without_environment_mutation(models, monkeypatch, command):
    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "0")
    before = dict(os.environ)
    calls = []

    def rewrite(graph, sql, **kwargs):
        calls.append(sql)
        return "SELECT 42 AS rust_result"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", rewrite)
    runner = CliRunner()
    arguments = [*command, "SELECT revenue FROM orders", "--models", str(models)]
    result = runner.invoke(app, [*arguments, "--engine", "rust"])
    assert result.exit_code == 0, result.output
    assert "42 AS rust_result" in result.stdout
    assert calls == ["SELECT revenue FROM orders"]
    assert dict(os.environ) == before
    result = runner.invoke(app, [*arguments, "--engine", "python"])
    assert result.exit_code == 0, result.output
    assert "SUM(" in result.stdout
    assert len(calls) == 1
    assert dict(os.environ) == before


@pytest.mark.parametrize("engine,success", [("rust", False), ("auto", True)])
def test_cli_typed_unavailable_respects_engine_fallback(models, monkeypatch, engine, success):
    def unavailable(*args, **kwargs):
        raise RustBackendUnavailableError("semantic entrypoint missing")

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", unavailable)
    result = CliRunner().invoke(
        app, ["rewrite", "SELECT revenue FROM orders", "--models", str(models), "--engine", engine]
    )
    assert (result.exit_code == 0) is success, result.output
    assert ("SUM(" in result.stdout) is success
    if success:
        assert "Using Python engine" in result.stderr
        assert "semantic entrypoint missing" in result.stderr
        assert "Using Python" not in result.stdout


def test_cli_auto_propagates_unexpected_compiler_failure(models, monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("compiler defect")

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", broken)
    result = CliRunner().invoke(
        app, ["rewrite", "SELECT revenue FROM orders", "--models", str(models), "--engine", "auto"]
    )
    assert result.exit_code != 0
    assert "compiler defect" in result.output


@pytest.mark.parametrize("source", ["native", "cube"])
def test_validate_uses_source_adapter_and_versioned_input(models, monkeypatch, source):
    import json

    if source == "cube":
        models.write_text(
            "cubes:\n- name: orders\n  sql_table: orders\n  measures:\n  - name: revenue\n    type: sum\n    sql: amount\n"
        )
    calls = []

    class RustModule:
        def validate_with_semantic_input(self, payload, query):
            calls.append((json.loads(payload), json.loads(query)))
            return []

    monkeypatch.setattr("sidemantic.rust_bridge.get_rust_module", RustModule)
    result = CliRunner().invoke(app, ["validate", str(models.parent), "--engine", "rust", "--json"])
    assert result.exit_code == 0, result.output
    payload, query = calls[0]
    assert len(calls) == 1
    assert payload["version"] == 1
    assert payload["models"][0]["name"] == "orders"
    assert payload["models"][0]["metrics"][0]["sql"] == "amount"
    assert query == {"metrics": [], "dimensions": []}
    assert json.loads(result.stdout)["rust_models"] == ["orders"]


@pytest.mark.parametrize("engine,success", [("rust", False), ("auto", True)])
def test_validate_carries_unsupported_policy_to_runtime(models, monkeypatch, engine, success):
    import json

    from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError

    models.write_text(
        models.read_text().replace("  table: orders", "  table: orders\n  invariant_filters: [amount > 0]")
    )
    calls = []

    class RustModule:
        def validate_with_semantic_input(self, payload, query):
            semantic_input = json.loads(payload)
            calls.append(semantic_input)
            assert semantic_input["models"][0]["invariant_filters"] == ["amount > 0"]
            assert "model.invariant_filters" in semantic_input["required_capabilities"]
            raise UnsupportedSemanticFeaturesError(["model.invariant_filters"])

    monkeypatch.setattr("sidemantic.rust_bridge.get_rust_module", RustModule)
    result = CliRunner().invoke(app, ["validate", str(models.parent), "--engine", engine, "--json"])
    assert (result.exit_code == 0) is success, result.output
    assert len(calls) == 1
    report = json.loads(result.stdout)
    assert report["rust_models"] is None
    assert "model.invariant_filters" in str(report)
    if success:
        assert "Using Python validation" in result.stderr


def test_validate_auto_does_not_hide_compiler_defect(models, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("compiler defect")

    monkeypatch.setattr("sidemantic.rust_bridge.validate_semantic_input", fail)
    result = CliRunner().invoke(app, ["validate", str(models.parent), "--engine", "auto", "--json"])
    assert result.exit_code == 1
    assert "compiler defect" in result.stdout
    assert "Using Python validation" not in result.stderr


def test_python_validation_ignores_rust_environment(models, monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_RS_SQL_GENERATOR", "1")
    monkeypatch.setenv("SIDEMANTIC_RS_NO_FALLBACK", "1")

    def fail():
        raise AssertionError("Python authoring validation must not initialize Rust")

    monkeypatch.setattr("sidemantic.core.semantic_layer.get_rust_module", fail)
    result = CliRunner().invoke(app, ["validate", str(models.parent), "--engine", "python", "--json"])
    assert result.exit_code == 0, result.output


def test_validate_runtime_uses_selected_ossie_scope(models, monkeypatch):
    models.unlink()
    (models.parent / "multiple.ossie.yaml").write_text(
        """version: 0.2.0.dev0
semantic_model:
  - name: finance
    datasets:
      - {name: orders, source: finance.orders}
  - name: marketing
    datasets:
      - {name: orders, source: marketing.orders}
"""
    )
    tables = []

    def validate(graph, *args, **kwargs):
        tables.append(graph.models["orders"].table)
        return []

    monkeypatch.setattr("sidemantic.rust_bridge.validate_semantic_input", validate)
    result = CliRunner().invoke(
        app, ["validate", str(models.parent), "--engine", "rust", "--ossie-scope", "marketing", "--json"]
    )
    assert result.exit_code == 0, result.output
    assert tables == ["marketing.orders"]
