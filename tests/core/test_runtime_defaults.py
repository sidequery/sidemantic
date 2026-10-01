"""Default runtime dispatch, with the native boundary supplied by a test double."""

import sys
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

import sidemantic.core.semantic_layer as semantic_layer_module
import sidemantic.runtime as runtime
import sidemantic.rust_bridge as rust_bridge
from sidemantic import Metric, Model, SemanticLayer
from sidemantic.config import RuntimeConfig
from sidemantic.semantic_handoff import RustBackendUnavailableError


@pytest.fixture(autouse=True)
def normal_runtime_environment(monkeypatch):
    monkeypatch.delenv("SIDEMANTIC_ENGINE", raising=False)
    for key in list(runtime.os.environ):
        if key.startswith("SIDEMANTIC_RS_"):
            monkeypatch.delenv(key)


class Runtime:
    def validate_with_semantic_input(self, *_args):
        return []

    def compile_with_semantic_input(self, *_args):
        return "SELECT 42 AS count"

    def rewrite_with_semantic_input(self, *_args):
        return "SELECT 42 AS count"

    def rewrite_with_semantic_input_context(self, *_args):
        return "SELECT 42 AS count"


def _count_layer(**kwargs):
    layer = SemanticLayer(auto_register=False, **kwargs)
    layer.add_model(Model(name="orders", table="orders", metrics=[Metric(name="count", agg="count")]))
    layer.adapter.execute("create table orders(id integer); insert into orders values (1), (2)")
    return layer


def test_default_compiles_with_python_without_native_import(monkeypatch):
    def unexpected_native_import():
        raise AssertionError("The Python default must not initialize Rust")

    monkeypatch.setattr(semantic_layer_module, "get_rust_module", unexpected_native_import)
    monkeypatch.setattr(rust_bridge, "get_rust_module", unexpected_native_import)
    with _count_layer() as layer:
        assert layer.adapter.execute(layer.compile(metrics=["orders.count"])).fetchall() == [(2,)]
        assert layer.last_engine_selection == {"engine": "python", "reason": None}
    assert RuntimeConfig().engine == "python"


def test_explicit_rust_compiles_with_native_runtime(monkeypatch):
    native = Runtime()
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: native)
    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: native)
    with _count_layer(engine="rust") as layer:
        assert layer.adapter.execute(layer.compile(metrics=["orders.count"])).fetchall() == [(42,)]
        assert layer.last_engine_selection == {"engine": "rust", "reason": None}


def test_missing_explicit_rust_runtime_does_not_silently_fall_back(monkeypatch):
    def unavailable():
        raise RustBackendUnavailableError("missing native package")

    monkeypatch.setattr(semantic_layer_module, "get_rust_module", unavailable)
    with pytest.raises(RustBackendUnavailableError, match="missing native package"):
        SemanticLayer(engine="rust")
    assert SemanticLayer(engine="python").engine == "python"
    assert SemanticLayer(engine="auto")._rust_unavailable_reason == "missing native package"


def test_pyodide_defaults_to_python_without_native_import(monkeypatch):
    monkeypatch.setattr(sys, "platform", "emscripten")
    assert RuntimeConfig().engine == "python"
    assert SemanticLayer().engine == "python"


@pytest.mark.parametrize("engine", ["python", "rust", "auto"])
@pytest.mark.parametrize("legacy_value", ["0", "1"])
def test_process_override_takes_precedence_over_legacy_flags(monkeypatch, engine, legacy_value):
    monkeypatch.setenv("SIDEMANTIC_ENGINE", engine)
    for flag in ("SQL_GENERATOR", "QUERY_VALIDATION", "REWRITER", "SQL_GENERATOR_VERIFY", "NO_FALLBACK"):
        monkeypatch.setenv(f"SIDEMANTIC_RS_{flag}", legacy_value)
    native = Runtime()
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: native)
    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: native)
    expected_engine = "python" if engine == "python" else "rust"
    expected_rows = [(2,)] if engine == "python" else [(42,)]
    with _count_layer() as layer:
        assert layer.engine == engine
        assert layer.adapter.execute(layer.compile(metrics=["orders.count"])).fetchall() == expected_rows
        assert layer.last_engine_selection == {"engine": expected_engine, "reason": None}
        assert layer.sql("select count from orders").fetchall() == expected_rows
        assert layer.last_engine_selection == {"engine": expected_engine, "reason": None}
        assert layer._rust_no_fallback is (engine == "rust")


@pytest.mark.parametrize("process_engine,explicit_engine", [("python", "rust"), ("rust", "python")])
def test_explicit_selection_overrides_process_engine(monkeypatch, process_engine, explicit_engine):
    monkeypatch.setenv("SIDEMANTIC_ENGINE", process_engine)
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", Runtime)
    with _count_layer(engine=explicit_engine) as layer:
        assert layer.engine == explicit_engine


def test_invalid_process_override_is_not_hidden_by_legacy_flags(monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_ENGINE", "sideways")
    monkeypatch.setenv("SIDEMANTIC_RS_SQL_GENERATOR", "0")
    with pytest.raises(ValueError, match="SIDEMANTIC_ENGINE must be one of"):
        SemanticLayer()


def test_cli_default_and_fallback_override(monkeypatch):
    import sidemantic.cli as cli

    monkeypatch.setattr(cli, "_loaded_config", None)
    assert cli._resolve_engine_options(None, None) == ("python", False)
    with pytest.raises(typer.BadParameter, match="only meaningful with the rust or auto engine"):
        cli._resolve_engine_options(None, True)
    assert cli._resolve_engine_options("rust", True) == ("rust", True)
    assert cli._resolve_engine_options("auto", None) == ("auto", True)
    monkeypatch.setattr(sys, "platform", "emscripten")
    assert cli._resolve_engine_options(None, None) == ("python", False)


def test_default_sql_rewrite_uses_python_without_native_import(monkeypatch):
    def unexpected_native_import():
        raise AssertionError("The Python default must not initialize Rust")

    monkeypatch.setattr(semantic_layer_module, "get_rust_module", unexpected_native_import)
    monkeypatch.setattr(rust_bridge, "get_rust_module", unexpected_native_import)
    with _count_layer() as layer:
        assert layer.sql("select count from orders").fetchall() == [(2,)]
        assert layer.last_engine_selection == {"engine": "python", "reason": None}


@pytest.mark.parametrize("command", [["query", "--dry-run"], ["rewrite"]])
def test_cli_default_compiles_supported_tmdl_metric(monkeypatch, command):
    import sidemantic.cli as cli

    monkeypatch.setattr(cli, "_loaded_config", None)
    monkeypatch.setattr(cli, "_project_context", None)
    models = Path(__file__).parents[1] / "fixtures" / "tmdl"
    result = CliRunner().invoke(cli.app, [*command, 'SELECT Sales."Total Sales" FROM Sales', "--models", str(models)])
    assert result.exit_code == 0, result.output
    assert "SUM(" in result.stdout
