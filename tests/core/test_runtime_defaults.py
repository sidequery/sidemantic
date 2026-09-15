"""Default runtime dispatch, with the native boundary supplied by a test double."""

import pytest

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


def test_default_compiles_with_rust(monkeypatch):
    native = Runtime()
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: native)
    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: native)
    layer = SemanticLayer()
    layer.add_model(Model(name="orders", table="orders", metrics=[Metric(name="count", agg="count")]))
    assert "42" in layer.compile(metrics=["orders.count"])
    assert layer.last_engine_selection == {"engine": "rust", "reason": None}
    assert RuntimeConfig().engine == "rust"


def test_missing_default_runtime_does_not_silently_fall_back(monkeypatch):
    def unavailable():
        raise RustBackendUnavailableError("missing native package")

    monkeypatch.setattr(semantic_layer_module, "get_rust_module", unavailable)
    with pytest.raises(RustBackendUnavailableError, match="missing native package"):
        SemanticLayer()
    assert SemanticLayer(engine="python").engine == "python"
    assert SemanticLayer(engine="auto")._rust_unavailable_reason == "missing native package"


def test_pyodide_defaults_to_python_without_native_import(monkeypatch):
    monkeypatch.setattr(runtime.sys, "platform", "emscripten")
    assert RuntimeConfig().engine == "python"
    assert SemanticLayer().engine == "python"


def test_process_override_and_explicit_selection(monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_ENGINE", "python")
    assert SemanticLayer().engine == "python"
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", Runtime)
    assert SemanticLayer(engine="rust").engine == "rust"


def test_cli_default_and_fallback_override(monkeypatch):
    import sidemantic.cli as cli

    monkeypatch.setattr(cli, "_loaded_config", None)
    assert cli._resolve_engine_options(None, None) == ("rust", False)
    assert cli._resolve_engine_options(None, True) == ("rust", True)
    assert cli._resolve_engine_options("auto", None) == ("auto", True)
    monkeypatch.setattr(runtime.sys, "platform", "emscripten")
    assert cli._resolve_engine_options(None, None) == ("python", False)


def test_default_sql_rewrite_uses_rust(monkeypatch):
    class RewritingRuntime(Runtime):
        def rewrite_with_semantic_input(self, *_args):
            return "SELECT 42 AS count"

        def rewrite_with_semantic_input_context(self, *_args):
            return "SELECT 42 AS count"

    native = RewritingRuntime()
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: native)
    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: native)
    layer = SemanticLayer()
    layer.add_model(Model(name="orders", table="orders", metrics=[Metric(name="count", agg="count")]))
    assert layer.sql("select count from orders").fetchall() == [(42,)]
    assert layer.last_engine_selection == {"engine": "rust", "reason": None}
