"""Explicit runtime engine selection tests."""

import pytest

import sidemantic.core.semantic_layer as semantic_layer_module
import sidemantic.rust_bridge as rust_bridge
from sidemantic import Dimension, Metric, Model, SemanticLayer


class FakeRustModule:
    def validate_with_semantic_input(self, _input_json, _query_json):
        return []

    def compile_with_semantic_input(self, _input_json, _query_json):
        return "SELECT 1 AS from_rust"


def test_semantic_layer_rust_engine_uses_rust_compile_without_sql_string_verification(monkeypatch):
    fake = FakeRustModule()
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: fake)
    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: fake)

    layer = SemanticLayer(engine="rust")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])

    assert sql.startswith("SELECT 1 AS from_rust")
    assert layer._rust_no_fallback is True
    assert layer._rust_sql_verify is False


def test_semantic_layer_python_engine_ignores_rust_env_flags(monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_RS_SQL_GENERATOR", "1")
    monkeypatch.setenv("SIDEMANTIC_RS_QUERY_VALIDATION", "1")

    layer = SemanticLayer(engine="python")

    assert layer._use_rust_sql_generator is False
    assert layer._use_rust_query_validation is False


def test_semantic_layer_invalid_engine_rejected():
    with pytest.raises(ValueError, match="engine must be one of"):
        SemanticLayer(engine="sideways")


def _engine_layer(monkeypatch, engine, fallback=None):
    fake = FakeRustModule()
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: fake)
    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: fake)
    layer = SemanticLayer(engine=engine, fallback=fallback, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[
                Dimension(name="created_at", type="time", granularity="day"),
                Dimension(name="status", type="categorical"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    return layer


@pytest.mark.parametrize(
    "engine,fallback,reject",
    [
        ("python", None, False),
        ("rust", None, True),
        ("auto", None, False),
        ("rust", True, False),
        ("auto", False, True),
    ],
)
@pytest.mark.parametrize(
    "capability",
    [
        "query.timezone",
        "query.totals",
        "query.consumption_base_model",
        "query.aliases",
    ],
)
def test_engine_selection_rejects_or_reports_each_unsupported_requirement(
    monkeypatch, engine, fallback, reject, capability
):
    from sidemantic.core.consumption import Explore
    from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError

    layer = _engine_layer(monkeypatch, engine, fallback)
    kwargs = {"metrics": ["orders.revenue"]}
    if capability == "query.timezone":
        kwargs.update(timezone="America/Los_Angeles", dimensions=["orders.created_at__day"])
    elif capability == "query.totals":
        kwargs.update(with_totals=True, dimensions=["orders.status"])
    elif capability == "query.consumption_base_model":
        layer.add_explore(Explore(name="sales", model="orders"))
        kwargs["explore"] = "sales"
    elif capability == "query.aliases":
        kwargs["aliases"] = {"orders.revenue": "total_revenue"}

    def reject_rust(*args, **kwargs):
        raise AssertionError("Unsupported input must not reach Rust validation or compilation")

    monkeypatch.setattr(rust_bridge, "validate_semantic_input", reject_rust)
    monkeypatch.setattr(layer, "_compile_with_rust", reject_rust)
    if reject:
        with pytest.raises(UnsupportedSemanticFeaturesError) as exc:
            layer.compile(**kwargs)
        assert capability in exc.value.capabilities
    else:
        sql = layer.compile(**kwargs)
        assert "SELECT" in sql
        assert layer.last_engine_selection["engine"] == "python"
        if engine != "python":
            assert capability in layer.last_engine_selection["reason"]
        else:
            assert layer.last_engine_selection["reason"] is None


@pytest.mark.parametrize(
    "failure", [ValueError("invalid model"), RuntimeError("compiler failed"), TypeError("bad API")]
)
@pytest.mark.parametrize("stage", ["initialize", "validate", "compile"])
def test_auto_engine_propagates_unexpected_failures(monkeypatch, stage, failure):
    def fail(*args, **kwargs):
        raise failure

    if stage == "initialize":
        monkeypatch.setattr(semantic_layer_module, "get_rust_module", fail)
        with pytest.raises(type(failure), match=str(failure)):
            SemanticLayer(engine="auto")
        return
    layer = _engine_layer(monkeypatch, "auto")
    if stage == "validate":
        monkeypatch.setattr(rust_bridge, "validate_semantic_input", fail)
    else:
        monkeypatch.setattr(layer._rust_module, "compile_with_semantic_input", fail)
    with pytest.raises(type(failure), match=str(failure)):
        layer.compile(metrics=["orders.revenue"])


def test_auto_engine_reports_missing_backend(monkeypatch):
    from sidemantic.semantic_handoff import RustBackendUnavailableError

    def unavailable():
        raise RustBackendUnavailableError("extension not installed")

    monkeypatch.setattr(semantic_layer_module, "get_rust_module", unavailable)
    layer = SemanticLayer(engine="auto", auto_register=False)
    layer.add_model(Model(name="orders", table="orders", metrics=[Metric(name="count", agg="count")]))
    assert "SELECT" in layer.compile(metrics=["orders.count"])
    assert layer.last_engine_selection == {"engine": "python", "reason": "extension not installed"}


def test_auto_engine_does_not_compile_with_rust_after_unsupported_validation(monkeypatch):
    from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError

    layer = _engine_layer(monkeypatch, "auto")

    def unsupported(*args, **kwargs):
        raise UnsupportedSemanticFeaturesError(["future_requirement"])

    monkeypatch.setattr(rust_bridge, "validate_semantic_input", unsupported)
    monkeypatch.setattr(layer, "_compile_with_rust", lambda **kwargs: pytest.fail("must preserve validation fallback"))
    assert "SELECT" in layer.compile(metrics=["orders.revenue"])
    assert "future_requirement" in layer.last_engine_selection["reason"]


@pytest.mark.parametrize("engine", ["rust", "auto"])
def test_engine_does_not_fall_back_when_compiler_returns_no_sql(monkeypatch, engine):
    layer = _engine_layer(monkeypatch, engine)
    monkeypatch.setattr(layer, "_compile_with_rust", lambda **kwargs: None)
    monkeypatch.setattr(layer, "_compile_with_python", lambda **kwargs: pytest.fail("unexpected fallback"))
    with pytest.raises(ValueError, match="returned no SQL"):
        layer.compile(metrics=["orders.revenue"])


@pytest.mark.parametrize("sql", ["", "  \n"])
def test_auto_engine_rejects_empty_compiler_output(monkeypatch, sql):
    layer = _engine_layer(monkeypatch, "auto")
    monkeypatch.setattr(layer._rust_module, "compile_with_semantic_input", lambda *args: sql)
    with pytest.raises(ValueError, match="returned empty SQL"):
        layer.compile(metrics=["orders.revenue"])


@pytest.mark.parametrize("engine,reject", [("rust", True), ("auto", False)])
def test_compiler_capability_failure_obeys_engine_mode(monkeypatch, engine, reject):
    from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError

    layer = _engine_layer(monkeypatch, engine)

    def unsupported(*args):
        raise UnsupportedSemanticFeaturesError(["future_requirement"])

    monkeypatch.setattr(layer._rust_module, "compile_with_semantic_input", unsupported)
    if reject:
        with pytest.raises(UnsupportedSemanticFeaturesError, match="future_requirement"):
            layer.compile(metrics=["orders.revenue"])
    else:
        assert "SELECT" in layer.compile(metrics=["orders.revenue"])
        assert layer.last_engine_selection["engine"] == "python"
        assert "future_requirement" in layer.last_engine_selection["reason"]
