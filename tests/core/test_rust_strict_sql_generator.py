"""Strict-mode behavior tests for Rust SQL generator entrypoint."""

import pytest
import yaml

import sidemantic.core.semantic_layer as semantic_layer_module
import sidemantic.rust_parity as rust_parity
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_layer import SemanticLayer


@pytest.fixture(autouse=True)
def _reset_strict_targets_cache():
    rust_parity.strict_targets.cache_clear()
    yield
    rust_parity.strict_targets.cache_clear()


def _configure_strict_sql_entrypoint(monkeypatch) -> None:
    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "sql_generator_entrypoint")
    monkeypatch.delenv("SIDEMANTIC_RS_SQL_GENERATOR", raising=False)
    monkeypatch.delenv("SIDEMANTIC_RS_SQL_GENERATOR_VERIFY", raising=False)
    rust_parity.strict_targets.cache_clear()


def _build_layer(monkeypatch) -> SemanticLayer:
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: object())
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    return layer


def test_strict_sql_entrypoint_forces_rust_compile(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)

    calls = {"rust": 0, "python": 0}

    def fake_rust_compile(**_kwargs):
        calls["rust"] += 1
        return "SELECT 1"

    def fake_python_compile(**_kwargs):
        calls["python"] += 1
        return "SELECT 2"

    monkeypatch.setattr(layer, "_compile_with_rust", fake_rust_compile)
    monkeypatch.setattr(layer, "_compile_with_python", fake_python_compile)

    sql = layer.compile(metrics=["orders.revenue"])
    assert sql.startswith("SELECT 1")
    assert calls["rust"] == 1
    assert calls["python"] == 0
    assert layer._use_rust_sql_generator is True


def test_strict_sql_entrypoint_rejects_python_fallback(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)

    monkeypatch.setattr(layer, "_compile_with_rust", lambda **_kwargs: None)
    monkeypatch.setattr(
        layer,
        "_compile_with_python",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("python fallback should not run")),
    )

    with pytest.raises(ValueError, match="returned no SQL in strict mode"):
        layer.compile(metrics=["orders.revenue"])


def test_strict_sql_entrypoint_disables_python_verify(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)
    assert layer._rust_sql_verify is False


def test_rust_compile_payload_includes_preaggregation_flags(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)
    captured = {}

    class FakeRustModule:
        def compile_with_yaml(self, _models_yaml, query_yaml):
            captured.update(yaml.safe_load(query_yaml))
            return "SELECT 1"

    layer._rust_module = FakeRustModule()
    sql = layer.compile(metrics=["orders.revenue"], offset=10, use_preaggregations=True)

    assert sql.startswith("SELECT 1")
    assert "OFFSET" not in sql
    assert captured["offset"] == 10
    assert captured["use_preaggregations"] is True
    assert "preagg_database" in captured
    assert "preagg_schema" in captured


def test_rust_compile_transpiles_from_rust_output_dialect(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)
    layer.dialect = "bigquery"

    class FakeRustModule:
        def compile_with_yaml(self, _models_yaml, _query_yaml):
            return "SELECT DATE_TRUNC('month', order_date) AS order_month FROM orders_cte"

    layer._rust_module = FakeRustModule()
    sql = layer.compile(metrics=["orders.revenue"], dialect=None)

    assert "DATE_TRUNC(order_date, MONTH)" in sql
    assert "DATE_TRUNC('month', order_date)" not in sql


def test_rust_compile_payload_includes_complex_metric_fields(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: object())
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="event_id",
            metrics=[
                Metric(
                    name="signup_funnel",
                    type="conversion",
                    entity="user_id",
                    steps=["event_type = 'signup'", "event_type = 'purchase'"],
                ),
                Metric(
                    name="signup_retention",
                    type="retention",
                    entity="user_id",
                    cohort_event="event_type = 'signup'",
                    activity_event="event_type = 'active'",
                    periods=7,
                    retention_granularity="day",
                ),
                Metric(
                    name="multi_platform_users",
                    type="cohort",
                    entity="user_id",
                    inner_metrics=[{"name": "platform_count", "agg": "count_distinct", "sql": "platform"}],
                    having="platform_count >= 2",
                    agg="count",
                ),
            ],
        )
    )
    captured = {}

    class FakeRustModule:
        def compile_with_yaml(self, models_yaml, _query_yaml):
            captured.update(yaml.safe_load(models_yaml))
            return "SELECT 1"

    layer._rust_module = FakeRustModule()
    sql = layer.compile(metrics=["events.signup_funnel"])

    assert sql.startswith("SELECT 1")
    metrics = {metric["name"]: metric for metric in captured["models"][0]["metrics"]}
    assert metrics["signup_funnel"]["steps"] == ["event_type = 'signup'", "event_type = 'purchase'"]
    assert metrics["signup_retention"]["cohort_event"] == "event_type = 'signup'"
    assert metrics["signup_retention"]["periods"] == 7
    assert metrics["signup_retention"]["retention_granularity"] == "day"
    assert metrics["multi_platform_users"]["inner_metrics"] == [
        {"name": "platform_count", "agg": "count_distinct", "sql": "platform"}
    ]
    assert metrics["multi_platform_users"]["having"] == "platform_count >= 2"
