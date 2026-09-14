"""Test adapter transport preserves production wrapper and parameter contracts."""

import pytest
import yaml

from sidemantic import Metric, Model, Parameter
from tests.rust_layer_adapter import RustSemanticLayerAdapter


@pytest.fixture
def layer(monkeypatch):
    requests = []

    def native_compile(self, payload):
        requests.append(payload)
        return {"status": "ok", "sql": "with native_rows as (select 7 as total) select * from native_rows"}

    monkeypatch.setattr(RustSemanticLayerAdapter, "_rust_request", native_compile)
    layer = RustSemanticLayerAdapter()
    try:
        yield layer, requests
    finally:
        layer.close()


def test_parameters_reach_native_interpolation_with_declared_types(layer):
    layer, requests = layer
    layer.graph.add_parameter(Parameter(name="include_pending", type="yesno", default_value=False))
    expression = "{% if include_pending %}status = 'pending'{% else %}status = 'done'{% endif %}"
    layer.compile(metrics=["orders.total"], filters=[expression], parameters={"include_pending": True})
    assert requests[-1]["parameter_values"] == {"include_pending": True}
    assert requests[-1]["filters"] == [expression]
    parameters = yaml.safe_load(requests[-1]["models_yaml"])["parameters"]
    assert parameters[0]["name"] == "include_pending"
    assert parameters[0]["type"] == "yesno"
    assert parameters[0]["default_value"] is False


def test_post_process_reuses_production_wrapper_and_keeps_cte_scopes(layer):
    layer, _ = layer
    result = layer.query(
        metrics=["orders.total"],
        post_process="with native_rows as (select 3 as factor) "
        "select total * factor from ({inner}) q cross join native_rows",
    )
    assert result.fetchall() == [(21,)]


def test_post_process_requires_inner_placeholder(layer):
    layer, _ = layer
    with pytest.raises(ValueError, match="must contain a"):
        layer.compile(metrics=["orders.total"], post_process="select 99")


def test_unowned_graph_metric_keeps_its_registration_scope(layer):
    layer, requests = layer
    layer.add_model(Model(name="orders", table="orders", primary_key="id"))
    layer.add_model(Model(name="customers", table="customers", primary_key="id"))
    layer.add_metric(Metric(name="total_orders", type="derived", sql="COUNT(*)"))
    document = yaml.safe_load(requests[-1]["models_yaml"])
    assert [metric["name"] for metric in document["graph_metrics"]] == ["total_orders"]
    assert "metrics" not in document
    assert all(not model["metrics"] for model in document["models"])
    assert layer.graph.metrics["total_orders"].sql == "COUNT(*)"


@pytest.mark.parametrize(
    "default,override,expected", [(False, None, False), (True, None, True), (True, False, False), (False, True, True)]
)
def test_preaggregation_options_reach_native_compiler(layer, default, override, expected):
    layer, requests = layer
    layer.use_preaggregations = default
    layer.preagg_database = "warehouse"
    layer.preagg_schema = "rollups"
    layer.compile(metrics=["orders.total"], use_preaggregations=override)
    assert requests[-1]["use_preaggregations"] is expected
    assert requests[-1]["preagg_database"] == "warehouse"
    assert requests[-1]["preagg_schema"] == "rollups"


def test_generator_preaggregation_options_reach_same_native_transport(monkeypatch):
    from sidemantic.core.semantic_graph import SemanticGraph
    from tests import rust_layer_adapter

    requests = []

    def native_compile(payload):
        requests.append(payload)
        return {"status": "ok", "sql": "select 1"}

    monkeypatch.setattr(rust_layer_adapter, "_rust_request", native_compile)
    generator = rust_layer_adapter.RustSQLGeneratorAdapter(SemanticGraph(), preagg_schema="rollups")
    assert generator.generate(metrics=["orders.total"], use_preaggregations=True) == "select 1"
    assert requests[-1]["use_preaggregations"] is True
    assert requests[-1]["preagg_schema"] == "rollups"
