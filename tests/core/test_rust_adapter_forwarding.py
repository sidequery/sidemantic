"""Test adapter transport preserves production wrapper and parameter contracts."""

import pytest
import yaml

from sidemantic import Parameter
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
