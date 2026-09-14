"""Query parameters are rendered by the selected compiler before filtering."""

import pytest

from sidemantic import Parameter
from tests.semantic_conformance import test_semantic_sql_binding

layer = test_semantic_sql_binding.layer


@pytest.mark.parametrize("include_pending,expected", [(False, 250), (True, 450)])
def test_conditional_filter_parameters_use_native_runtime(layer, include_pending, expected):
    layer.graph.add_parameter(Parameter(name="include_pending", type="yesno", default_value=False))
    expression = (
        "{% if include_pending %}orders.status in ('done', 'pending'){% else %}orders.status = 'done'{% endif %}"
    )
    result = layer.query(
        metrics=["orders.revenue"],
        filters=[expression],
        parameters={"include_pending": include_pending},
    )
    assert result.fetchall() == [(expected,)]
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"


def test_parameter_default_and_override_survive_post_process(layer):
    layer.graph.add_parameter(Parameter(name="selected_status", type="string", default_value="done"))
    options = {
        "metrics": ["orders.revenue"],
        "filters": ["orders.status = {{ selected_status }}"],
        "post_process": "with multiplier as (select 2 as factor) "
        "select revenue * factor as result from ({inner}) q cross join multiplier",
    }
    assert layer.query(**options).fetchall() == [(500,)]
    assert layer.query(**options, parameters={"selected_status": "pending"}).fetchall() == [(400,)]
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
