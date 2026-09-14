"""Selected segments share parameter semantics without becoming caller filters."""

import pytest
import sqlglot

from sidemantic import Dimension, Metric, Model, Parameter, Segment, SemanticLayer
from sidemantic.rust_bridge import compile_semantic_input
from sidemantic.semantic_handoff import graph_to_semantic_input
from sidemantic.sql.generator import SQLGenerator


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires matching native extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="label", type="categorical"),
                Dimension(name="region", type="categorical"),
                Dimension(name="enabled", type="boolean"),
                Dimension(name="amount", type="numeric"),
            ],
            metrics=[Metric(name="total", agg="sum", sql="amount")],
            segments=[Segment(name="unused", sql="{% if missing")],
        )
    )
    for parameter in [
        Parameter(name="label", type="string", default_value="plain"),
        Parameter(name="minimum", type="number", default_value=15),
        Parameter(name="enabled", type="yesno", default_value=True),
        Parameter(name="region", type="string", default_value="east"),
        Parameter(name="field", type="unquoted", default_value="amount"),
    ]:
        layer.graph.add_parameter(parameter)
    layer.adapter.execute(
        "CREATE TABLE orders(id INTEGER, label VARCHAR, region VARCHAR, enabled BOOLEAN, amount INTEGER)"
    )
    for row in [
        (1, "plain", "outer", True, 10),
        (2, "O'Reilly\\folder", "outer", True, 20),
        (3, "\\' OR 1=1 --", "outer", False, 30),
        (4, "é prefix O'Reilly\\folder suffix", "outer", True, 40),
        (5, "123", "outer", True, 50),
    ]:
        layer.conn.execute("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", row)
    layer.adapter.execute("CREATE TABLE allowed_orders(order_id INTEGER, region VARCHAR)")
    layer.adapter.execute("INSERT INTO allowed_orders VALUES (1, 'west'), (2, 'east'), (4, 'east')")
    yield layer
    layer.adapter.close()


def rows(layer, dialect, sql, parameters=None, *, as_filter=False):
    model = layer.graph.models["orders"]
    model.segments = [segment for segment in model.segments if segment.name != "selected"]
    model.segments.append(Segment(name="selected", sql=sql))
    before = graph_to_semantic_input(layer.graph, input_dialect=dialect)
    selections = {"filters": [sql]} if as_filter else {"segments": ["orders.selected"]}
    if layer.engine == "rust":
        generated = compile_semantic_input(
            layer.graph,
            {"metrics": ["orders.total"], **selections, "parameter_values": parameters or {}, "dialect": "duckdb"},
            input_dialect=dialect,
        )
    else:
        generated = SQLGenerator(layer.graph, dialect=dialect).generate(
            metrics=["orders.total"], **selections, parameters=parameters
        )
        generated = sqlglot.transpile(generated, read=dialect, write="duckdb")[0]
    assert graph_to_semantic_input(layer.graph, input_dialect=dialect) == before
    return layer.adapter.execute(generated).fetchall()


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
@pytest.mark.parametrize(
    "parameters,expected",
    [(None, 10), ({"label": "O'Reilly\\folder"}, 20), ({"label": "\\' OR 1=1 --"}, 30), ({"label": 123}, 50)],
)
def test_simple_segment_parameters_preserve_defaults_and_values(layer, dialect, parameters, expected):
    assert rows(layer, dialect, "{model}.label = {{ label }}", parameters) == [(expected,)]


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
@pytest.mark.parametrize("as_filter", [False, True])
@pytest.mark.parametrize(
    "parameters,expected",
    [(None, 140), ({"enabled": False}, 10), ({"minimum": "30", "enabled": True}, 120)],
)
def test_control_values_remain_raw_while_outputs_are_typed(layer, dialect, as_filter, parameters, expected):
    template = (
        "{% if enabled and minimum|float >= 15 %}"
        "orders.amount >= {{ minimum }} AND {{ enabled }}"
        "{% else %}orders.amount < {{ minimum }}{% endif %}"
    )
    assert rows(layer, dialect, template, parameters, as_filter=as_filter) == [(expected,)]


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
@pytest.mark.parametrize("as_filter", [False, True])
@pytest.mark.parametrize(
    "template,parameters,expected",
    [
        ("{# values #}orders.label = {{ label }}", {"label": "O'Reilly\\folder"}, 20),
        ("{# values #}orders.label = '{{ label }}'", {"label": "\\' OR 1=1 --"}, 30),
        ("{# values #}orders.label = 'é prefix {{ label }} suffix'", {"label": "O'Reilly\\folder"}, 40),
        ("{% if label == 'plain' %}orders.label = {{ label }}{% else %}FALSE{% endif %}", None, 10),
        ("{# values #}LOWER(orders.label) = {{ label|lower }}", {"label": "PLAIN"}, 10),
        ("{# values #}orders.label = {{ label }}", {"label": 123}, 50),
        ("{# values #}{{ field }} >= {{ minimum }}", {"field": "orders.amount", "minimum": 40}, 90),
    ],
)
def test_template_value_contexts_match_python(layer, dialect, as_filter, template, parameters, expected):
    assert rows(layer, dialect, template, parameters, as_filter=as_filter) == [(expected,)]


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
@pytest.mark.parametrize("parameters,expected", [(None, 60), ({"region": "west"}, 10)])
def test_segment_subqueries_keep_physical_scope(layer, dialect, parameters, expected):
    template = "{# filter #}{model}.id IN (SELECT order_id FROM allowed_orders WHERE region = {{ region }})"
    assert rows(layer, dialect, template, parameters) == [(expected,)]
