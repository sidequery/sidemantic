"""Default time grain must be resolved once, before splitting metric sources."""

from datetime import date

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.rust_bridge import compile_semantic_input
from sidemantic.sql.generator import SQLGenerator


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires real native extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="facts",
            table="facts",
            primary_key="id",
            default_time_dimension="day",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[Metric(name="total", agg="sum", sql="amount")],
            relationships=[Relationship(name="accounts", type="many_to_one", foreign_key="account_id")],
        )
    )
    layer.add_model(
        Model(
            name="accounts",
            table="accounts",
            primary_key="id",
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
        )
    )
    layer.add_metric(Metric(name="combined", type="derived", sql="facts.total + accounts.quota"))
    layer.adapter.execute("create table facts(id integer, account_id integer, day date, amount integer)")
    layer.adapter.execute("insert into facts values (1, 1, '2024-01-07', 2), (2, 1, '2024-02-01', 8)")
    layer.adapter.execute("create table accounts(id integer, quota integer)")
    layer.adapter.execute("insert into accounts values (1, 10), (2, 10)")
    yield layer
    layer.adapter.close()


def test_graph_calculation_without_dimensions_is_scalar(layer):
    result = layer.query(metrics=["combined"])
    assert [column[0] for column in result.description] == ["combined"]
    # Each source contributes its independent total: (2 + 8) + (10 + 10).
    assert result.fetchall() == [(30,)]


def test_skipped_model_defaults_remain_skipped_in_source_children(layer):
    # This lower-level compiler option is used by callers such as the widget.
    query = {"metrics": ["facts.total", "accounts.quota"], "skip_default_time_dimensions": True}
    sql = (
        compile_semantic_input(layer.graph, query)
        if layer.engine == "rust"
        else SQLGenerator(layer.graph).generate(**query)
    )
    result = layer.adapter.execute(sql)
    assert [column[0] for column in result.description] == ["total", "quota"]
    assert result.fetchall() == [(10, 20)]


@pytest.mark.parametrize("explicit", [False, True])
def test_model_default_or_explicit_time_grain_is_projected(layer, explicit):
    query = {"dimensions": ["facts.day"]} if explicit else {}
    result = layer.query(metrics=["facts.total", "accounts.quota"], **query)
    assert [column[0] for column in result.description] == ["day", "total", "quota"]
    # The account linked to both days contributes once per day. The unmatched
    # account retains its own NULL-day group in the full source reconciliation.
    assert set(result.fetchall()) == {
        (date(2024, 1, 7), 2, 10),
        (date(2024, 2, 1), 8, 10),
        (None, None, 10),
    }
