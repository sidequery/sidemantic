"""Declared local relationship keys remain usable as grouping fields."""

import pytest

from sidemantic import Metric, Model, Relationship, SemanticLayer
from sidemantic.validation import QueryValidationError


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_declared_foreign_key_groups_without_dimension(engine):
    if engine == "rust":
        pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    layer.add_model(Model(name="customers", table="customers", primary_key="id"))
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )
    try:
        layer.adapter.execute("create table orders(id integer, customer_id integer, amount integer)")
        layer.adapter.execute("insert into orders values (1, 10, 2), (2, 10, 3), (3, 20, 7)")
        sql = layer.compile(
            metrics=["orders.revenue"], dimensions=["orders.customer_id"], order_by=["orders.customer_id"]
        )
        assert layer.adapter.execute(sql).fetchall() == [(10, 5), (20, 7)]
        with pytest.raises(QueryValidationError, match="unknown_key"):
            layer.compile(metrics=["orders.revenue"], dimensions=["orders.unknown_key"])
    finally:
        layer.adapter.close()
