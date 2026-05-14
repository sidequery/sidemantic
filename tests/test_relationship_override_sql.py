"""Regression tests for metric-local relationship overrides in SQL generation."""

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship, RelationshipOverride
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


def test_metric_relationship_override_replaces_default_join_path():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="Sales",
            table="sales",
            primary_key="SalesKey",
            relationships=[
                Relationship(
                    name="Calendar",
                    type="many_to_one",
                    foreign_key="OrderDateKey",
                    primary_key="DateKey",
                ),
                Relationship(
                    name="Calendar",
                    type="many_to_one",
                    foreign_key="ShipDateKey",
                    primary_key="DateKey",
                    active=False,
                ),
            ],
            metrics=[
                Metric(
                    name="Ship Sales",
                    agg="sum",
                    sql="Amount",
                    required_models=["Calendar"],
                    relationship_overrides=[
                        RelationshipOverride(
                            from_model="Sales",
                            from_column="ShipDateKey",
                            to_model="Calendar",
                            to_column="DateKey",
                        )
                    ],
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="Calendar",
            table="calendar",
            primary_key="DateKey",
            dimensions=[Dimension(name="Date", type="time", sql="Date")],
        )
    )

    sql = SQLGenerator(graph).generate(metrics=["Sales.Ship Sales"], dimensions=["Calendar.Date"])

    assert "ShipDateKey" in sql
    assert "DateKey = Sales_cte.ShipDateKey" in sql
    assert "OrderDateKey = Calendar_cte.DateKey" not in sql
    assert "Calendar_cte.DateKey = Sales_cte.OrderDateKey" not in sql


def test_crossfilter_join_type_override_is_used_for_join():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="Sales",
            table="sales",
            primary_key="SalesKey",
            relationships=[
                Relationship(
                    name="Products",
                    type="many_to_one",
                    foreign_key="ProductKey",
                    primary_key="ProductKey",
                )
            ],
            metrics=[
                Metric(
                    name="Product Sales",
                    agg="sum",
                    sql="Amount",
                    required_models=["Products"],
                    relationship_overrides=[
                        RelationshipOverride(
                            from_model="Sales",
                            from_column="ProductKey",
                            to_model="Products",
                            to_column="ProductKey",
                            join_type="inner",
                            direction="Both",
                        )
                    ],
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="Products",
            table="products",
            primary_key="ProductKey",
            dimensions=[Dimension(name="Category", type="categorical", sql="Category")],
        )
    )

    sql = SQLGenerator(graph).generate(metrics=["Sales.Product Sales"], dimensions=["Products.Category"])

    assert "INNER JOIN" in sql
