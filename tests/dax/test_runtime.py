"""Tests for DAX runtime translation context helpers."""

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.dax.runtime import build_dax_translation_context


def test_build_dax_translation_context_includes_many_to_many_edges():
    sales = Model(
        name="Sales",
        table="sales",
        primary_key="SalesKey",
        relationships=[
            Relationship(
                name="Products",
                type="many_to_many",
                foreign_key="ProductKey",
                primary_key="ProductKey",
            )
        ],
    )
    products = Model(name="Products", table="products", primary_key="ProductKey")

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_model(products)

    context = build_dax_translation_context(graph)
    edges = context["relationship_edges"]
    assert len(edges) == 1
    assert edges[0].from_table == "Sales"
    assert edges[0].from_column == "ProductKey"
    assert edges[0].to_table == "Products"
    assert edges[0].to_column == "ProductKey"


def test_build_dax_translation_context_deduplicates_reverse_relationship_edges():
    sales = Model(
        name="Sales",
        table="sales",
        primary_key="SalesKey",
        relationships=[
            Relationship(
                name="Products",
                type="many_to_many",
                foreign_key="ProductKey",
                primary_key="ProductKey",
            )
        ],
    )
    products = Model(
        name="Products",
        table="products",
        primary_key="ProductKey",
        relationships=[
            Relationship(
                name="Sales",
                type="many_to_many",
                foreign_key="ProductKey",
                primary_key="ProductKey",
            )
        ],
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_model(products)

    context = build_dax_translation_context(graph)
    edges = context["relationship_edges"]
    assert len(edges) == 1


def test_build_dax_translation_context_uses_tmdl_from_column_for_one_to_many_edges():
    products_to_customers = Relationship(
        name="Customers",
        type="one_to_many",
        foreign_key="ProductKey",
    )
    products_to_customers._tmdl_from_column = "ProductKey"
    products = Model(
        name="Products",
        table="products",
        primary_key="InternalProductId",
        relationships=[products_to_customers],
    )
    customers = Model(name="Customers", table="customers", primary_key="CustomerKey")

    graph = SemanticGraph()
    graph.add_model(products)
    graph.add_model(customers)

    context = build_dax_translation_context(graph)
    edges = context["relationship_edges"]
    assert len(edges) == 1
    assert edges[0].from_table == "Products"
    assert edges[0].from_column == "ProductKey"
    assert edges[0].to_table == "Customers"
    assert edges[0].to_column == "ProductKey"


def test_build_dax_translation_context_includes_measure_sql_metadata():
    sales = Model(
        name="Sales",
        table="sales",
        primary_key="SalesKey",
        metrics=[Metric(name="Total Sales", agg="sum", sql="amount")],
    )

    graph = SemanticGraph()
    graph.add_model(sales)

    context = build_dax_translation_context(graph)
    assert context["measure_sql_by_table"]["Sales"]["Total Sales"] == "amount"


def test_build_dax_translation_context_includes_measure_filters():
    sales = Model(
        name="Sales",
        table="sales",
        primary_key="SalesKey",
        metrics=[
            Metric(
                name="West Sales",
                agg="sum",
                sql="Amount",
                filters=["Sales.Region = 'West'"],
            )
        ],
    )

    graph = SemanticGraph()
    graph.add_model(sales)

    context = build_dax_translation_context(graph)
    assert context["measure_filters_by_table"]["Sales"]["West Sales"] == ["Sales.Region = 'West'"]
