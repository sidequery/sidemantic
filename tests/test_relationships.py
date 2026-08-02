"""Test relationship property methods and edge cases."""

import pytest

from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


def test_relationship_sql_expr_with_explicit_foreign_key():
    """Test sql_expr property when foreign_key is explicitly set."""
    rel = Relationship(name="customers", type="many_to_one", foreign_key="cust_id")
    assert rel.sql_expr == "cust_id"


def test_relationship_sql_expr_does_not_invent_many_to_one_key():
    rel = Relationship(name="customers", type="many_to_one")
    assert rel.sql_expr is None


def test_relationship_sql_expr_does_not_invent_one_to_many_key():
    rel = Relationship(name="orders", type="one_to_many")
    assert rel.sql_expr is None


def test_relationship_sql_expr_does_not_invent_one_to_one_key():
    rel = Relationship(name="profile", type="one_to_one")
    assert rel.sql_expr is None


def test_relationship_related_key_with_explicit_primary_key():
    """Test related_key property when primary_key is explicitly set."""
    rel = Relationship(name="customers", type="many_to_one", primary_key="customer_uid")
    assert rel.related_key == "customer_uid"


def test_relationship_related_key_is_unknown_when_omitted():
    rel = Relationship(name="customers", type="many_to_one")
    assert rel.related_key is None


def test_relationship_many_to_many():
    """Test many_to_many relationship type."""
    rel = Relationship(
        name="products",
        type="many_to_many",
        foreign_key="order_product_id",
    )
    assert rel.type == "many_to_many"
    assert rel.sql_expr == "order_product_id"


def test_relationship_all_fields():
    """Test relationship with all fields specified."""
    rel = Relationship(
        name="organizations",
        type="many_to_one",
        foreign_key="org_id",
        primary_key="organization_id",
    )
    assert rel.name == "organizations"
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "org_id"
    assert rel.primary_key == "organization_id"
    assert rel.sql_expr == "org_id"
    assert rel.related_key == "organization_id"


def test_relationship_omitted_column_lists_remain_unknown():
    many_to_one = Relationship(name="customers", type="many_to_one")
    assert many_to_one.foreign_key_columns == []
    assert many_to_one.primary_key_columns == []

    one_to_many = Relationship(name="orders", type="one_to_many")
    assert one_to_many.foreign_key_columns == []
    assert one_to_many.primary_key_columns == []

    one_to_one = Relationship(name="profile", type="one_to_one")
    assert one_to_one.foreign_key_columns == []
    assert one_to_one.primary_key_columns == []


def test_graph_many_to_one_omitted_foreign_key_is_not_joinable():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            relationships=[Relationship(name="customers", type="many_to_one")],
        )
    )
    graph.add_model(Model(name="customers", table="customers", primary_key="customer_uid"))

    with pytest.raises(ValueError, match="No join path"):
        graph.find_relationship_path("orders", "customers")


def test_graph_one_to_many_omitted_foreign_key_is_not_joinable():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            relationships=[Relationship(name="orders", type="one_to_many")],
        )
    )
    graph.add_model(Model(name="orders", table="orders", primary_key="id"))

    with pytest.raises(ValueError, match="No join path"):
        graph.find_relationship_path("customers", "orders")


def test_graph_one_to_one_omitted_keys_are_not_joinable():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="users",
            table="users",
            primary_key="id",
            relationships=[Relationship(name="profiles", type="one_to_one")],
        )
    )
    graph.add_model(Model(name="profiles", table="profiles", primary_key="id"))

    with pytest.raises(ValueError, match="No join path"):
        graph.find_relationship_path("users", "profiles")


def test_graph_explicit_foreign_key_omitted_primary_key_uses_target_primary_key():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="invoices",
            table="invoices",
            primary_key="invoice_id",
            relationships=[
                Relationship(
                    name="vendors",
                    type="many_to_one",
                    foreign_key="vendor_ref",
                )
            ],
        )
    )
    graph.add_model(Model(name="vendors", table="vendors", primary_key="vendor_uid"))

    path = graph.find_relationship_path("invoices", "vendors")

    assert [(step.from_columns, step.to_columns, step.relationship) for step in path] == [
        (["vendor_ref"], ["vendor_uid"], "many_to_one")
    ]


def test_graph_mismatched_composite_key_arity_is_not_joinable():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key=["customer_id", "tenant_id"],
                    primary_key="customer_id",
                )
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers", primary_key="customer_id"))

    with pytest.raises(ValueError, match="No join path"):
        graph.find_relationship_path("orders", "customers")


def test_graph_custom_sql_relationship_does_not_require_structured_keys():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    sql="{from}.customer_id = {to}.customer_id",
                )
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers"))

    path = graph.find_relationship_path("orders", "customers")

    assert len(path) == 1
    assert path[0].custom_condition == "{from}.customer_id = {to}.customer_id"


def test_graph_cross_relationship_does_not_require_structured_keys():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            relationships=[Relationship(name="calendar", type="cross")],
        )
    )
    graph.add_model(Model(name="calendar", table="calendar"))

    path = graph.find_relationship_path("orders", "calendar")

    assert len(path) == 1
    assert path[0].relationship == "cross"
    assert path[0].from_columns == []
    assert path[0].to_columns == []
