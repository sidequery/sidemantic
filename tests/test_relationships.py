"""Test relationship property methods and edge cases."""

import pytest

from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.validation import validate_relationships


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
    assert "foreign_key is required" in "\n".join(validate_relationships(graph))


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
    assert "foreign_key is required" in "\n".join(validate_relationships(graph))


def test_graph_one_to_one_requires_explicit_unique_target_key():
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
    errors = "\n".join(validate_relationships(graph))
    assert "foreign_key is required" in errors


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


def test_many_to_one_explicit_alternate_target_key_is_relationship_scoped_key_declaration():
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
                    foreign_key="customer_external_id",
                    primary_key="external_id",
                )
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers", primary_key="customer_id"))

    assert validate_relationships(graph) == []


def test_one_to_one_cardinality_declares_target_foreign_key_unique():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="users",
            table="users",
            primary_key="user_id",
            relationships=[Relationship(name="profiles", type="one_to_one", foreign_key="user_id")],
        )
    )
    graph.add_model(Model(name="profiles", table="profiles", primary_key="profile_id"))

    assert validate_relationships(graph) == []
