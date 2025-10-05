"""Test relationship property methods and edge cases."""

from sidemantic.core.relationship import Relationship


def test_relationship_sql_expr_with_explicit_foreign_key():
    """Test sql_expr property when foreign_key is explicitly set."""
    rel = Relationship(name="customers", type="many_to_one", foreign_key="cust_id")
    assert rel.sql_expr == "cust_id"


def test_relationship_sql_expr_default_many_to_one():
    """Test sql_expr property defaults to {name}_id for many_to_one."""
    rel = Relationship(name="customers", type="many_to_one")
    assert rel.sql_expr == "customers_id"


def test_relationship_sql_expr_default_one_to_many():
    """Test sql_expr property defaults to 'id' for one_to_many."""
    rel = Relationship(name="orders", type="one_to_many")
    assert rel.sql_expr == "id"


def test_relationship_sql_expr_default_one_to_one():
    """Test sql_expr property defaults to 'id' for one_to_one."""
    rel = Relationship(name="profile", type="one_to_one")
    assert rel.sql_expr == "id"


def test_relationship_related_key_with_explicit_primary_key():
    """Test related_key property when primary_key is explicitly set."""
    rel = Relationship(name="customers", type="many_to_one", primary_key="customer_uid")
    assert rel.related_key == "customer_uid"


def test_relationship_related_key_default():
    """Test related_key property defaults to 'id'."""
    rel = Relationship(name="customers", type="many_to_one")
    assert rel.related_key == "id"


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
