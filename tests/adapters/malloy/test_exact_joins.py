"""Exact lowering contracts for unaliased Malloy joins."""

from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.malloy import MalloyAdapter, MalloySchemaExposureError
from sidemantic.core.consumption import Explore
from sidemantic.core.relationship import Relationship


def _parse(tmp_path: Path, text: str, *, strict: bool = False):
    path = tmp_path / "model.malloy"
    path.write_text(text)
    adapter = MalloyAdapter(strict=strict, warn_on_errors=False)
    return adapter, adapter.parse(path)


def _relationship(graph, model: str, target: str):
    return next(relationship for relationship in graph.get_model(model).relationships if relationship.name == target)


def test_join_one_on_alternate_keys_compiles_and_executes(tmp_path):
    _, graph = _parse(
        tmp_path,
        """source: customers is duckdb.table('customers') extend {
  primary_key: id
  dimension: name is name
}
source: orders is duckdb.table('orders') extend {
  primary_key: id
  measure: revenue is sum(amount)
  join_one: customers on customer_ref = customers.external_ref
}
""",
    )
    relationship = _relationship(graph, "orders", "customers")
    assert relationship.foreign_key == "customer_ref"
    assert relationship.primary_key == "external_ref"
    assert relationship.sql is None

    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("create table customers (id int, external_ref text, name text)")
    layer.adapter.execute("create table orders (id int, customer_ref text, amount int)")
    layer.adapter.execute("insert into customers values (1, 'C-1', 'Ada'), (2, 'C-2', 'Grace')")
    layer.adapter.execute("insert into orders values (10, 'C-2', 30), (11, 'C-1', 20)")
    for model in graph.models.values():
        layer.add_model(model)
    assert layer.query(
        metrics=["orders.revenue"], dimensions=["customers.name"], order_by=["customers.name"]
    ).fetchall() == [("Ada", 20), ("Grace", 30)]


def test_join_many_composite_keys_preserve_operand_order_and_roundtrip(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: items is duckdb.table('items') extend {
  dimension: sku is sku
}
source: orders is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.tenant_ref = tenant_id and order_number = items.order_ref
}
""",
    )
    relationship = _relationship(graph, "orders", "items")
    assert relationship.primary_key == ["tenant_id", "order_number"]
    assert relationship.foreign_key == ["tenant_ref", "order_ref"]
    assert relationship.metadata["join_key_pairs"] == [
        {"source": "tenant_id", "target": "tenant_ref"},
        {"source": "order_number", "target": "order_ref"},
    ]

    exported = tmp_path / "roundtrip.malloy"
    adapter.export(graph, exported)
    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported)
    reparsed_relationship = _relationship(reparsed, "orders", "items")
    assert reparsed_relationship.primary_key == ["tenant_id", "order_number"]
    assert reparsed_relationship.foreign_key == ["tenant_ref", "order_ref"]


def test_additional_predicate_uses_placeholders_and_preserves_keys(tmp_path):
    _, graph = _parse(
        tmp_path,
        """source: customers is duckdb.table('customers') extend {
  primary_key: id
  dimension: name is name
}
source: orders is duckdb.table('orders') extend {
  primary_key: id
  measure: revenue is sum(amount)
  join_one: customers on customer_id = customers.id and customers.active = true
}
""",
    )
    relationship = _relationship(graph, "orders", "customers")
    assert relationship.foreign_key == "customer_id"
    assert relationship.primary_key == "id"
    assert relationship.sql == "{from}.customer_id = {to}.id and {to}.active = true"
    assert relationship.metadata["join_key_pairs"] == [{"source": "customer_id", "target": "id"}]

    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("create table customers (id int, name text, active boolean)")
    layer.adapter.execute("create table orders (id int, customer_id int, amount int)")
    layer.adapter.execute("insert into customers values (1, 'kept', true), (2, 'inactive', false)")
    layer.adapter.execute("insert into orders values (10, 1, 20), (11, 2, 30)")
    for model in graph.models.values():
        layer.add_model(model)
    layer.add_explore(Explore(name="orders_root", model="orders"))
    rows = layer.query(
        explore="orders_root",
        metrics=["orders.revenue"],
        dimensions=["customers.name"],
    ).fetchall()
    assert sorted(rows, key=lambda row: (row[0] is not None, row[0] or "")) == [(None, 30), ("kept", 20)]

    # The same relationship must also reverse placeholders and preserve the
    # explicitly-rooted customer side when traversed in the opposite direction.
    layer.add_explore(Explore(name="customers_root", model="customers"))
    assert layer.query(
        explore="customers_root",
        metrics=["orders.revenue"],
        dimensions=["customers.name"],
        order_by=["customers.name"],
    ).fetchall() == [("inactive", None), ("kept", 20)]


def test_rooted_join_many_preserves_unmatched_source_and_exact_keys(tmp_path):
    _, graph = _parse(
        tmp_path,
        """source: items is duckdb.table('items') extend {
  primary_key: id
  dimension: sku is sku
}
source: orders is duckdb.table('orders') extend {
  primary_key: id
  measure: revenue is sum(amount)
  join_many: items on order_number = items.order_ref
}
""",
    )
    relationship = _relationship(graph, "orders", "items")
    assert relationship.primary_key == "order_number"
    assert relationship.foreign_key == "order_ref"

    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("create table items (id int, order_ref text, sku text)")
    layer.adapter.execute("create table orders (id int, order_number text, amount int)")
    layer.adapter.execute("insert into items values (1, 'O-1', 'A'), (2, 'O-1', 'B')")
    layer.adapter.execute("insert into orders values (10, 'O-1', 20), (11, 'O-2', 30)")
    for model in graph.models.values():
        layer.add_model(model)
    layer.add_explore(Explore(name="orders_many_root", model="orders"))

    rows = layer.query(
        explore="orders_many_root",
        metrics=["orders.revenue"],
        dimensions=["items.sku"],
    ).fetchall()
    assert sorted(rows, key=lambda row: (row[0] is not None, row[0] or "")) == [
        (None, 30),
        ("A", 20),
        ("B", 20),
    ]


def test_range_predicate_on_both_sides_lowers_to_custom_sql(tmp_path):
    _, graph = _parse(
        tmp_path,
        """source: prices is duckdb.table('prices') extend {
  primary_key: id
  dimension: label is label
}
source: orders is duckdb.table('orders') extend {
  primary_key: id
  measure: revenue is sum(amount)
  join_one: prices on created_at >= prices.valid_from and created_at < prices.valid_to
}
""",
    )
    relationship = _relationship(graph, "orders", "prices")
    assert relationship.foreign_key is None
    assert relationship.primary_key is None
    assert relationship.sql == ("{from}.created_at >= {to}.valid_from and {from}.created_at < {to}.valid_to")

    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("create table prices (id int, valid_from int, valid_to int, label text)")
    layer.adapter.execute("create table orders (id int, created_at int, amount int)")
    layer.adapter.execute("insert into prices values (1, 0, 10, 'early'), (2, 10, 20, 'late')")
    layer.adapter.execute("insert into orders values (10, 4, 20), (11, 14, 30)")
    for model in graph.models.values():
        layer.add_model(model)
    assert layer.query(
        metrics=["orders.revenue"], dimensions=["prices.label"], order_by=["prices.label"]
    ).fetchall() == [("early", 20), ("late", 30)]


@pytest.mark.parametrize("with_expression", ["customer_id + 1", "lower(customer_id)", "customers.id"])
def test_with_rejects_nonphysical_column_expressions(tmp_path, with_expression):
    adapter, graph = _parse(
        tmp_path,
        "source: customers is duckdb.table('customers') extend { primary_key: id }\n"
        "source: orders is duckdb.table('orders') extend {\n"
        f"  join_one: customers with {with_expression}\n"
        "}\n",
    )
    assert graph.get_model("orders").relationships == []
    assert any("must name one physical column" in issue for _, issue in adapter.unsupported_features)


def test_join_one_with_missing_target_primary_key_is_rejected(tmp_path):
    text = (
        "source: customers is duckdb.table('customers') extend { dimension: name is name }\n"
        "source: orders is duckdb.table('orders') extend { join_one: customers with customer_id }\n"
    )
    adapter, graph = _parse(tmp_path, text)
    assert graph.get_model("orders").relationships == []
    assert any("no declared primary key" in issue for _, issue in adapter.unsupported_features)
    with pytest.raises(MalloySchemaExposureError, match="no declared primary key"):
        _parse(tmp_path, text, strict=True)


def test_same_name_enriched_inline_source_conflicts_instead_of_reusing_canonical(tmp_path):
    text = """source: customers is duckdb.table('customers') extend {
  primary_key: id
  dimension: name is name
}
source: orders is duckdb.table('orders') extend {
  join_one: customers is duckdb.table('customers') extend {
    where: active
    dimension: inline_only is active
  } on customer_id = customers.id
}
"""

    adapter, graph = _parse(tmp_path, text)

    customers = graph.get_model("customers")
    assert customers.invariant_filters == []
    assert customers.get_dimension("inline_only") is None
    assert graph.get_model("orders").relationships == []
    assert any("conflicts with an existing source" in issue for _, issue in adapter.unsupported_features)

    with pytest.raises(MalloySchemaExposureError, match="conflicts with an existing source"):
        _parse(tmp_path, text, strict=True)


def test_bare_cross_is_kept_but_conditional_cross_is_rejected(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: regions is duckdb.table('regions') extend { primary_key: id }
source: orders is duckdb.table('orders') extend {
  join_cross: regions
  join_cross: conditional is duckdb.table('conditional') on conditional.id = id
}
""",
    )
    assert [(relationship.name, relationship.type) for relationship in graph.get_model("orders").relationships] == [
        ("regions", "cross")
    ]
    assert any("unconditional bare cross join" in issue for _, issue in adapter.unsupported_features)


@pytest.mark.parametrize("direction", ["inner", "right", "full"])
def test_explicit_left_is_kept_but_nondefault_direction_is_rejected(tmp_path, direction):
    adapter, graph = _parse(
        tmp_path,
        """source: customers is duckdb.table('customers') extend { primary_key: id }
source: orders is duckdb.table('orders') extend {
  join_one: customers left on customer_id = customers.id
  join_one: rejected is duckdb.table('rejected') DIRECTION on rejected.id = rejected_id
}
""".replace("DIRECTION", direction),
    )
    assert [relationship.name for relationship in graph.get_model("orders").relationships] == ["customers"]
    assert _relationship(graph, "orders", "customers").metadata["join_direction"] == "left"
    assert any(f"unsupported {direction} join direction" in issue for _, issue in adapter.unsupported_features)


def test_unqualified_condition_without_target_reference_is_rejected(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: customers is duckdb.table('customers') extend { primary_key: id }
source: orders is duckdb.table('orders') extend {
  join_one: customers on customer_id = id
}
""",
    )
    assert graph.get_model("orders").relationships == []
    assert any("cannot be lowered safely" in issue for _, issue in adapter.unsupported_features)


@pytest.mark.parametrize(
    "protected_sql",
    [
        "{from}.id = {to}.id and '{from}.literal' = 'x'",
        '{from}.id = {to}.id and "{to}.quoted" = 1',
        "{from}.id = {to}.id -- {from}.comment\n",
        "{from}.id = {to}.id /* {to}.comment */",
    ],
)
def test_native_join_export_rejects_placeholders_in_literals_identifiers_or_comments(protected_sql):
    relationship = Relationship(name="customers", type="many_to_one", sql=protected_sql)

    with pytest.raises(ValueError, match="inside a literal, quoted identifier, or comment"):
        MalloyAdapter._native_join_sql_to_malloy(relationship, "orders")


def test_native_join_export_rewrites_only_executable_qualifiers_and_validates_predicate():
    relationship = Relationship(
        name="customers",
        type="many_to_one",
        sql="{from}.customer_id = {to}.id and {to}.status = 'literal'",
    )
    assert MalloyAdapter._native_join_sql_to_malloy(relationship, "orders") == (
        "orders.customer_id = customers.id and customers.status = 'literal'"
    )

    invalid = Relationship(
        name="customers",
        type="many_to_one",
        sql="{from}.customer_id = {to}.id and (",
    )
    with pytest.raises(ValueError, match="not a valid predicate"):
        MalloyAdapter._native_join_sql_to_malloy(invalid, "orders")
