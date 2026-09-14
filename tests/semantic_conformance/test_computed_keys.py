"""Independent executable contracts for source-bound semantic identity expressions."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.rust_bridge import rewrite_semantic_input
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError


@pytest.fixture
def layer():
    pytest.importorskip("sidemantic_rs", reason="Computed-key acceptance requires the real Rust extension")
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="accounts",
            table="key_accounts",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="{model}.tenant * 100 + {model}.id")],
            metrics=[
                Metric(name="budget", agg="sum", sql="budget"),
                Metric(name="count", agg="count"),
                Metric(name="distinct_accounts", agg="count_distinct"),
            ],
        )
    )
    layer.add_model(
        Model(
            name="events",
            table="key_events",
            primary_key="id",
            dimensions=[
                Dimension(name="category", type="categorical"),
                Dimension(name="account_id", type="numeric", sql="tenant * 100 + raw_account_id"),
            ],
            metrics=[Metric(name="amount", agg="sum", sql="amount")],
            relationships=[Relationship(name="accounts", type="many_to_one", foreign_key="account_id")],
        )
    )
    layer.adapter.execute("""
        create table key_accounts(id integer, tenant integer, budget integer);
        insert into key_accounts values (1, 1, 10), (1, 2, 20);
        create table key_events(id integer, tenant integer, raw_account_id integer, account_id integer, category varchar, amount integer);
        insert into key_events values
            (11, 1, 1, 201, 'x', 3), (12, 1, 1, 201, 'x', 4),
            (13, 2, 1, 101, 'x', 8), (14, 2, 1, 101, 'y', 9);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def result(layer, query, columns, expected):
    sql = layer.compile(**query)
    assert layer.last_engine_selection["engine"] == "rust"
    data = layer.adapter.execute(sql)
    assert [column[0] for column in data.description] == columns
    assert data.fetchall() == expected


def test_display_and_both_join_keys_use_semantic_identity(layer):
    # Physical accounts.id is 1 for both tenants. Physical events.account_id
    # deliberately points at the other tenant; both semantic keys must be used.
    result(
        layer,
        {"metrics": ["events.amount"], "dimensions": ["accounts.id"], "order_by": ["accounts.id"]},
        ["id", "amount"],
        [(101, 7), (201, 17)],
    )


def test_symmetric_aggregation_uses_computed_identity(layer):
    result(
        layer,
        {
            "metrics": ["accounts.budget", "accounts.count", "accounts.distinct_accounts"],
            "dimensions": ["events.category"],
            "order_by": ["events.category"],
        },
        ["category", "budget", "count", "distinct_accounts"],
        [("x", 30, 2, 2), ("y", 20, 1, 1)],
    )


def test_default_distinct_counts_computed_keys_without_join(layer):
    result(layer, {"metrics": ["accounts.distinct_accounts"]}, ["distinct_accounts"], [(2,)])


def test_computed_key_predicate_and_raw_self_reference(layer):
    for predicate in ["accounts.id = 101", "id = 101"]:
        result(
            layer,
            {"metrics": ["accounts.budget"], "dimensions": ["accounts.id"], "filters": [predicate]},
            ["id", "budget"],
            [(101, 10)],
        )


def test_from_metrics_sql_uses_structured_key_planning(layer):
    sql = rewrite_semantic_input(
        layer.graph,
        "select accounts.id, events.amount from metrics order by accounts.id",
    )
    data = layer.adapter.execute(sql)
    assert [column[0] for column in data.description] == ["id", "amount"]
    assert data.fetchall() == [(101, 7), (201, 17)]


def test_legacy_sql_shape_is_explicitly_unsupported(layer):
    with pytest.raises(UnsupportedSemanticFeaturesError) as caught:
        rewrite_semantic_input(layer.graph, "select accounts.id, accounts.budget from accounts")
    assert "rewrite.computed_key_query_shape" in caught.value.capabilities


def test_compound_computed_keys_join_componentwise(layer):
    accounts = layer.graph.models["accounts"]
    accounts.primary_key = ["tenant_key", "id"]
    accounts.dimensions = [
        Dimension(name="tenant_key", type="numeric", sql="tenant + 10"),
        Dimension(name="id", type="numeric", sql="id + 100"),
    ]
    accounts.metrics = [metric for metric in accounts.metrics if metric.name != "distinct_accounts"]
    events = layer.graph.models["events"]
    events.dimensions.extend(
        [
            Dimension(name="tenant_ref", type="numeric", sql="tenant + 10"),
            Dimension(name="account_ref", type="numeric", sql="raw_account_id + 100"),
        ]
    )
    events.relationships = [
        Relationship(
            name="accounts",
            type="many_to_one",
            foreign_key=["tenant_ref", "account_ref"],
            primary_key=["tenant_key", "id"],
        )
    ]
    result(
        layer,
        {
            "metrics": ["events.amount"],
            "dimensions": ["accounts.tenant_key", "accounts.id"],
            "order_by": ["accounts.tenant_key"],
        },
        ["tenant_key", "id", "amount"],
        [(11, 101, 7), (12, 101, 17)],
    )
    result(
        layer,
        {
            "metrics": ["accounts.budget"],
            "dimensions": ["events.category"],
            "order_by": ["events.category"],
        },
        ["category", "budget"],
        [("x", 30), ("y", 20)],
    )
    accounts.metrics.append(Metric(name="distinct_accounts", agg="count_distinct"))
    result(layer, {"metrics": ["accounts.distinct_accounts"]}, ["distinct_accounts"], [(2,)])


@pytest.mark.parametrize(
    "expression", ["random()", "sum(id)", "other.id", "row_number() over ()", "(select id from other)", "1"]
)
def test_unsafe_key_expressions_are_rejected(layer, expression):
    layer.graph.models["accounts"].dimensions[0].sql = expression
    with pytest.raises(UnsupportedSemanticFeaturesError) as caught:
        layer.compile(metrics=["accounts.budget"])
    assert "dimension.computed_key_expression" in caught.value.capabilities


def test_disconnected_computed_model_does_not_block_temporal_query(layer):
    layer.add_model(
        Model(
            name="sales",
            table="key_sales",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="running", type="cumulative", sql="sales.revenue"),
            ],
        )
    )
    layer.adapter.execute("""
        create table key_sales(id integer, day date, amount integer);
        insert into key_sales values (1, '2026-01-01', 3), (2, '2026-01-02', 4);
    """)
    from datetime import date

    result(
        layer,
        {"metrics": ["sales.running"], "dimensions": ["sales.day"], "order_by": ["sales.day"]},
        ["day", "revenue", "running"],
        [(date(2026, 1, 1), 3, 3), (date(2026, 1, 2), 4, 7)],
    )


def test_cross_role_computed_key_predicate_keeps_role_aliases(layer):
    layer.graph.models["events"].relationships = [
        Relationship(name="buyer", target_model="accounts", type="many_to_one", foreign_key="account_id"),
        Relationship(name="seller", target_model="accounts", type="many_to_one", foreign_key="account_id"),
    ]
    result(
        layer,
        {
            "metrics": ["events.amount"],
            "dimensions": ["buyer.id", "seller.id"],
            "filters": ["buyer.id = 101 OR seller.id = 201"],
            "order_by": ["buyer.id"],
        },
        ["buyer_id", "seller_id", "amount"],
        [(101, 101, 7), (201, 201, 17)],
    )


@pytest.mark.parametrize("composite", [False, True])
def test_omitted_native_relationship_key_does_not_classify_ordinary_id(composite):
    rust = pytest.importorskip("sidemantic_rs")
    # YAML host preserves omitted keys, unlike the structured handoff decoder
    # which resolves them before graph construction.
    primary = "[tenant, account_key]" if composite else "account_key"
    foreign = "[tenant, account_ref]" if composite else "account_ref"
    yaml = f"""
models:
  - name: accounts
    table: ordinary_accounts
    primary_key: {primary}
    dimensions:
      - name: id
        type: categorical
        sql: upper(label)
    metrics:
      - name: budget
        agg: sum
        sql: budget
  - name: events
    table: ordinary_events
    primary_key: event_key
    relationships:
      - name: accounts
        type: many_to_one
        foreign_key: {foreign}
"""
    sql = rust.compile_with_yaml(yaml, "metrics: [accounts.budget]\ndimensions: [accounts.id]")
    import duckdb

    with duckdb.connect() as connection:
        connection.execute(
            "create table ordinary_accounts(tenant integer, account_key integer, label varchar, budget integer)"
        )
        connection.execute("insert into ordinary_accounts values (1, 9, 'hello', 12)")
        assert connection.execute(sql).fetchall() == [("HELLO", 12)]
