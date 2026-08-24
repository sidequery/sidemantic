"""Model invariant filters are unconditional source-row constraints."""

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.inheritance import merge_model
from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.core.transport_security import deny_raw_sql
from sidemantic.rust_bridge import _model_dump_for_rust
from sidemantic.sql.query_rewriter import QueryRewriter


def _orders() -> Model:
    return Model(
        name="orders",
        table="orders",
        primary_key="id",
        invariant_filters=["tenant_id = 1"],
        dimensions=[
            Dimension(name="id", type="numeric"),
            Dimension(name="customer_id", type="numeric"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[Metric(name="order_count", agg="count"), Metric(name="revenue", agg="sum", sql="amount")],
    )


def _layer() -> SemanticLayer:
    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("CREATE TABLE orders (id INT, customer_id INT, tenant_id INT, status TEXT, amount INT)")
    layer.adapter.execute(
        "INSERT INTO orders VALUES (1, 10, 1, 'new', 10), (2, 20, 2, 'new', 100), (3, 10, 1, 'paid', 20)"
    )
    layer.add_model(_orders())
    return layer


def test_structured_compile_and_query_apply_invariant_before_aggregation():
    layer = _layer()

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])

    assert "WHERE tenant_id = 1" in sql
    assert sorted(layer.query(metrics=["orders.revenue"], dimensions=["orders.status"]).fetchall()) == [
        ("new", 10),
        ("paid", 20),
    ]


def test_semantic_sql_applies_invariant():
    layer = _layer()

    assert sorted(layer.sql("SELECT orders.revenue, orders.status FROM orders").fetchall()) == [
        ("new", 10),
        ("paid", 20),
    ]


def test_joined_model_invariant_is_pushed_into_its_cte():
    layer = _layer()
    layer.adapter.execute("CREATE TABLE customers (id INT, active BOOLEAN, name TEXT)")
    layer.adapter.execute("INSERT INTO customers VALUES (10, true, 'kept'), (20, false, 'removed')")
    layer.graph.models["orders"].relationships = [
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id")
    ]
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            invariant_filters=["active"],
            dimensions=[Dimension(name="name", type="categorical")],
        )
    )

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["customers.name"])
    customer_cte = sql.split("customers_cte AS (", 1)[1].split("\n)", 1)[0]

    assert "WHERE active" in customer_cte
    assert layer.query(metrics=["orders.revenue"], dimensions=["customers.name"]).fetchall() == [("kept", 30)]


def test_joined_model_function_invariant_qualifies_list_arguments():
    layer = _layer()
    layer.adapter.execute("CREATE TABLE customers (id INT, active BOOLEAN, name TEXT)")
    layer.adapter.execute("INSERT INTO customers VALUES (10, true, 'kept'), (20, false, 'removed')")
    layer.graph.models["orders"].relationships = [
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id")
    ]
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            invariant_filters=["coalesce(NULL, active)"],
            dimensions=[Dimension(name="name", type="categorical")],
        )
    )

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["customers.name"])
    customer_cte = sql.split("customers_cte AS (", 1)[1].split("\n)", 1)[0]

    assert "COALESCE(NULL, active)" in customer_cte
    assert layer.query(metrics=["orders.revenue"], dimensions=["customers.name"]).fetchall() == [("kept", 30)]


def test_preaggregation_bakes_invariant_and_routes_without_filter_column():
    layer = _layer()
    model = layer.graph.models["orders"]
    preagg = PreAggregation(name="by_status", measures=["revenue"], dimensions=["status"])
    model.pre_aggregations = [preagg]
    source_sql = preagg.generate_materialization_sql(model)
    assert "WHERE (tenant_id = 1)" in source_sql
    layer.adapter.execute(f"CREATE TABLE orders_preagg_by_status AS {source_sql}")

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        use_preaggregations=True,
    )

    assert "orders_preagg_by_status" in sql
    assert "tenant_id" not in sql
    assert sorted(layer.adapter.execute(sql).fetchall()) == [("new", 10), ("paid", 20)]


def test_partitioned_preaggregation_scopes_bucket_discovery_and_materialization():
    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("CREATE TABLE scoped_orders (created_at TIMESTAMP, tenant_id INT, amount INT)")
    layer.adapter.execute(
        "INSERT INTO scoped_orders VALUES ('2024-01-01', 1, 10), ('2024-02-01', 2, 200), ('2024-03-01', 1, 30)"
    )
    model = Model(
        name="scoped_orders",
        table="scoped_orders",
        invariant_filters=["tenant_id = 1"],
        dimensions=[Dimension(name="created_at", type="time", granularity="day")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="monthly",
        measures=["revenue"],
        time_dimension="created_at",
        granularity="month",
        partition_granularity="month",
    )

    built = preagg.build_partitions(layer.adapter, model)

    assert len(built) == 2
    assert layer.adapter.execute("SELECT SUM(revenue_raw) FROM scoped_orders_preagg_monthly").fetchone()[0] == 40


def test_inheritance_conjoins_invariants_and_rust_rewriter_falls_back():
    parent = Model(name="base", table="records", invariant_filters=["tenant_id = 1"])
    child = Model(name="child", extends="base", invariant_filters=["active"])
    merged = merge_model(child, parent)

    assert merged.invariant_filters == ["tenant_id = 1", "active"]
    adapter = SidemanticAdapter()
    exported = adapter._export_model(merged)
    assert adapter._parse_model(exported).invariant_filters == merged.invariant_filters
    layer = SemanticLayer(auto_register=False)
    layer.add_model(merged)
    rewriter = QueryRewriter(layer.graph, use_rust_rewriter=True)
    assert rewriter._use_rust_rewriter is False
    assert rewriter.rust_fallback_reason == "model invariant filters require the Python rewriter"


def test_native_yaml_roundtrip_preserves_invariants(tmp_path):
    path = tmp_path / "semantic.yml"
    adapter = SidemanticAdapter()
    graph = SemanticGraph()
    graph.add_model(_orders())
    adapter.export(graph, path)

    loaded = adapter.parse(path).models["orders"]

    assert loaded.invariant_filters == ["tenant_id = 1"]


def test_transport_raw_sql_is_denied_when_invariants_are_active():
    layer = _layer()

    try:
        deny_raw_sql(layer, transport="test transport")
    except SecurityError as exc:
        assert "model invariant filters" in str(exc)
    else:
        raise AssertionError("raw SQL transport must fail closed for invariant-scoped models")


def test_rust_payload_excludes_unsupported_invariants():
    payload = _model_dump_for_rust(_orders())

    assert "invariant_filters" not in payload


def test_rust_sql_generator_falls_back_for_invariants(monkeypatch):
    layer = _layer()
    layer._use_rust_sql_generator = True

    def unexpected_rust_compile(**_kwargs):
        raise AssertionError("Rust generator must not receive invariant-scoped models")

    monkeypatch.setattr(layer, "_compile_with_rust", unexpected_rust_compile)

    sql = layer.compile(metrics=["orders.revenue"])

    assert "tenant_id = 1" in sql


def test_original_sql_preaggregation_scopes_model_sql():
    model = _orders()
    model.table = None
    model.sql = "SELECT * FROM raw_orders"
    preagg = PreAggregation(name="base", type="original_sql")

    sql = preagg.generate_materialization_sql(model)

    assert "FROM (SELECT * FROM raw_orders) AS t" in sql
    assert "WHERE (tenant_id = 1)" in sql


def test_custom_original_sql_without_source_placeholder_rejects_invariants():
    preagg = PreAggregation(name="base", type="original_sql", sql="SELECT * FROM custom_view")

    try:
        preagg.generate_materialization_sql(_orders())
    except ValueError as exc:
        assert "cannot be proven to apply" in str(exc)
    else:
        raise AssertionError("custom source SQL must not bypass model invariant filters")


def test_original_sql_placeholder_keeps_sql_model_cte_without_invariants():
    model = _orders()
    model.table = None
    model.sql = "SELECT * FROM raw_orders"
    model.invariant_filters = []
    preagg = PreAggregation(
        name="base",
        type="original_sql",
        sql="SELECT {model}.id FROM {model}",
    )

    sql = preagg.generate_materialization_sql(model)

    assert sql == ("WITH orders__base AS (\nSELECT * FROM raw_orders\n)\nSELECT orders__base.id FROM orders__base")
