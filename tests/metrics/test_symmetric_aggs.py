"""Tests for symmetric aggregates (fan-out join handling)."""

import duckdb
import pytest

from sidemantic.core.model import Dimension, Metric, Model, Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import UnsupportedMetricError
from sidemantic.core.symmetric_aggregate import build_symmetric_aggregate_sql
from sidemantic.sql.generator import SQLGenerator
from tests.utils import fetch_rows


def test_build_symmetric_aggregate_sum():
    """Test building symmetric aggregate SQL for SUM."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="amount",
        primary_key="order_id",
        agg_type="sum",
    )

    # Check key components are present
    assert "SUM(DISTINCT" in sql
    assert "HASH(order_id)" in sql
    assert "HUGEINT" in sql
    assert "+ COALESCE(amount, 0))" in sql


def test_build_symmetric_aggregate_sum_with_alias():
    """Test symmetric aggregate with table alias."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="amount",
        primary_key="order_id",
        agg_type="sum",
        model_alias="orders_cte",
    )

    # Check key components with alias are present
    assert "HASH(orders_cte.order_id)" in sql
    assert "COALESCE(orders_cte.amount, 0))" in sql


def test_build_symmetric_aggregate_avg():
    """Test building symmetric aggregate SQL for AVG."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="amount",
        primary_key="order_id",
        agg_type="avg",
    )

    # Average is sum divided by the count of entities with a non-NULL value
    assert "SUM(DISTINCT" in sql
    assert "HASH(order_id)" in sql
    assert "COUNT(DISTINCT CASE WHEN amount IS NOT NULL THEN order_id END)" in sql
    assert "NULLIF" in sql


def test_build_symmetric_aggregate_count():
    """Test building symmetric aggregate SQL for COUNT."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="amount",
        primary_key="order_id",
        agg_type="count",
    )

    assert sql == "COUNT(DISTINCT CASE WHEN amount IS NOT NULL THEN order_id END)"


def test_build_symmetric_aggregate_count_distinct():
    """Test building symmetric aggregate SQL for COUNT DISTINCT."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="customer_id",
        primary_key="order_id",
        agg_type="count_distinct",
    )

    # COUNT DISTINCT doesn't use symmetric aggregates - just counts distinct values
    assert sql == "COUNT(DISTINCT customer_id)"


def test_fanout_join_detection_single_join():
    """Test that single one-to-many join DOES trigger symmetric aggregates.

    With the CTE-based SQL generation approach, even a single one-to-many join
    creates fan-out for the "one" side, requiring symmetric aggregation.
    """
    graph = SemanticGraph()

    # Orders (base)
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[Relationship(name="order_items", type="one_to_many", sql="id", foreign_key="order_id")],
    )

    # Order items (many)
    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    graph.add_model(orders)
    graph.add_model(order_items)

    generator = SQLGenerator(graph)

    # Single one-to-many join DOES trigger symmetric aggregates for the "one" side
    needs_symmetric = generator._has_fanout_joins("orders", ["order_items"])

    assert needs_symmetric["orders"] is True
    assert needs_symmetric["order_items"] is False


def test_fanout_join_detection_multiple_joins():
    """Test that multiple one-to-many joins trigger symmetric aggregates."""
    graph = SemanticGraph()

    # Orders (base) - has two one-to-many relationships
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="order_items", type="one_to_many", sql="id", foreign_key="order_id"),
            Relationship(name="shipments", type="one_to_many", sql="id", foreign_key="order_id"),
        ],
    )

    # Order items (many)
    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    # Shipments (many)
    shipments = Model(
        name="shipments",
        table="raw_shipments",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="shipment_count", agg="count", sql="*")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    graph.add_model(orders)
    graph.add_model(order_items)
    graph.add_model(shipments)

    generator = SQLGenerator(graph)

    # Multiple one-to-many joins SHOULD trigger symmetric aggregates for base model
    needs_symmetric = generator._has_fanout_joins("orders", ["order_items", "shipments"])

    assert needs_symmetric["orders"] is True
    assert needs_symmetric["order_items"] is False
    assert needs_symmetric["shipments"] is False


def test_symmetric_aggregates_in_sql_generation():
    """Test that SQL generation uses pre-aggregation to handle fan-out.

    When metrics come from different models at different join levels,
    the generator uses pre-aggregation: each metric is aggregated separately
    to the dimension grain, then the results are joined together.
    """
    graph = SemanticGraph()

    # Orders (base) - has two one-to-many relationships
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="order_items", type="one_to_many", sql="id", foreign_key="order_id"),
            Relationship(name="shipments", type="one_to_many", sql="id", foreign_key="order_id"),
        ],
    )

    # Order items
    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    # Shipments
    shipments = Model(
        name="shipments",
        table="raw_shipments",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="shipment_count", agg="count", sql="*")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    graph.add_model(orders)
    graph.add_model(order_items)
    graph.add_model(shipments)

    generator = SQLGenerator(graph)

    # Query with measures from all three models
    sql = generator.generate(
        metrics=["orders.revenue", "order_items.quantity", "shipments.shipment_count"],
        dimensions=["orders.order_date"],
    )

    # Pre-aggregation approach: each model's metrics are aggregated separately
    # and then joined together with FULL OUTER JOIN
    assert "orders_preagg" in sql
    assert "order_items_preagg" in sql
    assert "shipments_preagg" in sql
    assert "FULL OUTER JOIN" in sql


def test_symmetric_aggregates_with_data():
    """Test symmetric aggregates prevent double-counting with actual data."""
    # Create in-memory DuckDB
    conn = duckdb.connect(":memory:")

    # Create test data with fan-out
    # Order 1 has 2 items and 2 shipments (2x2 = 4 rows after join)
    # Order 2 has 1 item and 1 shipment (1x1 = 1 row after join)
    conn.execute("""
        CREATE TABLE raw_orders AS
        SELECT * FROM (VALUES
            (1, '2024-01-01'::DATE, 100),
            (2, '2024-01-02'::DATE, 200)
        ) AS t(id, order_date, amount)
    """)

    conn.execute("""
        CREATE TABLE raw_order_items AS
        SELECT * FROM (VALUES
            (1, 1, 5),
            (2, 1, 3),
            (3, 2, 10)
        ) AS t(id, order_id, quantity)
    """)

    conn.execute("""
        CREATE TABLE raw_shipments AS
        SELECT * FROM (VALUES
            (1, 1),
            (2, 1),
            (3, 2)
        ) AS t(id, order_id)
    """)

    # Create graph
    graph = SemanticGraph()

    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="order_items", type="one_to_many", sql="id", foreign_key="order_id"),
            Relationship(name="shipments", type="one_to_many", sql="id", foreign_key="order_id"),
        ],
    )

    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    shipments = Model(
        name="shipments",
        table="raw_shipments",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="shipment_count", agg="count", sql="*")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    graph.add_model(orders)
    graph.add_model(order_items)
    graph.add_model(shipments)

    # Generate SQL - query all three models to create fan-out
    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["orders.revenue", "order_items.quantity", "shipments.shipment_count"],
        dimensions=["orders.order_date"],
    )

    # Execute query
    result = conn.execute(sql)
    rows = fetch_rows(result)

    # Without symmetric aggregates:
    # Order 1: revenue would be 100*2*2 = 400 (wrong!)
    # Order 2: revenue would be 200*1*1 = 200 (correct by luck)

    # With symmetric aggregates:
    # Order 1: revenue = 100 (correct)
    # Order 2: revenue = 200 (correct)

    assert len(rows) == 2
    revenues = sorted(row[1] for row in rows)
    assert revenues == [100, 200]  # Correct totals, not inflated

    conn.close()


def test_fanout_isolates_typed_entity_rows_for_double_sum_avg_and_nulls():
    """Fanout safety uses typed PK isolation, not lossy hash arithmetic.

    Explicit DOUBLE values used to be added to a roughly 1e31 hash term, which
    made DuckDB return enormous negative SUM/AVG results through cancellation.
    The all-NULL group also changed from SQL NULL to zero merely because a
    one-to-many dimension was present.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE raw_orders (
            id BIGINT PRIMARY KEY,
            amount DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO raw_orders VALUES
            (1, 100.25),
            (2, 50.75),
            (3, NULL)
    """)
    conn.execute("""
        CREATE TABLE raw_items AS
        SELECT * FROM (VALUES
            (1, 1, 'paid'),
            (2, 1, 'paid'),
            (3, 2, 'paid'),
            (4, 3, 'null-only')
        ) AS t(id, order_id, category)
    """)

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="average_order_value", agg="avg", sql="amount"),
            ],
            relationships=[Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id")],
        )
    )
    graph.add_model(
        Model(
            name="items",
            table="raw_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )

    sql = SQLGenerator(graph).generate(
        metrics=["orders.revenue", "orders.average_order_value"],
        dimensions=["items.category"],
        order_by=["items.category"],
    )
    rows = conn.execute(sql).fetchall()

    assert "HASH(" not in sql
    assert "SELECT DISTINCT" in sql
    assert rows == [("null-only", None, None), ("paid", 151.0, 75.5)]
    conn.close()


def test_fanout_evaluates_complete_sql_over_deduplicated_entity_rows():
    """Opaque imported aggregates retain their formula without counting join copies."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE raw_orders AS SELECT * FROM (VALUES (1, 100.0), (2, 200.0)) t(id, amount)")
    conn.execute(
        "CREATE TABLE raw_items AS "
        "SELECT * FROM (VALUES (1, 1, 'all'), (2, 1, 'all'), (3, 2, 'all')) t(id, order_id, category)"
    )

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            metrics=[
                Metric(
                    name="average_order_value",
                    sql="SUM({model}.amount) / COUNT(*)",
                    sql_is_complete=True,
                ),
                Metric(name="opaque_order_count", sql="COUNT(*)", sql_is_complete=True),
            ],
            relationships=[Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id")],
        )
    )
    graph.add_model(
        Model(
            name="items",
            table="raw_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )

    sql = SQLGenerator(graph).generate(
        metrics=["orders.average_order_value", "orders.opaque_order_count"],
        dimensions=["items.category"],
    )

    assert "SELECT DISTINCT" in sql
    assert conn.execute(sql).fetchall() == [("all", 150.0, 2)]


def test_fanout_rejects_filtered_zero_column_complete_sql():
    """A filtered COUNT(*) fails clearly when its filter cannot be moved into entity rows."""
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            metrics=[
                Metric(
                    name="completed_count",
                    sql="COUNT(*)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'completed'"],
                )
            ],
            relationships=[Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id")],
        )
    )
    graph.add_model(
        Model(
            name="items",
            table="raw_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )

    with pytest.raises(UnsupportedMetricError, match="cannot be evaluated safely"):
        SQLGenerator(graph).generate(
            metrics=["orders.completed_count"],
            dimensions=["items.category"],
        )


def test_fanout_typed_composite_keys_do_not_collide_on_delimiters():
    """Composite keys remain separate even when delimiter concatenation collides."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE raw_orders AS
        SELECT * FROM (VALUES
            ('a|b', 'c', 100.0::DOUBLE),
            ('a', 'b|c', 200.0::DOUBLE)
        ) AS t(part_a, part_b, amount)
    """)
    conn.execute("""
        CREATE TABLE raw_items AS
        SELECT * FROM (VALUES
            (1, 'a|b', 'c', 'all'),
            (2, 'a|b', 'c', 'all'),
            (3, 'a', 'b|c', 'all')
        ) AS t(id, part_a, part_b, category)
    """)

    join_sql = "{from}.part_a = {to}.part_a AND {from}.part_b = {to}.part_b"
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key=["part_a", "part_b"],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[Relationship(name="items", type="one_to_many", sql=join_sql)],
        )
    )
    graph.add_model(
        Model(
            name="items",
            table="raw_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
            relationships=[Relationship(name="orders", type="many_to_one", sql=join_sql)],
        )
    )

    sql = SQLGenerator(graph).generate(metrics=["orders.revenue"], dimensions=["items.category"])
    assert conn.execute(sql).fetchall() == [("all", 300.0)]
    assert "CONCAT(" not in sql
    conn.close()


def test_filter_only_sibling_fanout_is_deduplicated_for_non_base_metric():
    """A one-to-many model introduced only by WHERE cannot multiply a sibling metric."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE customers AS SELECT * FROM (VALUES (1, 'east')) AS t(id, region)")
    conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES (1, 1, 100), (2, 1, 50)) AS t(id, customer_id, amount)
    """)
    conn.execute("""
        CREATE TABLE tickets AS
        SELECT * FROM (VALUES
            (1, 1, 'open'),
            (2, 1, 'open'),
            (3, 1, 'closed')
        ) AS t(id, customer_id, kind)
    """)

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            relationships=[
                Relationship(name="orders", type="one_to_many", sql="id", foreign_key="customer_id"),
                Relationship(name="tickets", type="one_to_many", sql="id", foreign_key="customer_id"),
            ],
        )
    )
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )
    graph.add_model(
        Model(
            name="tickets",
            table="tickets",
            primary_key="id",
            dimensions=[Dimension(name="kind", type="categorical")],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )

    sql = SQLGenerator(graph).generate(
        metrics=["orders.revenue"],
        dimensions=["customers.region"],
        filters=["tickets.kind = 'open'"],
    )
    assert conn.execute(sql).fetchall() == [("east", 150)]
    assert "SELECT DISTINCT" in sql
    conn.close()


def test_derived_and_ratio_metrics_reuse_fanout_safe_leaf_aggregates():
    """Calculated metrics compose from deduplicated leaves instead of bypassing them."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE raw_orders AS
        SELECT * FROM (VALUES (1, 100.0::DOUBLE), (2, 200.0::DOUBLE)) AS t(id, amount)
    """)
    conn.execute("""
        CREATE TABLE raw_items AS
        SELECT * FROM (VALUES
            (1, 1, 'all'),
            (2, 1, 'all'),
            (3, 2, 'all')
        ) AS t(id, order_id, category)
    """)

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count"),
                Metric(name="double_revenue", type="derived", sql="revenue * 2"),
            ],
            relationships=[Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id")],
        )
    )
    graph.add_model(
        Model(
            name="items",
            table="raw_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )
    graph.add_metric(
        Metric(
            name="average_order_value",
            type="ratio",
            numerator="orders.revenue",
            denominator="orders.order_count",
        )
    )

    sql = SQLGenerator(graph).generate(
        metrics=[
            "orders.revenue",
            "orders.order_count",
            "orders.double_revenue",
            "average_order_value",
        ],
        dimensions=["items.category"],
    )
    assert conn.execute(sql).fetchall() == [("all", 300.0, 2, 600.0, 150.0)]
    assert "HASH(" not in sql
    assert sql.count("SELECT DISTINCT") == 1
    conn.close()


def test_preagg_grain_preserved_with_filters():
    """Test that preagg subqueries preserve the requested dimension grain when filters are applied.

    Filters should not introduce extra dimensions into preagg subqueries. Each
    preagg CTE must produce exactly one row per dimension key so the FULL OUTER
    JOIN does not inflate results.
    """
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE raw_orders AS
        SELECT * FROM (VALUES
            (1, '2024-01-01'::DATE, 100, 'shipped'),
            (2, '2024-01-01'::DATE, 200, 'pending'),
            (3, '2024-01-02'::DATE, 150, 'shipped')
        ) AS t(id, order_date, amount, status)
    """)

    conn.execute("""
        CREATE TABLE raw_items AS
        SELECT * FROM (VALUES
            (1, 1, 5),
            (2, 1, 3),
            (3, 2, 10),
            (4, 3, 7)
        ) AS t(id, order_id, quantity)
    """)

    graph = SemanticGraph()

    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="order_date", type="time", sql="order_date"),
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id"),
        ],
    )

    items = Model(
        name="items",
        table="raw_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="total_qty", agg="sum", sql="quantity")],
        relationships=[
            Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id"),
        ],
    )

    graph.add_model(orders)
    graph.add_model(items)

    generator = SQLGenerator(graph)

    # Query with a filter on orders.status: preagg should still produce one
    # row per order_date, not one row per (order_date, status).
    sql = generator.generate(
        metrics=["orders.revenue", "items.total_qty"],
        dimensions=["orders.order_date"],
        filters=["orders.status = 'shipped'"],
    )

    result = conn.execute(sql)
    rows = fetch_rows(result)

    # Verify no duplicate dimension keys
    dates = [row[0] for row in rows]
    assert len(dates) == len(set(dates)), f"Duplicate dimension keys in preagg result: {dates}"

    # Verify correct values:
    # A query-level filter describes ONE population: status='shipped' scopes
    # every child sub-query, so items_preagg only aggregates items belonging to
    # shipped orders (not all items sharing the date with a shipped order).
    # 2024-01-01: revenue=100 (order 1, shipped), total_qty=8 (order 1's items)
    # 2024-01-02: revenue=150 (order 3, shipped), total_qty=7 (order 3's items)
    rows_sorted = sorted(rows, key=lambda r: r[0])
    assert rows_sorted[0][1] == 100  # 2024-01-01 revenue (shipped only)
    assert rows_sorted[0][2] == 8  # 2024-01-01 total_qty (shipped orders' items only)
    assert rows_sorted[1][1] == 150  # 2024-01-02 revenue
    assert rows_sorted[1][2] == 7  # 2024-01-02 total_qty

    conn.close()


def test_three_way_preagg_joins_later_ctes_on_coalesced_dimension_key():
    """Later FULL OUTER joins merge keys that are absent from the first CTE.

    Sibling fact children can have structurally different dimension coverage:
    the 2024-01-02 order has shipments and returns but no items, so the first
    pre-aggregation (items) lacks that date while both later ones contain it.
    The shipments and returns values for that date must land on one row, not
    two split rows (which is what joining every CTE only to the first would
    produce when the first CTE's key is NULL).
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE raw_orders AS
        SELECT * FROM (VALUES
            (1, DATE '2024-01-01', 100),
            (2, DATE '2024-01-02', 200)
        ) AS t(id, order_date, amount)
    """)
    conn.execute("""
        CREATE TABLE raw_items AS
        SELECT * FROM (VALUES (1, 1, 3)) AS t(id, order_id, quantity)
    """)
    conn.execute("""
        CREATE TABLE raw_shipments AS
        SELECT * FROM (VALUES (1, 1), (2, 2), (3, 2)) AS t(id, order_id)
    """)
    conn.execute("""
        CREATE TABLE raw_returns AS
        SELECT * FROM (VALUES (1, 2)) AS t(id, order_id)
    """)

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[Dimension(name="order_date", type="time")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[
                Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id"),
                Relationship(name="shipments", type="one_to_many", sql="id", foreign_key="order_id"),
                Relationship(name="returns", type="one_to_many", sql="id", foreign_key="order_id"),
            ],
        )
    )
    graph.add_model(
        Model(
            name="items",
            table="raw_items",
            primary_key="id",
            metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )
    graph.add_model(
        Model(
            name="shipments",
            table="raw_shipments",
            primary_key="id",
            metrics=[Metric(name="shipment_count", agg="count")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )
    graph.add_model(
        Model(
            name="returns",
            table="raw_returns",
            primary_key="id",
            metrics=[Metric(name="return_count", agg="count")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )

    sql = SQLGenerator(graph).generate(
        metrics=["items.quantity", "shipments.shipment_count", "returns.return_count"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )
    rows = conn.execute(sql).fetchall()

    assert len(rows) == 2
    itemless_date = next(row for row in rows if str(row[0]) == "2024-01-02")
    assert itemless_date[1] is None
    assert itemless_date[2:] == (2, 1)

    conn.close()


def test_filtered_count_under_fanout():
    """Test that a filtered count metric honors its filter under fan-out.

    A Metric(agg="count", filters=[...]) must not collapse to an unfiltered
    COUNT(DISTINCT pk) when the symmetric-aggregate path is taken (fan-out via a
    one-to-many join). The filtered count and the unfiltered count must differ.
    """
    conn = duckdb.connect(":memory:")

    # Orders 1 & 3 completed, order 2 cancelled.
    conn.execute("""
        CREATE TABLE raw_orders AS
        SELECT * FROM (VALUES
            (1, 'completed'),
            (2, 'cancelled'),
            (3, 'completed')
        ) AS t(order_id, status)
    """)

    # Items fan each order out.
    conn.execute("""
        CREATE TABLE raw_order_items AS
        SELECT * FROM (VALUES
            (1, 1, 'X'),
            (2, 1, 'X'),
            (3, 2, 'X'),
            (4, 3, 'X'),
            (5, 3, 'X')
        ) AS t(item_id, order_id, region)
    """)

    graph = SemanticGraph()

    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=[
            Metric(name="completed_count", agg="count", filters=["{model}.status = 'completed'"]),
            Metric(name="order_count", agg="count"),
        ],
        relationships=[
            Relationship(name="order_items", type="one_to_many", sql="order_id", foreign_key="order_id"),
        ],
    )

    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="item_id",
        dimensions=[Dimension(name="region", type="categorical", sql="region")],
        metrics=[],
        relationships=[
            Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="order_id"),
        ],
    )

    graph.add_model(orders)
    graph.add_model(order_items)

    generator = SQLGenerator(graph)

    # Grouping by an order_items dimension forces the symmetric-aggregate path.
    sql = generator.generate(
        metrics=["orders.completed_count", "orders.order_count"],
        dimensions=["order_items.region"],
    )

    result = conn.execute(sql)
    rows = fetch_rows(result)

    assert len(rows) == 1
    row = rows[0]
    # row = (region, completed_count, order_count)
    assert row[0] == "X"
    assert row[1] == 2  # filtered: orders 1 & 3 are completed
    assert row[2] == 3  # unfiltered: orders 1, 2, 3
    # The filter must actually change the result.
    assert row[1] != row[2]

    conn.close()
