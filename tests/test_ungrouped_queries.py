"""Test ungrouped queries (raw row queries without GROUP BY)."""

from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_ungrouped_basic():
    """Test basic ungrouped query returns raw rows."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="customer_id", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Ungrouped query should return raw rows
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"], ungrouped=True)

    print("Ungrouped SQL:")
    print(sql)

    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql
    # Should NOT have aggregation function
    assert "SUM(" not in sql
    # Should select the raw column
    assert "revenue_raw" in sql or "amount" in sql
    # Should still have dimensions
    assert "status" in sql


def test_ungrouped_multiple_dimensions():
    """Test ungrouped query with multiple dimensions."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
            Dimension(name="customer_id", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"], dimensions=["orders.status", "orders.region"], ungrouped=True
    )

    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql
    # Should have all dimensions
    assert "status" in sql
    assert "region" in sql


def test_ungrouped_with_filters():
    """Test ungrouped query with filters."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        filters=["orders_cte.region = 'US'"],
        ungrouped=True,
    )

    # Should have WHERE clause
    assert "WHERE" in sql
    assert "region = 'US'" in sql
    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql


def test_ungrouped_with_order_and_limit():
    """Test ungrouped query with ORDER BY and LIMIT."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        order_by=["revenue DESC"],
        limit=100,
        ungrouped=True,
    )

    # Should have ORDER BY and LIMIT
    assert "ORDER BY" in sql
    assert "LIMIT" in sql
    assert "100" in sql
    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql


def test_grouped_vs_ungrouped():
    """Compare grouped and ungrouped queries."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Grouped query (default)
    grouped_sql = layer.compile(
        metrics=["orders.revenue"], dimensions=["orders.status"], ungrouped=False
    )

    # Ungrouped query
    ungrouped_sql = layer.compile(
        metrics=["orders.revenue"], dimensions=["orders.status"], ungrouped=True
    )

    # Grouped should have GROUP BY and aggregation
    assert "GROUP BY" in grouped_sql
    assert "SUM(" in grouped_sql

    # Ungrouped should NOT have GROUP BY or aggregation
    assert "GROUP BY" not in ungrouped_sql
    assert "SUM(" not in ungrouped_sql


def test_ungrouped_multiple_metrics():
    """Test ungrouped query with multiple metrics."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="quantity", agg="sum", sql="qty"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue", "orders.quantity"], dimensions=["orders.status"], ungrouped=True
    )

    # Should select both metrics as raw columns
    assert "revenue_raw" in sql or "amount" in sql
    assert "quantity_raw" in sql or "qty" in sql
    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql
    # Should NOT have aggregation
    assert "SUM(" not in sql
