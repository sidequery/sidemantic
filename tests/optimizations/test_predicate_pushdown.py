"""Tests for predicate pushdown optimization."""

import sqlglot
from sqlglot import exp

from sidemantic import Dimension, Metric, Model


def test_single_model_filter_pushdown(layer):
    """Test that filters on a single model get pushed into the CTE."""
    model = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="region", type="categorical", sql="region"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(model)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        filters=["orders.region = 'US'"],
    )

    # Parse with SQLGlot
    parsed = sqlglot.parse_one(sql)

    # Find the CTE
    cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "orders_cte":
            cte = cte_def
            break

    assert cte is not None, "CTE not found"

    # Check that CTE has a WHERE clause
    cte_select = cte.this
    where_clause = cte_select.find(exp.Where)

    assert where_clause is not None, "Filter was not pushed down into CTE"

    # Verify the filter condition
    where_sql = where_clause.sql()
    assert "region" in where_sql
    assert "US" in where_sql


def test_multi_model_filter_not_pushed(layer):
    """Test that filters referencing multiple models stay in main query."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="amount", type="numeric", sql="amount"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[{"name": "customer", "type": "many_to_one", "foreign_key": "customer_id"}],
    )

    customers = Model(
        name="customer",
        table="customers_table",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="region", type="categorical", sql="region"),
        ],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["customer.region"],
        filters=["orders.amount > 100", "customer.region = 'US'"],
    )

    parsed = sqlglot.parse_one(sql)

    # Find orders CTE - should have amount filter
    orders_cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "orders_cte":
            orders_cte = cte_def
            break

    assert orders_cte is not None
    orders_where = orders_cte.this.find(exp.Where)
    assert orders_where is not None
    assert "amount" in orders_where.sql()

    # Find customer CTE - should have region filter
    customer_cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "customer_cte":
            customer_cte = cte_def
            break

    assert customer_cte is not None
    customer_where = customer_cte.this.find(exp.Where)
    assert customer_where is not None
    assert "region" in customer_where.sql()


def test_filters_pushed_to_correct_ctes(layer):
    """Test that each filter goes to the correct CTE."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="amount", type="numeric", sql="amount"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[{"name": "customer", "type": "many_to_one", "foreign_key": "customer_id"}],
    )

    customers = Model(
        name="customer",
        table="customers_table",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="segment", type="categorical", sql="segment"),
        ],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["customer.region"],
        filters=[
            "orders.status = 'completed'",
            "orders.amount > 50",
            "customer.region = 'US'",
            "customer.segment = 'enterprise'",
        ],
    )

    parsed = sqlglot.parse_one(sql)

    # Check orders CTE
    orders_cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "orders_cte":
            orders_cte = cte_def
            break

    assert orders_cte is not None
    orders_where = orders_cte.this.find(exp.Where)
    assert orders_where is not None
    orders_where_sql = orders_where.sql()

    # Should have both order filters
    assert "status" in orders_where_sql
    assert "completed" in orders_where_sql
    assert "amount" in orders_where_sql
    assert "50" in orders_where_sql

    # Should NOT have customer filters
    assert "region" not in orders_where_sql
    assert "segment" not in orders_where_sql

    # Check customer CTE
    customer_cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "customer_cte":
            customer_cte = cte_def
            break

    assert customer_cte is not None
    customer_where = customer_cte.this.find(exp.Where)
    assert customer_where is not None
    customer_where_sql = customer_where.sql()

    # Should have both customer filters
    assert "region" in customer_where_sql
    assert "US" in customer_where_sql
    assert "segment" in customer_where_sql
    assert "enterprise" in customer_where_sql

    # Should NOT have order filters
    assert "status" not in customer_where_sql
    assert "amount" not in customer_where_sql


def test_no_filters_no_where_clause(layer):
    """Test that CTEs without filters don't have WHERE clauses."""
    model = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
        ],
    )

    layer.add_model(model)

    sql = layer.compile(
        metrics=["orders.count"],
        dimensions=["orders.status"],
    )

    parsed = sqlglot.parse_one(sql)

    # Find the CTE
    cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "orders_cte":
            cte = cte_def
            break

    assert cte is not None

    # Should NOT have a WHERE clause
    where_clause = cte.this.find(exp.Where)
    assert where_clause is None, "CTE should not have WHERE clause when no filters"


def test_segment_filters_pushed_down(layer):
    """Test that segment filters get pushed down into CTEs."""
    from sidemantic import Segment

    model = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="amount", type="numeric", sql="amount"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        segments=[
            Segment(name="completed", sql="{model}.status = 'completed'"),
        ],
    )

    layer.add_model(model)

    sql = layer.compile(
        metrics=["orders.revenue"],
        segments=["orders.completed"],
    )

    parsed = sqlglot.parse_one(sql)

    # Find the CTE
    cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "orders_cte":
            cte = cte_def
            break

    assert cte is not None

    # Segment filter should be pushed down
    where_clause = cte.this.find(exp.Where)
    assert where_clause is not None
    where_sql = where_clause.sql()
    assert "status" in where_sql
    assert "completed" in where_sql


def test_metric_level_filters_not_pushed(layer):
    """Test that metric-level filters stay in main query, not pushed to CTE."""
    model = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="region", type="categorical", sql="region"),
        ],
        metrics=[
            Metric(
                name="completed_revenue",
                agg="sum",
                sql="amount",
                filters=["{model}.status = 'completed'"],
            ),
        ],
    )

    layer.add_model(model)

    sql = layer.compile(
        metrics=["orders.completed_revenue"],
        filters=["orders.region = 'US'"],
    )

    parsed = sqlglot.parse_one(sql)

    # Find the CTE
    cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "orders_cte":
            cte = cte_def
            break

    assert cte is not None

    # Query-level filter (region) should be in CTE
    cte_where = cte.this.find(exp.Where)
    assert cte_where is not None
    cte_where_sql = cte_where.sql()
    assert "region" in cte_where_sql

    # Metric-level filter should NOT be in CTE (stays in main query)
    assert "status" not in cte_where_sql

    # Find main SELECT
    main_select = parsed.find(exp.Select)
    main_where = main_select.find(exp.Where)
    assert main_where is not None
    main_where_sql = main_where.sql()

    # Metric-level filter should be in main query
    assert "status" in main_where_sql
    assert "completed" in main_where_sql


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
