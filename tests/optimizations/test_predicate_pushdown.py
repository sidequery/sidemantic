"""Tests for predicate pushdown optimization."""

import sqlglot
from sqlglot import exp

from sidemantic import Dimension, Metric, Model, Segment
from sidemantic.sql.generator import SQLGenerator


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


def test_segment_filter_skips_subquery_columns(layer):
    """Test that segment filter qualification does not touch subquery columns."""
    model = Model(
        name="orders",
        table="orders_table",
        primary_key="id",
        metrics=[
            Metric(name="count", agg="count"),
        ],
        segments=[
            Segment(name="in_other", sql="id in (select id from other_table where flag = 'y')"),
        ],
    )

    layer.add_model(model)

    generator = SQLGenerator(layer.graph)
    filters = generator._resolve_segments(["orders.in_other"])
    assert len(filters) == 1

    filter_sql = filters[0]
    parsed = sqlglot.parse_one(filter_sql)

    assert any(col.table == "orders_cte" for col in parsed.find_all(exp.Column))

    subquery = None
    for subquery_def in parsed.find_all(exp.Subquery):
        subquery = subquery_def
        break

    assert subquery is not None

    for col in subquery.find_all(exp.Column):
        assert not col.table


def test_metric_level_filters_not_pushed(layer):
    """Test that metric-level filters are applied via CASE WHEN, not pushed to CTE.

    Metric-level filters should be applied inside the aggregation using CASE WHEN
    expressions, not in the WHERE clause. This ensures each metric's filter only
    affects that specific metric, not other metrics in the same query.
    """
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

    # Query-level filter (region) should be in CTE WHERE
    cte_where = cte.this.find(exp.Where)
    assert cte_where is not None
    cte_where_sql = cte_where.sql()
    assert "region" in cte_where_sql

    # Metric-level filter should NOT be in CTE WHERE (goes in CASE WHEN instead)
    assert "status" not in cte_where_sql

    # Metric-level filter should be in CASE WHEN expression in SELECT
    # Look for the Case expression inside the main SELECT
    main_select = parsed.find(exp.Select)
    case_exprs = list(main_select.find_all(exp.Case))
    assert len(case_exprs) > 0, "Expected CASE WHEN for metric-level filter"

    # The CASE WHEN should contain the status filter
    case_sql = case_exprs[0].sql()
    assert "status" in case_sql
    assert "completed" in case_sql


def test_window_dimension_filter_not_pushed_down(layer):
    """Test that filters on window dimensions stay in the outer query.

    Window functions (LEAD, LAG, ROW_NUMBER, etc.) are computed in the CTE
    SELECT but haven't been evaluated yet at WHERE-clause time, so pushing
    a filter on a window dimension into the CTE WHERE would produce invalid
    SQL. The filter must be applied in the outer query instead.
    """
    model = Model(
        name="events",
        table="events_table",
        primary_key="event_id",
        dimensions=[
            Dimension(name="person_id", type="categorical", sql="person_id"),
            Dimension(name="event_type", type="categorical", sql="event_type"),
            Dimension(
                name="next_event",
                type="categorical",
                sql="event_type",
                window="LEAD(event_type) OVER (PARTITION BY person_id ORDER BY created_at)",
            ),
        ],
        metrics=[
            Metric(name="event_count", agg="count"),
        ],
    )

    layer.add_model(model)

    sql = layer.compile(
        metrics=["events.event_count"],
        dimensions=["events.event_type"],
        filters=["events.next_event = 'purchase'"],
    )

    parsed = sqlglot.parse_one(sql)

    # Find the CTE
    cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "events_cte":
            cte = cte_def
            break

    assert cte is not None, "CTE not found"

    # CTE should NOT have a WHERE clause for the window dimension filter
    cte_where = cte.this.find(exp.Where)
    if cte_where is not None:
        cte_where_sql = cte_where.sql()
        assert "next_event" not in cte_where_sql, "Window dimension filter should NOT be in CTE WHERE clause"
        assert "purchase" not in cte_where_sql, "Window dimension filter value should NOT be in CTE WHERE clause"

    # The window expression should still appear in the CTE SELECT
    cte_sql = cte.sql()
    assert "LEAD" in cte_sql.upper(), "Window function should appear in CTE SELECT"

    # The filter should appear in the outer query WHERE
    outer_where = parsed.find(exp.Where)
    assert outer_where is not None, "Filter should be in outer query WHERE"
    outer_where_sql = outer_where.sql()
    assert "next_event" in outer_where_sql or "purchase" in outer_where_sql, (
        "Window dimension filter should appear in outer query"
    )


def test_window_dimension_filter_with_regular_filter(layer):
    """Test mixed filters: regular filter pushed down, window filter stays outer."""
    model = Model(
        name="events",
        table="events_table",
        primary_key="event_id",
        dimensions=[
            Dimension(name="person_id", type="categorical", sql="person_id"),
            Dimension(name="event_type", type="categorical", sql="event_type"),
            Dimension(
                name="next_event",
                type="categorical",
                sql="event_type",
                window="LEAD(event_type) OVER (PARTITION BY person_id ORDER BY created_at)",
            ),
        ],
        metrics=[
            Metric(name="event_count", agg="count"),
        ],
    )

    layer.add_model(model)

    sql = layer.compile(
        metrics=["events.event_count"],
        dimensions=["events.event_type"],
        filters=[
            "events.event_type = 'click'",
            "events.next_event = 'purchase'",
        ],
    )

    parsed = sqlglot.parse_one(sql)

    # Find the CTE
    cte = None
    for cte_def in parsed.find_all(exp.CTE):
        if cte_def.alias == "events_cte":
            cte = cte_def
            break

    assert cte is not None, "CTE not found"

    # Regular filter (event_type) should be pushed into CTE WHERE
    cte_where = cte.this.find(exp.Where)
    assert cte_where is not None, "Regular filter should be pushed into CTE WHERE"
    cte_where_sql = cte_where.sql()
    assert "event_type" in cte_where_sql, "Regular filter should be in CTE WHERE"
    assert "click" in cte_where_sql, "Regular filter value should be in CTE WHERE"

    # Window filter (next_event) should NOT be in CTE WHERE
    assert "next_event" not in cte_where_sql, "Window dimension filter should NOT be in CTE WHERE"
    assert "purchase" not in cte_where_sql, "Window dimension filter value should NOT be in CTE WHERE"

    # Window filter should be in outer query WHERE
    outer_where = parsed.find(exp.Where)
    assert outer_where is not None, "Window filter should be in outer query"
    outer_where_sql = outer_where.sql()
    assert "next_event" in outer_where_sql or "purchase" in outer_where_sql, (
        "Window dimension filter should appear in outer query WHERE"
    )


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])


def test_window_dim_filter_applied_as_outer_where_in_preagg(layer):
    """Test that window-dim filters are applied as outer WHERE in the preagg path.

    When metrics come from multiple models (triggering pre-aggregation), a filter
    on a window dimension must be applied as an outer WHERE on the final preagg
    join so that ALL models' metrics are constrained. The window dim column is
    projected through the owning model's preagg CTE to make it available.
    """
    from sidemantic.core.model import Relationship

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(
                name="next_status",
                type="categorical",
                sql="status",
                window="LEAD(status) OVER (PARTITION BY customer_id ORDER BY created_at)",
            ),
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[
            Relationship(
                name="order_items",
                type="one_to_many",
                sql="order_id",
                foreign_key="order_id",
            ),
        ],
    )

    order_items = Model(
        name="order_items",
        table="order_items_table",
        primary_key="item_id",
        dimensions=[],
        metrics=[
            Metric(name="quantity", agg="sum", sql="qty"),
        ],
        relationships=[
            Relationship(
                name="orders",
                type="many_to_one",
                foreign_key="order_id",
                primary_key="order_id",
            ),
        ],
    )

    layer.add_model(orders)
    layer.add_model(order_items)

    # This query spans two models, triggering preagg, and filters on a window dim
    sql = layer.compile(
        metrics=["orders.revenue", "order_items.quantity"],
        dimensions=["orders.order_date"],
        filters=["orders.next_status = 'complete'"],
    )

    # The SQL should use pre-aggregation (two model CTEs joined together)
    assert "orders_preagg" in sql, "Should use pre-aggregation path"
    assert "order_items_preagg" in sql, "Should use pre-aggregation path"

    # The window dim column should be projected in the orders preagg CTE
    preagg_start = sql.index("orders_preagg AS (")
    preagg_end = sql.index("order_items_preagg AS (")
    orders_subquery = sql[preagg_start:preagg_end]
    assert "next_status" in orders_subquery, "Window dim column should be projected in orders preagg CTE"

    # The filter should appear in the outer WHERE referencing the preagg CTE,
    # so that BOTH models' metrics are constrained by the filter
    outer_query = sql[sql.rindex("SELECT") :]
    assert "orders_preagg.next_status" in outer_query, (
        "Window-dim filter should be in outer WHERE referencing preagg CTE"
    )


def test_multi_model_window_dim_filter_applied_as_outer_where_in_preagg(layer):
    """Test that multi-model window-dim filters are applied as outer WHERE in preagg.

    When a filter references columns from multiple models and at least one is a
    window dimension, the window dim column is projected through the preagg CTE
    and the filter is applied on the outer WHERE so both models are constrained.
    """
    from sidemantic.core.model import Relationship

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(
                name="next_status",
                type="categorical",
                sql="status",
                window="LEAD(status) OVER (PARTITION BY customer_id ORDER BY created_at)",
            ),
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[
            Relationship(
                name="order_items",
                type="one_to_many",
                sql="order_id",
                foreign_key="order_id",
            ),
        ],
    )

    order_items = Model(
        name="order_items",
        table="order_items_table",
        primary_key="item_id",
        dimensions=[
            Dimension(name="item_status", type="categorical", sql="item_status"),
        ],
        metrics=[
            Metric(name="quantity", agg="sum", sql="qty"),
        ],
        relationships=[
            Relationship(
                name="orders",
                type="many_to_one",
                foreign_key="order_id",
                primary_key="order_id",
            ),
        ],
    )

    layer.add_model(orders)
    layer.add_model(order_items)

    # Multi-model filter: window dim from orders + regular dim from order_items
    sql = layer.compile(
        metrics=["orders.revenue", "order_items.quantity"],
        dimensions=["orders.order_date"],
        filters=["orders.next_status = order_items.item_status"],
    )

    # The SQL should use pre-aggregation (two model CTEs joined together)
    assert "orders_preagg" in sql, "Should use pre-aggregation path"
    assert "order_items_preagg" in sql, "Should use pre-aggregation path"

    # The window dim column should be projected in the orders preagg CTE
    preagg_start = sql.index("orders_preagg AS (")
    preagg_end = sql.index("order_items_preagg AS (")
    orders_subquery = sql[preagg_start:preagg_end]
    assert "next_status" in orders_subquery, "Window dim column should be projected in orders preagg CTE"

    # The filter should appear in the outer WHERE
    outer_query = sql[sql.rindex("SELECT") :]
    assert "orders_preagg.next_status" in outer_query, "Multi-model window-dim filter should be in outer WHERE"


def test_mixed_metric_and_window_dim_filter_applied_as_outer_where_in_preagg(layer):
    """Test that a filter referencing both a metric and a window dim is applied as outer WHERE.

    When a filter like "orders.next_status = 'complete' OR orders.revenue > 100"
    references both a window dimension (next_status) and a metric (revenue), the
    window dim check takes priority and the filter is applied on the outer preagg
    WHERE. The window dim column is projected through the preagg CTE.
    """
    from sidemantic.core.model import Relationship

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(
                name="next_status",
                type="categorical",
                sql="status",
                window="LEAD(status) OVER (PARTITION BY customer_id ORDER BY created_at)",
            ),
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[
            Relationship(
                name="order_items",
                type="one_to_many",
                sql="order_id",
                foreign_key="order_id",
            ),
        ],
    )

    order_items = Model(
        name="order_items",
        table="order_items_table",
        primary_key="item_id",
        dimensions=[],
        metrics=[
            Metric(name="quantity", agg="sum", sql="qty"),
        ],
        relationships=[
            Relationship(
                name="orders",
                type="many_to_one",
                foreign_key="order_id",
                primary_key="order_id",
            ),
        ],
    )

    layer.add_model(orders)
    layer.add_model(order_items)

    # Filter references BOTH a window dim (next_status) and a metric (revenue)
    sql = layer.compile(
        metrics=["orders.revenue", "order_items.quantity"],
        dimensions=["orders.order_date"],
        filters=["orders.next_status = 'complete' OR orders.revenue > 100"],
    )

    # Should use pre-aggregation path
    assert "orders_preagg" in sql, "Should use pre-aggregation path"
    assert "order_items_preagg" in sql, "Should use pre-aggregation path"

    # The window dim column should be projected in the orders preagg CTE
    preagg_start = sql.index("orders_preagg AS (")
    preagg_end = sql.index("order_items_preagg AS (")
    orders_subquery = sql[preagg_start:preagg_end]
    assert "next_status" in orders_subquery, "Window dim column should be projected in orders preagg CTE"

    # The filter should be in the outer WHERE, constraining both models
    outer_query = sql[sql.rindex("SELECT") :]
    assert "orders_preagg.next_status" in outer_query, "Mixed metric/window-dim filter should be in outer WHERE"
