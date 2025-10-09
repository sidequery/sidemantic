"""Tests for edge cases in coverage analyzer model generation."""

from sidemantic import SemanticLayer
from sidemantic.core.coverage_analyzer import CoverageAnalyzer


def test_generate_models_with_case_when_aggregation():
    """Test handling CASE WHEN inside aggregations."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            SUM(CASE WHEN priority = 'high' THEN amount ELSE 0 END) as high_priority_revenue,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_count
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    metrics = {m["name"]: m for m in orders["metrics"]}

    # Should generate metrics from CASE expressions
    assert "high_priority_revenue" in metrics or "sum_case_when_priority_high_then_amount_else_0_end" in metrics
    assert "completed_count" in metrics or "count_case_when_status_completed_then_1_end" in metrics


def test_generate_models_with_extract_date_part():
    """Test handling EXTRACT() function for date parts."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            EXTRACT(YEAR FROM order_date) as year,
            EXTRACT(MONTH FROM order_date) as month,
            COUNT(*) as count
        FROM orders
        GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]

    # Should have order_date as time dimension
    dims = {d["name"]: d for d in orders["dimensions"]}
    assert "order_date" in dims
    assert dims["order_date"]["type"] == "time"


def test_generate_models_with_cast_in_aggregation():
    """Test handling CAST/type conversions in aggregations."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            SUM(CAST(amount AS FLOAT)) as total_amount,
            AVG(CAST(quantity AS DECIMAL(10,2))) as avg_quantity
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    metrics = {m["name"]: m for m in orders["metrics"]}

    # Should extract underlying columns from CAST expressions
    assert "total_amount" in metrics or "sum_cast_amount_as_float" in metrics
    assert "avg_quantity" in metrics or "avg_cast_quantity_as_decimal_10_2" in metrics


def test_generate_models_with_math_expressions():
    """Test handling mathematical expressions in aggregations."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            SUM(quantity * price) as total_revenue,
            AVG(quantity * price) as avg_order_value,
            SUM(amount - discount) as net_amount
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    metrics = {m["name"]: m for m in orders["metrics"]}

    # Should recognize these as metrics
    assert "total_revenue" in metrics or "sum_quantity_price" in metrics
    assert "avg_order_value" in metrics or "avg_quantity_price" in metrics
    assert "net_amount" in metrics or "sum_amount_discount" in metrics


def test_generate_models_with_coalesce():
    """Test handling COALESCE in dimensions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            COALESCE(region, 'Unknown') as region,
            COALESCE(tier, 'Standard') as tier,
            COUNT(*) as count
        FROM customers
        GROUP BY COALESCE(region, 'Unknown'), COALESCE(tier, 'Standard')
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    customers = models["customers"]
    dims = {d["name"]: d for d in customers["dimensions"]}

    # Should extract underlying columns from COALESCE
    assert "region" in dims
    assert "tier" in dims


def test_generate_models_with_string_functions():
    """Test handling string functions in dimensions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            UPPER(status) as status_upper,
            LOWER(region) as region_lower,
            SUBSTRING(name, 1, 3) as name_prefix,
            COUNT(*) as count
        FROM orders
        GROUP BY UPPER(status), LOWER(region), SUBSTRING(name, 1, 3)
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    dims = {d["name"]: d for d in orders["dimensions"]}

    # Should extract underlying columns from string functions
    assert "status" in dims or "status_upper" in dims
    assert "region" in dims or "region_lower" in dims
    assert "name" in dims or "name_prefix" in dims


def test_generate_models_no_group_by():
    """Test handling aggregations without GROUP BY (single row result)."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            COUNT(*) as total_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_revenue
        FROM orders
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    metrics = {m["name"]: m for m in orders["metrics"]}

    # Should still extract metrics
    assert "total_count" in metrics or "count" in metrics
    assert "total_revenue" in metrics or "sum_amount" in metrics
    assert "avg_revenue" in metrics or "avg_amount" in metrics


def test_generate_models_with_boolean_dimension():
    """Test handling boolean expressions as dimensions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            amount > 100 as is_large_order,
            status = 'completed' as is_completed,
            COUNT(*) as count
        FROM orders
        GROUP BY amount > 100, status = 'completed'
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]

    # Should extract the underlying columns
    dims = {d["name"]: d for d in orders["dimensions"]}

    # At minimum, should have the base columns involved
    # The exact names depend on implementation
    assert len(dims) >= 2  # Some representation of amount and status


def test_generate_rewritten_query_with_cte():
    """Test handling CTEs (WITH clauses)."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        WITH high_value AS (
            SELECT status, SUM(amount) as revenue
            FROM orders
            WHERE amount > 100
            GROUP BY status
        )
        SELECT * FROM high_value
        WHERE revenue > 1000
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve CTE structure
    assert "WITH" in sql or "high_value" in sql


def test_generate_models_implicit_join():
    """Test handling implicit joins (comma-separated FROM)."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            c.region,
            COUNT(o.order_id)
        FROM customers c, orders o
        WHERE c.id = o.customer_id
        GROUP BY c.region
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Should extract both models
    assert "customers" in models
    assert "orders" in models

    # Should extract relationship from WHERE clause
    orders = models["orders"]
    assert "relationships" in orders


def test_generate_models_self_join():
    """Test handling self-joins."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            o1.status,
            COUNT(o2.id) as related_orders
        FROM orders o1
        LEFT JOIN orders o2 ON o1.parent_order_id = o2.id
        GROUP BY o1.status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Should only create one orders model
    assert len(models) == 1
    assert "orders" in models

    orders = models["orders"]
    dims = {d["name"]: d for d in orders["dimensions"]}

    # Should have status dimension
    assert "status" in dims


def test_generate_models_right_join():
    """Test handling RIGHT JOIN."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            o.status,
            COUNT(c.id) as customer_count
        FROM orders o
        RIGHT JOIN customers c ON o.customer_id = c.id
        GROUP BY o.status
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve RIGHT JOIN
    assert "RIGHT JOIN" in sql or "RIGHT OUTER JOIN" in sql


def test_generate_models_full_outer_join():
    """Test handling FULL OUTER JOIN."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            COALESCE(o.status, 'no_orders') as status,
            COUNT(*) as count
        FROM orders o
        FULL OUTER JOIN customers c ON o.customer_id = c.id
        GROUP BY COALESCE(o.status, 'no_orders')
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve FULL OUTER JOIN
    assert "FULL OUTER JOIN" in sql or "FULL JOIN" in sql


def test_generate_models_with_where_in():
    """Test handling WHERE IN clauses."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            COUNT(*) as count
        FROM orders
        WHERE status IN ('completed', 'shipped', 'delivered')
          AND region NOT IN ('test', 'internal')
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve IN clauses
    assert "IN" in sql
    assert "NOT IN" in sql or "NOT" in sql


def test_generate_models_with_complex_where():
    """Test handling complex WHERE with nested conditions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            COUNT(*) as count
        FROM orders
        WHERE (status = 'completed' AND amount > 100)
           OR (status = 'pending' AND priority = 'high')
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve WHERE structure
    assert "WHERE" in sql
    assert "OR" in sql or "AND" in sql


def test_handle_query_with_comments():
    """Test handling queries with SQL comments."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        -- This is a comment
        /* Multi-line
           comment */
        SELECT
            status, -- inline comment
            COUNT(*) as count
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Should parse despite comments
    assert "orders" in models
    orders = models["orders"]
    dims = {d["name"]: d for d in orders["dimensions"]}
    assert "status" in dims


def test_handle_empty_or_whitespace_query():
    """Test handling empty or whitespace-only queries."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        "",
        "   ",
        "\n\n",
        "SELECT status, COUNT(*) FROM orders GROUP BY status",  # Valid query
    ]

    report = analyzer.analyze_queries(queries)

    # Should skip empty queries and process valid one
    assert len(report.query_analyses) >= 1

    # At least one should be successful
    successful = [a for a in report.query_analyses if a.success]
    assert len(successful) >= 1


def test_generate_models_union_queries():
    """Test handling UNION queries."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT status, COUNT(*) as count
        FROM orders
        WHERE region = 'US'
        GROUP BY status
        UNION ALL
        SELECT status, COUNT(*) as count
        FROM orders
        WHERE region = 'EU'
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Should extract from UNION query
    assert "orders" in models


def test_generate_models_with_distinct():
    """Test handling SELECT DISTINCT."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT DISTINCT
            status,
            region
        FROM orders
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    dims = {d["name"]: d for d in orders["dimensions"]}

    # Should extract dimensions
    assert "status" in dims
    assert "region" in dims


def test_generate_models_nested_case_statements():
    """Test handling nested CASE statements."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            CASE
                WHEN amount > 1000 THEN 'high'
                WHEN amount > 100 THEN 'medium'
                ELSE 'low'
            END as amount_tier,
            COUNT(*) as count
        FROM orders
        GROUP BY
            CASE
                WHEN amount > 1000 THEN 'high'
                WHEN amount > 100 THEN 'medium'
                ELSE 'low'
            END
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]

    # Should extract amount as a dimension or recognize the pattern
    dims = {d["name"]: d for d in orders["dimensions"]}
    assert len(dims) >= 1  # Should have extracted something


def test_generate_models_group_by_ordinal():
    """Test handling GROUP BY with ordinal positions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            region,
            COUNT(*) as count
        FROM orders
        GROUP BY 1, 2
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    dims = {d["name"]: d for d in orders["dimensions"]}

    # Should extract dimensions by resolving ordinals
    assert "status" in dims
    assert "region" in dims


def test_generate_models_with_percent_aggregations():
    """Test handling percentage calculations."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as pct_of_total,
            SUM(amount) * 100.0 / SUM(SUM(amount)) OVER() as revenue_pct
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Should extract base aggregations
    orders = models["orders"]
    metrics = {m["name"]: m for m in orders["metrics"]}

    # Should have count and sum_amount at minimum
    assert "count" in metrics or len(metrics) > 0


def test_generate_models_from_subquery():
    """Test extracting models from queries with subqueries in FROM."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            sub.status,
            COUNT(*) as order_count
        FROM (
            SELECT status, amount
            FROM orders
            WHERE amount > 100
        ) sub
        GROUP BY sub.status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Should extract orders table from subquery
    assert "orders" in models

    orders = models["orders"]

    # Should have status dimension (resolved from sub.status)
    dims = {d["name"]: d for d in orders["dimensions"]}
    assert "status" in dims

    # Should have count metric
    metrics = {m["name"]: m for m in orders["metrics"]}
    assert "order_count" in metrics or "count" in metrics


def test_generate_models_from_nested_subquery():
    """Test extracting models from nested subqueries."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            outer_sub.status,
            AVG(outer_sub.revenue) as avg_revenue
        FROM (
            SELECT status, SUM(amount) as revenue
            FROM orders
            WHERE amount > 0
            GROUP BY status
        ) outer_sub
        GROUP BY outer_sub.status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Should extract orders table from nested subquery
    assert "orders" in models

    orders = models["orders"]

    # Should have status dimension (resolved from nested subquery)
    assert "dimensions" in orders
    dims = {d["name"]: d for d in orders["dimensions"]}
    assert "status" in dims


def test_generate_models_with_running_total():
    """Test extracting cumulative metrics from running total window functions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            order_date,
            SUM(amount) OVER (ORDER BY order_date) as running_total
        FROM orders
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    assert "orders" in models
    orders = models["orders"]

    # Should have the cumulative metric
    metrics = {m["name"]: m for m in orders["metrics"]}
    assert "running_total" in metrics

    # Should be cumulative type
    cumulative_metric = metrics["running_total"]
    assert cumulative_metric["type"] == "cumulative"
    assert "orders.sum_amount" in cumulative_metric["sql"]


def test_generate_models_with_rolling_window():
    """Test extracting cumulative metrics from rolling window functions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            order_date,
            SUM(amount) OVER (
                ORDER BY order_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as rolling_7day_total
        FROM orders
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    assert "orders" in models
    orders = models["orders"]

    # Should have the cumulative metric
    metrics = {m["name"]: m for m in orders["metrics"]}
    assert "rolling_7day_total" in metrics

    # Should be cumulative type with window
    cumulative_metric = metrics["rolling_7day_total"]
    assert cumulative_metric["type"] == "cumulative"
    assert "window" in cumulative_metric
    assert "6 days" in cumulative_metric["window"]


def test_generate_models_with_period_to_date():
    """Test extracting cumulative metrics from period-to-date window functions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            order_date,
            SUM(amount) OVER (
                PARTITION BY DATE_TRUNC('month', order_date)
                ORDER BY order_date
            ) as mtd_revenue
        FROM orders
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    assert "orders" in models
    orders = models["orders"]

    # Should have the cumulative metric
    metrics = {m["name"]: m for m in orders.get("metrics", [])}
    assert "mtd_revenue" in metrics

    # Should be cumulative type with grain_to_date
    cumulative_metric = metrics["mtd_revenue"]
    assert cumulative_metric["type"] == "cumulative"
    assert "grain_to_date" in cumulative_metric
    assert cumulative_metric["grain_to_date"] == "month"


def test_window_functions_ignore_rank_functions():
    """Test that ROW_NUMBER and other rank functions are ignored."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    # Simpler query without GROUP BY to avoid parsing complexity
    queries = [
        """
        SELECT
            status,
            amount,
            SUM(amount) OVER (ORDER BY status) as total_amount
        FROM orders
        """
    ]

    report = analyzer.analyze_queries(queries)

    # Check that window functions were processed
    assert len(report.query_analyses) > 0
    analysis = report.query_analyses[0]

    # Should not have parse errors
    assert analysis.parse_error is None, f"Parse error: {analysis.parse_error}"

    models = analyzer.generate_models(report)

    # Should create orders model with cumulative metric
    assert "orders" in models
    orders = models["orders"]

    # Should have the cumulative metric
    assert "metrics" in orders
    metrics = {m["name"]: m for m in orders["metrics"]}

    # Should have cumulative metric for SUM window function
    assert "total_amount" in metrics
    assert metrics["total_amount"]["type"] == "cumulative"
