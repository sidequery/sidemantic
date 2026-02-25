"""Tests for first critical bug fix commit.

These should have been included in the original commit but weren't.
"""

import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.table_calculation import TableCalculation
from sidemantic.sql.table_calc_processor import TableCalculationProcessor


def test_count_without_sql_generates_valid_cte(layer):
    """Test that COUNT(*) metrics don't generate invalid '* AS metric_raw' syntax.

    Bug: COUNT metrics without explicit sql generated SELECT ..., * AS order_count_raw ...
    which is invalid SQL syntax.

    Fix: Use '1 AS metric_raw' for COUNT(*) instead of '* AS metric_raw'.
    """
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="order_count", agg="count"),  # No sql field - should count rows
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.order_count"], dimensions=["orders.region"])

    # Should NOT have invalid "* AS order_count_raw"
    assert "* AS order_count_raw" not in sql
    # Should have valid "1 AS order_count_raw" for counting
    assert "1 AS order_count_raw" in sql


def test_table_calculation_uses_safe_eval():
    """Test that table calculations don't use eval() for security.

    Bug: _apply_formula used eval() on user-provided expressions, allowing arbitrary code.
    Fix: Use AST-based safe evaluator that only allows arithmetic operations.
    """
    calc = TableCalculation(name="profit_margin", type="formula", expression="${revenue} - ${cost}")

    processor = TableCalculationProcessor([calc])

    # Test with normal data (tuples format)
    results = [
        (100, 60),
        (200, 120),
    ]
    column_names = ["revenue", "cost"]

    processed_results, updated_columns = processor.process(results, column_names)

    # Should calculate correctly
    # Result tuples should now have profit_margin column appended
    assert len(processed_results[0]) == 3  # revenue, cost, profit_margin
    assert processed_results[0][2] == 40  # profit_margin for first row
    assert processed_results[1][2] == 80  # profit_margin for second row
    assert "profit_margin" in updated_columns


def test_table_calculation_rejects_dangerous_operations():
    """Test that safe evaluator rejects function calls and other dangerous operations.

    Even though the formula processor catches exceptions and returns None,
    the safe_eval should reject dangerous operations internally.
    """
    # Test that _safe_eval rejects dangerous operations directly
    processor = TableCalculationProcessor([])

    # Should raise ValueError for function calls
    with pytest.raises(ValueError, match="Invalid expression"):
        processor._safe_eval("__import__('os')")

    # Should raise ValueError for attribute access
    with pytest.raises(ValueError, match="Invalid expression"):
        processor._safe_eval("(5).__class__")


def test_conversion_metrics_use_correct_model(layer):
    """Test that conversion metrics find the correct model, not just first one.

    Bug: Conversion query generation always grabbed first model from graph.models.
    Fix: Search for model that owns the metric or has the entity dimension.
    """
    # Create two models
    users = Model(
        name="users",
        table="users_table",
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", type="numeric"),
        ],
    )

    events = Model(
        name="events",
        table="events_table",
        primary_key="event_id",
        dimensions=[
            Dimension(name="event_id", type="numeric"),
            Dimension(name="user_id", type="numeric"),
            Dimension(name="event_type", type="categorical"),
            Dimension(name="timestamp", type="time", granularity="day"),
        ],
        metrics=[
            Metric(
                name="conversion_rate",
                type="conversion",
                entity="user_id",
                base_event="signup",
                conversion_event="purchase",
                conversion_window="30 days",
            ),
        ],
    )

    # Add in order that would cause bug (users first, events second)
    layer.add_model(users)
    layer.add_model(events)

    # Should find events model (which has the conversion metric), not users
    sql = layer.compile(metrics=["events.conversion_rate"], dimensions=["events.timestamp__month"])

    # Should reference events_table and never users_table
    assert "FROM events_table" in sql
    assert "users_table" not in sql


def test_conversion_metrics_handle_table_backed_models(layer):
    """Test that conversion metrics work with models defined via table=, not sql=.

    Bug: Conversion query builder injected {model.sql} directly, breaking for table-backed models.
    Fix: Build FROM clause that handles both table= and sql= models.
    """
    events = Model(
        name="events",
        table="events_table",  # Using table, not sql
        primary_key="event_id",
        dimensions=[
            Dimension(name="user_id", type="numeric"),
            Dimension(name="event_type", type="categorical"),
            Dimension(name="timestamp", type="time", granularity="day"),
        ],
        metrics=[
            Metric(
                name="conversion_rate",
                type="conversion",
                entity="user_id",
                base_event="signup",
                conversion_event="purchase",
                conversion_window="30 days",
            ),
        ],
    )

    layer.add_model(events)

    sql = layer.compile(metrics=["events.conversion_rate"], dimensions=["events.timestamp__month"])

    # Should reference events_table directly, never (None)
    assert "FROM events_table" in sql
    assert "FROM (None)" not in sql


def test_derived_metric_substitution_uses_word_boundaries(layer):
    """Test that derived metric substitution doesn't mangle identifier substrings.

    Bug: Replacing "revenue" also replaced it inside "gross_revenue", breaking SQL.
    Fix: Use regex word boundaries and sort dependencies by length (longest first).
    """
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="gross_revenue", agg="sum", sql="gross_amount"),
            # Derived metric that references both (no agg needed for derived metrics)
            Metric(name="net_revenue", type="derived", sql="orders.gross_revenue - orders.revenue"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.net_revenue"], dimensions=["orders.region"])

    # Should substitute both dependencies without mangling names
    assert "SUM(orders_cte.gross_revenue_raw)" in sql
    assert "SUM(orders_cte.revenue_raw)" in sql


def test_model_ref_rewrite_matches_cte_identifier_quoting(layer):
    """CTE ref rewriting should follow the same identifier quoting as CTE definitions."""
    layer.dialect = "postgres"
    orders = Model(
        name="ORDERS",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="amount", type="numeric"),
        ],
        metrics=[
            Metric(name="inline_total", type="derived", sql="SUM(ORDERS.amount)"),
        ],
    )
    layer.add_model(orders)

    sql = layer.compile(metrics=["ORDERS.inline_total"])

    assert "WITH ORDERS_cte AS" in sql
    assert "SUM(ORDERS_cte.amount) AS inline_total" in sql
    assert 'SUM("ORDERS_cte".amount) AS inline_total' not in sql


def test_inline_aggregate_dependency_alias_uses_identifier_quoting(layer):
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="id",
        dimensions=[
            Dimension(name="id", type="numeric"),
            Dimension(name="amount", type="numeric"),
            Dimension(name="order total", type="categorical", sql='"order total"'),
        ],
        metrics=[
            Metric(
                name="inline_total",
                sql='SUM("order total") + 0',
            ),
        ],
    )
    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.inline_total"])

    assert 'AS "order total"' in sql
    assert "AS order total" not in sql


def test_count_metrics_with_filters(layer):
    """Test COUNT metrics work correctly with filters.

    Metric-level filters are applied via CASE WHEN inside the aggregation,
    not in the WHERE clause. This ensures each metric's filter only affects
    that specific metric.
    """
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="completed_orders", agg="count", filters=["{model}.status = 'completed'"]),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.completed_orders"], dimensions=["orders.status"])

    # Should have valid COUNT with filter in CASE WHEN
    assert "CASE WHEN" in sql
    assert "status = 'completed'" in sql
    # Should not have invalid * AS syntax
    assert "* AS completed_orders_raw" not in sql


def test_table_calculation_with_division():
    """Test safe evaluator handles division correctly."""
    calc = TableCalculation(name="margin_pct", type="formula", expression="(${revenue} - ${cost}) / ${revenue} * 100")

    processor = TableCalculationProcessor([calc])
    results = [(100, 60)]
    column_names = ["revenue", "cost"]

    processed_results, _ = processor.process(results, column_names)

    # Should calculate: (100-60)/100*100 = 40%
    assert processed_results[0][2] == 40.0


def test_table_calculation_handles_null_values():
    """Test that table calculations handle None/null gracefully."""
    calc = TableCalculation(name="profit", type="formula", expression="${revenue} - ${cost}")

    processor = TableCalculationProcessor([calc])
    results = [(None, 60)]
    column_names = ["revenue", "cost"]

    processed_results, _ = processor.process(results, column_names)

    # Should handle null (converted to 0 in formula): 0 - 60 = -60
    assert processed_results[0][2] == -60


def test_safe_eval_arithmetic_operations_work():
    """Test that safe eval allows valid arithmetic operations."""
    processor = TableCalculationProcessor([])

    # Should allow basic arithmetic
    assert processor._safe_eval("5 + 3") == 8
    assert processor._safe_eval("10 - 4") == 6
    assert processor._safe_eval("6 * 7") == 42
    assert processor._safe_eval("20 / 4") == 5
    assert processor._safe_eval("2 ** 3") == 8
    assert processor._safe_eval("-(5 + 3)") == -8
    assert processor._safe_eval("(10 + 5) * 2") == 30


def test_conversion_invalid_entity_rejected():
    """Conversion metric rejects entity names with SQL injection."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    events = Model(
        name="events",
        table="events_table",
        primary_key="event_id",
        dimensions=[
            Dimension(name="event_id", type="numeric"),
            Dimension(name="user_id", type="numeric"),
            Dimension(name="event_type", type="categorical"),
            Dimension(name="event_time", type="time"),
        ],
        metrics=[
            Metric(
                name="conv",
                type="conversion",
                entity="user_id; DROP TABLE--",
                base_event="signup",
                conversion_event="purchase",
                conversion_window="7 days",
            ),
        ],
    )
    graph = SemanticGraph()
    graph.add_model(events)
    gen = SQLGenerator(graph)

    with pytest.raises(ValueError, match="Invalid entity"):
        gen.generate(metrics=["events.conv"], dimensions=["events.event_time"])


def test_conversion_invalid_window_rejected():
    """Conversion metric rejects window values with SQL injection."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    events = Model(
        name="events",
        table="events_table",
        primary_key="event_id",
        dimensions=[
            Dimension(name="event_id", type="numeric"),
            Dimension(name="user_id", type="numeric"),
            Dimension(name="event_type", type="categorical"),
            Dimension(name="event_time", type="time"),
        ],
        metrics=[
            Metric(
                name="conv",
                type="conversion",
                entity="user_id",
                base_event="signup",
                conversion_event="purchase",
                conversion_window="7;DROP days",
            ),
        ],
    )
    graph = SemanticGraph()
    graph.add_model(events)
    gen = SQLGenerator(graph)

    with pytest.raises(ValueError, match="Invalid window"):
        gen.generate(metrics=["events.conv"], dimensions=["events.event_time"])


def test_conversion_event_name_quotes_escaped():
    """Conversion metric escapes single quotes in event names."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    events = Model(
        name="events",
        table="events_table",
        primary_key="event_id",
        dimensions=[
            Dimension(name="event_id", type="numeric"),
            Dimension(name="user_id", type="numeric"),
            Dimension(name="event_type", type="categorical"),
            Dimension(name="event_time", type="time"),
        ],
        metrics=[
            Metric(
                name="conv",
                type="conversion",
                entity="user_id",
                base_event="sign'up",
                conversion_event="pur'chase",
                conversion_window="7 days",
            ),
        ],
    )
    graph = SemanticGraph()
    graph.add_model(events)
    gen = SQLGenerator(graph)

    sql = gen.generate(metrics=["events.conv"], dimensions=["events.event_time"])
    assert "sign''up" in sql
    assert "pur''chase" in sql


def test_count_fanout_uses_column_reference():
    """COUNT metric uses column reference instead of COUNT(*) to prevent fan-out."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[Dimension(name="region", type="categorical")],
        metrics=[Metric(name="order_count", agg="count", sql="order_id")],
    )
    graph = SemanticGraph()
    graph.add_model(orders)
    gen = SQLGenerator(graph)

    sql = gen.generate(metrics=["orders.order_count"], dimensions=["orders.region"])
    assert "COUNT(*)" not in sql
    assert "COUNT(orders_cte.order_count_raw) AS order_count" in sql


def test_conversion_metric_executes_with_expected_rate(layer):
    """Conversion metric should execute and return a deterministic monthly rate."""
    events = Model(
        name="events",
        table="events_table",
        primary_key="event_id",
        dimensions=[
            Dimension(name="event_id", type="numeric"),
            Dimension(name="user_id", type="numeric"),
            Dimension(name="event_type", type="categorical"),
            Dimension(name="timestamp", type="time", granularity="day"),
        ],
        metrics=[
            Metric(
                name="conversion_rate",
                type="conversion",
                entity="user_id",
                base_event="signup",
                conversion_event="purchase",
                conversion_window="30 days",
            ),
        ],
    )
    layer.add_model(events)

    layer.conn.execute(
        """
        CREATE TABLE events_table (
            event_id INTEGER,
            user_id INTEGER,
            event_type VARCHAR,
            timestamp TIMESTAMP
        )
        """
    )
    layer.conn.execute(
        """
        INSERT INTO events_table VALUES
            (1, 1, 'signup', '2024-01-01'),
            (2, 1, 'purchase', '2024-01-05'),
            (3, 2, 'signup', '2024-01-02'),
            (4, 3, 'signup', '2024-01-03'),
            (5, 3, 'purchase', '2024-02-20')
        """
    )

    sql = layer.compile(metrics=["events.conversion_rate"], dimensions=["events.timestamp__month"])
    rows = layer.conn.execute(sql).fetchall()

    assert len(rows) == 1
    month_value, conversion_rate = rows[0]
    assert month_value is not None
    assert conversion_rate == pytest.approx(1.0 / 3.0, abs=1e-6)


def test_build_interval_duckdb():
    """_build_interval produces correct DuckDB INTERVAL syntax."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    graph = SemanticGraph()
    gen = SQLGenerator(graph, dialect="duckdb")
    assert gen._build_interval("7", "days") == "INTERVAL '7 days'"


def test_build_interval_bigquery():
    """_build_interval produces correct BigQuery INTERVAL syntax."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    graph = SemanticGraph()
    gen = SQLGenerator(graph, dialect="bigquery")
    assert gen._build_interval("7", "days") == "INTERVAL 7 DAY"
    assert gen._build_interval("3", "months") == "INTERVAL 3 MONTH"


def test_build_interval_postgres():
    """_build_interval produces correct Postgres INTERVAL syntax."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    graph = SemanticGraph()
    gen = SQLGenerator(graph, dialect="postgres")
    assert gen._build_interval("30", "days") == "INTERVAL '30 days'"
