"""Test simplified metric expression syntax."""

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.core.sql_definitions import parse_sql_definitions


def test_metric_with_full_expression():
    """Test that metrics can be defined with just sql='SUM(amount)' instead of agg + sql."""
    # SUM
    m1 = Metric(name="revenue", sql="SUM(amount)")
    assert m1.agg == "sum"
    assert m1.sql == "amount"

    # COUNT
    m2 = Metric(name="orders", sql="COUNT(*)")
    assert m2.agg == "count"
    assert m2.sql == "*"

    # COUNT DISTINCT
    m3 = Metric(name="customers", sql="COUNT(DISTINCT customer_id)")
    assert m3.agg == "count_distinct"
    assert m3.sql == "customer_id"

    # AVG
    m4 = Metric(name="avg_price", sql="AVG(price)")
    assert m4.agg == "avg"
    assert m4.sql == "price"

    # MIN
    m5 = Metric(name="min_price", sql="MIN(price)")
    assert m5.agg == "min"
    assert m5.sql == "price"

    # MAX
    m6 = Metric(name="max_price", sql="MAX(price)")
    assert m6.agg == "max"
    assert m6.sql == "price"

    # MEDIAN
    m7 = Metric(name="median_price", sql="MEDIAN(price)")
    assert m7.agg == "median"
    assert m7.sql == "price"

    # Statistical aggregations
    m8 = Metric(name="stddev_price", sql="STDDEV(price)")
    assert m8.agg == "stddev"
    assert m8.sql == "price"

    m9 = Metric(name="stddev_pop_price", sql="STDDEV_POP(price)")
    assert m9.agg == "stddev_pop"
    assert m9.sql == "price"

    m10 = Metric(name="variance_price", sql="VARIANCE(price)")
    assert m10.agg == "variance"
    assert m10.sql == "price"

    m11 = Metric(name="variance_pop_price", sql="VAR_POP(price)")
    assert m11.agg == "variance_pop"
    assert m11.sql == "price"

    m12 = Metric(name="variance_pop_price", sql="VARIANCE_POP(price)")
    assert m12.agg == "variance_pop"
    assert m12.sql == "price"


def test_metric_expression_case_insensitive():
    """Test that aggregation function parsing is case-insensitive."""
    m1 = Metric(name="revenue", sql="sum(amount)")
    assert m1.agg == "sum"
    assert m1.sql == "amount"

    m2 = Metric(name="revenue2", sql="SuM(amount)")
    assert m2.agg == "sum"
    assert m2.sql == "amount"


def test_metric_expression_with_whitespace():
    """Test that expression parsing handles whitespace."""
    m1 = Metric(name="revenue", sql=" SUM( amount ) ")
    assert m1.agg == "sum"
    assert m1.sql == "amount"


def test_metric_old_syntax_still_works():
    """Test that the old agg + sql syntax still works."""
    m = Metric(name="revenue", agg="sum", sql="amount")
    assert m.agg == "sum"
    assert m.sql == "amount"

    variance_pop = Metric(name="variance_pop_price", agg="variance_pop", sql="price")
    assert variance_pop.to_sql() == "VAR_POP(price)"


def test_graph_level_variance_pop_metric_compiles_to_var_pop():
    """Graph-level statistical aggregations should use DuckDB's VAR_POP spelling."""
    layer = SemanticLayer()
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="order_count", agg="count", sql="id")],
        )
    )
    layer.graph.add_metric(Metric(name="amount_variance_pop", agg="variance_pop", sql="orders.amount"))

    sql = layer.compile(metrics=["amount_variance_pop"])

    assert "VAR_POP(" in sql
    assert "VARIANCE_POP(" not in sql


def test_metric_expr_alias():
    """Test that expr can be used as alias for sql."""
    m = Metric(name="revenue", expr="SUM(amount)")
    assert m.agg == "sum"
    assert m.sql == "amount"


def test_metric_sql_definition_with_expression():
    """Test SQL syntax METRIC(name, sql SUM(amount))."""
    sql = """
    METRIC (
        name revenue,
        sql SUM(amount)
    );

    METRIC (
        name customers,
        sql COUNT(DISTINCT customer_id)
    );

    METRIC (
        name avg_price,
        sql AVG(price)
    );
    """

    result = parse_sql_definitions(sql)
    metrics = result[0] if isinstance(result, tuple) else result

    assert len(metrics) == 3

    assert metrics[0].name == "revenue"
    assert metrics[0].agg == "sum"
    assert metrics[0].sql == "amount"

    assert metrics[1].name == "customers"
    assert metrics[1].agg == "count_distinct"
    assert metrics[1].sql == "customer_id"

    assert metrics[2].name == "avg_price"
    assert metrics[2].agg == "avg"
    assert metrics[2].sql == "price"


def test_metric_complex_expression_not_parsed():
    """Test that complex expressions (not just aggregations) aren't parsed."""
    # Complex expression should not be parsed as simple aggregation
    m = Metric(name="profit", type="derived", sql="revenue - cost")
    assert m.agg is None
    assert m.sql == "revenue - cost"
    assert m.type == "derived"


def test_ratio_prefers_exact_graph_metric_with_dotted_name():
    """Dotted ratio refs can name graph metrics and must not be split first."""
    layer = SemanticLayer(auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            status VARCHAR,
            amount INTEGER
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders VALUES
            (1, 'paid', 100),
            (2, 'paid', 50),
            (3, 'open', 25)
    """)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    layer.add_metric(Metric(name="orders.revenue", type="derived", sql="SUM(orders.amount) * 2"))
    layer.add_metric(Metric(name="exact_ratio", type="ratio", numerator="orders.revenue", denominator="orders.revenue"))

    sql = layer.compile(metrics=["exact_ratio"], dimensions=["orders.status"])
    assert "orders_cte.revenue_raw" not in sql
    assert "SUM(orders_cte.amount) * 2" in sql

    rows = layer.query(metrics=["exact_ratio"], dimensions=["orders.status"], order_by=["orders.status"]).fetchall()
    assert rows == [("open", 1.0), ("paid", 1.0)]
