"""Test simplified metric expression syntax."""

from sidemantic.core.metric import Metric
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
