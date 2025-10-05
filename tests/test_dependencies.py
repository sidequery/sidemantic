"""Test automatic dependency detection."""

from sidemantic.core.dependency_analyzer import extract_column_references
from sidemantic.core.metric import Metric


def test_extract_column_references_simple():
    """Test extracting column references from simple expression."""
    sql = "revenue / cost"
    refs = extract_column_references(sql)
    assert refs == {"revenue", "cost"}


def test_extract_column_references_with_functions():
    """Test extracting from expression with SQL functions."""
    sql = "SUM(amount) / COUNT(*)"
    refs = extract_column_references(sql)
    assert "amount" in refs


def test_extract_column_references_with_case():
    """Test extracting from CASE expression."""
    sql = "CASE WHEN status = 'completed' THEN amount ELSE 0 END"
    refs = extract_column_references(sql)
    assert {"status", "amount"}.issubset(refs)


def test_simple_metric_dependencies():
    """Test untyped metric with simple reference returns its measure."""
    metric = Metric(name="total_revenue", sql="revenue")
    deps = metric.get_dependencies()
    assert deps == {"revenue"}


def test_ratio_metric_dependencies():
    """Test ratio metric returns numerator and denominator."""
    metric = Metric(name="profit_margin", type="ratio", numerator="profit", denominator="revenue")
    deps = metric.get_dependencies()
    assert deps == {"profit", "revenue"}


def test_derived_metric_dependencies():
    """Test derived metric parses expr for dependencies."""
    metric = Metric(name="net_margin", type="derived", sql="(revenue - cost) / revenue")
    deps = metric.get_dependencies()
    assert {"revenue", "cost"}.issubset(deps)


def test_cumulative_metric_dependencies():
    """Test cumulative metric returns its measure."""
    metric = Metric(name="running_total", type="cumulative", sql="daily_revenue", window="7 days")
    deps = metric.get_dependencies()
    assert deps == {"daily_revenue"}


def test_time_comparison_metric_dependencies():
    """Test time comparison metric returns base metric."""
    metric = Metric(
        name="revenue_yoy", type="time_comparison", base_metric="revenue", comparison_type="yoy"
    )
    deps = metric.get_dependencies()
    assert deps == {"revenue"}
