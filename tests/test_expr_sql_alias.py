"""Test that expr and sql are interchangeable aliases."""

import pytest

from sidemantic import Dimension, Metric


def test_metric_with_sql():
    """Test metric with sql parameter."""
    metric = Metric(name="revenue", agg="sum", sql="amount")
    assert metric.sql == "amount"
    assert metric.sql_expr == "amount"


def test_metric_with_expr():
    """Test metric with expr parameter (should alias to sql)."""
    metric = Metric(name="revenue", agg="sum", expr="amount")
    assert metric.sql == "amount"
    assert metric.sql_expr == "amount"


def test_metric_with_both_matching():
    """Test metric with both sql and expr when they match."""
    metric = Metric(name="revenue", agg="sum", sql="amount", expr="amount")
    assert metric.sql == "amount"
    assert metric.sql_expr == "amount"


def test_metric_with_both_different():
    """Test metric with both sql and expr when they differ (should error)."""
    with pytest.raises(ValueError, match="Cannot specify both sql=.*and expr=.*with different values"):
        Metric(name="revenue", agg="sum", sql="amount", expr="total")


def test_metric_expr_not_stored():
    """Test that expr is not stored as a separate field."""
    metric = Metric(name="revenue", agg="sum", expr="amount")
    # expr should not be in the model dump
    assert "expr" not in metric.model_dump()
    assert "sql" in metric.model_dump()


def test_dimension_with_sql():
    """Test dimension with sql parameter."""
    dim = Dimension(name="status", type="categorical", sql="order_status")
    assert dim.sql == "order_status"
    assert dim.sql_expr == "order_status"


def test_dimension_with_expr():
    """Test dimension with expr parameter (should alias to sql)."""
    dim = Dimension(name="status", type="categorical", expr="order_status")
    assert dim.sql == "order_status"
    assert dim.sql_expr == "order_status"


def test_dimension_with_both_matching():
    """Test dimension with both sql and expr when they match."""
    dim = Dimension(name="status", type="categorical", sql="order_status", expr="order_status")
    assert dim.sql == "order_status"
    assert dim.sql_expr == "order_status"


def test_dimension_with_both_different():
    """Test dimension with both sql and expr when they differ (should error)."""
    with pytest.raises(ValueError, match="Cannot specify both sql=.*and expr=.*with different values"):
        Dimension(name="status", type="categorical", sql="order_status", expr="status_code")


def test_dimension_expr_not_stored():
    """Test that expr is not stored as a separate field."""
    dim = Dimension(name="status", type="categorical", expr="order_status")
    # expr should not be in the model dump
    assert "expr" not in dim.model_dump()
    assert "sql" in dim.model_dump()


def test_metric_none_values():
    """Test that None values work correctly."""
    # Both None is fine
    metric1 = Metric(name="count", agg="count")
    assert metric1.sql is None

    # One None is fine
    metric2 = Metric(name="revenue", agg="sum", sql="amount", expr=None)
    assert metric2.sql == "amount"

    metric3 = Metric(name="revenue", agg="sum", sql=None, expr="amount")
    assert metric3.sql == "amount"


def test_dimension_none_values():
    """Test that None values work correctly."""
    # Both None is fine (sql defaults to name)
    dim1 = Dimension(name="status", type="categorical")
    assert dim1.sql is None
    assert dim1.sql_expr == "status"  # defaults to name

    # One None is fine
    dim2 = Dimension(name="status", type="categorical", sql="order_status", expr=None)
    assert dim2.sql == "order_status"

    dim3 = Dimension(name="status", type="categorical", sql=None, expr="order_status")
    assert dim3.sql == "order_status"


def test_metric_serialization_roundtrip():
    """Test that metrics can be serialized and deserialized with expr."""
    # Create with expr
    metric1 = Metric(name="revenue", agg="sum", expr="amount")

    # Serialize to dict
    data = metric1.model_dump()

    # Should not contain expr
    assert "expr" not in data
    assert data["sql"] == "amount"

    # Deserialize
    metric2 = Metric(**data)
    assert metric2.sql == "amount"


def test_dimension_serialization_roundtrip():
    """Test that dimensions can be serialized and deserialized with expr."""
    # Create with expr
    dim1 = Dimension(name="status", type="categorical", expr="order_status")

    # Serialize to dict
    data = dim1.model_dump()

    # Should not contain expr
    assert "expr" not in data
    assert data["sql"] == "order_status"

    # Deserialize
    dim2 = Dimension(**data)
    assert dim2.sql == "order_status"


def test_metric_dict_construction():
    """Test constructing Metric from dict with expr."""
    data = {"name": "revenue", "agg": "sum", "expr": "amount"}
    metric = Metric(**data)
    assert metric.sql == "amount"


def test_dimension_dict_construction():
    """Test constructing Dimension from dict with expr."""
    data = {"name": "status", "type": "categorical", "expr": "order_status"}
    dim = Dimension(**data)
    assert dim.sql == "order_status"
