"""Test symmetric aggregate SQL generation."""

import pytest

from sidemantic.core.symmetric_aggregate import (
    build_symmetric_aggregate_sql,
    needs_symmetric_aggregate,
)


def test_symmetric_aggregate_sum_basic():
    """Test building symmetric aggregate SQL for SUM."""
    sql = build_symmetric_aggregate_sql("amount", "order_id", "sum")

    assert "SUM(DISTINCT" in sql
    assert "HASH(order_id)" in sql
    assert "amount" in sql
    assert "HUGEINT" in sql


def test_symmetric_aggregate_sum_with_alias():
    """Test symmetric aggregate SUM with table alias."""
    sql = build_symmetric_aggregate_sql("amount", "order_id", "sum", "orders_cte")

    assert "HASH(orders_cte.order_id)" in sql
    assert "orders_cte.amount" in sql


def test_symmetric_aggregate_avg_basic():
    """Test building symmetric aggregate SQL for AVG."""
    sql = build_symmetric_aggregate_sql("amount", "order_id", "avg")

    assert "SUM(DISTINCT" in sql
    assert "COUNT(DISTINCT order_id)" in sql
    assert "NULLIF" in sql
    assert "/ NULLIF(" in sql


def test_symmetric_aggregate_avg_with_alias():
    """Test symmetric aggregate AVG with table alias."""
    sql = build_symmetric_aggregate_sql("amount", "order_id", "avg", "orders_cte")

    assert "HASH(orders_cte.order_id)" in sql
    assert "orders_cte.amount" in sql
    assert "COUNT(DISTINCT orders_cte.order_id)" in sql


def test_symmetric_aggregate_count():
    """Test building symmetric aggregate SQL for COUNT."""
    sql = build_symmetric_aggregate_sql("amount", "order_id", "count")

    assert sql == "COUNT(DISTINCT order_id)"


def test_symmetric_aggregate_count_with_alias():
    """Test symmetric aggregate COUNT with table alias."""
    sql = build_symmetric_aggregate_sql("amount", "order_id", "count", "orders_cte")

    assert sql == "COUNT(DISTINCT orders_cte.order_id)"


def test_symmetric_aggregate_count_distinct():
    """Test building symmetric aggregate SQL for COUNT_DISTINCT."""
    sql = build_symmetric_aggregate_sql("customer_id", "order_id", "count_distinct")

    # For count distinct, we count the measure itself, not the primary key
    assert sql == "COUNT(DISTINCT customer_id)"


def test_symmetric_aggregate_count_distinct_with_alias():
    """Test symmetric aggregate COUNT_DISTINCT with table alias."""
    sql = build_symmetric_aggregate_sql("customer_id", "order_id", "count_distinct", "orders_cte")

    assert sql == "COUNT(DISTINCT orders_cte.customer_id)"


def test_symmetric_aggregate_unsupported_type():
    """Test error for unsupported aggregation type."""
    with pytest.raises(ValueError, match="Unsupported aggregation type"):
        build_symmetric_aggregate_sql("amount", "order_id", "max")


def test_needs_symmetric_aggregate_one_to_many_base():
    """Test symmetric aggregate needed for one_to_many from base model."""
    result = needs_symmetric_aggregate("one_to_many", is_base_model=True)
    assert result is True


def test_needs_symmetric_aggregate_one_to_many_not_base():
    """Test symmetric aggregate not needed for one_to_many when not base model."""
    result = needs_symmetric_aggregate("one_to_many", is_base_model=False)
    assert result is False


def test_needs_symmetric_aggregate_many_to_one_base():
    """Test symmetric aggregate not needed for many_to_one from base model."""
    result = needs_symmetric_aggregate("many_to_one", is_base_model=True)
    assert result is False


def test_needs_symmetric_aggregate_one_to_one_base():
    """Test symmetric aggregate not needed for one_to_one from base model."""
    result = needs_symmetric_aggregate("one_to_one", is_base_model=True)
    assert result is False


def test_symmetric_aggregate_complex_expression():
    """Test symmetric aggregate with complex measure expression."""
    sql = build_symmetric_aggregate_sql("amount * discount_rate", "order_id", "sum", "orders")

    assert "orders.amount * discount_rate" in sql
    assert "HASH(orders.order_id)" in sql
