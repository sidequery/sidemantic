"""Test validation and error handling."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.validation import (
    MetricValidationError,
    ModelValidationError,
    QueryValidationError,
)


def test_model_has_default_primary_key():
    """Test that models have a default primary key."""
    sl = SemanticLayer()

    # Model without explicit primary_key should default to "id"
    model = Model(
        name="orders",
        table="orders",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[],
    )

    sl.add_model(model)
    assert model.primary_key == "id"


def test_model_validation_no_table():
    """Test that models without table or sql are rejected."""
    sl = SemanticLayer()

    invalid_model = Model(
        name="orders",
        primary_key="id",
        dimensions=[],
        metrics=[],
    )

    with pytest.raises(ModelValidationError) as exc_info:
        sl.add_model(invalid_model)

    assert "must have either 'table' or 'sql' defined" in str(exc_info.value)


def test_metric_validation_simple_no_measure():
    """Test that Pydantic rejects invalid metric types at model creation time."""
    # Try to create metric with invalid type - should fail at Pydantic validation
    with pytest.raises(Exception) as exc_info:
        Metric(name="bad_metric", type="invalid_type")

    assert "literal_error" in str(exc_info.value).lower() or "validation" in str(exc_info.value).lower()


def test_metric_validation_measure_not_found():
    """Test that metrics referencing non-existent measures are rejected."""
    sl = SemanticLayer()

    # Add valid model
    sl.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    # Try to reference non-existent measure
    invalid_metric = Metric(
        name="bad_metric",
        sql="orders.nonexistent"
    )

    with pytest.raises(MetricValidationError) as exc_info:
        sl.add_metric(invalid_metric)

    assert "measure 'nonexistent' not found" in str(exc_info.value)


def test_metric_validation_self_reference():
    """Test that self-referencing metrics are detected."""
    sl = SemanticLayer()

    # Add model
    sl.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    # Try to add self-referencing metric
    # Note: dependencies are auto-detected from expr now
    invalid_metric = Metric(
        name="metric_a",
        type="derived",
        sql="metric_a + 1",
    )

    with pytest.raises(MetricValidationError) as exc_info:
        sl.add_metric(invalid_metric)

    assert "cannot reference itself" in str(exc_info.value)


def test_query_validation_metric_not_found():
    """Test that queries with non-existent metrics fail validation."""
    sl = SemanticLayer()

    sl.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    with pytest.raises(QueryValidationError) as exc_info:
        sl.compile(
            metrics=["nonexistent_metric"],
            dimensions=["orders.status"]
        )

    assert "Metric 'nonexistent_metric' not found" in str(exc_info.value)


def test_query_validation_dimension_not_found():
    """Test that queries with non-existent dimensions fail validation."""
    sl = SemanticLayer()

    sl.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    with pytest.raises(QueryValidationError) as exc_info:
        sl.compile(
            metrics=["orders.revenue"],
            dimensions=["orders.nonexistent"]
        )

    assert "Dimension 'nonexistent' not found" in str(exc_info.value)


def test_query_validation_no_join_path():
    """Test that queries requiring non-existent joins fail validation."""
    sl = SemanticLayer()

    # Add two disconnected models
    sl.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    sl.add_model(
        Model(
            name="products",
            table="products",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
            metrics=[],
        )
    )

    with pytest.raises(QueryValidationError) as exc_info:
        sl.compile(
            metrics=["orders.revenue"],
            dimensions=["products.category"]
        )

    # Order of models in error message may vary
    assert "No join path found between models" in str(exc_info.value)
    assert "'orders'" in str(exc_info.value)
    assert "'products'" in str(exc_info.value)


def test_query_validation_invalid_granularity():
    """Test that invalid time granularities are rejected."""
    sl = SemanticLayer()

    sl.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="created_at")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    with pytest.raises(QueryValidationError) as exc_info:
        sl.compile(
            metrics=["orders.revenue"],
            dimensions=["orders.order_date__invalid_granularity"]
        )

    assert "Invalid time granularity 'invalid_granularity'" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
