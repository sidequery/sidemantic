"""Test error handling and edge cases in SemanticGraph."""

import pytest

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.table_calculation import TableCalculation


def test_add_duplicate_model():
    """Test error when adding duplicate model."""
    graph = SemanticGraph()

    model1 = Model(name="orders", table="orders", primary_key="id")
    model2 = Model(name="orders", table="orders2", primary_key="id")

    graph.add_model(model1)

    with pytest.raises(ValueError, match="Model orders already exists"):
        graph.add_model(model2)


def test_add_duplicate_metric():
    """Test error when adding duplicate metric."""
    graph = SemanticGraph()

    metric1 = Metric(name="revenue", agg="sum", sql="amount")
    metric2 = Metric(name="revenue", agg="sum", sql="total")

    graph.add_metric(metric1)

    with pytest.raises(ValueError, match="Measure revenue already exists"):
        graph.add_metric(metric2)


def test_add_duplicate_table_calculation():
    """Test error when adding duplicate table calculation."""
    graph = SemanticGraph()

    calc1 = TableCalculation(name="pct", type="percent_of_total", field="revenue")
    calc2 = TableCalculation(name="pct", type="percent_of_total", field="amount")

    graph.add_table_calculation(calc1)

    with pytest.raises(ValueError, match="Table calculation pct already exists"):
        graph.add_table_calculation(calc2)


def test_get_nonexistent_model():
    """Test error when getting nonexistent model."""
    graph = SemanticGraph()

    with pytest.raises(KeyError, match="Model orders not found"):
        graph.get_model("orders")


def test_get_nonexistent_metric():
    """Test error when getting nonexistent metric."""
    graph = SemanticGraph()

    with pytest.raises(KeyError, match="Measure revenue not found"):
        graph.get_metric("revenue")


def test_get_nonexistent_table_calculation():
    """Test error when getting nonexistent table calculation."""
    graph = SemanticGraph()

    with pytest.raises(KeyError, match="Table calculation pct not found"):
        graph.get_table_calculation("pct")


def test_find_path_nonexistent_from_model():
    """Test error when finding path with nonexistent source model."""
    graph = SemanticGraph()

    customers = Model(name="customers", table="customers", primary_key="id")
    graph.add_model(customers)

    with pytest.raises(KeyError, match="Model orders not found"):
        graph.find_relationship_path("orders", "customers")


def test_find_path_nonexistent_to_model():
    """Test error when finding path with nonexistent target model."""
    graph = SemanticGraph()

    orders = Model(name="orders", table="orders", primary_key="id")
    graph.add_model(orders)

    with pytest.raises(KeyError, match="Model customers not found"):
        graph.find_relationship_path("orders", "customers")


def test_find_path_same_model():
    """Test finding path returns empty for same model."""
    graph = SemanticGraph()

    orders = Model(name="orders", table="orders", primary_key="id")
    graph.add_model(orders)

    path = graph.find_relationship_path("orders", "orders")
    assert path == []


def test_find_path_no_relationship():
    """Test finding path when no relationship exists."""
    graph = SemanticGraph()

    orders = Model(name="orders", table="orders", primary_key="id")
    customers = Model(name="customers", table="customers", primary_key="id")

    # Add models without relationships
    graph.add_model(orders)
    graph.add_model(customers)

    with pytest.raises(ValueError, match="No join path found"):
        graph.find_relationship_path("orders", "customers")


def test_auto_register_time_comparison_metric():
    """Test that time_comparison metrics are auto-registered at graph level."""
    graph = SemanticGraph()

    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="revenue_wow", type="time_comparison", base_metric="revenue"),
        ],
    )

    graph.add_model(model)

    # time_comparison metric should be auto-registered at graph level
    assert "revenue_wow" in graph.metrics
    assert graph.metrics["revenue_wow"].type == "time_comparison"


def test_no_auto_register_regular_metrics():
    """Test that regular metrics are not auto-registered at graph level."""
    graph = SemanticGraph()

    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="count", agg="count"),
        ],
    )

    graph.add_model(model)

    # Regular metrics should NOT be at graph level
    assert "revenue" not in graph.metrics
    assert "count" not in graph.metrics

    # But should be accessible via model
    assert len(model.metrics) == 2


def test_adjacency_not_built_on_add():
    """Adjacency should not be rebuilt on every add_model."""
    from sidemantic.core.relationship import Relationship

    graph = SemanticGraph()

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
        ],
    )
    customers = Model(name="customers", table="customers", primary_key="id")

    graph.add_model(orders)
    graph.add_model(customers)

    assert graph._adjacency_dirty is True


def test_adjacency_built_on_find_path():
    """find_relationship_path should trigger lazy adjacency rebuild."""
    from sidemantic.core.relationship import Relationship

    graph = SemanticGraph()

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
        ],
    )
    customers = Model(name="customers", table="customers", primary_key="id")

    graph.add_model(orders)
    graph.add_model(customers)

    path = graph.find_relationship_path("orders", "customers")
    assert len(path) == 1
    assert graph._adjacency_dirty is False
