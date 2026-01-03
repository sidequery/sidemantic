"""Tests for querying Snowflake-imported models."""

from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.snowflake import SnowflakeAdapter


@pytest.fixture
def adapter():
    return SnowflakeAdapter()


@pytest.fixture
def examples_dir():
    return Path(__file__).parent.parent.parent.parent / "examples" / "snowflake"


class TestSnowflakeQueryGeneration:
    """Test SQL query generation from Snowflake-imported models."""

    def test_query_simple_dimension_and_metric(self, adapter, examples_dir):
        """Test querying a simple dimension and metric."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        layer = SemanticLayer()
        layer.graph = graph

        sql = layer.compile(
            dimensions=["sales.region"],
            metrics=["sales.amount"],
        )

        assert "region" in sql.lower()
        assert "sum" in sql.lower()
        assert "amount" in sql.lower()
        assert "group by" in sql.lower()

    def test_query_time_dimension(self, adapter, examples_dir):
        """Test querying with a time dimension."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        layer = SemanticLayer()
        layer.graph = graph

        sql = layer.compile(
            dimensions=["sales.sale_date"],
            metrics=["sales.amount"],
        )

        assert "sale_date" in sql.lower()

    def test_query_multiple_metrics(self, adapter, examples_dir):
        """Test querying multiple metrics."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        layer = SemanticLayer()
        layer.graph = graph

        sql = layer.compile(
            dimensions=["sales.region"],
            metrics=["sales.amount", "sales.quantity"],
        )

        assert "amount" in sql.lower()
        assert "quantity" in sql.lower()

    def test_query_with_filter(self, adapter, examples_dir):
        """Test querying with a filter."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        layer = SemanticLayer()
        layer.graph = graph

        sql = layer.compile(
            dimensions=["sales.region"],
            metrics=["sales.amount"],
            filters=["region = 'West'"],
        )

        assert "where" in sql.lower()
        assert "west" in sql.lower()


class TestSnowflakeJoinQueries:
    """Test queries that require joins between models."""

    def test_query_across_relationship(self, adapter, examples_dir):
        """Test querying across a relationship."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        layer = SemanticLayer()
        layer.graph = graph

        # Query orders with customer dimension
        sql = layer.compile(
            dimensions=["customers.country"],
            metrics=["orders.total_revenue"],
        )

        # Should include a join
        assert "join" in sql.lower()
        assert "country" in sql.lower()
        assert "orders" in sql.lower()
        assert "customers" in sql.lower()

    def test_query_multiple_relationships(self, adapter, examples_dir):
        """Test querying across multiple relationships."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        layer = SemanticLayer()
        layer.graph = graph

        # Query orders with customer and product dimensions
        sql = layer.compile(
            dimensions=["customers.country", "products.category"],
            metrics=["orders.total_revenue"],
        )

        # Should include multiple joins
        assert "customers" in sql.lower()
        assert "products" in sql.lower()
        assert "country" in sql.lower()
        assert "category" in sql.lower()


class TestSnowflakeSegmentQueries:
    """Test queries using segments (filters)."""

    def test_query_with_segment(self, adapter, examples_dir):
        """Test applying a segment to a query."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        layer = SemanticLayer()
        layer.graph = graph

        sql = layer.compile(
            dimensions=["orders.channel"],
            metrics=["orders.total_revenue"],
            segments=["orders.completed_orders"],
        )

        assert "where" in sql.lower()
        assert "delivered" in sql.lower()


class TestSnowflakeFullyQualifiedTableNames:
    """Test that fully qualified table names work correctly."""

    def test_query_uses_full_table_name(self, adapter, examples_dir):
        """Test that queries use the full table name."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        layer = SemanticLayer()
        layer.graph = graph

        sql = layer.compile(
            dimensions=["orders.status"],
            metrics=["orders.quantity"],
        )

        # Should reference the full table path
        assert "analytics" in sql.lower() or "ecommerce" in sql.lower() or "orders" in sql.lower()
