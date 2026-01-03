"""Tests for Snowflake adapter parsing."""

from pathlib import Path

import pytest

from sidemantic.adapters.snowflake import SnowflakeAdapter


@pytest.fixture
def adapter():
    return SnowflakeAdapter()


@pytest.fixture
def examples_dir():
    return Path(__file__).parent.parent.parent.parent / "examples" / "snowflake"


class TestSnowflakeBasicParsing:
    """Test basic Snowflake semantic model parsing."""

    def test_parse_simple_model(self, adapter, examples_dir):
        """Test parsing a simple Snowflake semantic model."""
        graph = adapter.parse(examples_dir / "simple.yaml")

        assert "sales" in graph.models
        model = graph.models["sales"]

        assert model.name == "sales"
        assert model.description == "Sales transactions"
        assert model.table == "public.sales"
        assert model.primary_key == "id"

    def test_parse_dimensions(self, adapter, examples_dir):
        """Test parsing dimensions from Snowflake model."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        model = graph.models["sales"]

        # Should have categorical dimensions
        dim_names = [d.name for d in model.dimensions]
        assert "id" in dim_names
        assert "region" in dim_names

        # Check region dimension
        region_dim = model.get_dimension("region")
        assert region_dim is not None
        assert region_dim.type == "categorical"
        assert region_dim.sql == "region"
        assert region_dim.description == "Sales region"

    def test_parse_time_dimensions(self, adapter, examples_dir):
        """Test parsing time dimensions from Snowflake model."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        model = graph.models["sales"]

        # Should have time dimension
        sale_date = model.get_dimension("sale_date")
        assert sale_date is not None
        assert sale_date.type == "time"
        assert sale_date.sql == "sale_date"
        assert sale_date.granularity == "day"

    def test_parse_facts(self, adapter, examples_dir):
        """Test parsing facts (measures with default aggregation)."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        model = graph.models["sales"]

        # Facts become metrics with agg
        amount = model.get_metric("amount")
        assert amount is not None
        assert amount.agg == "sum"
        assert amount.sql == "amount"

        quantity = model.get_metric("quantity")
        assert quantity is not None
        assert quantity.agg == "sum"

    def test_parse_metrics(self, adapter, examples_dir):
        """Test parsing metrics (aggregated expressions)."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        model = graph.models["sales"]

        # Metrics are parsed with their SQL expressions
        total_sales = model.get_metric("total_sales")
        assert total_sales is not None
        assert total_sales.agg == "sum"  # Parsed from SUM(amount)
        assert total_sales.sql == "amount"  # Extracted from SUM(amount)

        order_count = model.get_metric("order_count")
        assert order_count is not None
        assert order_count.agg == "count"


class TestSnowflakeEcommerceParsing:
    """Test parsing the full e-commerce example."""

    def test_parse_all_tables(self, adapter, examples_dir):
        """Test that all tables are parsed."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")

        assert "orders" in graph.models
        assert "customers" in graph.models
        assert "products" in graph.models

    def test_parse_fully_qualified_table_name(self, adapter, examples_dir):
        """Test parsing fully qualified table names."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")

        orders = graph.models["orders"]
        assert orders.table == "ANALYTICS.ECOMMERCE.ORDERS"

        customers = graph.models["customers"]
        assert customers.table == "ANALYTICS.ECOMMERCE.CUSTOMERS"

    def test_parse_relationships(self, adapter, examples_dir):
        """Test parsing relationships between tables."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")

        orders = graph.models["orders"]

        # Orders should have relationships to customers and products
        rel_names = [r.name for r in orders.relationships]
        assert "customers" in rel_names
        assert "products" in rel_names

        # Check relationship details
        customer_rel = next(r for r in orders.relationships if r.name == "customers")
        assert customer_rel.type == "many_to_one"
        assert customer_rel.foreign_key == "customer_id"
        assert customer_rel.primary_key == "customer_id"

    def test_parse_filters_as_segments(self, adapter, examples_dir):
        """Test parsing filters as segments."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")

        orders = graph.models["orders"]
        segment_names = [s.name for s in orders.segments]
        assert "completed_orders" in segment_names
        assert "web_orders" in segment_names

        completed = orders.get_segment("completed_orders")
        # Snowflake filters have bare column names, we convert to {model}.column format
        assert "{model}.status" in completed.sql
        assert "'delivered'" in completed.sql

    def test_parse_numeric_dimension_type(self, adapter, examples_dir):
        """Test parsing numeric dimension types."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")

        orders = graph.models["orders"]
        order_id = orders.get_dimension("order_id")
        assert order_id.type == "numeric"

    def test_parse_boolean_dimension_type(self, adapter, examples_dir):
        """Test parsing boolean dimension types."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")

        products = graph.models["products"]
        is_active = products.get_dimension("is_active")
        assert is_active.type == "boolean"

    def test_parse_different_aggregation_types(self, adapter, examples_dir):
        """Test parsing different aggregation types for facts."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")

        orders = graph.models["orders"]

        quantity = orders.get_metric("quantity")
        assert quantity.agg == "sum"

        unit_price = orders.get_metric("unit_price")
        assert unit_price.agg == "avg"


class TestSnowflakeDirectoryParsing:
    """Test parsing directory of Snowflake files."""

    def test_parse_directory(self, adapter, examples_dir):
        """Test parsing all files in a directory."""
        graph = adapter.parse(examples_dir)

        # Should have models from both files
        assert "sales" in graph.models
        assert "orders" in graph.models
        assert "customers" in graph.models
        assert "products" in graph.models


class TestSnowflakeEdgeCases:
    """Test edge cases in Snowflake parsing."""

    def test_parse_empty_file(self, adapter, tmp_path):
        """Test parsing an empty YAML file."""
        empty_file = tmp_path / "empty.yaml"
        empty_file.write_text("")

        graph = adapter.parse(empty_file)
        assert len(graph.models) == 0

    def test_parse_minimal_table(self, adapter, tmp_path):
        """Test parsing a minimal table definition."""
        minimal = tmp_path / "minimal.yaml"
        minimal.write_text("""
name: minimal
tables:
  - name: test_table
    base_table:
      table: test
""")
        graph = adapter.parse(minimal)

        assert "test_table" in graph.models
        model = graph.models["test_table"]
        assert model.table == "test"
        assert model.primary_key == "id"  # Default

    def test_parse_table_without_base_table(self, adapter, tmp_path):
        """Test parsing a table without base_table."""
        no_base = tmp_path / "no_base.yaml"
        no_base.write_text("""
name: test
tables:
  - name: virtual_table
    dimensions:
      - name: col1
        data_type: TEXT
""")
        graph = adapter.parse(no_base)

        model = graph.models["virtual_table"]
        assert model.table is None

    def test_parse_composite_primary_key_uses_first(self, adapter, tmp_path):
        """Test that composite primary keys use the first column."""
        composite = tmp_path / "composite.yaml"
        composite.write_text("""
name: test
tables:
  - name: test_table
    base_table:
      table: test
    primary_key:
      columns:
        - col1
        - col2
""")
        graph = adapter.parse(composite)

        model = graph.models["test_table"]
        assert model.primary_key == "col1"

    def test_parse_relationship_without_columns(self, adapter, tmp_path):
        """Test that relationships without columns are skipped."""
        no_cols = tmp_path / "no_cols.yaml"
        no_cols.write_text("""
name: test
tables:
  - name: table_a
    base_table:
      table: a
  - name: table_b
    base_table:
      table: b
relationships:
  - left_table: table_a
    right_table: table_b
""")
        graph = adapter.parse(no_cols)

        # Relationship should be skipped (no columns)
        table_a = graph.models["table_a"]
        assert len(table_a.relationships) == 0
