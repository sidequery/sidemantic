"""Tests for Snowflake adapter roundtrip (parse -> export -> parse)."""

from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.snowflake import SnowflakeAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph


@pytest.fixture
def adapter():
    return SnowflakeAdapter()


@pytest.fixture
def examples_dir():
    return Path(__file__).parent.parent.parent.parent / "examples" / "snowflake"


class TestSnowflakeRoundtrip:
    """Test roundtrip conversion: Snowflake -> Sidemantic -> Snowflake."""

    def test_roundtrip_simple_model(self, adapter, examples_dir, tmp_path):
        """Test roundtrip of a simple model."""
        # Parse original
        graph = adapter.parse(examples_dir / "simple.yaml")

        # Export to temp file
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)

        # Parse exported file
        graph2 = adapter.parse(output_file)

        # Compare models
        assert "sales" in graph2.models
        model = graph2.models["sales"]

        assert model.description == "Sales transactions"
        assert model.primary_key == "id"

    def test_roundtrip_preserves_dimensions(self, adapter, examples_dir, tmp_path):
        """Test that dimensions are preserved through roundtrip."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)
        graph2 = adapter.parse(output_file)

        model = graph2.models["sales"]
        dim_names = [d.name for d in model.dimensions]

        assert "region" in dim_names
        assert "sale_date" in dim_names

        region = model.get_dimension("region")
        assert region.type == "categorical"

        sale_date = model.get_dimension("sale_date")
        assert sale_date.type == "time"

    def test_roundtrip_preserves_facts(self, adapter, examples_dir, tmp_path):
        """Test that facts are preserved through roundtrip."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)
        graph2 = adapter.parse(output_file)

        model = graph2.models["sales"]

        amount = model.get_metric("amount")
        assert amount is not None
        assert amount.agg == "sum"

    def test_roundtrip_preserves_relationships(self, adapter, examples_dir, tmp_path):
        """Test that relationships are preserved through roundtrip."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)
        graph2 = adapter.parse(output_file)

        orders = graph2.models["orders"]
        rel_names = [r.name for r in orders.relationships]

        assert "customers" in rel_names
        assert "products" in rel_names

    def test_roundtrip_preserves_segments(self, adapter, examples_dir, tmp_path):
        """Test that segments/filters are preserved through roundtrip."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)
        graph2 = adapter.parse(output_file)

        orders = graph2.models["orders"]
        segment_names = [s.name for s in orders.segments]

        assert "completed_orders" in segment_names
        completed = orders.get_segment("completed_orders")
        # Segment SQL contains {model}.column format and gets re-qualified on re-parse
        assert "status" in completed.sql
        assert "'delivered'" in completed.sql


class TestSnowflakeExportFromSidemantic:
    """Test exporting Sidemantic models to Snowflake format."""

    def test_export_basic_model(self, adapter, tmp_path):
        """Test exporting a basic model to Snowflake format."""
        graph = SemanticGraph()
        model = Model(
            name="test_model",
            table="db.schema.table",
            description="Test model",
            primary_key="id",
            dimensions=[
                Dimension(name="category", type="categorical", sql="category"),
                Dimension(name="created_at", type="time", sql="created_at"),
            ],
            metrics=[
                Metric(name="total", agg="sum", sql="amount"),
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        # Verify output structure
        with open(output_file) as f:
            data = yaml.safe_load(f)

        assert data["name"] == "export"
        assert len(data["tables"]) == 1

        table = data["tables"][0]
        assert table["name"] == "test_model"
        assert table["description"] == "Test model"
        assert table["base_table"]["database"] == "db"
        assert table["base_table"]["schema"] == "schema"
        assert table["base_table"]["table"] == "table"

    def test_export_separates_time_dimensions(self, adapter, tmp_path):
        """Test that time dimensions are exported separately."""
        graph = SemanticGraph()
        model = Model(
            name="test",
            table="test",
            dimensions=[
                Dimension(name="cat", type="categorical"),
                Dimension(name="ts", type="time"),
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        table = data["tables"][0]
        assert "dimensions" in table
        assert "time_dimensions" in table
        assert len(table["dimensions"]) == 1
        assert len(table["time_dimensions"]) == 1
        assert table["dimensions"][0]["name"] == "cat"
        assert table["time_dimensions"][0]["name"] == "ts"

    def test_export_separates_facts_and_metrics(self, adapter, tmp_path):
        """Test that simple aggregations become facts, complex become metrics."""
        graph = SemanticGraph()
        model = Model(
            name="test",
            table="test",
            metrics=[
                Metric(name="sum_amount", agg="sum", sql="amount"),  # Simple -> fact
                Metric(
                    name="ratio", type="ratio", numerator="test.sum_amount", denominator="test.count"
                ),  # Complex -> metric
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        table = data["tables"][0]
        assert "facts" in table
        assert "metrics" in table
        assert len(table["facts"]) == 1
        assert len(table["metrics"]) == 1
        assert table["facts"][0]["name"] == "sum_amount"
        assert table["metrics"][0]["name"] == "ratio"

    def test_export_relationships(self, adapter, tmp_path):
        """Test exporting relationships."""
        graph = SemanticGraph()

        customers = Model(name="customers", table="customers", primary_key="customer_id")
        orders = Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    primary_key="customer_id",
                )
            ],
        )
        graph.add_model(customers)
        graph.add_model(orders)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        assert "relationships" in data
        assert len(data["relationships"]) == 1

        rel = data["relationships"][0]
        assert rel["left_table"] == "orders"
        assert rel["right_table"] == "customers"
        assert rel["relationship_type"] == "many_to_one"
        assert rel["relationship_columns"][0]["left_column"] == "customer_id"

    def test_export_segments_as_filters(self, adapter, tmp_path):
        """Test exporting segments as filters."""
        graph = SemanticGraph()
        model = Model(
            name="test",
            table="test",
            segments=[
                Segment(name="active", sql="status = 'active'", description="Active records"),
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        table = data["tables"][0]
        assert "filters" in table
        assert len(table["filters"]) == 1
        assert table["filters"][0]["name"] == "active"
        assert table["filters"][0]["expr"] == "status = 'active'"

    def test_export_dimension_data_types(self, adapter, tmp_path):
        """Test that dimension types map to Snowflake data types."""
        graph = SemanticGraph()
        model = Model(
            name="test",
            table="test",
            dimensions=[
                Dimension(name="cat", type="categorical"),
                Dimension(name="num", type="numeric"),
                Dimension(name="bool", type="boolean"),
                Dimension(name="ts", type="time"),
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        table = data["tables"][0]

        # Find dimensions by name
        dims = {d["name"]: d for d in table.get("dimensions", [])}
        time_dims = {d["name"]: d for d in table.get("time_dimensions", [])}

        assert dims["cat"]["data_type"] == "TEXT"
        assert dims["num"]["data_type"] == "NUMBER"
        assert dims["bool"]["data_type"] == "BOOLEAN"
        assert time_dims["ts"]["data_type"] == "TIMESTAMP"


class TestSnowflakeRoundtripYamlStructure:
    """Test that exported YAML has correct Snowflake structure."""

    def test_export_creates_valid_snowflake_yaml(self, adapter, examples_dir, tmp_path):
        """Test that exported YAML follows Snowflake spec."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        # Check top-level structure
        assert "name" in data
        assert "tables" in data

        # Check table structure
        for table in data["tables"]:
            assert "name" in table
            # base_table should have proper structure if present
            if "base_table" in table:
                base = table["base_table"]
                assert "table" in base

            # primary_key should have columns list
            if "primary_key" in table:
                assert "columns" in table["primary_key"]
                assert isinstance(table["primary_key"]["columns"], list)

            # facts should have default_aggregation
            for fact in table.get("facts", []):
                assert "name" in fact
                assert "default_aggregation" in fact

            # metrics should have expr
            for metric in table.get("metrics", []):
                assert "name" in metric
