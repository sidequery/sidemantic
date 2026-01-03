"""Tests for Snowflake adapter conversion to other formats."""

from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.snowflake import SnowflakeAdapter


@pytest.fixture
def snowflake_adapter():
    return SnowflakeAdapter()


@pytest.fixture
def cube_adapter():
    return CubeAdapter()


@pytest.fixture
def metricflow_adapter():
    return MetricFlowAdapter()


@pytest.fixture
def examples_dir():
    return Path(__file__).parent.parent.parent.parent / "examples" / "snowflake"


class TestSnowflakeToCube:
    """Test converting Snowflake models to Cube format."""

    def test_convert_simple_model(self, snowflake_adapter, cube_adapter, examples_dir, tmp_path):
        """Test converting a simple Snowflake model to Cube."""
        # Parse Snowflake
        graph = snowflake_adapter.parse(examples_dir / "simple.yaml")

        # Export to Cube
        output_file = tmp_path / "cube_output.yaml"
        cube_adapter.export(graph, output_file)

        # Parse back as Cube and verify
        graph2 = cube_adapter.parse(output_file)

        assert "sales" in graph2.models
        model = graph2.models["sales"]

        # Dimensions should be converted
        dim_names = [d.name for d in model.dimensions]
        assert "region" in dim_names

        # Metrics should be converted
        metric_names = [m.name for m in model.metrics]
        assert "amount" in metric_names

    def test_convert_preserves_relationships(self, snowflake_adapter, cube_adapter, examples_dir, tmp_path):
        """Test that relationships are preserved when converting to Cube."""
        graph = snowflake_adapter.parse(examples_dir / "ecommerce.yaml")

        output_file = tmp_path / "cube_output.yaml"
        cube_adapter.export(graph, output_file)

        graph2 = cube_adapter.parse(output_file)

        orders = graph2.models["orders"]
        rel_names = [r.name for r in orders.relationships]

        assert "customers" in rel_names
        assert "products" in rel_names


class TestSnowflakeToMetricFlow:
    """Test converting Snowflake models to MetricFlow format."""

    def test_convert_simple_model(self, snowflake_adapter, metricflow_adapter, examples_dir, tmp_path):
        """Test converting a simple Snowflake model to MetricFlow."""
        # Parse Snowflake
        graph = snowflake_adapter.parse(examples_dir / "simple.yaml")

        # Export to MetricFlow
        output_file = tmp_path / "metricflow_output.yaml"
        metricflow_adapter.export(graph, output_file)

        # Parse back as MetricFlow and verify
        graph2 = metricflow_adapter.parse(output_file)

        assert "sales" in graph2.models
        model = graph2.models["sales"]

        # Dimensions should be converted
        dim_names = [d.name for d in model.dimensions]
        assert "region" in dim_names

    def test_convert_preserves_time_dimensions(self, snowflake_adapter, metricflow_adapter, examples_dir, tmp_path):
        """Test that time dimensions are preserved when converting to MetricFlow."""
        graph = snowflake_adapter.parse(examples_dir / "simple.yaml")

        output_file = tmp_path / "metricflow_output.yaml"
        metricflow_adapter.export(graph, output_file)

        graph2 = metricflow_adapter.parse(output_file)

        model = graph2.models["sales"]
        sale_date = model.get_dimension("sale_date")

        assert sale_date is not None
        assert sale_date.type == "time"


class TestCubeToSnowflake:
    """Test converting Cube models to Snowflake format."""

    def test_convert_cube_to_snowflake(self, snowflake_adapter, cube_adapter, tmp_path):
        """Test converting a Cube model to Snowflake format."""
        cube_examples = Path(__file__).parent.parent.parent.parent / "examples" / "cube"

        if not cube_examples.exists():
            pytest.skip("Cube examples not found")

        # Parse Cube
        graph = cube_adapter.parse(cube_examples)

        if not graph.models:
            pytest.skip("No Cube models found")

        # Export to Snowflake
        output_file = tmp_path / "snowflake_output.yaml"
        snowflake_adapter.export(graph, output_file)

        # Parse back as Snowflake and verify
        graph2 = snowflake_adapter.parse(output_file)

        # Should have same models
        assert len(graph2.models) == len(graph.models)


class TestMetricFlowToSnowflake:
    """Test converting MetricFlow models to Snowflake format."""

    def test_convert_metricflow_to_snowflake(self, snowflake_adapter, metricflow_adapter, tmp_path):
        """Test converting a MetricFlow model to Snowflake format."""
        mf_examples = Path(__file__).parent.parent.parent.parent / "examples" / "metricflow"

        if not mf_examples.exists():
            pytest.skip("MetricFlow examples not found")

        # Parse MetricFlow
        graph = metricflow_adapter.parse(mf_examples)

        if not graph.models:
            pytest.skip("No MetricFlow models found")

        # Export to Snowflake
        output_file = tmp_path / "snowflake_output.yaml"
        snowflake_adapter.export(graph, output_file)

        # Parse back as Snowflake and verify
        graph2 = snowflake_adapter.parse(output_file)

        # Should have same models
        assert len(graph2.models) == len(graph.models)
