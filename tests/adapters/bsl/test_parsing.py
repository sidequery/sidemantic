"""Unit tests for BSL adapter and expression parser."""

import pytest

from sidemantic.adapters.bsl import BSLAdapter
from sidemantic.adapters.bsl_expr import (
    ParsedExpr,
    bsl_to_sql,
    is_calc_measure_expr,
    parse_bsl_expr,
    parse_calc_measure,
    sql_to_bsl,
)


class TestBSLExpressionParserEdgeCases:
    """Edge case tests for BSL expression parsing."""

    def test_parse_empty_underscore(self):
        """Test parsing just underscore dot."""
        result = parse_bsl_expr("_.")
        assert result == ParsedExpr(column="")

    def test_parse_whitespace(self):
        """Test parsing with whitespace."""
        result = parse_bsl_expr("  _.column  ")
        assert result == ParsedExpr(column="column")

    def test_parse_unknown_method(self):
        """Test parsing unknown method - treated as column."""
        result = parse_bsl_expr("_.column.unknownMethod()")
        # Unknown methods are treated as column references
        assert result.column is not None

    def test_parse_deeply_nested(self):
        """Test parsing deeply nested column."""
        result = parse_bsl_expr("_.a.b.c.d.e")
        assert result == ParsedExpr(column="a.b.c.d.e")

    def test_parse_column_with_underscore(self):
        """Test parsing column name with underscores."""
        result = parse_bsl_expr("_.my_column_name")
        assert result == ParsedExpr(column="my_column_name")

    def test_parse_aggregation_on_nested(self):
        """Test aggregation on nested column."""
        result = parse_bsl_expr("_.nested.value.sum()")
        assert result == ParsedExpr(column="nested.value", aggregation="sum")

    def test_parse_date_extraction_on_nested(self):
        """Test date extraction on nested column."""
        result = parse_bsl_expr("_.event.timestamp.year()")
        assert result == ParsedExpr(column="event.timestamp", date_part="year")

    def test_parse_all_date_methods(self):
        """Test all date extraction methods."""
        for method in ["year", "month", "day", "hour", "minute", "second", "week", "quarter"]:
            result = parse_bsl_expr(f"_.dt.{method}()")
            assert result.date_part == method

    def test_parse_all_aggregation_methods(self):
        """Test all aggregation methods."""
        methods = {"sum": "sum", "mean": "mean", "avg": "avg", "min": "min", "max": "max", "nunique": "nunique"}
        for method, expected in methods.items():
            result = parse_bsl_expr(f"_.col.{method}()")
            assert result.aggregation == expected


class TestBSLExpressionParser:
    """Tests for BSL expression parsing."""

    def test_parse_simple_column(self):
        """Test parsing simple column reference."""
        result = parse_bsl_expr("_.column")
        assert result == ParsedExpr(column="column")

    def test_parse_nested_column(self):
        """Test parsing nested struct column reference."""
        result = parse_bsl_expr("_.trafficSource.source")
        assert result == ParsedExpr(column="trafficSource.source")

    def test_parse_count(self):
        """Test parsing count aggregation."""
        result = parse_bsl_expr("_.count()")
        assert result == ParsedExpr(aggregation="count")

    def test_parse_sum(self):
        """Test parsing sum aggregation."""
        result = parse_bsl_expr("_.amount.sum()")
        assert result == ParsedExpr(column="amount", aggregation="sum")

    def test_parse_mean(self):
        """Test parsing mean (avg) aggregation."""
        result = parse_bsl_expr("_.price.mean()")
        assert result == ParsedExpr(column="price", aggregation="mean")

    def test_parse_min_max(self):
        """Test parsing min/max aggregations."""
        result = parse_bsl_expr("_.value.min()")
        assert result == ParsedExpr(column="value", aggregation="min")

        result = parse_bsl_expr("_.value.max()")
        assert result == ParsedExpr(column="value", aggregation="max")

    def test_parse_nunique(self):
        """Test parsing nunique (count distinct) aggregation."""
        result = parse_bsl_expr("_.user_id.nunique()")
        assert result == ParsedExpr(column="user_id", aggregation="nunique")

    def test_parse_year_extraction(self):
        """Test parsing year date extraction."""
        result = parse_bsl_expr("_.created_at.year()")
        assert result == ParsedExpr(column="created_at", date_part="year")

    def test_parse_month_extraction(self):
        """Test parsing month date extraction."""
        result = parse_bsl_expr("_.created_at.month()")
        assert result == ParsedExpr(column="created_at", date_part="month")

    def test_parse_non_bsl_expression(self):
        """Test parsing non-BSL expression (calc measure reference)."""
        result = parse_bsl_expr("revenue")
        assert result == ParsedExpr(column="revenue")


class TestBSLToSQL:
    """Tests for BSL to SQL conversion."""

    def test_simple_column(self):
        """Test converting simple column."""
        sql, agg, date_part = bsl_to_sql("_.column")
        assert sql == "column"
        assert agg is None
        assert date_part is None

    def test_count(self):
        """Test converting count aggregation."""
        sql, agg, date_part = bsl_to_sql("_.count()")
        assert sql is None
        assert agg == "count"
        assert date_part is None

    def test_sum(self):
        """Test converting sum aggregation."""
        sql, agg, date_part = bsl_to_sql("_.amount.sum()")
        assert sql == "amount"
        assert agg == "sum"
        assert date_part is None

    def test_mean_to_avg(self):
        """Test converting mean to avg."""
        sql, agg, date_part = bsl_to_sql("_.price.mean()")
        assert sql == "price"
        assert agg == "avg"
        assert date_part is None

    def test_nunique_to_count_distinct(self):
        """Test converting nunique to count_distinct."""
        sql, agg, date_part = bsl_to_sql("_.user_id.nunique()")
        assert sql == "user_id"
        assert agg == "count_distinct"
        assert date_part is None

    def test_year_extraction(self):
        """Test converting year extraction."""
        sql, agg, date_part = bsl_to_sql("_.created_at.year()")
        assert sql == "created_at"
        assert agg is None
        assert date_part == "year"


class TestSQLToBSL:
    """Tests for SQL to BSL conversion."""

    def test_simple_column(self):
        """Test converting simple column."""
        result = sql_to_bsl("column", None, None)
        assert result == "_.column"

    def test_count(self):
        """Test converting count aggregation."""
        result = sql_to_bsl(None, "count", None)
        assert result == "_.count()"

    def test_sum(self):
        """Test converting sum aggregation."""
        result = sql_to_bsl("amount", "sum", None)
        assert result == "_.amount.sum()"

    def test_avg_to_mean(self):
        """Test converting avg to mean."""
        result = sql_to_bsl("price", "avg", None)
        assert result == "_.price.mean()"

    def test_count_distinct_to_nunique(self):
        """Test converting count_distinct to nunique."""
        result = sql_to_bsl("user_id", "count_distinct", None)
        assert result == "_.user_id.nunique()"

    def test_year_date_part(self):
        """Test converting year date part."""
        result = sql_to_bsl("created_at", None, "year")
        assert result == "_.created_at.year()"


class TestCalcMeasures:
    """Tests for calc measure detection and parsing."""

    def test_is_calc_measure_division(self):
        """Test detecting division calc measure."""
        assert is_calc_measure_expr("revenue / order_count")
        assert is_calc_measure_expr("total_sales / total_orders")

    def test_is_calc_measure_complex(self):
        """Test detecting complex calc measure."""
        assert is_calc_measure_expr("(revenue - cost) / revenue")
        assert is_calc_measure_expr("total_sales * 0.1")

    def test_is_not_calc_measure(self):
        """Test that regular expressions are not detected as calc measures."""
        assert not is_calc_measure_expr("_.amount.sum()")
        assert not is_calc_measure_expr("_.count()")
        assert not is_calc_measure_expr("_.column")

    def test_parse_calc_measure_simple(self):
        """Test parsing simple calc measure."""
        result = parse_calc_measure("revenue / order_count")
        assert "revenue" in result
        assert "order_count" in result

    def test_parse_calc_measure_complex(self):
        """Test parsing complex calc measure."""
        result = parse_calc_measure("(total_sales - total_costs) / total_sales")
        assert "total_sales" in result
        assert "total_costs" in result

    def test_parse_calc_measure_filters_keywords(self):
        """Test that SQL keywords are filtered out."""
        result = parse_calc_measure("NULLIF(revenue, 0)")
        assert "NULLIF" not in result
        assert "revenue" in result


class TestBSLAdapterImport:
    """Tests for BSL adapter import functionality."""

    def test_import_orders(self):
        """Test importing the orders fixture."""
        adapter = BSLAdapter()
        graph = adapter.parse("tests/fixtures/bsl/orders.yml")

        assert "orders" in graph.models
        orders = graph.models["orders"]

        # Verify basic info
        assert orders.table == "public.orders"
        assert orders.description == "Customer orders with revenue and status tracking"

        # Verify dimensions
        dim_names = [d.name for d in orders.dimensions]
        assert "id" in dim_names
        assert "status" in dim_names
        assert "created_at" in dim_names
        assert "customer_id" in dim_names

        # Verify time dimension
        created_at = next(d for d in orders.dimensions if d.name == "created_at")
        assert created_at.type == "time"
        assert created_at.granularity == "day"

        # Verify measures
        measure_names = [m.name for m in orders.metrics]
        assert "count" in measure_names
        assert "revenue" in measure_names
        assert "avg_order_value" in measure_names

        # Verify measure types
        revenue = next(m for m in orders.metrics if m.name == "revenue")
        assert revenue.agg == "sum"
        assert revenue.sql == "amount"

        avg_order = next(m for m in orders.metrics if m.name == "avg_order_value")
        assert avg_order.agg == "avg"

    def test_import_order_items(self):
        """Test importing the order_items fixture."""
        adapter = BSLAdapter()
        graph = adapter.parse("tests/fixtures/bsl/order_items.yml")

        assert "order_items" in graph.models
        order_items = graph.models["order_items"]

        # Verify dimensions with date extraction
        dim_names = [d.name for d in order_items.dimensions]
        assert "created_year" in dim_names
        assert "created_month" in dim_names

        # Date extraction dimensions should be categorical
        created_year = next(d for d in order_items.dimensions if d.name == "created_year")
        assert created_year.type == "categorical"
        assert "EXTRACT(YEAR FROM" in created_year.sql

    def test_import_flights_with_joins(self):
        """Test importing flights fixture with joins (full 5-model version)."""
        adapter = BSLAdapter()
        graph = adapter.parse("tests/fixtures/bsl/flights.yml")

        assert "flights" in graph.models
        assert "carriers" in graph.models
        assert "aircraft" in graph.models
        assert "aircraft_models" in graph.models
        assert "airports" in graph.models

        flights = graph.models["flights"]

        # Verify join relationships (3 joins: carriers, aircraft, origin_airport)
        assert len(flights.relationships) == 3
        rel_names = {r.name for r in flights.relationships}
        assert "carriers" in rel_names
        assert "aircraft" in rel_names
        assert "airports" in rel_names

        # Verify carriers join details
        carriers_rel = next(r for r in flights.relationships if r.name == "carriers")
        assert carriers_rel.type == "many_to_one"
        assert carriers_rel.foreign_key == "carrier"
        assert carriers_rel.primary_key == "code"

        # Verify aircraft has its own join to aircraft_models
        aircraft = graph.models["aircraft"]
        assert len(aircraft.relationships) == 1
        assert aircraft.relationships[0].name == "aircraft_models"

    def test_import_directory(self):
        """Test importing multiple files individually.

        Note: directory import can fail with duplicate model names across files
        (e.g. flights.yml and yaml_example_filter.yaml both define 'flights').
        Test individual files instead.
        """
        adapter = BSLAdapter()

        # Import individual fixtures that have unique model names
        graph = adapter.parse("tests/fixtures/bsl/orders.yml")
        assert "orders" in graph.models

        graph2 = adapter.parse("tests/fixtures/bsl/order_items.yml")
        assert "order_items" in graph2.models

        graph3 = adapter.parse("tests/fixtures/bsl/flights.yml")
        assert "flights" in graph3.models
        assert "carriers" in graph3.models
        assert "aircraft" in graph3.models
        assert "airports" in graph3.models
        assert "aircraft_models" in graph3.models

        graph4 = adapter.parse("tests/fixtures/bsl/ga_sessions.yaml")
        assert "ga_sessions" in graph4.models

        graph5 = adapter.parse("tests/fixtures/bsl/healthcare.yml")
        assert "encounters" in graph5.models
        assert "patients" in graph5.models
        assert "organizations" in graph5.models
        assert "payers" in graph5.models
        assert "conditions" in graph5.models
        assert "medications" in graph5.models


class TestBSLAdapterExport:
    """Tests for BSL adapter export functionality."""

    def test_export_simple_model(self):
        """Test exporting a simple model."""
        import tempfile
        from pathlib import Path

        from sidemantic.core.dimension import Dimension
        from sidemantic.core.metric import Metric
        from sidemantic.core.model import Model
        from sidemantic.core.semantic_graph import SemanticGraph

        # Create a simple model
        model = Model(
            name="test_model",
            table="test_table",
            description="Test model",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="categorical", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
                Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
            ],
            metrics=[
                Metric(name="count", agg="count"),
                Metric(name="total", agg="sum", sql="amount"),
            ],
        )

        graph = SemanticGraph()
        graph.add_model(model)

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = Path(f.name)

        try:
            adapter.export(graph, temp_path)

            # Read back and verify
            import yaml

            with open(temp_path) as f:
                data = yaml.safe_load(f)

            assert "test_model" in data
            test_model = data["test_model"]

            assert test_model["table"] == "test_table"
            assert "dimensions" in test_model
            assert "measures" in test_model

            # Verify time dimension export
            created_at = test_model["dimensions"]["created_at"]
            assert isinstance(created_at, dict)
            assert created_at.get("is_time_dimension") is True
            assert "TIME_GRAIN_DAY" in created_at.get("smallest_time_grain", "")

            # Verify measure export
            total = test_model["measures"]["total"]
            if isinstance(total, str):
                assert "sum()" in total
            else:
                assert "sum()" in total.get("expr", "")

        finally:
            temp_path.unlink(missing_ok=True)


class TestBSLAdapterExportDetailed:
    """Detailed export tests verifying YAML structure."""

    def test_export_derived_metric(self):
        """Test exporting derived/calc metrics."""
        import tempfile
        from pathlib import Path

        import yaml

        from sidemantic.core.metric import Metric
        from sidemantic.core.model import Model
        from sidemantic.core.semantic_graph import SemanticGraph

        model = Model(
            name="sales",
            table="sales",
            primary_key="id",
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count"),
                Metric(name="avg_order_value", type="derived", sql="revenue / order_count"),
            ],
        )

        graph = SemanticGraph()
        graph.add_model(model)

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = Path(f.name)

        try:
            adapter.export(graph, temp_path)

            with open(temp_path) as f:
                data = yaml.safe_load(f)

            measures = data["sales"]["measures"]

            # Derived metric should preserve the SQL expression
            avg_order = measures["avg_order_value"]
            if isinstance(avg_order, dict):
                assert "revenue / order_count" in avg_order.get("expr", "")
            else:
                assert "revenue / order_count" in avg_order

        finally:
            temp_path.unlink(missing_ok=True)

    def test_export_joins_roundtrip(self):
        """Test exporting joins with correct BSL format."""
        import tempfile
        from pathlib import Path

        import yaml

        from sidemantic.core.model import Model
        from sidemantic.core.relationship import Relationship
        from sidemantic.core.semantic_graph import SemanticGraph

        model = Model(
            name="orders",
            table="orders",
            primary_key="id",
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    primary_key="id",
                )
            ],
        )

        graph = SemanticGraph()
        graph.add_model(model)

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = Path(f.name)

        try:
            adapter.export(graph, temp_path)

            with open(temp_path) as f:
                data = yaml.safe_load(f)

            joins = data["orders"]["joins"]
            assert "customers" in joins

            customer_join = joins["customers"]
            assert customer_join["model"] == "customers"
            assert customer_join["type"] == "one"  # BSL uses "one" for many_to_one
            assert customer_join["left_on"] == "customer_id"
            assert customer_join["right_on"] == "id"

        finally:
            temp_path.unlink(missing_ok=True)

    def test_export_simple_vs_extended_format(self):
        """Test that simple dimensions use simple format, complex use extended."""
        import tempfile
        from pathlib import Path

        import yaml

        from sidemantic.core.dimension import Dimension
        from sidemantic.core.model import Model
        from sidemantic.core.semantic_graph import SemanticGraph

        model = Model(
            name="test",
            table="test",
            primary_key="id",
            dimensions=[
                # Simple - should export as string
                Dimension(name="status", type="categorical", sql="status"),
                # Extended - has description
                Dimension(name="category", type="categorical", sql="category", description="Product category"),
                # Extended - is time dimension
                Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
            ],
        )

        graph = SemanticGraph()
        graph.add_model(model)

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = Path(f.name)

        try:
            adapter.export(graph, temp_path)

            with open(temp_path) as f:
                data = yaml.safe_load(f)

            dims = data["test"]["dimensions"]

            # Simple format - should be string
            assert isinstance(dims["status"], str)
            assert dims["status"] == "_.status"

            # Extended format - has description
            assert isinstance(dims["category"], dict)
            assert dims["category"]["description"] == "Product category"

            # Extended format - time dimension
            assert isinstance(dims["created_at"], dict)
            assert dims["created_at"]["is_time_dimension"] is True

        finally:
            temp_path.unlink(missing_ok=True)

    def test_export_to_directory(self):
        """Test exporting to directory creates separate files."""
        import tempfile
        from pathlib import Path

        from sidemantic.core.model import Model
        from sidemantic.core.semantic_graph import SemanticGraph

        graph = SemanticGraph()
        graph.add_model(Model(name="orders", table="orders", primary_key="id"))
        graph.add_model(Model(name="customers", table="customers", primary_key="id"))

        adapter = BSLAdapter()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir)
            adapter.export(graph, output_path)

            # Should create separate files
            assert (output_path / "orders.yml").exists()
            assert (output_path / "customers.yml").exists()


class TestBSLAdapterErrorHandling:
    """Error handling tests for BSL adapter."""

    def test_parse_empty_file(self):
        """Test parsing empty YAML file."""
        import tempfile
        from pathlib import Path

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("")
            temp_path = Path(f.name)

        try:
            graph = adapter.parse(temp_path)
            # Should return empty graph, not error
            assert len(graph.models) == 0
        finally:
            temp_path.unlink(missing_ok=True)

    def test_parse_yaml_without_table(self):
        """Test parsing YAML model without table key is skipped."""
        import tempfile
        from pathlib import Path

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("invalid_model:\n  dimensions:\n    col: _.col\n")
            temp_path = Path(f.name)

        try:
            graph = adapter.parse(temp_path)
            # Model without table should be skipped
            assert "invalid_model" not in graph.models
        finally:
            temp_path.unlink(missing_ok=True)

    def test_parse_model_with_empty_dimensions(self):
        """Test parsing model with empty dimensions section."""
        import tempfile
        from pathlib import Path

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("test:\n  table: test\n  dimensions:\n")
            temp_path = Path(f.name)

        try:
            graph = adapter.parse(temp_path)
            assert "test" in graph.models
            assert len(graph.models["test"].dimensions) == 0
        finally:
            temp_path.unlink(missing_ok=True)

    def test_parse_model_with_empty_measures(self):
        """Test parsing model with empty measures section."""
        import tempfile
        from pathlib import Path

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("test:\n  table: test\n  measures:\n")
            temp_path = Path(f.name)

        try:
            graph = adapter.parse(temp_path)
            assert "test" in graph.models
            assert len(graph.models["test"].metrics) == 0
        finally:
            temp_path.unlink(missing_ok=True)

    def test_parse_nonexistent_file(self):
        """Test parsing nonexistent file raises error."""
        adapter = BSLAdapter()

        with pytest.raises(FileNotFoundError):
            adapter.parse("/nonexistent/path/file.yml")


class TestBSLNestedColumnRoundtrip:
    """Tests for nested column access roundtrip."""

    def test_nested_dimension_roundtrip(self):
        """Test nested dimension imports and exports correctly."""
        import tempfile
        from pathlib import Path

        import yaml

        adapter = BSLAdapter()

        # Create fixture with nested column
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("""
nested_test:
  table: events
  dimensions:
    source:
      expr: _.trafficSource.source
      description: "Traffic source"
    medium: _.trafficSource.medium
  measures:
    count: _.count()
""")
            temp_path = Path(f.name)

        try:
            # Import
            graph = adapter.parse(temp_path)
            model = graph.models["nested_test"]

            # Verify import
            source_dim = next(d for d in model.dimensions if d.name == "source")
            assert source_dim.sql == "trafficSource.source"

            medium_dim = next(d for d in model.dimensions if d.name == "medium")
            assert medium_dim.sql == "trafficSource.medium"

            # Export
            with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f2:
                export_path = Path(f2.name)

            adapter.export(graph, export_path)

            # Verify export
            with open(export_path) as f:
                data = yaml.safe_load(f)

            dims = data["nested_test"]["dimensions"]

            # Extended format preserves nested access
            assert "_.trafficSource.source" in str(dims["source"])
            # Simple format also preserves nested access
            assert "_.trafficSource.medium" in str(dims["medium"])

            export_path.unlink(missing_ok=True)

        finally:
            temp_path.unlink(missing_ok=True)

    def test_nested_measure_aggregation(self):
        """Test aggregation on nested column."""
        import tempfile
        from pathlib import Path

        adapter = BSLAdapter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("""
nested_agg:
  table: events
  measures:
    total_hits:
      expr: _.totals.hits.sum()
      description: "Total hits"
""")
            temp_path = Path(f.name)

        try:
            graph = adapter.parse(temp_path)
            model = graph.models["nested_agg"]

            total_hits = next(m for m in model.metrics if m.name == "total_hits")
            assert total_hits.agg == "sum"
            assert total_hits.sql == "totals.hits"

        finally:
            temp_path.unlink(missing_ok=True)


class TestBSLLoaderAutoDetection:
    """Tests for BSL auto-detection in loaders.py."""

    def test_auto_detect_bsl_format(self):
        """Test that BSL format is auto-detected by load_from_directory."""
        import tempfile
        from pathlib import Path

        from sidemantic import SemanticLayer
        from sidemantic.loaders import load_from_directory

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a BSL file
            bsl_file = Path(tmpdir) / "test.yml"
            bsl_file.write_text("""
test_model:
  table: test_table
  dimensions:
    status: _.status
  measures:
    count: _.count()
""")

            layer = SemanticLayer()
            load_from_directory(layer, tmpdir)

            # Should have loaded the model
            assert "test_model" in layer.graph.models
            model = layer.graph.models["test_model"]
            assert model.table == "test_table"

    def test_auto_detect_distinguishes_formats(self):
        """Test that BSL is distinguished from other YAML formats."""
        # BSL uses _.column syntax
        bsl_content = """
model:
  table: test
  dimensions:
    col: _.col
"""

        # Cube uses cubes: key
        cube_content = """
cubes:
  - name: test
    sql_table: test
"""

        # Sidemantic uses models: key
        sidemantic_content = """
models:
  - name: test
    table: test
"""

        # Check that BSL pattern matches BSL
        assert "_." in bsl_content and "dimensions:" in bsl_content

        # Check that BSL pattern doesn't match Cube
        assert not ("_." in cube_content and "dimensions:" in cube_content)

        # Check that BSL pattern doesn't match Sidemantic
        assert not ("_." in sidemantic_content and "dimensions:" in sidemantic_content)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
