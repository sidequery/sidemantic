"""Tests for TMDL adapter."""

import difflib
import json
import tempfile
import textwrap
from collections import Counter
from pathlib import Path

import pytest
import yaml

import sidemantic.adapters.tmdl as tmdl_module
from sidemantic import SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.adapters.tmdl import TMDLAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.introspection import describe_graph
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.loaders import load_from_directory
from sidemantic.sql.generator import SQLGenerator
from sidemantic.validation import QueryValidationError

# =============================================================================
# BASIC PARSING TESTS
# =============================================================================


def _dax_parser_available() -> bool:
    try:
        from sidemantic_dax import parse_expression
    except Exception:
        return False

    try:
        parse_expression("SUM(Sales[Amount])")
    except Exception:
        return False

    return True


def _assert_expected_import_warnings(
    warnings: list[dict], *, dax_parser_unavailable: int = 0, dax_not_translated: int = 0
):
    expected = Counter()
    if dax_parser_unavailable and not _dax_parser_available():
        expected["dax_parser_unavailable"] = dax_parser_unavailable
    if dax_not_translated and _dax_parser_available():
        # When the DAX parser is available, measures whose DAX cannot be reduced to a native
        # aggregation are preserved as derived metrics and surfaced via a dax_not_translated warning.
        expected["dax_not_translated"] = dax_not_translated
    assert Counter(warning["code"] for warning in warnings) == expected


def test_import_tmdl_directory():
    """Test importing a TMDL folder structure."""
    adapter = TMDLAdapter()
    graph = adapter.parse("tests/fixtures/tmdl")

    assert "Sales" in graph.models
    assert "Products" in graph.models

    sales = graph.models["Sales"]
    products = graph.models["Products"]

    assert sales.description == "Sales fact table"
    assert products.description == "Product dimension"

    assert sales.primary_key == "Sale ID"
    assert sales.default_time_dimension == "Order Date"
    assert sales.default_grain == "day"

    order_date = sales.get_dimension("Order Date")
    assert order_date.type == "time"
    assert order_date.granularity == "day"

    amount = sales.get_dimension("Amount")
    assert amount.type == "numeric"
    assert amount.format == "$#,##0.00"

    total_sales = sales.get_metric("Total Sales")
    assert total_sales.agg == "sum"
    assert total_sales.sql == "Amount"
    assert total_sales.format == "$#,##0.00"

    sales_ly = sales.get_metric("Sales LY")
    assert sales_ly.type == "derived"
    assert sales_ly.expression_language == "dax"
    assert sales_ly.sql is None
    assert "SAMEPERIODLASTYEAR" in sales_ly.dax

    backtick = sales.get_metric("Backtick Measure")
    assert backtick.agg == "sum"

    rel = next(r for r in sales.relationships if r.name == "Products")
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "Product Key"
    assert rel.primary_key == "Product Key"


def test_import_tmdl_directory_does_not_warn_for_model_relationship_refs():
    adapter = TMDLAdapter()
    graph = adapter.parse("tests/fixtures/tmdl")

    warnings = getattr(graph, "import_warnings", [])
    relationship_warnings = [
        warning
        for warning in warnings
        if warning.get("code") == "relationship_parse_skip" and warning.get("context") == "relationship"
    ]
    assert relationship_warnings == []


def test_tmdl_untranslated_dax_metric_is_not_compiled_as_sql():
    layer = SemanticLayer()
    load_from_directory(layer, "tests/fixtures/tmdl")

    with pytest.raises(QueryValidationError, match="DAX expression but has no SQL translation"):
        layer.compile(metrics=["Sales.Sales LY"])


def test_tmdl_untranslated_dax_dimension_is_not_compiled_as_sql():
    layer = SemanticLayer()
    load_from_directory(layer, "tests/fixtures/tmdl_realistic")

    amount_x2 = layer.graph.models["Sales"].get_dimension("Amount x2")
    assert amount_x2.sql is None
    assert amount_x2.has_untranslated_dax

    with pytest.raises(QueryValidationError, match="DAX expression but has no SQL translation"):
        layer.compile(metrics=["Sales.Total Sales"], dimensions=["Sales.Amount x2"])

    with pytest.raises(ValueError, match="DAX expression but has no SQL translation"):
        SQLGenerator(layer.graph).generate(metrics=["Sales.Total Sales"], dimensions=["Sales.Amount x2"])


def test_tmdl_dax_only_calculated_table_is_not_compiled_as_sql():
    tmdl = textwrap.dedent(
        """
        calculatedTable SalesByCategory = SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))
            column Category
                dataType: string
            column Revenue
                dataType: decimal
            measure Revenue = SUM(SalesByCategory[Revenue])
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
        model = graph.models["SalesByCategory"]
        assert model.has_untranslated_dax

        layer = SemanticLayer()
        layer.graph = graph
        with pytest.raises(QueryValidationError, match="DAX table expression but has no SQL/table translation"):
            layer.compile(metrics=["SalesByCategory.Revenue"], dimensions=["SalesByCategory.Category"])

        with pytest.raises(ValueError, match="DAX table expression but has no SQL/table translation"):
            SQLGenerator(graph).generate(metrics=["SalesByCategory.Revenue"], dimensions=["SalesByCategory.Category"])
    finally:
        temp_path.unlink()


def test_tmdl_export_preserves_model_ref_table_literals_and_order():
    graph = TMDLAdapter().parse("tests/fixtures/tmdl")

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        content = model_file.read_text()
        sales_ref = "    ref table 'Sales'"
        products_ref = "    ref table 'Products'"
        assert sales_ref in content
        assert products_ref in content
        assert content.index(sales_ref) < content.index(products_ref)


def test_tmdl_export_preserves_backtick_measure_expression_delimiters():
    graph = TMDLAdapter().parse("tests/fixtures/tmdl")

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert "measure 'Backtick Measure' = ```" in content
        assert "\n        SUM('Sales'[Amount])\n        ```" in content


def test_tmdl_export_preserves_imported_column_core_property_order():
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
                formatString: "$#,##0.00"
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert content.index("sourceColumn: Amount") < content.index('formatString: "$#,##0.00"')


def test_tmdl_export_preserves_imported_measure_core_property_order():
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            measure Revenue = SUM(Sales[Amount])
                formatString: "0.00"
                description: "Revenue Desc"
                caption: "Revenue Label"
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert content.index('formatString: "0.00"') < content.index('description: "Revenue Desc"')
        assert content.index('description: "Revenue Desc"') < content.index('caption: "Revenue Label"')


def test_tmdl_export_preserves_table_leading_comments():
    graph = TMDLAdapter().parse("tests/fixtures/tmdl")

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert content.startswith("# comment that should be ignored\n")


def test_tmdl_export_preserves_column_and_measure_leading_comments():
    tmdl = textwrap.dedent(
        """
        table Sales
            # Amount column comment
            column Amount
                dataType: decimal
                sourceColumn: Amount
            // Revenue measure comment
            measure Revenue = SUM(Sales[Amount])
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert "    # Amount column comment\n    column Amount" in content
        assert "    // Revenue measure comment\n    measure Revenue = SUM(Sales[Amount])" in content


def test_tmdl_fixture_definition_roundtrip_is_byte_stable():
    graph = TMDLAdapter().parse("tests/fixtures/tmdl")

    fixture_root = Path("tests/fixtures/tmdl/definition")
    fixture_files = sorted(path.relative_to(fixture_root) for path in fixture_root.rglob("*.tmdl"))
    assert fixture_files, "Expected fixture TMDL files"

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        export_root = Path(tmpdir) / "definition"
        export_files = sorted(path.relative_to(export_root) for path in export_root.rglob("*.tmdl"))
        assert export_files == fixture_files

        for rel_path in fixture_files:
            fixture_text = (fixture_root / rel_path).read_text()
            export_text = (export_root / rel_path).read_text()
            if export_text != fixture_text:
                diff = "\n".join(
                    difflib.unified_diff(
                        fixture_text.splitlines(),
                        export_text.splitlines(),
                        fromfile=f"fixture/{rel_path}",
                        tofile=f"export/{rel_path}",
                        lineterm="",
                    )
                )
                raise AssertionError(f"Roundtrip mismatch for {rel_path}:\n{diff}")


def test_tmdl_realistic_fixture_import_export_contract(tmp_path):
    fixture_root = Path("tests/fixtures/tmdl_realistic")
    graph = TMDLAdapter().parse(fixture_root)

    _assert_expected_import_warnings(getattr(graph, "import_warnings"), dax_parser_unavailable=5, dax_not_translated=1)
    assert set(graph.models) == {"Sales", "Products", "Calendar", "Sales By Category"}

    sales = graph.models["Sales"]
    assert sales.description == "Sales fact table"
    assert sales.get_dimension("Amount x2").dax == "Sales[Amount] * 2"
    assert sales.get_dimension("Amount x2").sql is None
    assert sales.get_metric("Total Sales").dax == "SUM(Sales[Amount])"
    assert sales.get_metric("Total Sales").sql == "Amount"
    assert getattr(sales, "_tmdl_child_nodes")[0].name == "TableTag"

    calculated = graph.models["Sales By Category"]
    assert calculated.table is None
    assert calculated.sql is None
    assert calculated.dax == 'SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))'
    assert getattr(calculated, "_tmdl_child_nodes")[0].name == "CalculationTag"

    sales_products = next(rel for rel in sales.relationships if rel.name == "Products")
    assert getattr(sales_products, "_tmdl_child_nodes")[0].name == "RelationshipLineage"

    description = describe_graph(graph, model_names=["Sales", "Sales By Category"])
    json.dumps(description)
    sales_info = next(model for model in description["models"] if model["name"] == "Sales")
    calculated_info = next(model for model in description["models"] if model["name"] == "Sales By Category")
    products_rel = next(rel for rel in sales_info["relationships"] if rel["name"] == "Products")
    total_sales = next(metric for metric in sales_info["metrics"] if metric["name"] == "Total Sales")
    assert sales_info["source_format"] == "TMDL"
    assert products_rel["tmdl"]["child_nodes"][0]["name"] == "RelationshipLineage"
    assert total_sales["dax"] == "SUM(Sales[Amount])"
    assert total_sales["expression_language"] == "dax"
    assert calculated_info["kind"] == "calculated_table"
    assert calculated_info["dax"] == 'SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))'
    assert calculated_info["tmdl"]["child_nodes"][0]["name"] == "CalculationTag"

    layer = SemanticLayer()
    load_from_directory(layer, fixture_root)
    export_dir = tmp_path / "exported"
    TMDLAdapter().export(layer.graph, export_dir)

    fixture_definition_root = fixture_root / "definition"
    export_definition_root = export_dir / "definition"
    fixture_files = sorted(
        path.relative_to(fixture_definition_root) for path in fixture_definition_root.rglob("*.tmdl")
    )
    export_files = sorted(path.relative_to(export_definition_root) for path in export_definition_root.rglob("*.tmdl"))
    assert export_files == fixture_files

    reparsed_graph = TMDLAdapter().parse(export_dir)
    _assert_expected_import_warnings(
        getattr(reparsed_graph, "import_warnings"), dax_parser_unavailable=5, dax_not_translated=1
    )
    assert set(reparsed_graph.models) == set(graph.models)
    reparsed_sales = reparsed_graph.models["Sales"]
    reparsed_calculated = reparsed_graph.models["Sales By Category"]
    reparsed_rel = next(rel for rel in reparsed_sales.relationships if rel.name == "Products")
    assert reparsed_sales.get_dimension("Amount x2").dax == "Sales[Amount] * 2"
    assert reparsed_sales.get_dimension("Amount x2").sql is None
    assert reparsed_sales.get_metric("Total Sales").dax == "SUM(Sales[Amount])"
    assert getattr(reparsed_calculated, "_tmdl_child_nodes")[0].name == "CalculationTag"
    assert getattr(reparsed_rel, "_tmdl_child_nodes")[0].name == "RelationshipLineage"
    assert getattr(reparsed_rel, "_tmdl_from_column") == "ProductKey"
    assert getattr(reparsed_rel, "_tmdl_to_column") == "ProductKey"

    database_content = (export_dir / "definition" / "database.tmdl").read_text()
    model_content = (export_dir / "definition" / "model.tmdl").read_text()
    sales_content = (export_dir / "definition" / "tables" / "Sales.tmdl").read_text()
    assert (export_dir / "definition" / "tables" / "Sales By Category.tmdl").is_file()
    assert not (export_dir / "definition" / "tables" / "Sales_By_Category.tmdl").exists()
    calculated_content = (export_dir / "definition" / "tables" / "Sales By Category.tmdl").read_text()
    relationship_content = (export_dir / "definition" / "relationships.tmdl").read_text()

    assert "database 'Retail Analytics'" in database_content
    assert "compatibilityLevel: 1601" in database_content
    assert "annotation DatabaseTag" in database_content
    assert "perspective Executive" in model_content
    assert "culture en-US" in model_content
    assert "role 'Sales Managers'" in model_content
    assert "partition Sales = m" in sales_content
    assert "Sql.Database" in sales_content
    assert "annotation CalculationTag" in calculated_content
    assert "annotation RelationshipLineage" in relationship_content


def test_tmdl_calculated_table_multitable_summarizecolumns():
    tmdl = textwrap.dedent(
        """
        table Sales
            column SaleID
                dataType: int64
                isKey
                sourceColumn: SaleID
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
            column Amount
                dataType: decimal
                sourceColumn: Amount
        table Products
            column ProductKey
                dataType: int64
                isKey
                sourceColumn: ProductKey
            column Category
                dataType: string
                sourceColumn: Category
        calculatedTable SalesByCategory = SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))
        relationship SalesProducts
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        adapter = TMDLAdapter()
        graph = adapter.parse(temp_path)
        model = graph.models["SalesByCategory"]

        assert model.table is None
        assert model.sql is None
        assert model.dax == 'SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))'

        description = describe_graph(graph)
        model_info = next(item for item in description["models"] if item["name"] == "SalesByCategory")
        assert model_info["kind"] == "calculated_table"
        assert model_info["calculated_table"] is True
        assert (
            model_info["original_expression"] == 'SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))'
        )
        assert model_info["dax"] == 'SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))'
    finally:
        temp_path.unlink()


def test_tmdl_parses_dax_ast_when_available():
    """Ensure DAX AST is attached when sidemantic_dax is installed."""
    try:
        import sidemantic_dax
        import sidemantic_dax.ast as dax_ast
    except Exception:
        pytest.skip("sidemantic_dax not installed")

    try:
        sidemantic_dax.parse_expression("1")
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    adapter = TMDLAdapter()
    graph = adapter.parse("tests/fixtures/tmdl")

    total_sales = graph.models["Sales"].get_metric("Total Sales")
    assert total_sales.dax == "SUM('Sales'[Amount])"
    assert isinstance(total_sales._dax_ast, dax_ast.FunctionCall)


# =============================================================================
# TYPE AND MEASURE MAPPING TESTS
# =============================================================================


def test_tmdl_column_type_mapping():
    """Test TMDL column data types map to sidemantic types."""
    tmdl = textwrap.dedent(
        """
        table test
            column status
                dataType: string
            column is_active
                dataType: boolean
            column amount
                dataType: decimal
            column event_date
                dataType: date
            column created_at
                dataType: dateTime
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        adapter = TMDLAdapter()
        graph = adapter.parse(temp_path)
        model = graph.models["test"]

        assert model.get_dimension("status").type == "categorical"
        assert model.get_dimension("is_active").type == "boolean"
        assert model.get_dimension("amount").type == "numeric"
        assert model.get_dimension("event_date").type == "time"
        assert model.get_dimension("event_date").granularity == "day"
        assert model.get_dimension("created_at").granularity == "hour"
    finally:
        temp_path.unlink()


def test_tmdl_measure_aggregation_mapping():
    """Test simple DAX measures map to sidemantic aggregations."""
    tmdl = textwrap.dedent(
        """
        table test
            column amount
                dataType: decimal
            column user_id
                dataType: int64
            measure total_amount = SUM('test'[amount])
            measure distinct_users = DISTINCTCOUNT('test'[user_id])
            measure row_count = COUNTROWS('test')
            measure median_amount = MEDIAN('test'[amount])
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        adapter = TMDLAdapter()
        graph = adapter.parse(temp_path)
        model = graph.models["test"]

        total_amount = model.get_metric("total_amount")
        assert total_amount.agg == "sum"
        assert total_amount.sql == "amount"

        distinct_users = model.get_metric("distinct_users")
        assert distinct_users.agg == "count_distinct"
        assert distinct_users.sql == "user_id"

        row_count = model.get_metric("row_count")
        assert row_count.agg == "count"
        assert row_count.sql is None

        median_amount = model.get_metric("median_amount")
        assert median_amount.agg == "median"
        assert median_amount.sql == "amount"
    finally:
        temp_path.unlink()


def test_tmdl_countrows_only_translates_current_table_counts():
    tmdl = textwrap.dedent(
        """
        table Sales
            column SaleID
                dataType: int64
            column Amount
                dataType: decimal
            measure 'Sales Rows' = COUNTROWS(Sales)
            measure 'Product Rows' = COUNTROWS(Products)
            measure 'Filtered Sales Rows' = COUNTROWS(FILTER(Sales, Sales[Amount] > 0))
        table Products
            column ProductID
                dataType: int64
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
        sales = graph.models["Sales"]

        sales_rows = sales.get_metric("Sales Rows")
        assert sales_rows.agg == "count"
        assert sales_rows.sql is None
        assert not sales_rows.has_untranslated_dax

        product_rows = sales.get_metric("Product Rows")
        assert product_rows.type == "derived"
        assert product_rows.agg is None
        assert product_rows.sql is None
        assert product_rows.has_untranslated_dax

        filtered_rows = sales.get_metric("Filtered Sales Rows")
        assert filtered_rows.type == "derived"
        assert filtered_rows.agg is None
        assert filtered_rows.sql is None
        assert filtered_rows.has_untranslated_dax
    finally:
        temp_path.unlink()


def test_tmdl_cross_table_dax_aggregate_stays_untranslated():
    tmdl = textwrap.dedent(
        """
        table Sales
            column SaleID
                dataType: int64
            measure 'Product Price' = SUM(Products[Price])
        table Products
            column Price
                dataType: decimal
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
        metric = graph.models["Sales"].get_metric("Product Price")
        assert metric.type == "derived"
        assert metric.agg is None
        assert metric.sql is None
        assert metric.has_untranslated_dax
    finally:
        temp_path.unlink()


def test_sidemantic_yaml_export_preserves_dax_metric_sql_translation(tmp_path):
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            measure Revenue = SUM(Sales[Amount])
        """
    )

    source = tmp_path / "Sales.tmdl"
    source.write_text(tmdl)
    graph = TMDLAdapter().parse(source)

    output = tmp_path / "models.yml"
    SidemanticAdapter().export(graph, output)

    data = yaml.safe_load(output.read_text())
    sales = data["models"][0]
    revenue = sales["metrics"][0]
    assert revenue["dax"] == "SUM(Sales[Amount])"
    assert revenue["expression_language"] == "dax"
    assert revenue["sql"] == "Amount"

    reparsed = SidemanticAdapter().parse(output)
    metric = reparsed.models["Sales"].get_metric("Revenue")
    assert metric.agg == "sum"
    assert metric.sql == "Amount"
    assert metric.dax == "SUM(Sales[Amount])"
    assert not metric.has_untranslated_dax


def test_tmdl_measure_derived_expression():
    """Test complex DAX measures are treated as derived."""
    tmdl = textwrap.dedent(
        """
        table test
            column amount
                dataType: decimal
            column quantity
                dataType: int64
            measure avg_price = SUM('test'[amount]) / SUM('test'[quantity])
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        adapter = TMDLAdapter()
        graph = adapter.parse(temp_path)
        metric = graph.models["test"].get_metric("avg_price")
        assert metric.type == "derived"
        assert metric.expression_language == "dax"
        assert metric.sql is None
        assert "SUM" in metric.dax
    finally:
        temp_path.unlink()


def test_tmdl_measure_preserves_complex_dax_source():
    tmdl = textwrap.dedent(
        """
        table 'Sales'
            column Amount
                dataType: decimal
                sourceColumn: Amount
            column 'Order Date'
                dataType: date
                sourceColumn: OrderDate
            measure 'Sales LY Inline' = CALCULATE(SUM('Sales'[Amount]), SAMEPERIODLASTYEAR('Sales'[Order Date]))
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        adapter = TMDLAdapter()
        graph = adapter.parse(temp_path)
        model = graph.models["Sales"]

        sales_ly_inline = model.get_metric("Sales LY Inline")
        assert sales_ly_inline.type == "derived"
        assert sales_ly_inline.expression_language == "dax"
        assert sales_ly_inline.dax == "CALCULATE(SUM('Sales'[Amount]), SAMEPERIODLASTYEAR('Sales'[Order Date]))"
        assert sales_ly_inline.sql is None
        assert [metric.name for metric in model.metrics] == ["Sales LY Inline"]
    finally:
        temp_path.unlink()


def test_tmdl_measure_preserves_totalytd_dax_source():
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
            column OrderDate
                dataType: date
                sourceColumn: OrderDate
            measure SalesYTDFiltered = TOTALYTD(CALCULATE(SUM(Sales[Amount]), Sales[ProductKey] = 1), Sales[OrderDate])
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        adapter = TMDLAdapter()
        graph = adapter.parse(temp_path)
        metric = graph.models["Sales"].get_metric("SalesYTDFiltered")
        assert metric.type == "derived"
        assert metric.expression_language == "dax"
        assert metric.dax == "TOTALYTD(CALCULATE(SUM(Sales[Amount]), Sales[ProductKey] = 1), Sales[OrderDate])"
        assert metric.sql is None
    finally:
        temp_path.unlink()


def test_tmdl_import_many_to_many_relationship_preserves_join_keys():
    tmdl = textwrap.dedent(
        """
        table Sales
            column SalesKey
                dataType: int64
                isKey
                sourceColumn: SalesKey
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                isKey
                sourceColumn: ProductKey
            column SalesKey
                dataType: int64
                sourceColumn: SalesKey
        relationship SalesProductsMany
            fromColumn: Sales[ProductKey]
            toColumn: Products[SalesKey]
            fromCardinality: many
            toCardinality: many
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        adapter = TMDLAdapter()
        graph = adapter.parse(temp_path)
        rel = graph.models["Sales"].relationships[0]
        assert rel.type == "many_to_many"
        assert rel.foreign_key == "ProductKey"
        assert rel.primary_key == "SalesKey"
    finally:
        temp_path.unlink()


def test_tmdl_import_collects_dax_parse_warnings(monkeypatch):
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            calculatedColumn BadColumn = BADFUNC(Sales[Amount])
            measure BadMeasure = BADFUNC(Sales[Amount])
        calculatedTable BadTable = BADTABLE(Sales)
        """
    )

    monkeypatch.setattr(
        tmdl_module,
        "_parse_dax_expression",
        lambda expression, node, context: (_ for _ in ()).throw(ValueError("simulated parse error")),
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
        warnings = getattr(graph, "import_warnings")
        assert len(warnings) == 3
        assert {warning["context"] for warning in warnings} == {"column", "measure", "calculated_table"}
        assert {warning["code"] for warning in warnings} == {"dax_parse_error"}
        assert graph.models["Sales"].get_dimension("BadColumn") is not None
        assert graph.models["Sales"].get_metric("BadMeasure") is not None
        assert graph.models["BadTable"].dax == "BADTABLE(Sales)"
    finally:
        temp_path.unlink()


def test_tmdl_import_collects_relationship_skip_warnings():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship BadReference
            fromColumn: SalesProductKey
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
        relationship MissingModel
            fromColumn: Sales[ProductKey]
            toColumn: Missing[ProductKey]
            fromCardinality: many
            toCardinality: one
        relationship BadCardinality
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: several
            toCardinality: one
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
        warnings = getattr(graph, "import_warnings")
        relationship_warnings = [
            warning
            for warning in warnings
            if warning.get("code") == "relationship_parse_skip" and warning.get("context") == "relationship"
        ]
        assert len(relationship_warnings) == 3
        messages = [warning["message"] for warning in relationship_warnings]
        assert any("invalid fromColumn/toColumn reference" in message for message in messages)
        assert any("unknown model reference" in message for message in messages)
        assert any("unsupported cardinality" in message for message in messages)
    finally:
        temp_path.unlink()


def test_tmdl_inactive_relationship_is_preserved_and_excluded_from_graph_paths(tmp_path):
    pytest.importorskip("sidemantic_dax")
    _write_tmdl_dax_relationship_fixture(
        tmp_path,
        """
        relationship SalesProducts
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
            isActive: false
        """,
    )

    graph = TMDLAdapter().parse(tmp_path)

    warnings = getattr(graph, "import_warnings")
    assert warnings == []
    assert [(rel.name, rel.active) for rel in graph.models["Sales"].relationships] == [("Products", False)]
    assert graph.models["Sales By Category"].sql is None
    assert (
        graph.models["Sales By Category"].dax == 'SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))'
    )
    with pytest.raises(ValueError, match="No join path found"):
        graph.find_relationship_path("Sales", "Products")


def test_tmdl_invalid_relationship_edges_are_skipped(tmp_path):
    pytest.importorskip("sidemantic_dax")
    _write_tmdl_dax_relationship_fixture(
        tmp_path,
        """
        relationship SalesProducts
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: several
            toCardinality: one
        """,
    )

    graph = TMDLAdapter().parse(tmp_path)
    warnings = getattr(graph, "import_warnings")

    assert [warning["code"] for warning in warnings] == ["relationship_parse_skip"]
    assert warnings[0]["context"] == "relationship"
    assert "unsupported cardinality" in warnings[0]["message"]
    assert graph.models["Sales"].relationships == []
    assert graph.models["Sales By Category"].sql is None
    assert (
        graph.models["Sales By Category"].dax == 'SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))'
    )


def _write_tmdl_dax_relationship_fixture(root: Path, relationship_text: str) -> None:
    definition_dir = root / "definition"
    tables_dir = definition_dir / "tables"
    tables_dir.mkdir(parents=True)
    (definition_dir / "model.tmdl").write_text(
        textwrap.dedent(
            """
            model Test
                ref table Sales
                ref table Products
                ref table 'Sales By Category'
                ref relationship SalesProducts
            """
        ).strip()
        + "\n"
    )
    (tables_dir / "Sales.tmdl").write_text(
        textwrap.dedent(
            """
            table Sales
                column ProductKey
                    dataType: int64
                    sourceColumn: ProductKey
                column Amount
                    dataType: decimal
                    sourceColumn: Amount
            """
        ).strip()
        + "\n"
    )
    (tables_dir / "Products.tmdl").write_text(
        textwrap.dedent(
            """
            table Products
                column ProductKey
                    dataType: int64
                    isKey
                    sourceColumn: ProductKey
                column Category
                    dataType: string
                    sourceColumn: Category
            """
        ).strip()
        + "\n"
    )
    (tables_dir / "Sales By Category.tmdl").write_text(
        textwrap.dedent(
            """
            calculatedTable 'Sales By Category' = SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))
                column Category
                    dataType: string
                    sourceColumn: Category
                column Revenue
                    dataType: decimal
                    sourceColumn: Revenue
            """
        ).strip()
        + "\n"
    )
    (definition_dir / "relationships.tmdl").write_text(textwrap.dedent(relationship_text).strip() + "\n")


def test_tmdl_import_valid_relationship_cardinalities_do_not_emit_skip_warnings():
    tmdl = textwrap.dedent(
        """
        table Sales
            column SalesKey
                dataType: int64
                sourceColumn: SalesKey
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
            column CustomerKey
                dataType: int64
                sourceColumn: CustomerKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
            column SalesKey
                dataType: int64
                sourceColumn: SalesKey
        table Customers
            column CustomerKey
                dataType: int64
                sourceColumn: CustomerKey
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesProductsManyToOne
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
        relationship ProductsCustomersOneToMany
            fromColumn: Products[ProductKey]
            toColumn: Customers[ProductKey]
            fromCardinality: one
            toCardinality: many
        relationship SalesCustomersOneToOne
            fromColumn: Sales[CustomerKey]
            toColumn: Customers[CustomerKey]
            fromCardinality: one
            toCardinality: one
        relationship SalesProductsManyToMany
            fromColumn: Sales[SalesKey]
            toColumn: Products[SalesKey]
            fromCardinality: many
            toCardinality: many
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
        warnings = getattr(graph, "import_warnings")
        relationship_warnings = [
            warning
            for warning in warnings
            if warning.get("code") == "relationship_parse_skip" and warning.get("context") == "relationship"
        ]
        assert relationship_warnings == []
        sales_relationships = {
            getattr(rel, "_tmdl_relationship_name"): rel for rel in graph.models["Sales"].relationships
        }
        products_relationships = {
            getattr(rel, "_tmdl_relationship_name"): rel for rel in graph.models["Products"].relationships
        }

        many_to_one = sales_relationships["SalesProductsManyToOne"]
        assert many_to_one.type == "many_to_one"
        assert many_to_one.foreign_key == "ProductKey"
        assert many_to_one.primary_key == "ProductKey"
        assert getattr(many_to_one, "_tmdl_from_column") == "ProductKey"
        assert getattr(many_to_one, "_tmdl_to_column") == "ProductKey"

        one_to_many = products_relationships["ProductsCustomersOneToMany"]
        assert one_to_many.type == "one_to_many"
        assert one_to_many.foreign_key == "ProductKey"
        assert one_to_many.primary_key is None
        assert getattr(one_to_many, "_tmdl_from_column") == "ProductKey"
        assert getattr(one_to_many, "_tmdl_to_column") == "ProductKey"

        one_to_one = sales_relationships["SalesCustomersOneToOne"]
        assert one_to_one.type == "one_to_one"
        assert one_to_one.foreign_key == "CustomerKey"
        assert one_to_one.primary_key is None
        assert getattr(one_to_one, "_tmdl_from_column") == "CustomerKey"
        assert getattr(one_to_one, "_tmdl_to_column") == "CustomerKey"

        many_to_many = sales_relationships["SalesProductsManyToMany"]
        assert many_to_many.type == "many_to_many"
        assert many_to_many.foreign_key == "SalesKey"
        assert many_to_many.primary_key == "SalesKey"

        with tempfile.TemporaryDirectory() as tmpdir:
            export_dir = Path(tmpdir)
            TMDLAdapter().export(graph, export_dir)
            relationships = (export_dir / "definition/relationships.tmdl").read_text()
            assert "relationship ProductsCustomersOneToMany" in relationships
            assert "fromColumn: Products[ProductKey]" in relationships
            assert "toColumn: Customers[ProductKey]" in relationships
            assert "relationship SalesCustomersOneToOne" in relationships
            assert "fromColumn: Sales[CustomerKey]" in relationships
            assert "toColumn: Customers[CustomerKey]" in relationships
    finally:
        temp_path.unlink()


def test_tmdl_warning_fixture_collects_relationship_warnings():
    pytest.importorskip("sidemantic_dax")
    graph = TMDLAdapter().parse("tests/fixtures/tmdl_warning")

    warnings = getattr(graph, "import_warnings")
    assert [(warning["code"], warning["context"], warning["name"]) for warning in warnings] == [
        ("dax_not_translated", "measure", "Bad Measure"),
        ("relationship_parse_skip", "relationship", "Bad-Relationship"),
    ]
    assert all(warning.get("file") for warning in warnings)
    assert all(isinstance(warning.get("line"), int) and warning["line"] >= 1 for warning in warnings)
    assert all(isinstance(warning.get("column"), int) and warning["column"] >= 1 for warning in warnings)


# =============================================================================
# LOADER TESTS
# =============================================================================


def test_tmdl_loader_auto_detection():
    """Test load_from_directory auto-detects TMDL projects."""
    layer = SemanticLayer()
    load_from_directory(layer, "tests/fixtures/tmdl")
    assert "Sales" in layer.graph.models
    assert "Products" in layer.graph.models


def test_tmdl_loader_auto_detects_standalone_tmdl_files(tmp_path):
    """Directory loading should treat root .tmdl files as one TMDL source."""
    (tmp_path / "Sales.tmdl").write_text(
        textwrap.dedent(
            """
            model DemoModel
                ref table Sales
            table Sales
                column SaleID
                    dataType: int64
                    isKey
                    sourceColumn: SaleID
                column Amount
                    dataType: decimal
                    sourceColumn: Amount
                measure Revenue = SUM(Sales[Amount])
            """
        )
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert set(layer.graph.models) == {"Sales"}
    description = layer.describe_models()
    _assert_expected_import_warnings(description["import_warnings"], dax_parser_unavailable=1)

    sales = description["models"][0]
    revenue = next(metric for metric in sales["metrics"] if metric["name"] == "Revenue")
    assert sales["source_format"] == "TMDL"
    assert sales["source_file"] == "Sales.tmdl"
    assert revenue["source_format"] == "TMDL"
    assert revenue["source_file"] == "Sales.tmdl"
    assert revenue["dax"] == "SUM(Sales[Amount])"
    assert revenue["expression_language"] == "dax"


def test_tmdl_loader_preserves_graph_passthrough_for_export(tmp_path):
    """CLI-style directory loading should keep graph-level TMDL metadata."""
    definition_dir = tmp_path / "definition"
    tables_dir = definition_dir / "tables"
    tables_dir.mkdir(parents=True)
    (definition_dir / "database.tmdl").write_text(
        textwrap.dedent(
            """
            database DemoDB
                compatibilityLevel: 1601
                model DemoModel
            """
        )
    )
    (definition_dir / "model.tmdl").write_text(
        textwrap.dedent(
            """
            model DemoModel
                perspective SalesView
                    annotation Scope
                        value: "all"
                ref table Sales
            role Analysts
                modelPermission: read
            """
        )
    )
    (tables_dir / "Sales.tmdl").write_text(
        textwrap.dedent(
            """
            table Sales
                column ID
                    dataType: int64
                    isKey
                    sourceColumn: ID
            """
        )
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    export_dir = tmp_path / "exported"
    TMDLAdapter().export(layer.graph, export_dir)

    database_content = (export_dir / "definition" / "database.tmdl").read_text()
    model_content = (export_dir / "definition" / "model.tmdl").read_text()
    assert "database DemoDB" in database_content
    assert "compatibilityLevel: 1601" in database_content
    assert "model DemoModel" in database_content
    assert "model DemoModel" in model_content
    assert "perspective SalesView" in model_content
    assert 'value: "all"' in model_content
    assert "role Analysts" in model_content


def test_tmdl_loader_auto_detects_standalone_tmdl_file_in_directory(tmp_path):
    """Test load_from_directory auto-detects standalone TMDL files outside definition/."""
    (tmp_path / "Sales.tmdl").write_text(
        textwrap.dedent(
            """
            table Sales
                column SaleID
                    dataType: int64
                    isKey
                    sourceColumn: SaleID
                column Amount
                    dataType: decimal
                    sourceColumn: Amount
                measure Revenue = SUM(Sales[Amount])
            """
        )
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "Sales" in layer.graph.models
    assert layer.graph.models["Sales"]._source_format == "TMDL"
    assert layer.graph.models["Sales"].get_metric("Revenue").sql == "Amount"


def test_semantic_layer_from_yaml_loads_standalone_tmdl_file(tmp_path):
    """Sidequery's single-file bridge calls from_yaml, so .tmdl must dispatch there too."""
    tmdl_file = tmp_path / "Sales.tmdl"
    tmdl_file.write_text(
        textwrap.dedent(
            """
            table Sales
                column SaleID
                    dataType: int64
                    isKey
                    sourceColumn: SaleID
                column Amount
                    dataType: decimal
                    sourceColumn: Amount
                measure Revenue = SUM(Sales[Amount])
            """
        )
    )

    layer = SemanticLayer.from_yaml(tmdl_file)

    assert "Sales" in layer.graph.models
    sales = layer.graph.models["Sales"]
    assert sales._source_format == "TMDL"
    assert sales.get_metric("Revenue").dax == "SUM(Sales[Amount])"
    assert sales.get_metric("Revenue").sql == "Amount"


def test_tmdl_loader_propagates_import_warnings(monkeypatch, tmp_path):
    definition_dir = tmp_path / "definition"
    definition_dir.mkdir(parents=True)
    (definition_dir / "model.tmdl").write_text("model Demo")

    def _fake_parse(self, source):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="orders",
                table="orders",
                primary_key="id",
                dimensions=[Dimension(name="id", type="numeric", sql="id")],
                metrics=[Metric(name="count", agg="count")],
            )
        )
        graph.import_warnings = [
            {
                "code": "dax_parse_error",
                "context": "measure",
                "name": "Revenue",
                "message": "Simulated parse error",
            }
        ]
        return graph

    monkeypatch.setattr(TMDLAdapter, "parse", _fake_parse)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)
    warnings = getattr(layer.graph, "import_warnings")
    assert len(warnings) == 1
    assert warnings[0]["code"] == "dax_parse_error"


def test_tmdl_import_warns_when_dax_parser_unavailable(monkeypatch):
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            measure Revenue = SUM(Sales[Amount])
        """
    )

    monkeypatch.setattr(
        tmdl_module,
        "_parse_dax_expression",
        lambda expression, node, context: (_ for _ in ()).throw(
            tmdl_module.DaxRuntimeUnavailableError("simulated missing parser")
        ),
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    warnings = getattr(graph, "import_warnings")
    assert warnings[0]["code"] == "dax_parser_unavailable"
    assert warnings[0]["model"] == "Sales"
    assert graph.models["Sales"].get_metric("Revenue").dax == "SUM(Sales[Amount])"


def test_tmdl_import_warnings_are_model_qualified_for_duplicate_names(monkeypatch):
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            measure Revenue = BROKEN(Sales[Amount])
        table Returns
            column Amount
                dataType: decimal
                sourceColumn: Amount
            measure Revenue = BROKEN(Returns[Amount])
        """
    )

    monkeypatch.setattr(
        tmdl_module,
        "_parse_dax_expression",
        lambda expression, node, context: (_ for _ in ()).throw(ValueError("metric parse unsupported")),
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    warnings = getattr(graph, "import_warnings")
    assert {(warning["model"], warning["name"]) for warning in warnings} == {
        ("Sales", "Revenue"),
        ("Returns", "Revenue"),
    }

    description = describe_graph(graph)
    sales = next(model for model in description["models"] if model["name"] == "Sales")
    returns = next(model for model in description["models"] if model["name"] == "Returns")
    sales_revenue = next(metric for metric in sales["metrics"] if metric["name"] == "Revenue")
    returns_revenue = next(metric for metric in returns["metrics"] if metric["name"] == "Revenue")

    assert sales_revenue["import_warnings"][0]["model"] == "Sales"
    assert returns_revenue["import_warnings"][0]["model"] == "Returns"


def test_tmdl_describe_graph_exposes_source_metadata_for_models_fields_and_relationships():
    graph = TMDLAdapter().parse("tests/fixtures/tmdl")
    description = describe_graph(graph)
    json.dumps(description)
    sales = next(model for model in description["models"] if model["name"] == "Sales")
    order_date = next(dimension for dimension in sales["dimensions"] if dimension["name"] == "Order Date")
    total_sales = next(metric for metric in sales["metrics"] if metric["name"] == "Total Sales")
    products_rel = next(relationship for relationship in sales["relationships"] if relationship["name"] == "Products")

    assert sales["source_format"] == "TMDL"
    assert sales["source_file"] == "tables/Sales.tmdl"
    assert order_date["source_format"] == "TMDL"
    assert order_date["source_file"] == "tables/Sales.tmdl"
    assert total_sales["source_format"] == "TMDL"
    assert total_sales["source_file"] == "tables/Sales.tmdl"
    assert products_rel["source_format"] == "TMDL"
    assert products_rel["source_file"] == "relationships.tmdl"
    assert products_rel["tmdl_name"] == "Sales-Products"
    assert sales["tmdl"]["name_raw"] == "'Sales'"
    assert sales["tmdl"]["leading_comments"] == ["# comment that should be ignored"]
    assert order_date["tmdl"]["data_type"] == "date"
    assert order_date["tmdl"]["raw_value_properties"]["sourcecolumn"] == "OrderDate"
    assert total_sales["tmdl"]["raw_value_properties"]["formatstring"] == '"$#,##0.00"'
    assert products_rel["tmdl"]["relationship_name"] == "Sales-Products"
    assert products_rel["tmdl"]["relationship_name_raw"] == "'Sales-Products'"
    assert products_rel["tmdl"]["raw_value_properties"]["fromcolumn"] == "'Sales'[Product Key]"
    assert products_rel["tmdl"]["is_active_explicit"] is True


# =============================================================================
# EXPORT TESTS
# =============================================================================


def test_tmdl_export_simple_model():
    """Test exporting a simple model to TMDL."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="id", type="numeric", sql="id"),
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="median_revenue", agg="median", sql="amount"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = TMDLAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        table_file = Path(tmpdir) / "definition" / "tables" / "orders.tmdl"
        assert table_file.exists()

        content = table_file.read_text()
        assert "table orders" in content
        assert "column id" in content
        assert "isKey" in content
        assert "measure revenue = SUM(orders[amount])" in content
        assert "measure median_revenue = MEDIAN(orders[amount])" in content

        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        assert model_file.exists()


def test_tmdl_export_relationships():
    """Test exporting relationships to relationships.tmdl."""
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="customer_id", type="numeric", sql="customer_id")],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )
    customers = Model(name="customers", table="customers", primary_key="id")

    graph = SemanticGraph()
    graph.add_model(orders)
    graph.add_model(customers)

    adapter = TMDLAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        assert rel_file.exists()

        content = rel_file.read_text()
        assert "fromColumn: orders[customer_id]" in content
        assert "toColumn: customers[id]" in content
        assert "fromCardinality: many" in content
        assert "toCardinality: one" in content


def test_tmdl_export_many_to_many_relationships():
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        relationships=[
            Relationship(
                name="products",
                type="many_to_many",
                foreign_key="order_product_key",
                primary_key="product_order_key",
                active=False,
            )
        ],
    )
    products = Model(name="products", table="products", primary_key="id")

    graph = SemanticGraph()
    graph.add_model(orders)
    graph.add_model(products)

    adapter = TMDLAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        rel_content = rel_file.read_text()
        model_content = model_file.read_text()

        assert "fromColumn: orders[order_product_key]" in rel_content
        assert "toColumn: products[product_order_key]" in rel_content
        assert "fromCardinality: many" in rel_content
        assert "toCardinality: many" in rel_content
        assert "isActive: false" in rel_content
        assert "ref relationship orders_products" in model_content


def test_tmdl_export_collects_relationship_skip_warnings():
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    graph = SemanticGraph()
    graph.add_model(orders)

    adapter = TMDLAdapter()
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)
        warnings = getattr(graph, "export_warnings")
        assert len(warnings) == 1
        warning = warnings[0]
        assert warning["code"] == "relationship_export_skip"
        assert warning["context"] == "relationship"
        assert warning["from_model"] == "orders"
        assert warning["to_model"] == "customers"
        assert "related model not found" in warning["message"]
        assert not (Path(tmpdir) / "definition" / "relationships.tmdl").exists()


def test_tmdl_export_supported_relationship_types_do_not_emit_skip_warnings():
    sales = Model(
        name="sales",
        table="sales",
        primary_key="sales_key",
        dimensions=[
            Dimension(name="product_key", type="numeric", sql="product_key"),
            Dimension(name="customer_key", type="numeric", sql="customer_key"),
        ],
        relationships=[
            Relationship(name="products", type="many_to_one", foreign_key="product_key", primary_key="product_key"),
            Relationship(name="customers", type="one_to_one", foreign_key="customer_key", primary_key="customer_key"),
        ],
    )
    products = Model(
        name="products",
        table="products",
        primary_key="product_key",
        dimensions=[
            Dimension(name="sales_key", type="numeric", sql="sales_key"),
            Dimension(name="customer_key", type="numeric", sql="customer_key"),
        ],
        relationships=[
            Relationship(name="customers", type="one_to_many", foreign_key="customer_key"),
            Relationship(name="sales", type="many_to_many", foreign_key="sales_key", primary_key="sales_key"),
        ],
    )
    customers = Model(name="customers", table="customers", primary_key="customer_key")

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_model(products)
    graph.add_model(customers)

    adapter = TMDLAdapter()
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)
        warnings = getattr(graph, "export_warnings")
        assert warnings == []
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        content = rel_file.read_text()
        assert "fromCardinality: many" in content
        assert "toCardinality: one" in content
        assert "fromCardinality: one" in content
        assert "toCardinality: many" in content


def test_tmdl_export_preserves_calculated_table_declaration():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
            column Amount
                dataType: decimal
                sourceColumn: Amount
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
            column Category
                dataType: string
                sourceColumn: Category
        calculatedTable SalesByCategory = SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))
        relationship SalesProducts
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "SalesByCategory.tmdl"
        content = table_file.read_text()
        assert (
            'calculatedTable SalesByCategory = SUMMARIZECOLUMNS(Products[Category], "Revenue", SUM(Sales[Amount]))'
            in content
        )


def test_tmdl_export_preserves_imported_relationship_names():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesToProductsByKey
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        rel_content = rel_file.read_text()
        model_content = model_file.read_text()
        assert "relationship SalesToProductsByKey" in rel_content
        assert "ref relationship SalesToProductsByKey" in model_content


def test_tmdl_export_preserves_imported_relationship_properties():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesToProductsByKey
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
            crossFilteringBehavior: bothDirections
            relyOnReferentialIntegrity: true
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert "crossFilteringBehavior: bothDirections" in rel_content
        assert "relyOnReferentialIntegrity: true" in rel_content


def test_tmdl_export_preserves_relationship_child_nodes():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesToProductsByKey
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
            annotation RelationshipTag
                value: "relationship_meta"
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    rel = graph.models["Sales"].relationships[0]
    relationship_info = describe_graph(graph)["models"][0]["relationships"][0]
    assert relationship_info["tmdl"]["child_nodes"][0]["name"] == "RelationshipTag"
    assert getattr(rel, "_tmdl_child_nodes")[0].name == "RelationshipTag"

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert "annotation RelationshipTag" in rel_content
        assert 'value: "relationship_meta"' in rel_content


def test_tmdl_export_preserves_core_relationship_raw_literals():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesToProductsByKey
            fromColumn: "Sales[ProductKey]"
            toColumn: "Products[ProductKey]"
            fromCardinality: "many"
            toCardinality: "one"
            isActive: FALSE
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert 'fromColumn: "Sales[ProductKey]"' in rel_content
        assert 'toColumn: "Products[ProductKey]"' in rel_content
        assert 'fromCardinality: "many"' in rel_content
        assert 'toCardinality: "one"' in rel_content
        assert "isActive: FALSE" in rel_content


def test_tmdl_export_preserves_imported_relationship_core_property_order():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesToProductsByKey
            toColumn: Products[ProductKey]
            fromColumn: Sales[ProductKey]
            toCardinality: one
            fromCardinality: many
            isActive: FALSE
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert rel_content.index("toColumn: Products[ProductKey]") < rel_content.index("fromColumn: Sales[ProductKey]")
        assert rel_content.index("fromColumn: Sales[ProductKey]") < rel_content.index("toCardinality: one")
        assert rel_content.index("toCardinality: one") < rel_content.index("fromCardinality: many")
        assert rel_content.index("fromCardinality: many") < rel_content.index("isActive: FALSE")


def test_tmdl_export_preserves_relationship_isactive_true_raw_literal():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesToProductsByKey
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
            isActive: TRUE
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert "isActive: TRUE" in rel_content


def test_tmdl_export_preserves_relationship_isactive_bare_property():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesToProductsByKey
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
            isActive
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert "\n    isActive\n" in rel_content
        assert "isActive: true" not in rel_content


def test_tmdl_export_preserves_relationship_description_raw_literal():
    tmdl = textwrap.dedent(
        '''
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        relationship SalesToProductsByKey
            description: "Rel ""Desc"""
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
        '''
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert 'description: "Rel ""Desc"""' in rel_content
        assert "/// Rel" not in rel_content


def test_tmdl_export_preserves_imported_relationship_description():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        /// Sales to products relationship
        relationship SalesToProductsByKey
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert "/// Sales to products relationship" in rel_content
        assert "relationship SalesToProductsByKey" in rel_content


def test_tmdl_export_preserves_relationship_leading_comments():
    tmdl = textwrap.dedent(
        """
        table Sales
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        table Products
            column ProductKey
                dataType: int64
                sourceColumn: ProductKey
        // Relationship comment
        relationship SalesToProductsByKey
            fromColumn: Sales[ProductKey]
            toColumn: Products[ProductKey]
            fromCardinality: many
            toCardinality: one
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"
        rel_content = rel_file.read_text()
        assert "// Relationship comment\nrelationship SalesToProductsByKey" in rel_content


def test_tmdl_export_preserves_imported_measure_and_calculated_column_expressions():
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            column Quantity
                dataType: int64
                sourceColumn: Quantity
            calculatedColumn Net = Sales[Amount] - 1
                dataType: decimal
            measure 'Avg Price' = DIVIDE(SUM(Sales[Amount]), SUM(Sales[Quantity]), 0)
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert "calculatedColumn Net" in content
        assert "expression = Sales[Amount] - 1" in content
        assert "measure 'Avg Price' = DIVIDE(SUM(Sales[Amount]), SUM(Sales[Quantity]), 0)" in content


def test_tmdl_export_preserves_native_dax_authored_sources(tmp_path):
    pytest.importorskip("sidemantic_dax")
    source = tmp_path / "models.yml"
    source.write_text(
        """
models:
  - name: Sales
    table: Sales
    primary_key: ID
    dimensions:
      - name: ID
        type: numeric
      - name: Amount
        type: numeric
      - name: Quantity
        type: numeric
      - name: Net
        type: numeric
        dax: "Sales[Amount] - 1"
    metrics:
      - name: Avg Price
        dax: "DIVIDE(SUM(Sales[Amount]), SUM(Sales[Quantity]), 0)"
  - name: Positive Sales
    primary_key: ID
    dax: "FILTER(Sales, Sales[Amount] > 0)"
    dimensions:
      - name: ID
        type: numeric
"""
    )
    layer = SemanticLayer.from_yaml(source)
    export_dir = tmp_path / "exported_tmdl"

    TMDLAdapter().export(layer.graph, export_dir)

    sales_tmdl = (export_dir / "definition" / "tables" / "Sales.tmdl").read_text()
    positive_tmdl = next((export_dir / "definition" / "tables").glob("Positive*.tmdl")).read_text()
    assert "calculatedColumn Net" in sales_tmdl
    assert "expression = Sales[Amount] - 1" in sales_tmdl
    assert "measure 'Avg Price' = DIVIDE(SUM(Sales[Amount]), SUM(Sales[Quantity]), 0)" in sales_tmdl
    assert "calculatedTable 'Positive Sales' = FILTER(Sales, Sales[Amount] > 0)" in positive_tmdl

    reparsed = TMDLAdapter().parse(export_dir)
    sales = reparsed.models["Sales"]
    positive_sales = reparsed.models["Positive Sales"]
    assert sales.get_dimension("Net").dax == "Sales[Amount] - 1"
    assert sales.get_dimension("Net").sql is None
    assert sales.get_metric("Avg Price").dax == "DIVIDE(SUM(Sales[Amount]), SUM(Sales[Quantity]), 0)"
    assert positive_sales.dax == "FILTER(Sales, Sales[Amount] > 0)"
    assert positive_sales.table is None
    # "Avg Price" is DIVIDE(SUM(...), SUM(...)) -- a ratio that cannot reduce to a single native
    # aggregation, so it is preserved as derived and surfaced via a dax_not_translated warning.
    reparsed_warnings = getattr(reparsed, "import_warnings")
    if _dax_parser_available():
        assert Counter(w["code"] for w in reparsed_warnings) == Counter({"dax_not_translated": 1})
    else:
        assert reparsed_warnings == []


def test_tmdl_export_preserves_expression_meta_for_measure_and_calculated_column():
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            calculatedColumn Net
                dataType: decimal
                expression = Sales[Amount] - 1 meta [lineageTag="NetLineage", isHidden=true]
            measure Revenue = SUM(Sales[Amount]) meta [displayFolder="KPIs", isSimpleMeasure=true]
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    warnings = getattr(graph, "import_warnings")
    assert not any(warning.get("code") == "dax_parse_error" and warning.get("name") == "Net" for warning in warnings)

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert 'expression = Sales[Amount] - 1 meta [lineageTag="NetLineage", isHidden=true]' in content
        assert 'measure Revenue = SUM(Sales[Amount]) meta [displayFolder="KPIs", isSimpleMeasure=true]' in content


def test_tmdl_export_preserves_imported_column_and_measure_passthrough_properties():
    tmdl = textwrap.dedent(
        """
        table Sales
            lineageTag: SalesLineage
            column DateKey
                dataType: date
                sourceColumn: DateKey
                sortByColumn: Sales[SortKey]
                summarizeBy: none
                isHidden: true
                displayFolder: Time
            column SortKey
                dataType: int64
                sourceColumn: SortKey
            measure 'Total Sales' = SUM(Sales[Amount])
                displayFolder: KPIs
                detailRowsExpression = Sales
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert "lineageTag: SalesLineage" in content
        assert "sortByColumn: Sales[SortKey]" in content
        assert "summarizeBy: none" in content
        assert "isHidden: true" in content
        assert "displayFolder: Time" in content
        assert "displayFolder: KPIs" in content
        assert "detailRowsExpression = Sales" in content
        assert "column SortKey" in content
        assert "dataType: int64" in content


def test_tmdl_is_hidden_maps_to_public_false_and_exports():
    tmdl = textwrap.dedent(
        """
        table Sales
            column InternalCategory
                dataType: string
                sourceColumn: Category
                isHidden: true
            column VisibleCategory
                dataType: string
                sourceColumn: Category
            column Amount
                dataType: decimal
                sourceColumn: Amount
            measure 'Internal Revenue' = SUM(Sales[Amount])
                isHidden: true
            measure 'Visible Revenue' = SUM(Sales[Amount])
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    sales = graph.models["Sales"]
    assert sales.get_dimension("InternalCategory").public is False
    assert sales.get_dimension("VisibleCategory").public is True
    assert sales.get_metric("Internal Revenue").public is False
    assert sales.get_metric("Visible Revenue").public is True

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert "column InternalCategory" in content
        assert "isHidden: true" in content
        assert "measure 'Internal Revenue' = SUM(Sales[Amount])" in content


def test_tmdl_export_preserves_passthrough_expression_meta_with_block():
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            measure Revenue = SUM(Sales[Amount])
                detailRowsExpression = meta [lineageTag="DetailRowsExpr"]
                    FILTER(Sales, Sales[Amount] > 0)
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert 'detailRowsExpression = meta [lineageTag="DetailRowsExpr"]' in content
        assert "FILTER(Sales, Sales[Amount] > 0)" in content


def test_tmdl_export_preserves_passthrough_child_nodes():
    tmdl = textwrap.dedent(
        """
        table Sales
            annotation TableTag
                value: "table_meta"
            column Amount
                dataType: decimal
                sourceColumn: Amount
                annotation ColumnTag
                    value: "column_meta"
            measure Revenue = SUM(Sales[Amount])
                annotation MeasureTag
                    value: "measure_meta"
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert "annotation TableTag" in content
        assert 'value: "table_meta"' in content
        assert "annotation ColumnTag" in content
        assert 'value: "column_meta"' in content
        assert "annotation MeasureTag" in content
        assert 'value: "measure_meta"' in content


def test_tmdl_multiline_dax_expression_preserves_embedded_comments():
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: decimal
                sourceColumn: Amount
            measure Revenue =
                // preserve this DAX comment
                SUM(Sales[Amount])
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    revenue = graph.models["Sales"].get_metric("Revenue")
    assert "// preserve this DAX comment" in revenue.dax

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert "// preserve this DAX comment" in content
        assert "SUM(Sales[Amount])" in content


def test_tmdl_export_preserves_model_level_passthrough_nodes_and_properties():
    tmdl = textwrap.dedent(
        """
        model Demo
            defaultPowerBIDataSourceVersion: powerBI_V3
            perspective SalesView
                annotation Scope
                    value: "all"
        table Sales
            column ID
                dataType: int64
                isKey
                sourceColumn: ID
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        content = model_file.read_text()
        assert "defaultPowerBIDataSourceVersion: powerBI_V3" in content
        assert "perspective SalesView" in content
        assert "annotation Scope" in content
        assert 'value: "all"' in content


def test_tmdl_export_preserves_root_level_passthrough_nodes():
    tmdl = textwrap.dedent(
        """
        model Demo
        table Sales
            column ID
                dataType: int64
                isKey
                sourceColumn: ID
        role Analysts
            modelPermission: read
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        content = model_file.read_text()
        assert "role Analysts" in content
        assert "modelPermission: read" in content


def test_tmdl_export_preserves_database_passthrough_and_names():
    tmdl = textwrap.dedent(
        """
        /// Demo database
        database DemoDB
            compatibilityLevel: 1601
            annotation DbTag
                value: "db_meta"
            model DemoModel
        model DemoModel
        table Sales
            column ID
                dataType: int64
                isKey
                sourceColumn: ID
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        database_file = Path(tmpdir) / "definition" / "database.tmdl"
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        database_content = database_file.read_text()
        model_content = model_file.read_text()
        assert "/// Demo database" in database_content
        assert "database DemoDB" in database_content
        assert "compatibilityLevel: 1601" in database_content
        assert "annotation DbTag" in database_content
        assert 'value: "db_meta"' in database_content
        assert "model DemoModel" in database_content
        assert "model DemoModel" in model_content


def test_tmdl_export_preserves_database_and_model_leading_comments():
    tmdl = textwrap.dedent(
        """
        # Database heading comment
        database DemoDB
            model DemoModel
        // Model heading comment
        model DemoModel
        table Sales
            column ID
                dataType: int64
                isKey
                sourceColumn: ID
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        database_file = Path(tmpdir) / "definition" / "database.tmdl"
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        database_content = database_file.read_text()
        model_content = model_file.read_text()
        assert database_content.startswith("# Database heading comment\n")
        assert model_content.startswith("// Model heading comment\n")


def test_tmdl_export_preserves_database_and_model_description_raw_literals():
    tmdl = textwrap.dedent(
        '''
        database DemoDB
            description: "DB ""Desc"""
            model DemoModel
        model DemoModel
            description: "Model ""Desc"""
        table Sales
            column ID
                dataType: int64
                isKey
                sourceColumn: ID
        '''
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        database_file = Path(tmpdir) / "definition" / "database.tmdl"
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        database_content = database_file.read_text()
        model_content = model_file.read_text()
        assert 'description: "DB ""Desc"""' in database_content
        assert 'description: "Model ""Desc"""' in model_content
        assert "/// DB" not in database_content
        assert "/// Model" not in model_content


def test_tmdl_export_script_file():
    """Test exporting to a single TMDL script file."""
    model = Model(name="orders", table="orders", primary_key="id")
    graph = SemanticGraph()
    graph.add_model(model)

    adapter = TMDLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)
        content = temp_path.read_text()
        assert "createOrReplace" in content
        assert "table orders" in content
        reparsed = adapter.parse(temp_path)
        assert set(reparsed.models) == {"orders"}
        # The model's primary_key "id" is not backed by an isKey column, so TMDL cannot encode it;
        # the round-trip yields no key (None) rather than re-fabricating a phantom "id".
        assert reparsed.models["orders"].primary_key is None
    finally:
        temp_path.unlink()


def test_tmdl_export_script_preserves_database_model_and_root_passthrough():
    tmdl = textwrap.dedent(
        """
        database DemoDB
            compatibilityLevel: 1601
            model DemoModel
        model DemoModel
            perspective SalesView
                annotation Scope
                    value: "all"
        table Sales
            column ID
                dataType: int64
                isKey
                sourceColumn: ID
        role Analysts
            modelPermission: read
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        src_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(src_path)
    finally:
        src_path.unlink()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        out_path = Path(f.name)

    try:
        TMDLAdapter().export(graph, out_path)
        content = out_path.read_text()
        assert "createOrReplace" in content
        assert "database DemoDB" in content
        assert "compatibilityLevel: 1601" in content
        assert "model DemoModel" in content
        assert "perspective SalesView" in content
        assert "role Analysts" in content
        assert "modelPermission: read" in content
        assert "table Sales" in content
        reparsed = TMDLAdapter().parse(out_path)
        assert set(reparsed.models) == {"Sales"}
        assert getattr(reparsed, "_tmdl_database_name") == "DemoDB"
        assert getattr(reparsed, "_tmdl_model_name") == "DemoModel"
        assert getattr(reparsed, "_tmdl_database_properties")[0]["name"] == "compatibilityLevel"
        assert getattr(reparsed, "_tmdl_model_child_nodes")[0].name == "SalesView"
        assert getattr(reparsed, "_tmdl_root_nodes")[0].type == "role"
    finally:
        out_path.unlink()


def test_tmdl_export_script_preserves_realistic_project_metadata(tmp_path):
    fixture_root = Path("tests/fixtures/tmdl_realistic")
    graph = TMDLAdapter().parse(fixture_root)
    out_path = tmp_path / "retail_analytics.tmdl"

    TMDLAdapter().export(graph, out_path)
    content = out_path.read_text()

    assert "createOrReplace" in content
    assert "database 'Retail Analytics'" in content
    assert "compatibilityLevel: 1601" in content
    assert "annotation DatabaseTag" in content
    assert "perspective Executive" in content
    assert "culture en-US" in content
    assert "role 'Sales Managers'" in content
    assert "partition Sales = m" in content
    assert "calculatedTable 'Sales By Category'" in content
    assert "annotation CalculationTag" in content
    assert "relationship 'Sales-Products'" in content
    assert "annotation RelationshipLineage" in content

    reparsed = TMDLAdapter().parse(out_path)
    _assert_expected_import_warnings(
        getattr(reparsed, "import_warnings"), dax_parser_unavailable=5, dax_not_translated=1
    )
    assert set(reparsed.models) == {"Sales", "Products", "Calendar", "Sales By Category"}
    assert getattr(reparsed, "_tmdl_database_name") == "Retail Analytics"
    assert getattr(reparsed, "_tmdl_model_child_nodes")[0].name == "Executive"

    reparsed_sales = reparsed.models["Sales"]
    reparsed_calculated = reparsed.models["Sales By Category"]
    reparsed_rel = next(rel for rel in reparsed_sales.relationships if rel.name == "Products")
    assert reparsed_sales.get_dimension("Amount x2").dax == "Sales[Amount] * 2"
    assert reparsed_sales.get_dimension("Amount x2").sql is None
    assert reparsed_sales.get_metric("Total Sales").dax == "SUM(Sales[Amount])"
    assert getattr(reparsed_calculated, "_tmdl_child_nodes")[0].name == "CalculationTag"
    assert getattr(reparsed_rel, "_tmdl_child_nodes")[0].name == "RelationshipLineage"
    assert getattr(reparsed_rel, "_tmdl_from_column") == "ProductKey"
    assert getattr(reparsed_rel, "_tmdl_to_column") == "ProductKey"


def test_tmdl_export_preserves_core_property_raw_literals():
    tmdl = textwrap.dedent(
        """
        table Sales
            column DateKey
                dataType: "date"
                sourceColumn: "Date Key"
                caption: "Order Date"
                formatString: "yyyy-MM-dd"
            measure Revenue = SUM(Sales[DateKey])
                caption: "Revenue Label"
                formatString: "0.00"
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert 'dataType: "date"' in content
        assert 'sourceColumn: "Date Key"' in content
        assert 'caption: "Order Date"' in content
        assert 'formatString: "yyyy-MM-dd"' in content
        assert 'caption: "Revenue Label"' in content
        assert 'formatString: "0.00"' in content


def test_tmdl_export_preserves_iskey_raw_literals():
    tmdl = textwrap.dedent(
        """
        table Sales
            column DateKey
                dataType: int64
                isKey: TRUE
                sourceColumn: DateKey
            column ProductKey
                dataType: int64
                isKey: FALSE
                sourceColumn: ProductKey
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert "isKey: TRUE" in content
        assert "isKey: FALSE" in content
        assert "\n        isKey\n" not in content


def test_tmdl_export_preserves_table_and_measure_description_raw_literals():
    tmdl = textwrap.dedent(
        '''
        table Sales
            description: "Table ""Desc"""
            column ID
                dataType: int64
                isKey
                sourceColumn: ID
            measure Revenue = SUM(Sales[ID])
                description: "Measure ""Desc"""
        '''
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert 'description: "Table ""Desc"""' in content
        assert 'description: "Measure ""Desc"""' in content
        assert "/// Table" not in content
        assert "/// Measure" not in content


def test_tmdl_export_preserves_raw_identifier_literals():
    tmdl = textwrap.dedent(
        """
        database "Demo DB"
            model "Demo Model"
        model "Demo Model"
        table "Sales Table"
            column "Sale ID"
                dataType: int64
                isKey
                sourceColumn: "Sale ID"
        table "Products Table"
            column "Product ID"
                dataType: int64
                isKey
                sourceColumn: "Product ID"
        relationship "Sales To Products"
            fromColumn: "Sales Table[Sale ID]"
            toColumn: "Products Table[Product ID]"
            fromCardinality: many
            toCardinality: one
        """
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        database_file = Path(tmpdir) / "definition" / "database.tmdl"
        model_file = Path(tmpdir) / "definition" / "model.tmdl"
        sales_table_file = Path(tmpdir) / "definition" / "tables" / "Sales Table.tmdl"
        products_table_file = Path(tmpdir) / "definition" / "tables" / "Products Table.tmdl"
        rel_file = Path(tmpdir) / "definition" / "relationships.tmdl"

        assert sales_table_file.exists()
        assert products_table_file.exists()

        database_content = database_file.read_text()
        model_content = model_file.read_text()
        sales_content = sales_table_file.read_text()
        rel_content = rel_file.read_text()

        assert 'database "Demo DB"' in database_content
        assert 'model "Demo Model"' in database_content
        assert 'model "Demo Model"' in model_content
        assert 'ref table "Sales Table"' in model_content
        assert 'ref table "Products Table"' in model_content
        assert 'ref relationship "Sales To Products"' in model_content
        assert 'table "Sales Table"' in sales_content
        assert 'column "Sale ID"' in sales_content
        assert 'relationship "Sales To Products"' in rel_content


def test_tmdl_export_preserves_escaped_quote_value_literals():
    tmdl = textwrap.dedent(
        '''
        table Sales
            column ID
                dataType: int64
                isKey
                sourceColumn: ID
                caption: "Order ""ID"""
        '''
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)

    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    sales = graph.models["Sales"]
    dim = sales.get_dimension("ID")
    assert dim is not None
    assert dim.label == 'Order "ID"'

    with tempfile.TemporaryDirectory() as tmpdir:
        TMDLAdapter().export(graph, tmpdir)
        table_file = Path(tmpdir) / "definition" / "tables" / "Sales.tmdl"
        content = table_file.read_text()
        assert 'caption: "Order ""ID"""' in content


def test_tmdl_dax_reduction_semantics():
    """Regression: DAX measures reduce to the CORRECT (agg, column) or stay derived.

    Guards the mistranslation fixes -- COUNTBLANK must not invert to count, an aggregate over a
    measure reference must not be treated as a column, an unbracketed dotted column must not leak
    the table prefix, and any reduced simple-aggregation sql must reference a real column.
    """
    pytest.importorskip("sidemantic_dax")
    tmdl = textwrap.dedent(
        """
        table Sales
            column Amount
                dataType: double
                sourceColumn: Amount
            column Region
                dataType: string
                sourceColumn: Region
            measure 'Total' = SUM(Sales[Amount])
            measure 'Blanks' = COUNTBLANK(Sales[Region])
            measure 'Ref Max' = MAX([Total])
            measure 'Dotted' = SUM(Sales.Amount)
        """
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)
    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()
    sales = graph.models["Sales"]
    columns = {dim.name for dim in sales.dimensions}

    # The genuinely-simple aggregation still reduces correctly.
    total = sales.get_metric("Total")
    assert (total.agg, total.sql) == ("sum", "Amount")

    # COUNTBLANK counts blanks; it must NOT invert into a plain count over the column.
    blanks = sales.get_metric("Blanks")
    assert not (blanks.agg == "count" and blanks.sql == "Region")

    # MAX([Total]) references a measure, not a column -> must not become max(Total).
    ref_max = sales.get_metric("Ref Max")
    assert not (ref_max.agg == "max" and ref_max.sql == "Total")

    # An unbracketed dotted column must never leak the table prefix into sql.
    dotted = sales.get_metric("Dotted")
    assert dotted.sql != "Sales.Amount"
    if dotted.agg is not None:
        assert dotted.sql == "Amount"

    # Every reduced simple-aggregation sql must reference a real column of the table.
    for metric in sales.metrics:
        if metric.agg and metric.sql is not None:
            assert metric.sql in columns, f"{metric.name} reduced to non-column {metric.sql!r}"


def test_tmdl_real_fixture_reductions_reference_real_columns():
    """Every simple-aggregation measure reduced from a real Power BI model must reference an actual
    column of its table (guards the column-membership gating against silent mistranslation)."""
    pytest.importorskip("sidemantic_dax")
    graph = TMDLAdapter().parse("tests/fixtures/external_powerbi/microsoft-analysis-services-sales")
    for model in graph.models.values():
        columns = {dim.name for dim in model.dimensions}
        for metric in model.metrics:
            if metric.agg and metric.sql is not None:
                assert metric.sql in columns, f"{model.name}.{metric.name} -> non-column {metric.sql!r}"


def test_tmdl_deeply_nested_dax_does_not_crash_import():
    """Regression: a pathologically nested DAX measure surfaces a catchable parse error (the Rust
    recursion-depth guard) and a dax_parse_error warning, rather than overflowing the stack and
    killing the host process."""
    pytest.importorskip("sidemantic_dax")
    import sidemantic_dax

    with pytest.raises(Exception):
        sidemantic_dax.parse_expression("(" * 5000 + "1" + ")" * 5000)

    nested = "(" * 5000 + "Sales[Amount]" + ")" * 5000
    tmdl = textwrap.dedent(
        f"""
        table Sales
            column Amount
                dataType: double
                sourceColumn: Amount
            measure 'Deep' = {nested}
        """
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)
    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()
    codes = {w["code"] for w in getattr(graph, "import_warnings", [])}
    assert "dax_parse_error" in codes


def test_tmdl_keyless_many_to_many_joins_without_keying_off_endpoints():
    """Regression: a direct (no-bridge) many-to-many between keyless tables joins on the
    relationship's own from/to columns and does NOT adopt either as a primary key.

    A many-to-many endpoint sits on the "many" side and can repeat, so it is not a valid unique key
    -- promoting it would make symmetric aggregates silently undercount. Each table therefore stays
    keyless (primary_key None), while the join still references and projects each side's own column
    (using differently named columns so the related side cannot accidentally reuse the other name).
    """
    pytest.importorskip("sidemantic_dax")
    tmdl = textwrap.dedent(
        """
        table Authors
            column author_id
                dataType: string
                sourceColumn: author_id
            column region
                dataType: string
                sourceColumn: region
        table Books
            column book_author_id
                dataType: string
                sourceColumn: book_author_id
            column genre
                dataType: string
                sourceColumn: genre
        relationship AuthorsBooks
            fromColumn: Authors[author_id]
            toColumn: Books[book_author_id]
            fromCardinality: many
            toCardinality: many
        """
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)
    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    authors = graph.models["Authors"]
    books = graph.models["Books"]
    assert authors.relationships[0].type == "many_to_many"
    # Neither table adopts its non-unique many-to-many endpoint as a primary key.
    assert authors.primary_key is None
    assert books.primary_key is None

    # The join still works: it references each table's OWN column (not the other side's name) and
    # both columns are projected, so it is not a join on a column missing from a CTE.
    layer = SemanticLayer()
    layer.graph = graph
    sql = layer.compile(dimensions=["Authors.region", "Books.genre"])
    assert "Authors_cte.author_id" in sql
    assert "Books_cte.book_author_id" in sql
    assert "author_id AS author_id" in sql
    assert "book_author_id AS book_author_id" in sql


def test_tmdl_keyless_many_to_many_metric_raises_clear_error():
    """Regression: a fan-out metric on a keyless table reached through a direct many-to-many cannot
    be de-duplicated, so it must raise a clear QueryValidationError rather than emitting an empty
    CONCAT() that crashes the SQL parser downstream."""
    pytest.importorskip("sidemantic_dax")
    tmdl = textwrap.dedent(
        """
        table Authors
            column author_id
                dataType: string
                sourceColumn: author_id
            column n
                dataType: int64
                sourceColumn: n
            measure total = SUM(Authors[n])
        table Books
            column book_author_id
                dataType: string
                sourceColumn: book_author_id
            column genre
                dataType: string
                sourceColumn: genre
        relationship AuthorsBooks
            fromColumn: Authors[author_id]
            toColumn: Books[book_author_id]
            fromCardinality: many
            toCardinality: many
        """
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)
    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    layer = SemanticLayer()
    layer.graph = graph
    with pytest.raises(QueryValidationError, match="no primary key"):
        layer.compile(metrics=["Authors.total"], dimensions=["Books.genre"])


def test_tmdl_many_to_many_on_non_primary_key_column():
    """Regression: a direct many-to-many that joins on columns which are NOT either table's declared
    primary key must project and join on those exact columns, on both sides.

    Here both A and B declare isKey ``id`` but the relationship joins ``A[a_alt]`` to ``B[b_alt]``.
    The join must reference each table's alternate column (never its declared ``id``) and each CTE
    must project its alternate column, otherwise the join is on a column missing from the CTE.
    """

    import re

    pytest.importorskip("sidemantic_dax")
    tmdl = textwrap.dedent(
        """
        table A
            column id
                dataType: string
                isKey
                sourceColumn: id
            column a_alt
                dataType: string
                sourceColumn: a_alt
            measure a_count = COUNTROWS(A)
        table B
            column id
                dataType: string
                isKey
                sourceColumn: id
            column b_alt
                dataType: string
                sourceColumn: b_alt
            column b_label
                dataType: string
                sourceColumn: b_label
        relationship AB
            fromColumn: A[a_alt]
            toColumn: B[b_alt]
            fromCardinality: many
            toCardinality: many
        """
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)
    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    # Both tables keep their declared primary keys; the many-to-many columns are independent of them.
    assert graph.models["A"].primary_key == "id"
    assert graph.models["B"].primary_key == "id"

    layer = SemanticLayer()
    layer.graph = graph
    sql = layer.compile(metrics=["A.a_count"], dimensions=["B.b_label"])
    # The join pairs each side's alternate column (not the declared id key), and each CTE projects
    # its alternate column, so neither side joins on a column missing from its CTE.
    assert re.search(r"ON\s+B_cte\.b_alt\s*=\s*A_cte\.a_alt", sql) or re.search(
        r"ON\s+A_cte\.a_alt\s*=\s*B_cte\.b_alt", sql
    ), sql
    assert "a_alt AS a_alt" in sql
    assert "b_alt AS b_alt" in sql


def test_tmdl_one_to_one_recovers_keyless_source_key():
    """Regression: a one-to-one relationship has a unique key on BOTH sides, so a keyless table on
    the fromColumn (source) side must recover its endpoint as its primary key.

    Otherwise only the to-side gets a key; the source stays None, and while the join is still built
    on the source's from-column, the source CTE -- keyed off the now-empty primary_key_columns --
    never selects that column, producing a join on a column missing from the CTE.
    """
    pytest.importorskip("sidemantic_dax")
    tmdl = textwrap.dedent(
        """
        table A
            column a_key
                dataType: string
                sourceColumn: a_key
            measure a_count = COUNTROWS(A)
        table B
            column b_key
                dataType: string
                sourceColumn: b_key
            column b_label
                dataType: string
                sourceColumn: b_label
        relationship AB
            fromColumn: A[a_key]
            toColumn: B[b_key]
            fromCardinality: one
            toCardinality: one
        """
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)
    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    assert graph.models["A"].relationships[0].type == "one_to_one"
    # Both unique sides recover their endpoint key -- the source side is no longer left None.
    assert graph.models["A"].primary_key == "a_key"
    assert graph.models["B"].primary_key == "b_key"

    # The source key is projected into the source CTE and referenced by the join, so the join is not
    # on a column that is absent from the CTE.
    layer = SemanticLayer()
    layer.graph = graph
    sql = layer.compile(metrics=["A.a_count"], dimensions=["B.b_label"])
    assert "a_key AS a_key" in sql
    assert "A_cte.a_key" in sql


def test_tmdl_one_to_one_alternate_key_does_not_shadow_real_key():
    """Regression: a one-to-one source-side endpoint must not compete with a real one-side key.

    A table that is the fromColumn side of a one-to-one on an alternate key and also the one-side of
    a one-to-many on its real key must keep the real one-to-many key. The one-to-one source endpoint
    is recovered only as a fallback for otherwise-keyless tables, so it never makes the primary key
    ambiguous (which would drop it to None and emit empty-key SQL).
    """
    pytest.importorskip("sidemantic_dax")
    tmdl = textwrap.dedent(
        """
        table Orders
            column order_id
                dataType: int64
                sourceColumn: order_id
            column alt_key
                dataType: string
                sourceColumn: alt_key
            measure cnt = COUNTROWS(Orders)
        table LineItems
            column order_id
                dataType: int64
                sourceColumn: order_id
        table OrderMeta
            column meta_key
                dataType: string
                sourceColumn: meta_key
            column channel
                dataType: string
                sourceColumn: channel
        relationship OrdersLineItems
            fromColumn: Orders[order_id]
            toColumn: LineItems[order_id]
            fromCardinality: one
            toCardinality: many
        relationship OrdersMeta
            fromColumn: Orders[alt_key]
            toColumn: OrderMeta[meta_key]
            fromCardinality: one
            toCardinality: one
        """
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)
    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    # Orders keeps its real one-to-many key; the alternate one-to-one key must not shadow it to None.
    assert graph.models["Orders"].primary_key == "order_id"
    # The one-to-one to-side still resolves its key authoritatively.
    assert graph.models["OrderMeta"].primary_key == "meta_key"

    # A query THROUGH the one-to-one joins on Orders' alt_key (the relationship's fromColumn), so
    # Orders' CTE must project alt_key -- not only its declared order_id key -- otherwise the join
    # references a column missing from the CTE.
    layer = SemanticLayer()
    layer.graph = graph
    sql = layer.compile(metrics=["Orders.cnt"], dimensions=["OrderMeta.channel"])
    assert "Orders_cte.alt_key" in sql
    assert "alt_key AS alt_key" in sql


def test_tmdl_ambiguous_one_side_target_not_recovered_from_endpoint():
    """Regression: a table whose authoritative one-side key is ambiguous (referenced by multiple
    relationships using different key columns) must stay keyless, not adopt a many-to-many endpoint.

    Substituting a many-to-many/one-to-one endpoint would mask the ambiguity with a non-authoritative
    row identity, so symmetric aggregates on the table would silently dedupe on the wrong column.
    """
    pytest.importorskip("sidemantic_dax")
    tmdl = textwrap.dedent(
        """
        table D
            column key1
                dataType: string
                sourceColumn: key1
            column key2
                dataType: string
                sourceColumn: key2
            column tag
                dataType: string
                sourceColumn: tag
        table F1
            column key1
                dataType: string
                sourceColumn: key1
        table F2
            column key2
                dataType: string
                sourceColumn: key2
        table X
            column x_tag
                dataType: string
                sourceColumn: x_tag
        relationship F1D
            fromColumn: F1[key1]
            toColumn: D[key1]
            fromCardinality: many
            toCardinality: one
        relationship F2D
            fromColumn: F2[key2]
            toColumn: D[key2]
            fromCardinality: many
            toCardinality: one
        relationship DX
            fromColumn: D[tag]
            toColumn: X[x_tag]
            fromCardinality: many
            toCardinality: many
        """
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(tmdl)
        temp_path = Path(f.name)
    try:
        graph = TMDLAdapter().parse(temp_path)
    finally:
        temp_path.unlink()

    # D's one-side key is ambiguous, so it stays unresolved (never the many-to-many "tag" endpoint),
    # and the ambiguity is surfaced as a warning.
    assert graph.models["D"].primary_key is None
    codes = {w["code"] for w in getattr(graph, "import_warnings", [])}
    assert "primary_key_unresolved" in codes


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
