"""Tests for ThoughtSpot adapter - parsing."""

import tempfile
from pathlib import Path

import yaml

from sidemantic import SemanticLayer
from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter
from sidemantic.loaders import load_from_directory

# =============================================================================
# BASIC PARSING TESTS
# =============================================================================


def test_import_real_thoughtspot_examples():
    """Test importing ThoughtSpot TML examples."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/orders.table.tml")

    assert "orders" in graph.models

    orders = graph.models["orders"]
    assert orders.table == "analytics.public.orders"
    assert orders.primary_key == "id"

    order_date = orders.get_dimension("order_date")
    assert order_date is not None
    assert order_date.type == "time"
    assert order_date.granularity == "day"

    amount = orders.get_metric("amount")
    assert amount is not None
    assert amount.agg == "sum"
    assert amount.format == "$#,##0.00"

    order_count = orders.get_metric("order_count")
    assert order_count is not None
    assert order_count.agg == "count"


def test_import_thoughtspot_kitchen_sink_table():
    """Test parsing a kitchen sink table TML fixture."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/kitchen_sink.table.tml")

    assert "sales" in graph.models
    model = graph.models["sales"]

    order_date = model.get_dimension("order_date")
    assert order_date is not None
    assert order_date.type == "time"
    assert order_date.granularity == "day"

    order_week = model.get_dimension("order_week")
    assert order_week is not None
    assert order_week.type == "time"
    assert order_week.granularity == "week"

    order_hour = model.get_dimension("order_hour")
    assert order_hour is not None
    assert order_hour.type == "time"
    assert order_hour.granularity == "hour"

    order_month = model.get_dimension("order_month")
    assert order_month is not None
    assert order_month.type == "time"
    assert order_month.granularity == "month"

    order_quarter = model.get_dimension("order_quarter")
    assert order_quarter is not None
    assert order_quarter.type == "time"
    assert order_quarter.granularity == "quarter"

    order_year = model.get_dimension("order_year")
    assert order_year is not None
    assert order_year.type == "time"
    assert order_year.granularity == "year"

    is_active = model.get_dimension("is_active")
    assert is_active is not None
    assert is_active.type == "boolean"

    status = model.get_dimension("status")
    assert status is not None
    assert status.label == "Order Status"
    assert status.description == "Current order state"

    gross_revenue = model.get_metric("gross_revenue")
    assert gross_revenue is not None
    assert gross_revenue.agg == "sum"
    assert gross_revenue.format == "$#,##0.00"
    assert gross_revenue.label == "Gross Revenue"
    assert gross_revenue.description == "Total revenue before discounts"

    avg_order_value = model.get_metric("avg_order_value")
    assert avg_order_value is not None
    assert avg_order_value.agg == "avg"

    min_order_value = model.get_metric("min_order_value")
    assert min_order_value is not None
    assert min_order_value.agg == "min"

    max_order_value = model.get_metric("max_order_value")
    assert max_order_value is not None
    assert max_order_value.agg == "max"

    median_order_value = model.get_metric("median_order_value")
    assert median_order_value is not None
    assert median_order_value.agg == "median"

    distinct_customers = model.get_metric("distinct_customers")
    assert distinct_customers is not None
    assert distinct_customers.agg == "count_distinct"

    revenue_stddev = model.get_metric("revenue_stddev")
    assert revenue_stddev is not None
    assert revenue_stddev.type == "derived"
    assert "STDDEV" in (revenue_stddev.sql or "")

    revenue_raw = model.get_metric("revenue_raw")
    assert revenue_raw is not None
    assert revenue_raw.type == "derived"
    assert revenue_raw.sql == "gross_revenue"


def test_import_thoughtspot_kitchen_sink_worksheet():
    """Test parsing a kitchen sink worksheet TML fixture."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/kitchen_sink.worksheet.tml")

    assert "sales_worksheet_full" in graph.models
    model = graph.models["sales_worksheet_full"]

    assert model.sql is not None
    assert "JOIN customers" in model.sql

    relationships = {rel.name: rel for rel in model.relationships}
    assert relationships["customers"].type == "many_to_one"
    assert relationships["customers"].foreign_key == "customer_id"
    assert relationships["customers"].primary_key == "id"
    assert relationships["regions"].type == "one_to_one"

    order_date = model.get_dimension("order_date")
    assert order_date is not None
    assert order_date.type == "time"
    assert order_date.label == "Order Date"

    is_active = model.get_dimension("is_active")
    assert is_active is not None
    assert is_active.type == "boolean"

    net_revenue = model.get_metric("net_revenue")
    assert net_revenue is not None
    assert net_revenue.agg == "sum"
    assert "gross_revenue" in (net_revenue.sql or "")

    avg_order_value = model.get_metric("avg_order_value")
    assert avg_order_value is not None
    assert avg_order_value.agg == "avg"
    assert "/" in (avg_order_value.sql or "")

    revenue_stddev = model.get_metric("revenue_stddev")
    assert revenue_stddev is not None
    assert revenue_stddev.type == "derived"
    assert "STDDEV" in (revenue_stddev.sql or "")

    distinct_customers = model.get_metric("distinct_customers")
    assert distinct_customers is not None
    assert distinct_customers.agg == "count_distinct"

    min_order_value = model.get_metric("min_order_value")
    assert min_order_value is not None
    assert min_order_value.agg == "min"

    max_order_value = model.get_metric("max_order_value")
    assert max_order_value is not None
    assert max_order_value.agg == "max"

    median_order_value = model.get_metric("median_order_value")
    assert median_order_value is not None
    assert median_order_value.agg == "median"


def test_import_thoughtspot_basic_worksheet():
    """Test importing a basic worksheet example."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/sales.worksheet.tml")

    assert "sales_worksheet" in graph.models
    worksheet = graph.models["sales_worksheet"]
    assert worksheet.sql is not None
    assert "JOIN customers" in worksheet.sql

    total_revenue = worksheet.get_metric("total_revenue")
    assert total_revenue is not None
    assert total_revenue.agg == "sum"
    assert "amount" in (total_revenue.sql or "")

    relationships = worksheet.relationships
    assert len(relationships) == 1
    rel = relationships[0]
    assert rel.name == "customers"
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "customer_id"
    assert rel.primary_key == "id"


def test_import_thoughtspot_table_joins():
    """Test table-level joins_with relationships."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/table_joins.table.tml")

    assert "orders" in graph.models
    model = graph.models["orders"]

    assert len(model.relationships) == 1
    rel = model.relationships[0]
    assert rel.name == "customers"
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "customer_id"
    assert rel.primary_key == "id"


def test_import_thoughtspot_worksheet_ids():
    """Test worksheet parsing when tables use ids and fqn."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/worksheet_ids.worksheet.tml")

    assert "sales_ids" in graph.models
    model = graph.models["sales_ids"]

    assert model.sql is not None
    assert "JOIN customers_table" in model.sql

    customer_name = model.get_dimension("customer_name")
    assert customer_name is not None
    assert customer_name.sql == "customers_table.name"

    net_revenue = model.get_metric("net_revenue")
    assert net_revenue is not None
    assert "gross_revenue" in (net_revenue.sql or "")


def test_import_thoughtspot_multi_join_worksheet():
    """Test multi-join worksheet with chained formulas and mixed join predicates."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/worksheet_multi_join.worksheet.tml")

    assert "sales_multi_join" in graph.models
    model = graph.models["sales_multi_join"]
    assert model.sql is not None
    assert "JOIN dim_product_main" in model.sql
    assert "JOIN dim_product_alt" in model.sql
    assert "country_code" in model.sql

    relationships = {rel.name: rel for rel in model.relationships}
    assert relationships["dim_product_main"].type == "many_to_one"
    assert relationships["dim_product_alt"].type == "one_to_one"

    gross_revenue = model.get_metric("gross_revenue")
    assert gross_revenue is not None
    assert gross_revenue.agg == "sum"
    assert "price" in (gross_revenue.sql or "")

    net_revenue = model.get_metric("net_revenue")
    assert net_revenue is not None
    assert net_revenue.agg == "sum"
    assert "gross_revenue" in (net_revenue.sql or "")


def test_import_thoughtspot_formula_name_id():
    """Test formula_id matching when formula has no id field."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/worksheet_formula_name.worksheet.tml")

    assert "sales_formula_name" in graph.models
    model = graph.models["sales_formula_name"]
    metric = model.get_metric("net_revenue")
    assert metric is not None
    assert "gross_revenue" in (metric.sql or "")


def test_import_thoughtspot_db_column_properties():
    """Test parsing data types from db_column_properties."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/table_db_column_properties.table.tml")

    assert "inventory" in graph.models
    model = graph.models["inventory"]

    sku = model.get_dimension("sku")
    assert sku is not None
    assert sku.type == "categorical"

    in_stock = model.get_dimension("in_stock")
    assert in_stock is not None
    assert in_stock.type == "boolean"

    last_updated = model.get_dimension("last_updated")
    assert last_updated is not None
    assert last_updated.type == "time"
    assert last_updated.granularity == "hour"

    quantity = model.get_metric("quantity")
    assert quantity is not None
    assert quantity.agg == "sum"


def test_import_thoughtspot_unbracketed_expr():
    """Test parsing unbracketed table.column expressions."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/worksheet_unbracketed_expr.worksheet.tml")

    assert "sales_unbracketed" in graph.models
    model = graph.models["sales_unbracketed"]
    net_revenue = model.get_metric("net_revenue")
    assert net_revenue is not None
    assert "sales.gross_revenue" in (net_revenue.sql or "")




# =============================================================================
# DATA TYPE MAPPING TESTS
# =============================================================================


def test_thoughtspot_table_data_type_mapping():
    """Test ThoughtSpot data type mappings."""
    tml_def = {
        "table": {
            "name": "test",
            "db_table": "test",
            "columns": [
                {
                    "name": "flag",
                    "db_column_name": "flag",
                    "data_type": "BOOL",
                    "properties": {"column_type": "ATTRIBUTE"},
                },
                {
                    "name": "amount",
                    "db_column_name": "amount",
                    "data_type": "DOUBLE",
                    "properties": {"column_type": "ATTRIBUTE"},
                },
                {
                    "name": "event_date",
                    "db_column_name": "event_date",
                    "data_type": "DATE",
                    "properties": {"column_type": "ATTRIBUTE", "default_date_bucket": "DAILY"},
                },
            ],
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tml", delete=False) as f:
        yaml.safe_dump(tml_def, f, sort_keys=False)
        temp_path = Path(f.name)

    try:
        adapter = ThoughtSpotAdapter()
        graph = adapter.parse(temp_path)
        model = graph.models["test"]

        assert model.get_dimension("flag").type == "boolean"
        assert model.get_dimension("amount").type == "numeric"
        assert model.get_dimension("event_date").type == "time"
    finally:
        temp_path.unlink(missing_ok=True)


# =============================================================================
# AGGREGATION MAPPING TESTS
# =============================================================================


def test_thoughtspot_aggregation_mapping():
    """Test ThoughtSpot aggregation mappings."""
    tml_def = {
        "table": {
            "name": "metrics",
            "db_table": "metrics",
            "columns": [
                {
                    "name": "total",
                    "db_column_name": "amount",
                    "data_type": "DOUBLE",
                    "properties": {"column_type": "MEASURE", "aggregation": "SUM"},
                },
                {
                    "name": "unique_users",
                    "db_column_name": "user_id",
                    "data_type": "INT64",
                    "properties": {"column_type": "MEASURE", "aggregation": "COUNT_DISTINCT"},
                },
                {
                    "name": "avg_amount",
                    "db_column_name": "amount",
                    "data_type": "DOUBLE",
                    "properties": {"column_type": "MEASURE", "aggregation": "AVERAGE"},
                },
            ],
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tml", delete=False) as f:
        yaml.safe_dump(tml_def, f, sort_keys=False)
        temp_path = Path(f.name)

    try:
        adapter = ThoughtSpotAdapter()
        graph = adapter.parse(temp_path)
        model = graph.models["metrics"]

        assert model.get_metric("total").agg == "sum"
        assert model.get_metric("unique_users").agg == "count_distinct"
        assert model.get_metric("avg_amount").agg == "avg"
    finally:
        temp_path.unlink(missing_ok=True)


# =============================================================================
# LOADER AUTO-DETECTION
# =============================================================================


def test_thoughtspot_auto_detect_loader():
    """Test ThoughtSpot TML auto-detection in loaders.py."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tml_file = Path(tmpdir) / "orders.table.tml"
        tml_file.write_text(
            """
            table:
              name: orders
              db_table: orders
              columns: []
            """
        )

        layer = SemanticLayer()
        load_from_directory(layer, tmpdir)

        assert "orders" in layer.graph.models
