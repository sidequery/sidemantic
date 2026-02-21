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


# =============================================================================
# TPC-H FIXTURES (from thoughtspot/ps_tools, MIT license)
# =============================================================================


def test_import_tpch_customer_table():
    """Parse TPC-H CUSTOMER table with joins_with containing qualified destination refs."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/tpch_customer.table.tml")

    assert "CUSTOMER" in graph.models
    model = graph.models["CUSTOMER"]

    # 5 ATTRIBUTE columns -> dimensions, 3 MEASURE columns -> metrics
    assert len(model.dimensions) == 5
    assert len(model.metrics) == 3

    # Verify dimension types from db_column_properties
    for dim_name in ("C_ADDRESS", "C_COMMENT", "C_MKTSEGMENT", "C_NAME", "C_PHONE"):
        dim = model.get_dimension(dim_name)
        assert dim is not None, f"Missing dimension {dim_name}"
        assert dim.type == "categorical"

    # Verify measure aggregations
    acctbal = model.get_metric("C_ACCTBAL")
    assert acctbal is not None
    assert acctbal.agg == "sum"

    custkey = model.get_metric("C_CUSTKEY")
    assert custkey is not None
    assert custkey.agg == "sum"

    # 2 joins_with relationships (NATION, ORDERS)
    assert len(model.relationships) == 2
    rels = {r.name: r for r in model.relationships}
    assert "NATION" in rels
    assert rels["NATION"].foreign_key == "C_NATIONKEY"
    assert rels["NATION"].primary_key == "N_NATIONKEY"
    assert "ORDERS" in rels
    assert rels["ORDERS"].foreign_key == "C_CUSTKEY"
    assert rels["ORDERS"].primary_key == "O_CUSTKEY"


def test_import_tpch_lineitem_table():
    """Parse TPC-H LINEITEM table: mixed types, date columns, two joins."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/tpch_lineitem.table.tml")

    assert "LINEITEM" in graph.models
    model = graph.models["LINEITEM"]

    # 5 VARCHAR + 3 DATE = 8 dims; 4 DOUBLE + 4 INT64 = 8 metrics
    assert len(model.dimensions) == 8
    assert len(model.metrics) == 8

    # DATE columns detected as time dims with day granularity
    for date_col in ("L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPDATE"):
        dim = model.get_dimension(date_col)
        assert dim is not None, f"Missing date dimension {date_col}"
        assert dim.type == "time"
        assert dim.granularity == "day"

    # DOUBLE measures
    for metric_name in ("L_DISCOUNT", "L_EXTENDEDPRICE", "L_QUANTITY", "L_TAX"):
        metric = model.get_metric(metric_name)
        assert metric is not None, f"Missing metric {metric_name}"
        assert metric.agg == "sum"

    # 2 joins: PART and SUPPLIER
    assert len(model.relationships) == 2
    rels = {r.name: r for r in model.relationships}
    assert rels["PART"].foreign_key == "L_PARTKEY"
    assert rels["PART"].primary_key == "P_PARTKEY"
    assert rels["SUPPLIER"].foreign_key == "L_SUPPKEY"
    assert rels["SUPPLIER"].primary_key == "S_SUPPKEY"


def test_import_tpch_orders_table():
    """Parse TPC-H ORDERS table: date column, calculated field name, one join."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/tpch_orders.table.tml")

    assert "ORDERS" in graph.models
    model = graph.models["ORDERS"]

    # O_ORDERDATE is a DATE -> time dimension
    order_date = model.get_dimension("O_ORDERDATE")
    assert order_date is not None
    assert order_date.type == "time"
    assert order_date.granularity == "day"

    # Tableau-generated calculated field name preserved
    calc = model.get_metric("Calculation_396316787159064930")
    assert calc is not None
    assert calc.agg == "sum"

    # O_TOTALPRICE measure
    total_price = model.get_metric("O_TOTALPRICE")
    assert total_price is not None
    assert total_price.agg == "sum"

    # Single join to LINEITEM
    assert len(model.relationships) == 1
    rel = model.relationships[0]
    assert rel.name == "LINEITEM"
    assert rel.foreign_key == "O_ORDERKEY"
    assert rel.primary_key == "L_ORDERKEY"


def test_import_tpch_region_table():
    """Parse TPC-H REGION table: no joins, includes Tableau calc column."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/tpch_region.table.tml")

    assert "REGION" in graph.models
    model = graph.models["REGION"]

    # No joins
    assert len(model.relationships) == 0

    # Tableau calc field preserved as categorical dimension
    calc = model.get_dimension("Calculation_396316787158536545")
    assert calc is not None
    assert calc.type == "categorical"

    # R_REGIONKEY is a MEASURE
    regionkey = model.get_metric("R_REGIONKEY")
    assert regionkey is not None
    assert regionkey.agg == "sum"


def test_import_tpch_supplier_table():
    """Parse TPC-H SUPPLIER table: no joins, mix of measures and attributes."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/tpch_supplier.table.tml")

    assert "SUPPLIER" in graph.models
    model = graph.models["SUPPLIER"]
    assert len(model.relationships) == 0
    assert len(model.dimensions) == 4  # S_ADDRESS, S_COMMENT, S_NAME, S_PHONE
    assert len(model.metrics) == 3  # S_ACCTBAL, S_NATIONKEY, S_SUPPKEY


def test_import_tpch_partsupp_table():
    """Parse TPC-H PARTSUPP table: all measures, no joins, no attributes except PS_COMMENT."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/tpch_partsupp.table.tml")

    assert "PARTSUPP" in graph.models
    model = graph.models["PARTSUPP"]
    assert len(model.relationships) == 0
    assert len(model.dimensions) == 1  # PS_COMMENT only
    assert len(model.metrics) == 4  # PS_SUPPLYCOST, PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY

    comment = model.get_dimension("PS_COMMENT")
    assert comment is not None
    assert comment.type == "categorical"


def test_import_tpch_worksheet():
    """Parse TPC-H 8-table worksheet: 7 joins, 8 table_paths, 3 formulas, 63 columns."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/tpch_worksheet.worksheet.tml")

    assert "SF Dashboard" in graph.models
    model = graph.models["SF Dashboard"]

    # 7 joins produce 7 relationships
    assert len(model.relationships) == 7
    rel_names = {r.name for r in model.relationships}
    for expected in ("NATION", "ORDERS", "PART", "SUPPLIER", "REGION", "LINEITEM", "PARTSUPP"):
        assert expected in rel_names, f"Missing relationship {expected}"

    # Worksheet joins lack ON clauses, so sql is None and table is the base table
    assert model.sql is None
    assert model.table == "CUSTOMER"

    # 63 worksheet_columns (all column_id-based); formulas exist but have no
    # corresponding worksheet_column entries, so they are not materialized
    total_columns = len(model.dimensions) + len(model.metrics)
    assert total_columns == 63
    assert len(model.dimensions) == 34
    assert len(model.metrics) == 29

    # Verify dimensions resolve table_path ids to table names
    c_name = model.get_dimension("C_NAME")
    assert c_name is not None
    assert c_name.sql == "CUSTOMER.C_NAME"

    r_name = model.get_dimension("R_NAME")
    assert r_name is not None
    assert r_name.sql == "REGION.R_NAME"

    s_name = model.get_dimension("S_NAME")
    assert s_name is not None
    assert s_name.sql == "SUPPLIER.S_NAME"

    # Verify metrics from different source tables
    o_totalprice = model.get_metric("O_TOTALPRICE")
    assert o_totalprice is not None
    assert o_totalprice.agg == "sum"
    assert o_totalprice.sql == "ORDERS.O_TOTALPRICE"

    l_extprice = model.get_metric("L_EXTENDEDPRICE")
    assert l_extprice is not None
    assert l_extprice.agg == "sum"
    assert l_extprice.sql == "LINEITEM.L_EXTENDEDPRICE"

    # Metrics spanning all 8 source tables via table_path resolution
    assert model.get_metric("C_ACCTBAL").sql == "CUSTOMER.C_ACCTBAL"
    assert model.get_metric("PS_SUPPLYCOST").sql == "PARTSUPP.PS_SUPPLYCOST"
    assert model.get_metric("P_RETAILPRICE").sql == "PART.P_RETAILPRICE"
    assert model.get_metric("S_ACCTBAL").sql == "SUPPLIER.S_ACCTBAL"

    # Date columns from ORDERS and LINEITEM tables
    assert model.get_dimension("O_ORDERDATE") is not None
    assert model.get_dimension("L_SHIPDATE") is not None
    assert model.get_dimension("L_COMMITDATE") is not None
    assert model.get_dimension("L_RECEIPTDATE") is not None

    # Tableau-generated calc field names are preserved
    assert model.get_dimension("Calculation_396316787158536545") is not None
    assert model.get_metric("Calculation_396316787159064930") is not None


def test_import_tpch_liveboard_skipped():
    """Liveboard TML files are not tables/worksheets and should produce no models."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/tpch_liveboard.liveboard.tml")
    assert len(graph.models) == 0
