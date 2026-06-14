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


# =============================================================================
# TML MODEL OBJECT (export_schema_version v2)
# =============================================================================


def test_import_thoughtspot_model():
    """Parse a first-class TML Model object (model_tables + columns + nested joins)."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/sales.model.tml")

    assert "sales_model" in graph.models
    model = graph.models["sales_model"]

    # Nested model_tables joins produce a joined SQL and relationships.
    assert model.sql is not None
    assert "JOIN customers" in model.sql
    assert "JOIN regions" in model.sql
    # Composite ON predicate from the regions join is preserved.
    assert "country_code" in model.sql

    relationships = {rel.name: rel for rel in model.relationships}
    # cardinality: MANY_TO_ONE
    assert relationships["customers"].type == "many_to_one"
    assert relationships["customers"].foreign_key == "customer_id"
    assert relationships["customers"].primary_key == "id"
    # cardinality: ONE_TO_ONE with a composite ON predicate. Sidemantic treats
    # one_to_one (like one_to_many) as an edge where the related model owns the
    # foreign key and the local model owns the primary key, so the parsed
    # source-side FK/PK are stored swapped. Both key pairs from the composite
    # predicate are preserved.
    assert relationships["regions"].type == "one_to_one"
    assert relationships["regions"].foreign_key == ["id", "country_code"]
    assert relationships["regions"].primary_key == ["region_id", "country_code"]

    # A joined model becomes a derived table. Its `SELECT *` is rewritten into an
    # explicit projection that aliases each inner `table.column` to a stable,
    # unqualified output name so the columns stay in scope when queried.
    assert "SELECT *" not in model.sql
    assert "sales.id AS sales__id" in model.sql
    assert "customers.name AS customers__name" in model.sql

    # Dimensions reference the unqualified aliased output names, not inner
    # `table.column` qualifiers that are out of scope in the derived subquery.
    order_id = model.get_dimension("order_id")
    assert order_id is not None
    assert order_id.sql == "sales__id"
    assert order_id.description == "Order identifier"

    order_date = model.get_dimension("order_date")
    assert order_date is not None
    assert order_date.type == "time"
    assert order_date.granularity == "day"
    assert order_date.label == "Order Date"

    is_active = model.get_dimension("is_active")
    assert is_active is not None
    assert is_active.type == "boolean"

    # Column from a joined table resolves to that table's aliased output name.
    customer_name = model.get_dimension("customer_name")
    assert customer_name is not None
    assert customer_name.sql == "customers__name"
    assert customer_name.label == "Customer"

    region_name = model.get_dimension("region_name")
    assert region_name is not None
    assert region_name.sql == "regions__name"

    # Simple aggregated measure.
    gross_revenue = model.get_metric("gross_revenue")
    assert gross_revenue is not None
    assert gross_revenue.agg == "sum"
    assert gross_revenue.format == "$#,##0.00"
    assert gross_revenue.sql == "sales__gross_revenue"

    # Formula-backed measures.
    net_revenue = model.get_metric("net_revenue")
    assert net_revenue is not None
    assert net_revenue.agg == "sum"
    assert "gross_revenue" in (net_revenue.sql or "")

    avg_order_value = model.get_metric("avg_order_value")
    assert avg_order_value is not None
    assert avg_order_value.agg == "avg"
    assert "/" in (avg_order_value.sql or "")

    # Aggregation coverage.
    assert model.get_metric("distinct_customers").agg == "count_distinct"
    assert model.get_metric("order_count").agg == "count"
    assert model.get_metric("min_order_value").agg == "min"
    assert model.get_metric("max_order_value").agg == "max"
    assert model.get_metric("median_order_value").agg == "median"

    # Unsupported aggregation falls back to a derived metric.
    revenue_stddev = model.get_metric("revenue_stddev")
    assert revenue_stddev is not None
    assert revenue_stddev.type == "derived"
    assert "STDDEV" in (revenue_stddev.sql or "")


def test_import_thoughtspot_model_alias():
    """Parse a TML Model with id-based tables and an aliased (role-playing) join."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_alias.model.tml")

    assert "orders_model" in graph.models
    model = graph.models["orders_model"]

    # The alias `ship_country` is the role identifier and is preserved as the
    # join relation (`countries AS ship_country`) and relationship name, instead
    # of being resolved away to the backing `countries` table.
    assert model.sql is not None
    assert "JOIN countries AS ship_country" in model.sql

    relationships = {rel.name: rel for rel in model.relationships}
    assert "ship_country" in relationships
    assert relationships["ship_country"].type == "many_to_one"
    assert relationships["ship_country"].foreign_key == "ship_country_id"
    assert relationships["ship_country"].primary_key == "id"

    ship_country = model.get_dimension("ship_country_name")
    assert ship_country is not None
    assert ship_country.sql == "ship_country__name"

    # `column_id` paths that use the table `name` (even when an `id` exists)
    # keep their table qualifier instead of collapsing to a bare column; the
    # qualifier is carried through to the aliased output name.
    order_id = model.get_dimension("order_id")
    assert order_id is not None
    assert order_id.sql == "orders__id"

    amount = model.get_metric("amount")
    assert amount is not None
    assert amount.agg == "sum"
    assert amount.sql == "orders__amount"


def test_thoughtspot_joined_model_is_queryable():
    """A joined Model TML compiles to SQL that executes (columns stay in scope).

    Regression: joined models were exported as `FROM (SELECT * FROM sales JOIN ...) AS t`
    while dimensions/metrics kept inner qualifiers like `sales.gross_revenue`, so a
    normal query produced `SELECT sales.gross_revenue FROM (...) AS t` and failed with
    "table sales not found".
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/sales.model.tml")

    layer = SemanticLayer()
    for model in graph.models.values():
        layer.add_model(model)

    con = duckdb.connect()
    con.execute(
        "CREATE TABLE sales (id INT, order_date DATE, is_active BOOL, gross_revenue DOUBLE, "
        "discount DOUBLE, order_count INT, customer_id INT, region_id INT, country_code VARCHAR)"
    )
    con.execute(
        "INSERT INTO sales VALUES (1, DATE '2024-01-01', true, 100.0, 10.0, 2, 5, 7, 'US'), "
        "(2, DATE '2024-01-01', true, 50.0, 5.0, 1, 5, 7, 'US')"
    )
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")
    con.execute("CREATE TABLE regions (id INT, country_code VARCHAR, name VARCHAR)")
    con.execute("INSERT INTO regions VALUES (7, 'US', 'West')")

    sql = layer.compile(
        metrics=["sales_model.gross_revenue", "sales_model.net_revenue", "sales_model.order_count"],
        dimensions=["sales_model.order_date", "sales_model.customer_name", "sales_model.region_name"],
    )
    rows = con.execute(sql).fetchall()
    assert rows == [(__import__("datetime").date(2024, 1, 1), "Acme", "West", 150.0, 135.0, 2)]


def test_thoughtspot_role_playing_joins_stay_distinct():
    """Two aliases backed by the same table become distinct role-playing joins.

    Regression: resolving `with:` aliases to the backing table name collapsed
    `ship_country` and `bill_country` (both backed by `countries`) into a single
    ambiguous `countries` join/relationship.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/role_playing.model.tml")
    model = graph.models["shipments_model"]

    # Both aliases survive as distinct, aliased joins and relationships.
    assert "JOIN countries AS ship_country" in model.sql
    assert "JOIN countries AS bill_country" in model.sql
    rel_names = {rel.name for rel in model.relationships}
    assert {"ship_country", "bill_country"} <= rel_names
    assert model.get_dimension("ship_country_name").sql == "ship_country__name"
    assert model.get_dimension("bill_country_name").sql == "bill_country__name"

    layer = SemanticLayer()
    for m in graph.models.values():
        layer.add_model(m)

    con = duckdb.connect()
    con.execute("CREATE TABLE orders (id INT, amount DOUBLE, ship_country_id INT, bill_country_id INT)")
    con.execute("INSERT INTO orders VALUES (1, 100.0, 7, 8)")
    con.execute("CREATE TABLE countries (id INT, name VARCHAR)")
    con.execute("INSERT INTO countries VALUES (7, 'US'), (8, 'CA')")

    sql = layer.compile(
        metrics=["shipments_model.amount"],
        dimensions=["shipments_model.ship_country_name", "shipments_model.bill_country_name"],
    )
    rows = con.execute(sql).fetchall()
    # The two roles resolve to different countries (US shipped, CA billed).
    assert rows == [("US", "CA", 100.0)]


def test_thoughtspot_unqualified_formula_ref_is_queryable():
    """A joined-model formula using an unqualified column ref stays queryable.

    Regression: ThoughtSpot formulas often use the unqualified reference form,
    e.g. `[gross_revenue] - [sales::discount]`. The derived projection exposed
    `sales.gross_revenue AS sales__gross_revenue`, but the metric SQL kept the
    bare `gross_revenue`, so a normal query failed with
    `Referenced column "gross_revenue" not found`. The bare ref must resolve to
    the projected output alias.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_unqualified_formula.model.tml")
    model = graph.models["sales_model"]

    # The unqualified `gross_revenue` ref resolves to the projected output alias,
    # and the qualified `sales::discount` ref to its alias.
    net_revenue = model.get_metric("net_revenue")
    assert net_revenue is not None
    assert net_revenue.sql == "sales__gross_revenue - sales__discount"

    # A string literal that matches a column name (`'status'`) must NOT be
    # rewritten; only the bare column reference `status` is qualified.
    is_open = model.get_dimension("is_open")
    assert is_open is not None
    assert is_open.sql == "sales__status = 'status'"

    layer = SemanticLayer()
    for m in graph.models.values():
        layer.add_model(m)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, gross_revenue DOUBLE, discount DOUBLE, customer_id INT, status VARCHAR)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 10.0, 5, 'status'), (2, 50.0, 5.0, 5, 'closed')")
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")

    sql = layer.compile(
        metrics=["sales_model.gross_revenue", "sales_model.net_revenue"],
        dimensions=["sales_model.customer_name"],
    )
    rows = con.execute(sql).fetchall()
    # gross_revenue = 150, net_revenue = (100-10) + (50-5) = 135
    assert rows == [("Acme", 150.0, 135.0)]


def test_thoughtspot_aliased_base_table_is_queryable():
    """A joined model whose base/source table is aliased stays queryable.

    Regression: aliases were applied only to the joined `right` relation, so a
    base table declared as `name: orders, alias: o` (with `column_id: o::amount`
    and an `on` clause using `[o::id]`) emitted `FROM orders` while the columns
    referenced `o.*`, failing with `Referenced table "o" not found`.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_base_alias.model.tml")
    model = graph.models["orders_model"]

    # The base table is aliased in the FROM clause so the `o` qualifier resolves.
    assert model.sql is not None
    assert "FROM orders AS o" in model.sql
    # Base-table columns are projected under stable aliases keyed on the alias.
    assert model.get_dimension("order_id").sql == "o__id"
    assert model.get_metric("amount").sql == "o__amount"

    layer = SemanticLayer()
    for m in graph.models.values():
        layer.add_model(m)

    con = duckdb.connect()
    con.execute("CREATE TABLE orders (id INT, amount DOUBLE, customer_id INT)")
    con.execute("INSERT INTO orders VALUES (1, 100.0, 5), (2, 25.0, 5)")
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")

    sql = layer.compile(
        metrics=["orders_model.amount"],
        dimensions=["orders_model.customer_name"],
    )
    rows = con.execute(sql).fetchall()
    assert rows == [("Acme", 125.0)]


def test_thoughtspot_single_aliased_table_is_queryable():
    """A single (join-less) aliased table stays queryable.

    Regression: `model_tables` with one entry `name: orders, alias: o` and
    columns like `o::amount` converted fields to `o.amount` but emitted
    `table=orders` with no SQL, so the compiled query selected `o.amount` from
    `orders` with no `o` alias in scope. The base table must be aliased.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_single_table_alias.model.tml")
    model = graph.models["orders_model"]

    # The alias is in scope: SQL wraps the single table as `orders AS o`.
    assert model.sql is not None
    assert "FROM orders AS o" in model.sql
    assert model.get_metric("amount").sql == "o__amount"

    layer = SemanticLayer()
    for m in graph.models.values():
        layer.add_model(m)

    con = duckdb.connect()
    con.execute("CREATE TABLE orders (id INT, amount DOUBLE)")
    con.execute("INSERT INTO orders VALUES (1, 100.0), (2, 25.0)")

    sql = layer.compile(metrics=["orders_model.amount"], dimensions=["orders_model.order_id"])
    rows = sorted(con.execute(sql).fetchall())
    assert rows == [(1, 100.0), (2, 25.0)]


def test_thoughtspot_renamed_column_formula_ref_is_queryable():
    """A formula referencing a TML column whose backing DB column differs stays queryable.

    Regression: a column `gross_revenue` mapped to `column_id: sales::gross_amt`
    is projected as `sales.gross_amt AS sales__gross_amt`, but the bare-reference
    map was keyed only on DB column names. A formula `[gross_revenue] - [discount]`
    kept the bare model names `gross_revenue`/`discount`, which are out of scope in
    the derived subquery, so a normal query failed. The bare model names must
    resolve to their projected output aliases.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_renamed_formula.model.tml")
    model = graph.models["sales_model"]

    # The formula's bare model-name refs resolve to the projected DB-column aliases.
    net_revenue = model.get_metric("net_revenue")
    assert net_revenue is not None
    assert net_revenue.sql == "sales__gross_amt - sales__disc_amt"

    layer = SemanticLayer()
    for m in graph.models.values():
        layer.add_model(m)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, gross_amt DOUBLE, disc_amt DOUBLE, customer_id INT)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 10.0, 5), (2, 50.0, 5.0, 5)")
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")

    sql = layer.compile(
        metrics=["sales_model.gross_revenue", "sales_model.net_revenue"],
        dimensions=["sales_model.customer_name"],
    )
    rows = con.execute(sql).fetchall()
    # gross_revenue = 150, net_revenue = (100-10) + (50-5) = 135
    assert rows == [("Acme", 150.0, 135.0)]


def test_thoughtspot_relationship_fk_is_projected_for_cross_model_query():
    """A relationship foreign key is exposed even when it is not also a column.

    Regression: the derived projection only emitted columns referenced by
    dimensions/metrics plus the primary key. When this model joined a separately
    loaded related model, the SQL generator selected the relationship's foreign
    key (e.g. `region_id`) as a bare column from the derived subquery, but it was
    never projected, so the cross-model query failed with "Column region_id ...
    cannot be referenced before it is defined".
    """
    import duckdb

    from sidemantic.core.dimension import Dimension
    from sidemantic.core.model import Model

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_relationship_fk.model.tml")
    sales_model = graph.models["sales_model"]

    # The foreign key is projected from the base table under its bare name even
    # though no dimension/metric references it.
    assert sales_model.sql is not None
    assert "sales.region_id AS region_id" in sales_model.sql
    rel = {r.name: r for r in sales_model.relationships}["regions"]
    assert rel.foreign_key == "region_id"

    # A separately loaded `regions` model joined via the parsed relationship.
    regions = Model(
        name="regions",
        table="regions",
        primary_key="id",
        dimensions=[Dimension(name="name", type="categorical", sql="name")],
    )

    layer = SemanticLayer()
    layer.add_model(sales_model)
    layer.add_model(regions)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, amount DOUBLE, region_id INT)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 7), (2, 25.0, 7)")
    con.execute("CREATE TABLE regions (id INT, name VARCHAR)")
    con.execute("INSERT INTO regions VALUES (7, 'West')")

    sql = layer.compile(metrics=["sales_model.amount"], dimensions=["regions.name"])
    rows = con.execute(sql).fetchall()
    assert rows == [("West", 125.0)]


def test_thoughtspot_single_table_renamed_formula_ref_is_queryable():
    """A join-less model's formula referencing a renamed column stays queryable.

    Regression: the renamed-column rewrite only ran on the derived (joined) SQL
    path. For a single-table model (`sql is None`), a formula like
    `[gross_revenue] - [discount]` kept the bare model names, so the compiled
    `SELECT gross_revenue - discount FROM sales` failed because the table only has
    the backing columns `gross_amt`/`disc_amt`.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_single_table_renamed_formula.model.tml")
    model = graph.models["sales_model"]

    # No join, so the model queries the base table directly; the formula's bare
    # model-name refs are rewritten to the backing DB columns.
    assert model.sql is None
    assert model.table == "sales"
    assert model.get_metric("net_revenue").sql == "gross_amt - disc_amt"

    layer = SemanticLayer()
    layer.add_model(model)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, gross_amt DOUBLE, disc_amt DOUBLE)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 10.0), (2, 50.0, 5.0)")

    sql = layer.compile(
        metrics=["sales_model.gross_revenue", "sales_model.net_revenue"],
        dimensions=["sales_model.order_id"],
    )
    rows = sorted(con.execute(sql).fetchall())
    # net_revenue = gross_amt - disc_amt
    assert rows == [(1, 100.0, 90.0), (2, 50.0, 45.0)]


def test_thoughtspot_one_to_many_join_keys_match_direction():
    """A `one_to_many` join maps the foreign key to the related (child) model.

    Regression: keys were assigned with the `many_to_one` convention (foreign key
    on the local/source side). For a `one_to_many` join `[customers::id] =
    [orders::customer_id]`, Sidemantic expects `foreign_key` on the related model
    (`orders.customer_id`) and `primary_key` on the local model (`customers.id`),
    so cross-model queries joined the wrong columns before this fix.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_one_to_many.model.tml")
    model = graph.models["customers_model"]

    rel = {r.name: r for r in model.relationships}["orders"]
    assert rel.type == "one_to_many"
    # Related (child) model holds the FK; local model holds the PK.
    assert rel.foreign_key == "customer_id"
    assert rel.primary_key == "id"

    layer = SemanticLayer()
    for m in graph.models.values():
        layer.add_model(m)

    con = duckdb.connect()
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")
    con.execute("CREATE TABLE orders (id INT, customer_id INT, amount DOUBLE)")
    con.execute("INSERT INTO orders VALUES (1, 5, 100.0), (2, 5, 25.0)")

    sql = layer.compile(
        metrics=["customers_model.order_amount"],
        dimensions=["customers_model.customer_name"],
    )
    rows = con.execute(sql).fetchall()
    assert rows == [("Acme", 125.0)]


def test_thoughtspot_one_to_one_join_keys_match_direction():
    """A `one_to_one` join with a source-side FK joins correctly cross-model.

    Regression: `one_to_one` keys were stored in the `many_to_one` positions
    (FK on the source/local side). Sidemantic reads `one_to_one` like a has-one
    edge where the related model owns the foreign key, so a cross-model query with
    a separately loaded `regions` model joined `regions.region_id` to
    `sales_model.id` and failed instead of joining `regions.id` to
    `sales_model.region_id`.
    """
    import duckdb

    from sidemantic.core.dimension import Dimension
    from sidemantic.core.model import Model

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_one_to_one.model.tml")
    sales_model = graph.models["sales_model"]

    rel = {r.name: r for r in sales_model.relationships}["regions"]
    assert rel.type == "one_to_one"
    # Related model owns the FK (regions.id), local model owns the PK (region_id).
    assert rel.foreign_key == "id"
    assert rel.primary_key == "region_id"

    # A separately loaded `regions` model joined via the parsed relationship.
    regions = Model(
        name="regions",
        table="regions",
        primary_key="id",
        dimensions=[Dimension(name="zone", type="categorical", sql="zone")],
    )

    layer = SemanticLayer()
    layer.add_model(sales_model)
    layer.add_model(regions)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, amount DOUBLE, region_id INT)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 7), (2, 25.0, 7)")
    con.execute("CREATE TABLE regions (id INT, name VARCHAR, zone VARCHAR)")
    con.execute("INSERT INTO regions VALUES (7, 'West', 'PACIFIC')")

    sql = layer.compile(metrics=["sales_model.amount"], dimensions=["regions.zone"])
    rows = con.execute(sql).fetchall()
    assert rows == [("PACIFIC", 125.0)]


def test_thoughtspot_non_id_primary_key_is_queryable():
    """A joined model whose key is not literally `id` stays queryable.

    Regression: `primary_key` defaulted to `id`, and the derived projection
    injected `orders.id AS id`. The SQL generator always selects the primary key
    from derived models, so even a basic aggregate failed with
    `Table "orders" does not have a column named "id"`. The key must be inferred
    from a real base-table column (here `order_key`).
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_non_id_key.model.tml")
    model = graph.models["orders_model"]

    # The primary key is inferred from a real base-table column, not the default.
    assert model.primary_key == "order_key"
    assert model.sql is not None
    assert "orders.order_key AS order_key" in model.sql
    assert "orders.id AS id" not in model.sql

    layer = SemanticLayer()
    layer.add_model(model)

    con = duckdb.connect()
    con.execute("CREATE TABLE orders (order_key INT, amount DOUBLE, customer_id INT)")
    con.execute("INSERT INTO orders VALUES (1, 100.0, 5), (2, 25.0, 5)")
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")

    sql = layer.compile(metrics=["orders_model.amount"], dimensions=["orders_model.customer_name"])
    rows = con.execute(sql).fetchall()
    assert rows == [("Acme", 125.0)]


def test_thoughtspot_composite_join_keys_are_preserved():
    """A composite-key join keeps every key pair instead of dropping all but one.

    Regression: only the first `left = right` pair of a composite ON predicate was
    stored, so `[sales::region_id] = [regions::id] AND [sales::country_code] =
    [regions::country_code]` became `region_id -> id` only. Cross-model joins then
    used a truncated key and could mis-join rows.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_composite_key.model.tml")
    model = graph.models["sales_model"]

    rel = {r.name: r for r in model.relationships}["regions"]
    assert rel.type == "many_to_one"
    # Both key pairs are preserved as composite lists.
    assert rel.foreign_key == ["region_id", "country_code"]
    assert rel.primary_key == ["id", "country_code"]

    # Both join columns are projected by the derived subquery so a composite
    # cross-model join stays in scope.
    assert model.sql is not None
    assert "sales.region_id AS region_id" in model.sql
    assert "sales.country_code AS country_code" in model.sql

    # The model still compiles and runs standalone.
    layer = SemanticLayer()
    layer.add_model(model)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, amount DOUBLE, region_id INT, country_code VARCHAR)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 7, 'US'), (2, 25.0, 7, 'US')")
    con.execute("CREATE TABLE regions (id INT, country_code VARCHAR, name VARCHAR)")
    con.execute("INSERT INTO regions VALUES (7, 'US', 'West')")

    sql = layer.compile(metrics=["sales_model.amount"], dimensions=["sales_model.region_name"])
    rows = con.execute(sql).fetchall()
    assert rows == [("West", 125.0)]


def test_thoughtspot_nested_formula_ref_is_inlined_and_queryable():
    """A formula referencing another formula inlines the nested expression.

    Regression: a formula like `[net_revenue] / [gross_revenue]` (where
    `net_revenue` is itself a formula) kept the bare `net_revenue` token, which the
    derived subquery never projected, so the query failed with
    `Referenced column "net_revenue" not found`. The nested formula must be
    expanded inline so the expression resolves to physical columns.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_nested_formula.model.tml")
    model = graph.models["sales_model"]

    # The nested `net_revenue` formula is inlined into `margin`.
    assert model.get_metric("net_revenue").sql == "sales__gross_revenue - sales__discount"
    assert model.get_metric("margin").sql == "(sales__gross_revenue - sales__discount) / sales__gross_revenue"

    layer = SemanticLayer()
    layer.add_model(model)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, gross_revenue DOUBLE, discount DOUBLE, customer_id INT)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 10.0, 5)")
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")

    sql = layer.compile(metrics=["sales_model.margin"], dimensions=["sales_model.customer_name"])
    rows = con.execute(sql).fetchall()
    # margin = (100 - 10) / 100 = 0.9
    assert rows == [("Acme", 0.9)]


def test_thoughtspot_formula_ref_prefers_tml_field_over_physical_name():
    """A bare formula ref resolves to the TML field, not a colliding physical name.

    Regression: physical column names were recorded first, so adding TML field
    names could mark a valid reference ambiguous and drop it. With
    `gross_revenue -> sales::amount` and `amount -> sales::cost`, the formula
    `[amount] + [gross_revenue]` must resolve the bare `amount` to `sales__cost`
    (the TML field), not be dropped because `amount` also names a physical column.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_name_collision.model.tml")
    model = graph.models["sales_model"]

    # The bare `amount` ref resolves to the TML field's backing column (cost),
    # not the physical `amount` column.
    assert model.get_metric("total").sql == "sales__cost + sales__amount"

    layer = SemanticLayer()
    layer.add_model(model)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, amount DOUBLE, cost DOUBLE, customer_id INT)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 30.0, 5)")
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")

    sql = layer.compile(metrics=["sales_model.total"], dimensions=["sales_model.customer_name"])
    rows = con.execute(sql).fetchall()
    # total = cost (30) + amount (100) = 130
    assert rows == [("Acme", 130.0)]


def test_thoughtspot_renamed_id_key_uses_backing_column():
    """A semantic column named `id` backed by another column resolves to it.

    Regression: `_infer_model_primary_key` returned the semantic name `id` even
    when the column mapped to a different physical column (`column_id:
    orders::order_key`). The derived projection then injected `orders.id AS id`,
    failing because the base table only has `order_key`.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_renamed_id_key.model.tml")
    model = graph.models["orders_model"]

    # The primary key resolves to the backing physical column, not the name `id`.
    assert model.primary_key == "order_key"
    assert model.sql is not None
    assert "orders.order_key AS order_key" in model.sql
    assert "orders.id AS id" not in model.sql

    layer = SemanticLayer()
    layer.add_model(model)

    con = duckdb.connect()
    con.execute("CREATE TABLE orders (order_key INT, amount DOUBLE, customer_id INT)")
    con.execute("INSERT INTO orders VALUES (1, 100.0, 5), (2, 25.0, 5)")
    con.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    con.execute("INSERT INTO customers VALUES (5, 'Acme')")

    sql = layer.compile(metrics=["orders_model.amount"], dimensions=["orders_model.customer_name"])
    rows = con.execute(sql).fetchall()
    assert rows == [("Acme", 125.0)]


def test_thoughtspot_joined_id_dimension_is_not_used_as_primary_key():
    """A joined-table column named `id` is not treated as the base model's key.

    Regression: `_infer_model_primary_key` returned the backing column of the
    first dimension named `id` even when it came from a joined table
    (`customers::id`). The derived projection then emitted `orders.id AS id`,
    failing because the base `orders` table has no `id` (its key is `order_key`).
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_joined_id_key.model.tml")
    model = graph.models["orders_model"]

    # The joined-table id is skipped; the base-table column is used as the key.
    assert model.primary_key == "order_key"
    assert model.sql is not None
    assert "orders.order_key AS order_key" in model.sql
    assert "orders.id AS id" not in model.sql

    layer = SemanticLayer()
    layer.add_model(model)

    con = duckdb.connect()
    con.execute("CREATE TABLE orders (order_key INT, amount DOUBLE, customer_id INT)")
    con.execute("INSERT INTO orders VALUES (1, 100.0, 5), (2, 25.0, 5)")
    con.execute("CREATE TABLE customers (id INT)")
    con.execute("INSERT INTO customers VALUES (5)")

    sql = layer.compile(metrics=["orders_model.amount"], dimensions=["orders_model.order_key"])
    rows = sorted(con.execute(sql).fetchall())
    assert rows == [(1, 100.0), (2, 25.0)]


def test_thoughtspot_non_equi_join_predicate_is_not_a_relationship_key():
    """A range predicate in a join ON clause is not mistaken for an equality key.

    Regression: the composite-key extractor paired every two consecutive refs, so
    `[sales::region_id] = [regions::id] AND [sales::date] BETWEEN
    [regions::start_date] AND [regions::end_date]` produced bogus extra keys
    (`date -> start_date`). Only the real equality conjunct should become the
    relationship key; the range predicate stays in the join SQL but is ignored as
    a key.
    """
    import duckdb

    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/model_non_equi_join.model.tml")
    model = graph.models["sales_model"]

    rel = {r.name: r for r in model.relationships}["regions"]
    # Only the equality key pair is captured; the BETWEEN refs are not keys.
    assert rel.foreign_key == "region_id"
    assert rel.primary_key == "id"

    # The range predicate is preserved in the join SQL, but its columns are not
    # projected as spurious key columns.
    assert model.sql is not None
    assert "BETWEEN regions.start_date AND regions.end_date" in model.sql
    assert "AS start_date" not in model.sql
    assert "AS date" not in model.sql

    # The model still compiles and runs.
    layer = SemanticLayer()
    layer.add_model(model)

    con = duckdb.connect()
    con.execute("CREATE TABLE sales (id INT, amount DOUBLE, region_id INT, date DATE)")
    con.execute("INSERT INTO sales VALUES (1, 100.0, 7, DATE '2024-06-15'), (2, 25.0, 7, DATE '2024-06-20')")
    con.execute("CREATE TABLE regions (id INT, name VARCHAR, start_date DATE, end_date DATE)")
    con.execute("INSERT INTO regions VALUES (7, 'West', DATE '2024-06-01', DATE '2024-06-30')")

    sql = layer.compile(metrics=["sales_model.amount"], dimensions=["sales_model.region_name"])
    rows = con.execute(sql).fetchall()
    assert rows == [("West", 125.0)]


def test_thoughtspot_model_auto_detect_loader():
    """A model + model_tables + columns YAML file is auto-detected as ThoughtSpot."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tml_file = Path(tmpdir) / "sales.model.yaml"
        tml_file.write_text(
            """
model:
  name: sales_model
  description: Sales model
  model_tables:
    - name: sales
      fqn: ANALYTICS.PUBLIC.sales
  columns:
    - name: order_id
      column_id: sales::id
      properties:
        column_type: ATTRIBUTE
    - name: amount
      column_id: sales::amount
      properties:
        column_type: MEASURE
        aggregation: SUM
"""
        )

        layer = SemanticLayer()
        load_from_directory(layer, tmpdir)

        assert "sales_model" in layer.graph.models
        model = layer.graph.models["sales_model"]
        assert model.get_metric("amount").agg == "sum"


def test_thoughtspot_legacy_model_key_still_worksheet():
    """Legacy worksheet content nested under `model:` still parses as a worksheet."""
    adapter = ThoughtSpotAdapter()
    tml_def = {
        "model": {
            "name": "legacy_sheet",
            "tables": [{"name": "orders"}],
            "worksheet_columns": [
                {
                    "name": "amount",
                    "column_id": "orders::amount",
                    "properties": {"column_type": "MEASURE", "aggregation": "SUM"},
                },
            ],
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tml", delete=False) as f:
        yaml.safe_dump(tml_def, f, sort_keys=False)
        temp_path = Path(f.name)

    try:
        graph = adapter.parse(temp_path)
        assert "legacy_sheet" in graph.models
        model = graph.models["legacy_sheet"]
        assert model.get_metric("amount").agg == "sum"
    finally:
        temp_path.unlink(missing_ok=True)
