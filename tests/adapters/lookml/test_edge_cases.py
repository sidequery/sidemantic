"""Tests for LookML adapter edge cases.

These tests verify handling of complex LookML patterns found in real-world deployments,
inspired by fixtures from joshtemple/lkml, node-lookml-parser, and lookml-tools.
"""

from pathlib import Path

import pytest

from sidemantic.adapters.lookml import LookMLAdapter

# =============================================================================
# EXTENDS AND REFINEMENTS TESTS
# =============================================================================


def test_lookml_extends_base_view():
    """Test parsing base views for extension."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    # Check base_entity was parsed
    assert "base_entity" in graph.models
    base = graph.get_model("base_entity")

    # Check base dimensions
    assert base.get_dimension("id") is not None
    assert base.get_dimension("name") is not None
    assert base.get_dimension("is_active") is not None

    # Check time dimensions
    assert base.get_dimension("created_date") is not None
    assert base.get_dimension("created_week") is not None

    # Check measure
    count_measure = base.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"


def test_lookml_extends_extended_view():
    """Test parsing views that extend other views."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    # Note: sidemantic doesn't currently resolve extends - it just parses each view
    # The customers_extended view only has its own dimensions, not inherited ones
    assert "customers_extended" in graph.models
    extended = graph.get_model("customers_extended")

    # Check customer-specific dimensions
    assert extended.get_dimension("email") is not None
    assert extended.get_dimension("tier") is not None
    assert extended.get_dimension("lifetime_value") is not None

    # Check measures
    assert extended.get_metric("total_ltv") is not None
    assert extended.get_metric("avg_ltv") is not None


def test_lookml_refinement_syntax():
    """Test parsing refinement syntax (+view_name)."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    # Refinement views are parsed as separate models
    # The +base_entity becomes a model named "+base_entity"
    assert "+base_entity" in graph.models
    refinement = graph.get_model("+base_entity")

    # Should have the added dimension
    assert refinement.get_dimension("refined_field") is not None


def test_lookml_abstract_view():
    """Test parsing abstract views (extension: required)."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    assert "abstract_metrics" in graph.models
    abstract = graph.get_model("abstract_metrics")

    # Abstract view has only measures
    assert len(abstract.dimensions) == 0
    assert len(abstract.metrics) == 5

    # Check all measures exist
    assert abstract.get_metric("record_count") is not None
    assert abstract.get_metric("sum_amount") is not None
    assert abstract.get_metric("avg_amount") is not None
    assert abstract.get_metric("min_amount") is not None
    assert abstract.get_metric("max_amount") is not None


def test_lookml_concrete_extends_abstract():
    """Test parsing concrete view extending abstract."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    assert "transactions" in graph.models
    transactions = graph.get_model("transactions")

    # Check dimensions
    assert transactions.get_dimension("id") is not None
    assert transactions.get_dimension("amount") is not None
    assert transactions.get_dimension("status") is not None

    # Check time dimensions
    assert transactions.get_dimension("transaction_time") is not None
    assert transactions.get_dimension("transaction_date") is not None


# =============================================================================
# LIQUID TEMPLATING TESTS
# =============================================================================


def test_lookml_liquid_case_dimension():
    """Test parsing case dimensions (similar to CASE WHEN)."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_liquid.lkml"))

    assert "dynamic_sales" in graph.models
    sales = graph.get_model("dynamic_sales")

    # Check case dimension exists
    region_group = sales.get_dimension("region_group")
    assert region_group is not None
    assert region_group.type == "categorical"


def test_lookml_liquid_html_dimension():
    """Test parsing dimensions with HTML formatting."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_liquid.lkml"))

    sales = graph.get_model("dynamic_sales")
    status_dim = sales.get_dimension("status")
    assert status_dim is not None


def test_lookml_liquid_dimension_reference_in_sql():
    """Test parsing dimension references in SQL."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_liquid.lkml"))

    sales = graph.get_model("dynamic_sales")

    # days_since_sale references sale_date dimension
    days_dim = sales.get_dimension("days_since_sale")
    assert days_dim is not None
    # The SQL should contain the reference (may be resolved or not)
    assert days_dim.sql is not None


def test_lookml_value_formats():
    """Test parsing various value_format patterns."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_liquid.lkml"))

    assert "format_examples" in graph.models
    formats = graph.get_model("format_examples")

    # Check dimensions with value formats exist
    assert formats.get_dimension("percentage_value") is not None
    assert formats.get_dimension("currency_value") is not None

    # Check measures
    assert formats.get_metric("sum_value") is not None
    assert formats.get_metric("sum_currency") is not None
    assert formats.get_metric("formatted_total") is not None


def test_lookml_derived_table():
    """Test parsing derived tables."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_liquid.lkml"))

    assert "templated_orders" in graph.models
    orders = graph.get_model("templated_orders")

    # Derived table should have SQL, no table name
    assert orders.table is None
    assert orders.sql is not None
    assert "SELECT" in orders.sql.upper()


# =============================================================================
# COMPLEX FILTER TESTS
# =============================================================================


def test_lookml_numeric_comparison_filters():
    """Test parsing numeric comparison operators in filters."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # Greater than filter
    high_value = filters_view.get_metric("high_value_count")
    assert high_value is not None
    assert high_value.filters is not None
    assert any("> 1000" in f or ">1000" in f for f in high_value.filters)

    # Less than filter
    low_value = filters_view.get_metric("low_value_count")
    assert low_value is not None
    assert low_value.filters is not None
    assert any("< 100" in f or "<100" in f for f in low_value.filters)

    # Not equal filter
    non_zero = filters_view.get_metric("non_zero_count")
    assert non_zero is not None
    assert non_zero.filters is not None
    assert any("!= 0" in f or "!=0" in f or "<>" in f for f in non_zero.filters)


def test_lookml_string_filters():
    """Test parsing string filters."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # Simple string match
    completed = filters_view.get_metric("completed_count")
    assert completed is not None
    assert completed.filters is not None
    # Filter should contain 'completed'
    assert any("completed" in f.lower() for f in completed.filters)


def test_lookml_boolean_filters():
    """Test parsing yes/no boolean filters."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # Yes filter
    premium = filters_view.get_metric("premium_count")
    assert premium is not None
    assert premium.filters is not None
    # Should contain true or yes
    assert any("true" in f.lower() or "yes" in f.lower() for f in premium.filters)

    # No filter
    non_premium = filters_view.get_metric("non_premium_count")
    assert non_premium is not None
    assert non_premium.filters is not None
    assert any("false" in f.lower() or "no" in f.lower() for f in non_premium.filters)


def test_lookml_multiple_filters():
    """Test parsing measures with multiple filters."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # Measure with multiple filters (AND condition)
    multi_filter = filters_view.get_metric("high_value_premium")
    assert multi_filter is not None
    assert multi_filter.filters is not None
    assert len(multi_filter.filters) >= 2


def test_lookml_segments():
    """Test parsing filter definitions as segments."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # Check segments
    assert len(filters_view.segments) == 4
    segment_names = [s.name for s in filters_view.segments]
    assert "high_value" in segment_names
    assert "premium_segment" in segment_names
    assert "active_period" in segment_names
    assert "successful_transactions" in segment_names


# =============================================================================
# COMPLEX SQL TESTS
# =============================================================================


def test_lookml_subquery_dimension():
    """Test parsing dimensions with subqueries."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_sql.lkml"))

    complex_view = graph.get_model("complex_sql_view")

    # Check dimensions with subqueries
    rank_dim = complex_view.get_dimension("customer_order_rank")
    assert rank_dim is not None
    assert "SELECT" in rank_dim.sql.upper()

    ltv_dim = complex_view.get_dimension("customer_lifetime_value")
    assert ltv_dim is not None
    assert "SELECT" in ltv_dim.sql.upper()
    assert "SUM" in ltv_dim.sql.upper()


def test_lookml_case_expression_dimension():
    """Test parsing CASE expression dimensions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_sql.lkml"))

    complex_view = graph.get_model("complex_sql_view")

    # Check CASE dimension
    bucket_dim = complex_view.get_dimension("order_size_bucket")
    assert bucket_dim is not None
    assert "CASE" in bucket_dim.sql.upper()
    assert "WHEN" in bucket_dim.sql.upper()


def test_lookml_derived_table_cte():
    """Test parsing derived tables with CTEs."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_sql.lkml"))

    assert "customer_cohorts" in graph.models
    cohorts = graph.get_model("customer_cohorts")

    # Should have SQL with CTE
    assert cohorts.table is None
    assert cohorts.sql is not None
    assert "WITH" in cohorts.sql.upper()


def test_lookml_sql_table_name_reference():
    """Test parsing views referencing other views' SQL_TABLE_NAME."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_sql.lkml"))

    # order_facts references customer_cohorts.SQL_TABLE_NAME
    assert "order_facts" in graph.models
    order_facts = graph.get_model("order_facts")

    assert order_facts.sql is not None
    # The SQL should contain the reference
    assert "customer_cohorts" in order_facts.sql.lower()


# =============================================================================
# ACTIONS AND DRILL FIELDS TESTS
# =============================================================================


def test_lookml_links():
    """Test parsing dimensions with links."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_actions.lkml"))

    orders = graph.get_model("interactive_orders")

    # Links are parsed but not stored in sidemantic model
    # We just verify the dimension exists and is parsed correctly
    id_dim = orders.get_dimension("id")
    assert id_dim is not None

    customer_id_dim = orders.get_dimension("customer_id")
    assert customer_id_dim is not None


def test_lookml_html_formatting():
    """Test parsing dimensions with HTML formatting."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_actions.lkml"))

    orders = graph.get_model("interactive_orders")

    # HTML is parsed but stored in dimension
    amount_dim = orders.get_dimension("amount")
    assert amount_dim is not None

    status_dim = orders.get_dimension("status")
    assert status_dim is not None


def test_lookml_drill_fields():
    """Test parsing measures with drill_fields."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_actions.lkml"))

    orders = graph.get_model("interactive_orders")

    # Drill fields are parsed but not stored in sidemantic
    # We verify measures exist
    count_measure = orders.get_metric("count")
    assert count_measure is not None

    revenue_measure = orders.get_metric("total_revenue")
    assert revenue_measure is not None


def test_lookml_sets():
    """Test parsing set definitions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_actions.lkml"))

    # Sets are parsed by lkml but not currently stored in sidemantic
    # We verify the view parses correctly
    orders = graph.get_model("interactive_orders")
    assert orders is not None


def test_lookml_filtered_measures_various():
    """Test various filtered measure patterns."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_actions.lkml"))

    orders = graph.get_model("interactive_orders")

    # Check various filtered measures exist
    assert orders.get_metric("completed_orders") is not None
    assert orders.get_metric("pending_orders") is not None
    assert orders.get_metric("cancelled_orders") is not None
    assert orders.get_metric("web_orders") is not None
    assert orders.get_metric("mobile_orders") is not None


# =============================================================================
# SPECIAL TYPES TESTS
# =============================================================================


def test_lookml_tier_dimension():
    """Test parsing tier dimensions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_special_types.lkml"))

    special = graph.get_model("special_types")

    # Tier dimensions should be parsed
    age_tier = special.get_dimension("age_tier")
    assert age_tier is not None
    # Tier maps to categorical
    assert age_tier.type == "categorical"

    income_tier = special.get_dimension("income_tier")
    assert income_tier is not None


def test_lookml_case_dimension():
    """Test parsing case dimensions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_special_types.lkml"))

    special = graph.get_model("special_types")

    # Case dimensions
    segment = special.get_dimension("customer_value_segment")
    assert segment is not None
    assert segment.type == "categorical"


def test_lookml_location_dimension():
    """Test parsing location (geo) dimensions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_special_types.lkml"))

    special = graph.get_model("special_types")

    # Location dimension (type: location combines lat/lng)
    location = special.get_dimension("location")
    assert location is not None

    # Underlying lat/lng dimensions
    lat = special.get_dimension("latitude")
    assert lat is not None
    assert lat.type == "numeric"


def test_lookml_yesno_dimension():
    """Test parsing yesno dimensions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_special_types.lkml"))

    special = graph.get_model("special_types")

    # yesno dimensions
    is_active = special.get_dimension("is_active")
    assert is_active is not None
    # yesno maps to categorical
    assert is_active.type == "categorical"

    is_verified = special.get_dimension("is_verified")
    assert is_verified is not None

    has_purchases = special.get_dimension("has_purchases")
    assert has_purchases is not None


def test_lookml_json_extraction():
    """Test parsing JSON extraction dimensions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_special_types.lkml"))

    json_view = graph.get_model("json_array_types")

    # JSON extraction dimensions
    source = json_view.get_dimension("property_source")
    assert source is not None
    assert "JSON_EXTRACT" in source.sql.upper()

    browser = json_view.get_dimension("user_agent_browser")
    assert browser is not None


# =============================================================================
# EXPLORES TESTS
# =============================================================================


def test_lookml_explore_views():
    """Test parsing views that are part of explores."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # All views should be parsed
    assert "fact_orders" in graph.models
    assert "dim_customers" in graph.models
    assert "dim_products" in graph.models
    assert "dim_stores" in graph.models
    assert "dim_regions" in graph.models


def test_lookml_explore_derived_table():
    """Test parsing derived table in explore context."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # date_spine is a derived table
    assert "date_spine" in graph.models
    date_spine = graph.get_model("date_spine")

    assert date_spine.sql is not None
    assert "GENERATE_DATE_ARRAY" in date_spine.sql.upper() or "SELECT" in date_spine.sql.upper()


def test_lookml_explore_persisted_derived_table():
    """Test parsing persisted derived table."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    assert "order_daily_metrics" in graph.models
    metrics = graph.get_model("order_daily_metrics")

    assert metrics.sql is not None
    assert "GROUP BY" in metrics.sql.upper()


# =============================================================================
# KITCHEN SINK TESTS
# =============================================================================


def test_lookml_kitchen_sink_comprehensive():
    """Test the comprehensive kitchen_sink fixture covers all patterns."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/kitchen_sink.lkml"))

    # Verify all models exist
    assert "regions" in graph.models
    assert "categories" in graph.models
    assert "customers" in graph.models
    assert "products" in graph.models
    assert "orders" in graph.models
    assert "order_items" in graph.models
    assert "shipments" in graph.models
    assert "reviews" in graph.models


def test_lookml_kitchen_sink_dimension_references():
    """Test dimension reference resolution in kitchen_sink."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/kitchen_sink.lkml"))

    order_items = graph.get_model("order_items")

    # line_total references quantity, unit_price, line_discount
    line_total = order_items.get_dimension("line_total")
    assert line_total is not None
    assert line_total.sql is not None
    # The dimension references should be resolved to their SQL
    # Original: ${quantity} * ${unit_price} - ${line_discount}
    # Resolved: should contain {model}.quantity etc.
    assert "{model}" in line_total.sql


def test_lookml_kitchen_sink_measure_references():
    """Test measure-to-measure reference resolution in kitchen_sink."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/kitchen_sink.lkml"))

    orders = graph.get_model("orders")

    # delivery_rate references delivered_orders and count
    delivery_rate = orders.get_metric("delivery_rate")
    assert delivery_rate is not None
    assert delivery_rate.type == "derived"
    assert delivery_rate.sql is not None
    # Should reference the measures
    assert "delivered_orders" in delivery_rate.sql
    assert "count" in delivery_rate.sql


def test_lookml_kitchen_sink_segments():
    """Test segment parsing in kitchen_sink."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/kitchen_sink.lkml"))

    orders = graph.get_model("orders")

    # Check segments
    segment_names = [s.name for s in orders.segments]
    assert "completed" in segment_names
    assert "high_value" in segment_names
    assert "discounted" in segment_names


# =============================================================================
# FILTER PARSING TESTS
# =============================================================================


def test_lookml_filter_in_clause():
    """Test parsing comma-separated filter values as IN clause."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # pending_or_processing_count uses filters: [status: "pending,processing"]
    pending_processing = filters_view.get_metric("pending_or_processing_count")
    assert pending_processing is not None
    assert pending_processing.filters is not None
    # Should be an IN clause
    filter_str = pending_processing.filters[0]
    assert "IN" in filter_str.upper()
    assert "pending" in filter_str
    assert "processing" in filter_str


def test_lookml_filter_not_in_clause():
    """Test parsing negated comma-separated values as NOT IN clause."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # excluding_cancelled_amount uses filters: [status: "-cancelled,-refunded"]
    excluding = filters_view.get_metric("excluding_cancelled_amount")
    assert excluding is not None
    assert excluding.filters is not None
    filter_str = excluding.filters[0]
    assert "NOT IN" in filter_str.upper()
    assert "cancelled" in filter_str
    assert "refunded" in filter_str


def test_lookml_filter_single_negation():
    """Test parsing single negation filter."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # not_cancelled_count uses filters: [status: "-cancelled"]
    not_cancelled = filters_view.get_metric("not_cancelled_count")
    assert not_cancelled is not None
    assert not_cancelled.filters is not None
    filter_str = not_cancelled.filters[0]
    assert "!=" in filter_str or "NOT" in filter_str.upper()
    assert "cancelled" in filter_str


def test_lookml_filter_null():
    """Test parsing NULL filters."""
    adapter = LookMLAdapter()

    # NULL filters are in special_filter_cases view in edge_cases_filters.lkml
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))
    special = graph.get_model("special_filter_cases")

    # null_values uses filters: [nullable_field: "NULL"]
    null_values = special.get_metric("null_values")
    assert null_values is not None
    assert null_values.filters is not None
    assert "IS NULL" in null_values.filters[0].upper()

    # not_null_values uses filters: [nullable_field: "-NULL"]
    not_null = special.get_metric("not_null_values")
    assert not_null is not None
    assert not_null.filters is not None
    assert "IS NOT NULL" in not_null.filters[0].upper()


def test_lookml_filter_wildcard():
    """Test parsing wildcard/LIKE filters."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_filters.lkml"))

    filters_view = graph.get_model("filter_edge_cases")

    # a_region_count uses filters: [region: "A%"]
    a_region = filters_view.get_metric("a_region_count")
    assert a_region is not None
    assert a_region.filters is not None
    assert "LIKE" in a_region.filters[0].upper()
    assert "A%" in a_region.filters[0]


def test_lookml_filter_numeric_in_clause():
    """Test parsing numeric comma-separated filter values as IN clause."""
    import tempfile

    lkml_content = """
view: numeric_filter_test {
  sql_table_name: orders ;;

  dimension: order_id { type: number sql: ${TABLE}.order_id ;; }
  dimension: price { type: number sql: ${TABLE}.price ;; }

  measure: specific_orders {
    type: count
    filters: [order_id: "1,2,3"]
  }

  measure: specific_prices {
    type: sum
    sql: ${price} ;;
    filters: [price: "10.5,20.0,30.99"]
  }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("numeric_filter_test")

        # Check integer IN clause
        specific_orders = model.get_metric("specific_orders")
        assert specific_orders is not None
        assert specific_orders.filters is not None
        filter_str = specific_orders.filters[0]
        assert "IN" in filter_str.upper()
        # Should be unquoted integers
        assert "IN (1, 2, 3)" in filter_str

        # Check decimal IN clause
        specific_prices = model.get_metric("specific_prices")
        assert specific_prices is not None
        assert specific_prices.filters is not None
        filter_str = specific_prices.filters[0]
        assert "IN" in filter_str.upper()
        # Should be unquoted decimals
        assert "10.5" in filter_str
        assert "20.0" in filter_str


# =============================================================================
# DURATION DIMENSION GROUP TESTS
# =============================================================================


def test_lookml_duration_dimension_group():
    """Test parsing dimension_group with type: duration."""
    import tempfile

    lkml_content = """
view: duration_test {
  sql_table_name: t ;;

  dimension_group: process_time {
    type: duration
    intervals: [second, minute, hour, day]
    sql_start: ${TABLE}.started_at ;;
    sql_end: ${TABLE}.completed_at ;;
  }

  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("duration_test")

        # Check that duration dimensions were created
        dim_names = [d.name for d in model.dimensions]
        assert "process_time_seconds" in dim_names
        assert "process_time_minutes" in dim_names
        assert "process_time_hours" in dim_names
        assert "process_time_days" in dim_names

        # Check SQL contains DATE_DIFF
        seconds_dim = model.get_dimension("process_time_seconds")
        assert seconds_dim is not None
        assert "DATE_DIFF" in seconds_dim.sql.upper()
        assert "SECOND" in seconds_dim.sql.upper()


# =============================================================================
# NATIVE DERIVED TABLE (EXPLORE_SOURCE) TESTS
# =============================================================================


def test_lookml_native_derived_table():
    """Test parsing native derived tables with explore_source."""
    import tempfile

    lkml_content = """
view: native_dt_test {
  derived_table: {
    explore_source: orders {
      column: customer_id {}
      column: total_revenue { field: orders.revenue }
    }
  }

  dimension: customer_id {
    primary_key: yes
    sql: ${TABLE}.customer_id ;;
  }

  dimension: total_revenue {
    type: number
    sql: ${TABLE}.total_revenue ;;
  }

  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("native_dt_test")

        # Check model was created and has SQL (even if placeholder)
        assert model is not None
        assert model.table is None  # It's a derived table
        assert model.sql is not None


# =============================================================================
# ADDITIONAL MEASURE TYPES TESTS
# =============================================================================


def test_lookml_measure_type_median():
    """Test parsing median measure type."""
    import tempfile

    lkml_content = """
view: median_test {
  sql_table_name: t ;;

  dimension: value { type: number sql: ${TABLE}.value ;; }

  measure: median_value {
    type: median
    sql: ${value} ;;
  }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("median_test")

        median_measure = model.get_metric("median_value")
        assert median_measure is not None
        assert median_measure.agg == "median"


def test_lookml_measure_type_percentile():
    """Test parsing percentile measure type (becomes derived)."""
    import tempfile

    lkml_content = """
view: percentile_test {
  sql_table_name: t ;;

  dimension: value { type: number sql: ${TABLE}.value ;; }

  measure: p90_value {
    type: percentile
    percentile: 90
    sql: ${value} ;;
  }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("percentile_test")

        percentile_measure = model.get_metric("p90_value")
        assert percentile_measure is not None
        # Percentile is not supported as agg, so it becomes derived or None
        # The key is that it parses without error


# =============================================================================
# ADDITIONAL EDGE CASES TESTS
# =============================================================================


def test_lookml_cross_view_reference():
    """Test parsing cross-view field references (${other_view.field})."""
    import tempfile

    lkml_content = """
view: orders {
  sql_table_name: orders ;;
  dimension: id { type: number sql: ${TABLE}.id ;; }
  dimension: customer_name {
    type: string
    sql: ${customers.name} ;;
  }
  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        orders = graph.get_model("orders")

        # Cross-view references should be preserved
        customer_name = orders.get_dimension("customer_name")
        assert customer_name is not None
        assert "customers.name" in customer_name.sql


def test_lookml_recursive_dimension_references():
    """Test recursive dimension references (dim_a -> dim_b -> dim_c)."""
    import tempfile

    lkml_content = """
view: recursive_test {
  sql_table_name: t ;;

  dimension: base_amount { type: number sql: ${TABLE}.amount ;; }
  dimension: doubled { type: number sql: ${base_amount} * 2 ;; }
  dimension: quadrupled { type: number sql: ${doubled} * 2 ;; }

  measure: sum_quad { type: sum sql: ${quadrupled} ;; }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("recursive_test")

        # All dimensions should resolve back to {model}.amount
        assert "{model}.amount" in model.get_dimension("base_amount").sql
        assert "{model}.amount" in model.get_dimension("doubled").sql
        assert "{model}.amount" in model.get_dimension("quadrupled").sql
        assert "{model}.amount" in model.get_metric("sum_quad").sql


def test_lookml_special_characters_in_sql():
    """Test SQL with special characters (quotes, brackets, backticks)."""
    import tempfile

    lkml_content = """
view: special_chars {
  sql_table_name: "schema"."table" ;;

  dimension: quoted_col { type: string sql: ${TABLE}."column name" ;; }
  dimension: escaped_quote { type: string sql: CONCAT(${TABLE}.name, '''s value') ;; }

  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("special_chars")

        assert model.get_dimension("quoted_col") is not None
        # Escaped quotes should be preserved
        assert "'''" in model.get_dimension("escaped_quote").sql


def test_lookml_window_functions():
    """Test parsing window functions in dimensions."""
    import tempfile

    lkml_content = """
view: window_funcs {
  sql_table_name: orders ;;

  dimension: id { type: number sql: ${TABLE}.id ;; primary_key: yes }
  dimension: customer_id { type: number sql: ${TABLE}.customer_id ;; }
  dimension: amount { type: number sql: ${TABLE}.amount ;; }

  dimension: customer_order_rank {
    type: number
    sql: ROW_NUMBER() OVER (PARTITION BY ${customer_id} ORDER BY ${TABLE}.created_at) ;;
  }

  dimension: running_total {
    type: number
    sql: SUM(${amount}) OVER (PARTITION BY ${customer_id} ORDER BY ${TABLE}.created_at) ;;
  }

  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("window_funcs")

        rank_dim = model.get_dimension("customer_order_rank")
        assert rank_dim is not None
        assert "ROW_NUMBER" in rank_dim.sql
        assert "OVER" in rank_dim.sql

        running_dim = model.get_dimension("running_total")
        assert running_dim is not None
        assert "SUM" in running_dim.sql
        assert "OVER" in running_dim.sql


def test_lookml_complex_measure_expressions():
    """Test complex derived measure SQL expressions."""
    import tempfile

    lkml_content = """
view: complex_measures {
  sql_table_name: metrics ;;

  dimension: revenue { type: number sql: ${TABLE}.revenue ;; }
  dimension: cost { type: number sql: ${TABLE}.cost ;; }
  dimension: units { type: number sql: ${TABLE}.units ;; }

  measure: count { type: count }
  measure: total_revenue { type: sum sql: ${revenue} ;; }
  measure: total_cost { type: sum sql: ${cost} ;; }
  measure: total_units { type: sum sql: ${units} ;; }

  measure: margin_pct {
    type: number
    sql: 100.0 * (${total_revenue} - ${total_cost}) / NULLIF(${total_revenue}, 0) ;;
  }

  measure: revenue_per_unit {
    type: number
    sql: CASE WHEN ${total_units} > 0 THEN ${total_revenue} / ${total_units} ELSE 0 END ;;
  }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("complex_measures")

        margin = model.get_metric("margin_pct")
        assert margin is not None
        assert margin.type == "derived"
        assert "NULLIF" in margin.sql

        rpu = model.get_metric("revenue_per_unit")
        assert rpu is not None
        assert "CASE" in rpu.sql


def test_lookml_nested_ctes():
    """Test parsing derived tables with nested CTEs."""
    import tempfile

    lkml_content = """
view: nested_ctes {
  derived_table: {
    sql:
      WITH daily AS (
        SELECT date, SUM(amount) as daily_total
        FROM orders
        GROUP BY date
      ),
      weekly AS (
        SELECT DATE_TRUNC('week', date) as week, SUM(daily_total) as weekly_total
        FROM daily
        GROUP BY 1
      )
      SELECT * FROM weekly
    ;;
  }

  dimension: week { type: date sql: ${TABLE}.week ;; }
  dimension: weekly_total { type: number sql: ${TABLE}.weekly_total ;; }

  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("nested_ctes")

        assert model.table is None
        assert model.sql is not None
        assert "WITH" in model.sql
        assert "daily" in model.sql
        assert "weekly" in model.sql


def test_lookml_circular_reference_no_crash():
    """Test that circular dimension references don't crash the parser."""
    import tempfile

    lkml_content = """
view: circular {
  sql_table_name: t ;;

  dimension: dim_a { type: number sql: ${dim_b} + 1 ;; }
  dimension: dim_b { type: number sql: ${dim_a} + 1 ;; }

  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        # Should not crash, even with circular references
        graph = adapter.parse(Path(f.name))
        model = graph.get_model("circular")

        assert model is not None
        assert model.get_dimension("dim_a") is not None
        assert model.get_dimension("dim_b") is not None


def test_lookml_empty_view():
    """Test parsing empty/minimal views."""
    import tempfile

    lkml_content = """
view: empty_view {
  sql_table_name: empty ;;
}

view: minimal_view {
  sql_table_name: minimal ;;
  dimension: id { sql: ${TABLE}.id ;; }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))

        empty = graph.get_model("empty_view")
        assert empty is not None
        assert len(empty.dimensions) == 0

        minimal = graph.get_model("minimal_view")
        assert minimal is not None
        assert len(minimal.dimensions) == 1


def test_lookml_many_dimensions():
    """Test parsing views with many dimensions (stress test)."""
    import tempfile

    lkml_content = "view: many_dims {\n  sql_table_name: big_table ;;\n\n"
    for i in range(50):
        lkml_content += f"  dimension: dim_{i} {{ type: number sql: ${{TABLE}}.col_{i} ;; }}\n"
    lkml_content += "\n  measure: count { type: count }\n  measure: total { type: sum sql: ${dim_0} ;; }\n}\n"

    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("many_dims")

        assert len(model.dimensions) == 50
        assert "{model}.col_0" in model.get_dimension("dim_0").sql
        assert "{model}.col_49" in model.get_dimension("dim_49").sql


def test_lookml_json_struct_access():
    """Test parsing JSON and struct field access patterns."""
    import tempfile

    lkml_content = """
view: json_data {
  sql_table_name: events ;;

  dimension: id { type: number sql: ${TABLE}.id ;; }

  dimension: bq_json_value {
    type: string
    sql: JSON_VALUE(${TABLE}.data, '$.user.name') ;;
  }

  dimension: pg_json_value {
    type: string
    sql: ${TABLE}.data->>'user'->>'name' ;;
  }

  dimension: struct_field {
    type: string
    sql: ${TABLE}.nested.field.value ;;
  }

  dimension: array_access {
    type: string
    sql: ${TABLE}.items[OFFSET(0)] ;;
  }

  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("json_data")

        assert "JSON_VALUE" in model.get_dimension("bq_json_value").sql
        assert "->>" in model.get_dimension("pg_json_value").sql
        assert "nested.field.value" in model.get_dimension("struct_field").sql
        assert "OFFSET" in model.get_dimension("array_access").sql


def test_lookml_regex_in_sql():
    """Test parsing SQL with regex functions."""
    import tempfile

    lkml_content = """
view: regex_view {
  sql_table_name: logs ;;

  dimension: id { type: number sql: ${TABLE}.id ;; }

  dimension: extracted_email {
    type: string
    sql: REGEXP_EXTRACT(${TABLE}.text, r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+') ;;
  }

  dimension: has_phone {
    type: yesno
    sql: REGEXP_CONTAINS(${TABLE}.text, r'\\d{3}-\\d{3}-\\d{4}') ;;
  }

  measure: count { type: count }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("regex_view")

        assert model.get_dimension("extracted_email") is not None
        assert "REGEXP_EXTRACT" in model.get_dimension("extracted_email").sql


def test_lookml_hidden_fields():
    """Test parsing hidden dimensions and measures."""
    import tempfile

    lkml_content = """
view: hidden_test {
  sql_table_name: data ;;

  dimension: visible_id {
    type: number
    sql: ${TABLE}.id ;;
    primary_key: yes
  }

  dimension: hidden_internal_id {
    type: number
    sql: ${TABLE}.internal_id ;;
    hidden: yes
  }

  measure: visible_count { type: count }

  measure: hidden_sum {
    type: sum
    sql: ${TABLE}.amount ;;
    hidden: yes
  }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("hidden_test")

        # All fields should be parsed, hidden or not
        assert model.get_dimension("visible_id") is not None
        assert model.get_dimension("hidden_internal_id") is not None
        assert model.get_metric("visible_count") is not None
        assert model.get_metric("hidden_sum") is not None


def test_lookml_date_range_filters():
    """Test parsing Looker date range filter syntax."""
    import tempfile

    lkml_content = """
view: date_filters {
  sql_table_name: events ;;

  dimension: id { type: number sql: ${TABLE}.id ;; }

  dimension_group: created {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.created_at ;;
  }

  measure: last_30_days {
    type: count
    filters: [created_date: "last 30 days"]
  }

  measure: this_year {
    type: count
    filters: [created_date: "this year"]
  }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("date_filters")

        # Date filters are preserved as string literals (Looker-specific runtime syntax)
        last_30 = model.get_metric("last_30_days")
        assert last_30 is not None
        assert last_30.filters is not None
        # The filter value should be preserved
        assert any("30" in f for f in last_30.filters)


def test_lookml_block_style_filters():
    """Test parsing block-style filter syntax: filters: { field: x value: y }."""
    import tempfile

    lkml_content = """
view: block_filters {
  sql_table_name: flights ;;

  dimension: flight_length { type: number sql: ${TABLE}.flight_length ;; }
  dimension: is_delayed { type: yesno sql: ${TABLE}.is_delayed ;; }

  measure: count { type: count }

  measure: long_flights {
    type: count
    filters: {
      field: flight_length
      value: ">120"
    }
  }

  measure: delayed_flights {
    type: count
    filters: {
      field: is_delayed
      value: "yes"
    }
  }

  measure: long_delayed_flights {
    type: count
    filters: {
      field: flight_length
      value: ">120"
    }
    filters: {
      field: is_delayed
      value: "yes"
    }
  }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("block_filters")

        # Check long_flights filter
        long_flights = model.get_metric("long_flights")
        assert long_flights is not None
        assert long_flights.filters is not None
        assert len(long_flights.filters) == 1
        assert "> 120" in long_flights.filters[0] or ">120" in long_flights.filters[0]

        # Check delayed_flights filter
        delayed = model.get_metric("delayed_flights")
        assert delayed is not None
        assert delayed.filters is not None
        assert "true" in delayed.filters[0].lower()

        # Check long_delayed_flights has both filters
        long_delayed = model.get_metric("long_delayed_flights")
        assert long_delayed is not None
        assert long_delayed.filters is not None
        assert len(long_delayed.filters) == 2


def test_lookml_placeholder_measure_skipped():
    """Test that placeholder measures (type: number with no SQL) are skipped."""
    import tempfile

    lkml_content = """
view: template_view {
  extension: required

  measure: placeholder_measure {
    type: number
    hidden: yes
  }

  measure: real_derived_measure {
    type: number
    sql: ${some_field} * 2 ;;
  }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))
        model = graph.get_model("template_view")

        # Placeholder measure should be skipped
        assert model.get_metric("placeholder_measure") is None

        # Real derived measure should exist
        assert model.get_metric("real_derived_measure") is not None


def test_lookml_duplicate_refinement_skipped():
    """Test that duplicate refinements (+view) are skipped."""
    import tempfile

    lkml_content = """
view: base_view {
  sql_table_name: t ;;
  dimension: id { type: number sql: ${TABLE}.id ;; }
  measure: count { type: count }
}

view: +base_view {
  dimension: new_field_1 { type: string sql: ${TABLE}.field1 ;; }
}

view: +base_view {
  dimension: new_field_2 { type: string sql: ${TABLE}.field2 ;; }
}
"""
    adapter = LookMLAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        # Should not raise an error
        graph = adapter.parse(Path(f.name))

        # Both base_view and first +base_view should exist
        assert "base_view" in graph.models
        assert "+base_view" in graph.models

        # Only the first refinement is kept
        refinement = graph.get_model("+base_view")
        assert refinement.get_dimension("new_field_1") is not None
        # Second refinement's fields are not added (skipped)
        assert refinement.get_dimension("new_field_2") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
