"""Tests for LookML adapter edge cases.

These tests verify handling of complex LookML patterns found in real-world deployments,
inspired by fixtures from joshtemple/lkml, node-lookml-parser, and lookml-tools.
"""

from pathlib import Path

import pytest

from sidemantic.adapters.lookml import LookMLAdapter

# =============================================================================
# PROPERTY PASS-THROUGH TESTS (labels, formats, drill_fields, meta)
# =============================================================================


def test_lookml_dimension_label():
    """Test that dimension labels are captured."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    extended = graph.get_model("customers_extended")
    name_dim = extended.get_dimension("name")
    assert name_dim is not None
    assert name_dim.label == "Customer Name"


def test_lookml_dimension_value_format():
    """Test that dimension value_format_name and value_format are captured."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    extended = graph.get_model("customers_extended")
    ltv_dim = extended.get_dimension("lifetime_value")
    assert ltv_dim is not None
    assert ltv_dim.value_format_name == "usd"


def test_lookml_measure_value_format():
    """Test that measure value_format_name and value_format are captured."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    extended = graph.get_model("customers_extended")
    total_ltv = extended.get_metric("total_ltv")
    assert total_ltv is not None
    assert total_ltv.value_format_name == "usd"

    # Test value_format (not value_format_name)
    graph2 = adapter.parse(Path("tests/fixtures/lookml/edge_cases_special_types.lkml"))
    special = graph2.get_model("special_types")
    avg_score = special.get_metric("avg_score")
    assert avg_score is not None
    assert avg_score.format == "0.00"


def test_lookml_measure_label():
    """Test that measure labels are captured."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_actions.lkml"))

    orders = graph.get_model("interactive_orders")
    revenue = orders.get_metric("total_revenue")
    assert revenue is not None
    assert revenue.value_format_name == "usd"


def test_lookml_hidden_in_meta():
    """Test that hidden fields are stored in meta."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_special_types.lkml"))

    special = graph.get_model("special_types")

    lat_dim = special.get_dimension("latitude")
    assert lat_dim is not None
    assert lat_dim.meta is not None
    assert lat_dim.meta.get("hidden") is True

    lng_dim = special.get_dimension("longitude")
    assert lng_dim is not None
    assert lng_dim.meta is not None
    assert lng_dim.meta.get("hidden") is True


def test_lookml_extension_required_in_meta():
    """Test that extension: required is stored in model meta."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    abstract = graph.get_model("abstract_metrics")
    assert abstract.meta is not None
    assert abstract.meta.get("extension_required") is True

    # Non-abstract views should not have this flag
    base = graph.get_model("base_entity")
    assert base.meta is None or not base.meta.get("extension_required")


def test_lookml_explore_description():
    """Test that explore description is set on the model via from: aliasing."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # explore: orders { from: fact_orders description: "Main orders explore..." }
    # fact_orders has no description of its own, so it gets the explore's
    fact_orders = graph.get_model("fact_orders")
    assert fact_orders is not None
    assert fact_orders.description == "Main orders explore with all dimensions"


def test_lookml_explore_from_aliasing():
    """Test that from: on explores resolves to the correct base model."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # explore: orders { from: fact_orders } should add relationships to fact_orders
    fact_orders = graph.get_model("fact_orders")
    assert fact_orders is not None
    rel_names = [r.name for r in fact_orders.relationships]
    assert "dim_customers" in rel_names
    assert "dim_products" in rel_names


def test_lookml_explore_from_join_aliasing():
    """Test that from: on joins resolves to the actual view name."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # explore: customers { from: dim_customers } has join: dim_regions (direct)
    dim_customers = graph.get_model("dim_customers")
    rel_names = [r.name for r in dim_customers.relationships]
    assert "dim_regions" in rel_names

    # explore: customers has join: customer_orders { from: fact_orders }
    # This should create a relationship named "fact_orders" (the actual model)
    assert "fact_orders" in rel_names


def test_lookml_sql_always_where_segment():
    """Test that sql_always_where creates a segment with translated refs."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    fact_orders = graph.get_model("fact_orders")
    segment_names = [s.name for s in fact_orders.segments]
    # Segment names include the explore name for uniqueness
    assert "_sql_always_where_orders" in segment_names

    sql_where_seg = fact_orders.get_segment("_sql_always_where_orders")
    assert sql_where_seg is not None
    assert "deleted" in sql_where_seg.sql
    # ${fact_orders.status} should be translated to {model}.status
    assert "${fact_orders.status}" not in sql_where_seg.sql
    assert "{model}.status" in sql_where_seg.sql


def test_lookml_always_filter_strips_view_qualifier():
    """Test that always_filter strips view qualifiers from field names."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # always_filter: { filters: [fact_orders.created_date: "last 365 days"] }
    fact_orders = graph.get_model("fact_orders")
    seg = fact_orders.get_segment("_always_filter_orders_fact_orders.created_date")
    assert seg is not None
    # Should reference {model}.created_date, NOT {model}.fact_orders.created_date
    assert "fact_orders.created_date" not in seg.sql
    assert "created_date" in seg.sql


def test_lookml_refinement_preserves_base_scalars():
    """Test that refinements don't overwrite base view's table, PK, or description."""
    import tempfile

    lkml_content = """
view: orders {
  sql_table_name: analytics.orders ;;
  description: "All customer orders"

  dimension: order_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.order_id ;;
  }

  measure: count {
    type: count
  }
}

view: +orders {
  dimension: new_status {
    type: string
    sql: ${TABLE}.new_status ;;
  }
}
"""
    adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(suffix=".lkml", mode="w", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))

        orders = graph.get_model("orders")
        # Base scalars must survive the refinement merge
        assert orders.table == "analytics.orders"
        assert orders.primary_key == "order_id"
        assert orders.description == "All customer orders"
        # Refinement's dimension should be merged in
        assert orders.get_dimension("new_status") is not None
        # Original dimensions preserved
        assert orders.get_dimension("order_id") is not None


def test_lookml_join_sql_on_with_explore_alias():
    """Test that sql_on referencing explore alias (not view name) still works."""
    import tempfile

    lkml_content = """
view: fact_orders {
  sql_table_name: analytics.orders ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }
}

view: dim_customers {
  sql_table_name: analytics.customers ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }
}

explore: orders {
  from: fact_orders

  join: dim_customers {
    type: left_outer
    relationship: many_to_one
    sql_on: ${orders.customer_id} = ${dim_customers.id} ;;
  }
}
"""
    adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(suffix=".lkml", mode="w", delete=False) as f:
        f.write(lkml_content)
        f.flush()

        graph = adapter.parse(Path(f.name))

        fact_orders = graph.get_model("fact_orders")
        rel_names = [r.name for r in fact_orders.relationships]
        # Join should NOT be silently dropped just because sql_on uses explore alias
        assert "dim_customers" in rel_names
        cust_rel = next(r for r in fact_orders.relationships if r.name == "dim_customers")
        assert cust_rel.foreign_key == "customer_id"


def test_lookml_inheritance_resilient_to_missing_parent():
    """Test that one broken extends chain doesn't block valid ones."""
    import tempfile

    lkml_content = """
view: good_parent {
  sql_table_name: schema.parent ;;
  dimension: id { type: number primary_key: yes sql: ${TABLE}.id ;; }
  measure: count { type: count }
}

view: good_child {
  extends: [good_parent]
  sql_table_name: schema.child ;;
  dimension: extra { type: string sql: ${TABLE}.extra ;; }
}

view: orphan_child {
  extends: [nonexistent_parent]
  sql_table_name: schema.orphan ;;
  dimension: id { type: number primary_key: yes sql: ${TABLE}.id ;; }
}
"""
    adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(suffix=".lkml", mode="w", delete=False) as f:
        f.write(lkml_content)
        f.flush()
        graph = adapter.parse(Path(f.name))

    # good_child should have inherited from good_parent despite orphan_child's broken chain
    good_child = graph.get_model("good_child")
    assert good_child.get_dimension("id") is not None  # inherited
    assert good_child.get_dimension("extra") is not None  # own
    assert good_child.get_metric("count") is not None  # inherited

    # orphan_child should still exist, just unresolved
    orphan = graph.get_model("orphan_child")
    assert orphan is not None
    assert orphan.extends == "nonexistent_parent"


def test_lookml_percentile_without_sql_skipped():
    """Test that percentile measures without SQL are skipped, not crash."""
    import tempfile

    lkml_content = """
view: test_view {
  sql_table_name: schema.test ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  measure: p50_no_sql {
    type: percentile
    percentile: 50
  }

  measure: p90_with_sql {
    type: percentile
    percentile: 90
    sql: ${TABLE}.score ;;
  }
}
"""
    adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(suffix=".lkml", mode="w", delete=False) as f:
        f.write(lkml_content)
        f.flush()
        graph = adapter.parse(Path(f.name))

    test_view = graph.get_model("test_view")
    # No-SQL percentile should be gracefully skipped
    assert test_view.get_metric("p50_no_sql") is None
    # With-SQL percentile should work
    p90 = test_view.get_metric("p90_with_sql")
    assert p90 is not None
    assert "PERCENTILE_CONT" in p90.sql
    assert "0.9" in p90.sql


def test_lookml_explore_meta():
    """Test that explore label/group_label are stored in model meta."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    fact_orders = graph.get_model("fact_orders")
    assert fact_orders.meta is not None
    # Multiple explores reference fact_orders, so the label may be from any of them
    assert fact_orders.meta.get("explore_label") is not None
    assert fact_orders.meta.get("explore_group_label") is not None


def test_lookml_join_type_left_outer():
    """Test that left_outer join type is captured in relationship metadata."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # explore: orders { from: fact_orders } has join: dim_customers { type: left_outer }
    fact_orders = graph.get_model("fact_orders")
    rels_by_name = {r.name: r for r in fact_orders.relationships}
    dim_customers_rel = rels_by_name.get("dim_customers")
    assert dim_customers_rel is not None
    assert dim_customers_rel.metadata is not None
    assert dim_customers_rel.metadata["join_type"] == "left_outer"


def test_lookml_join_type_inner():
    """Test that inner join type is captured in relationship metadata."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # explore: completed_orders { from: fact_orders } has join: dim_customers { type: inner }
    # Both the "orders" and "completed_orders" explores add rels to fact_orders,
    # so there may be multiple dim_customers relationships. Find the inner one.
    fact_orders = graph.get_model("fact_orders")
    inner_rels = [
        r
        for r in fact_orders.relationships
        if r.name == "dim_customers" and r.metadata and r.metadata.get("join_type") == "inner"
    ]
    assert len(inner_rels) >= 1


def test_lookml_join_type_full_outer():
    """Test that full_outer join type is captured in relationship metadata."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # explore: all_customers_orders { from: dim_customers } has join: fact_orders { type: full_outer }
    dim_customers = graph.get_model("dim_customers")
    full_outer_rels = [
        r
        for r in dim_customers.relationships
        if r.name == "fact_orders" and r.metadata and r.metadata.get("join_type") == "full_outer"
    ]
    assert len(full_outer_rels) >= 1


def test_lookml_join_type_cross():
    """Test that cross join type is captured in relationship metadata."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # explore: date_product_matrix { from: dim_products } has join: date_spine { type: cross }
    dim_products = graph.get_model("dim_products")
    cross_rels = [
        r
        for r in dim_products.relationships
        if r.name == "date_spine" and r.metadata and r.metadata.get("join_type") == "cross"
    ]
    assert len(cross_rels) >= 1


def test_lookml_join_type_all_four_on_same_graph():
    """Test that all four join types coexist correctly on a single parsed graph."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))

    # Collect all join_type values across the entire graph
    all_join_types = set()
    for model in graph.models.values():
        for rel in model.relationships:
            if rel.metadata and "join_type" in rel.metadata:
                all_join_types.add(rel.metadata["join_type"])

    assert "left_outer" in all_join_types
    assert "inner" in all_join_types
    assert "full_outer" in all_join_types
    assert "cross" in all_join_types


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
    """Test parsing views that extend other views - inheritance is resolved."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    assert "customers_extended" in graph.models
    extended = graph.get_model("customers_extended")

    # Inherited dimensions from base_entity
    assert extended.get_dimension("id") is not None
    assert extended.get_dimension("is_active") is not None
    assert extended.get_dimension("created_date") is not None
    assert extended.get_dimension("created_week") is not None

    # Overridden dimension: name should have child's SQL (CONCAT)
    name_dim = extended.get_dimension("name")
    assert name_dim is not None
    assert "CONCAT" in name_dim.sql

    # Customer-specific dimensions
    assert extended.get_dimension("email") is not None
    assert extended.get_dimension("tier") is not None
    assert extended.get_dimension("lifetime_value") is not None

    # Inherited measure from base_entity
    assert extended.get_metric("count") is not None

    # Customer-specific measures
    assert extended.get_metric("total_ltv") is not None
    assert extended.get_metric("avg_ltv") is not None

    # Table should be overridden by child
    assert extended.table == "analytics.customers"


def test_lookml_refinement_syntax():
    """Test parsing refinement syntax (+view_name) - merged into base view."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    # Refinements are merged into the base view, not stored separately
    assert "+base_entity" not in graph.models

    # The refined_field should now be on base_entity
    base = graph.get_model("base_entity")
    assert base.get_dimension("refined_field") is not None
    assert base.get_dimension("refined_field").description == "Added via refinement"

    # Original dimensions should still be present
    assert base.get_dimension("id") is not None
    assert base.get_dimension("name") is not None


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
    """Test parsing concrete view extending abstract - inherits measures."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("tests/fixtures/lookml/edge_cases_extends.lkml"))

    assert "transactions" in graph.models
    transactions = graph.get_model("transactions")

    # Check dimensions (own)
    assert transactions.get_dimension("id") is not None
    assert transactions.get_dimension("amount") is not None
    assert transactions.get_dimension("status") is not None

    # Check time dimensions (own)
    assert transactions.get_dimension("transaction_time") is not None
    assert transactions.get_dimension("transaction_date") is not None

    # Inherited measures from abstract_metrics
    assert transactions.get_metric("record_count") is not None
    assert transactions.get_metric("record_count").agg == "count"
    assert transactions.get_metric("sum_amount") is not None
    assert transactions.get_metric("sum_amount").agg == "sum"
    assert transactions.get_metric("avg_amount") is not None
    assert transactions.get_metric("min_amount") is not None
    assert transactions.get_metric("max_amount") is not None


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


def test_lookml_segment_resolves_self_qualified_and_bare_field_refs():
    """A `filter:` segment's ${...} field refs must resolve, not leak into the WHERE clause.

    A self-qualified ${orders.status} (and a bare ${status}) reference a dimension; like
    dimensions and measures, they must resolve through the dimension SQL. Otherwise the literal
    ${...} reaches the generated WHERE clause and the database rejects it.
    """
    import tempfile

    from sidemantic import SemanticLayer

    src = """view: orders {
      sql_table_name: raw_orders ;;
      dimension: id { primary_key: yes sql: ${TABLE}.id ;; }
      dimension: status { sql: ${TABLE}.order_status ;; }
      filter: completed_seg { sql: ${orders.status} = 'completed' ;; }
      filter: bare_seg { sql: ${status} = 'x' ;; }
      measure: total { type: sum sql: ${TABLE}.amount ;; }
    }
    """
    path = tempfile.mktemp(suffix=".lkml")
    with open(path, "w") as f:
        f.write(src)
    graph = LookMLAdapter().parse(Path(path))
    model = graph.get_model("orders")
    by_name = {s.name: s.sql for s in model.segments}
    # The self-qualified reference resolves to the real column (order_status), no leaked ${...}.
    assert by_name["completed_seg"] == "({model}.order_status) = 'completed'", by_name
    assert by_name["bare_seg"] == "({model}.order_status) = 'x'", by_name

    # End-to-end: querying with the segment must produce valid SQL (no ${...}).
    layer = SemanticLayer()
    for mdl in graph.models.values():
        layer.add_model(mdl)
    sql = layer.compile(metrics=["orders.total"], segments=["orders.completed_seg"])
    assert "${" not in sql
    assert "order_status" in sql and "completed" in sql


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

    # Drill fields are now captured on metrics
    count_measure = orders.get_metric("count")
    assert count_measure is not None
    assert count_measure.drill_fields is not None
    assert "order_details*" in count_measure.drill_fields

    revenue_measure = orders.get_metric("total_revenue")
    assert revenue_measure is not None
    assert revenue_measure.drill_fields is not None
    assert "order_details*" in revenue_measure.drill_fields
    assert "region" in revenue_measure.drill_fields


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


def test_lookml_duration_group_resolves_field_references():
    """sql_start/sql_end ${field} references resolve to real columns, not leaked ${...} literals.

    The duration path only replaced ${TABLE}, so a self-view ref (${started_at}) leaked into
    DATE_DIFF and every query on the duration dimension emitted invalid SQL. Resolve them.
    """
    from sidemantic.core.semantic_layer import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: started_at { type: time  sql: ${TABLE}.started_at ;; }
  dimension: ended_at { type: time  sql: ${TABLE}.ended_at ;; }
  dimension_group: elapsed {
    type: duration
    intervals: [day, hour]
    sql_start: ${started_at} ;;
    sql_end: ${ended_at} ;;
  }
}
"""
    )
    elapsed = graph.get_model("orders").get_dimension("elapsed_days")
    assert elapsed is not None
    assert "${" not in elapsed.sql  # refs resolved, not leaked
    assert "started_at" in elapsed.sql and "ended_at" in elapsed.sql
    layer = SemanticLayer()
    layer.graph = graph
    assert "${" not in layer.compile(dimensions=["orders.elapsed_days"])


def test_lookml_duration_group_resolves_compact_dimension_references():
    """A ref to a COMPACT dimension (declared with no sql) resolves to its default column.

    The resolver needs the declared dimension set to use its compact-dimension fallback; without
    it a bare ${started_at} for a `dimension: started_at { type: time }` leaked into DATE_DIFF.
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: started_at { type: time }
  dimension: ended_at { type: time }
  dimension_group: elapsed {
    type: duration
    intervals: [day]
    sql_start: ${started_at} ;;
    sql_end: ${ended_at} ;;
  }
}
"""
    )
    elapsed = graph.get_model("orders").get_dimension("elapsed_days")
    assert elapsed is not None
    assert "${" not in elapsed.sql  # compact refs resolved to their default columns
    assert "started_at" in elapsed.sql and "ended_at" in elapsed.sql


def test_lookml_duration_group_cross_view_ref_dropped():
    """A duration group whose sql_start/sql_end references another view inline is dropped."""
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension_group: elapsed {
    type: duration
    intervals: [day]
    sql_start: ${other.a} ;;
    sql_end: ${TABLE}.ended_at ;;
  }
}
view: other { sql_table_name: other ;; dimension: a { type: time  sql: ${TABLE}.a ;; } }
"""
    )
    assert not any(d.name.startswith("elapsed") for d in graph.get_model("orders").dimensions)


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
    """A cross-view field reference (${other_view.field}) is unqueryable, so it is dropped.

    Sidemantic has no inline cross-model column; keeping the dimension would leak the literal
    ${customers.name} into the model CTE, so every query touching it fails. Drop the field.
    """
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

        # The cross-view dimension is dropped rather than imported with a leaked literal.
        assert orders.get_dimension("customer_name") is None
        # The ordinary dimension is unaffected.
        assert orders.get_dimension("id") is not None


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


def test_lookml_multiple_refinements_merged():
    """Test that multiple refinements (+view) are all merged into the base."""
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

        graph = adapter.parse(Path(f.name))

        # Refinements are merged into base, not stored separately
        assert "base_view" in graph.models
        assert "+base_view" not in graph.models

        base = graph.get_model("base_view")
        # Original dimensions preserved
        assert base.get_dimension("id") is not None
        # Both refinements merged in
        assert base.get_dimension("new_field_1") is not None
        assert base.get_dimension("new_field_2") is not None
        # Original measure preserved
        assert base.get_metric("count") is not None


def test_lookml_filter_grammar_conversion():
    """Lock in the corrected Looker filter-expression -> SQL conversion.

    Regression coverage for the audit findings: date/numeric ranges, NOT/negation,
    EMPTY's NULL case, wildcard NOT LIKE, mixed lists, and single-quote escaping.
    """
    adapter = LookMLAdapter()
    conv = adapter._convert_lookml_filter_to_sql
    f = "{model}.f"

    # Single-quote escaping (was a SQL-injection / breakage bug)
    assert conv("f", "O'Brien") == f"{f} = 'O''Brien'"

    # EMPTY must include the NULL case
    assert conv("f", "EMPTY") == f"({f} IS NULL OR {f} = '')"
    assert conv("f", "-EMPTY") == f"({f} IS NOT NULL AND {f} <> '')"

    # NULL passthrough
    assert conv("f", "NULL") == f"{f} IS NULL"
    assert conv("f", "-NULL") == f"{f} IS NOT NULL"

    # Numeric ranges and interval syntax (were string-equality / garbage IN)
    assert conv("f", "5 to 10") == f"({f} >= 5 AND {f} <= 10)"
    assert conv("f", "5 to") == f"{f} >= 5"
    assert conv("f", "to 10") == f"{f} <= 10"
    assert conv("f", "[1,10]") == f"({f} >= 1 AND {f} <= 10)"
    assert conv("f", "(1,10)") == f"({f} > 1 AND {f} < 10)"
    assert conv("f", "[1,10)") == f"({f} >= 1 AND {f} < 10)"
    assert conv("f", "(1,10]") == f"({f} > 1 AND {f} <= 10)"

    # NOT / negation. Numeric NOT is a comparison; string negation uses the "-" form.
    assert conv("f", "NOT 5") == f"{f} != 5"
    assert conv("f", "-Completed") == f"{f} != 'Completed'"
    # "not <string>" is a LITERAL value (Looker uses -FOO for string negation), not a negation.
    assert conv("f", "not started") == f"{f} = 'not started'"
    # Leading-NOT interval lists split at top level (the interval comma is preserved).
    assert conv("f", "NOT [0,10],20") == f"(({f} < 0 OR {f} > 10) AND {f} != 20)"

    # before / after bounds (Looker: before exclusive, after inclusive)
    assert conv("f", "before 2020-01-01") == f"{f} < '2020-01-01'"
    assert conv("f", "after 2020-01-01") == f"{f} >= '2020-01-01'"
    assert conv("f", "before 2020-01") == f"{f} < '2020-01'"  # year-month is absolute
    # Relative bounds are NOT translated as absolute literals (left for the date warning)
    assert conv("f", "before 3 days ago") == f"{f} = 'before 3 days ago'"
    assert conv("f", "after Monday") == f"{f} = 'after Monday'"
    # A TRUNCATED date (single-digit month) is not a full absolute date -> not translated
    assert conv("f", "before 2016-1") == f"{f} = 'before 2016-1'"

    # Leading NOT over a mixed interval + range list negates the WHOLE list (De Morgan):
    # "NOT [0,10], 20 to 30" excludes both, not OR-include the positive range.
    assert conv("f", "NOT [0,10], 20 to 30") == f"(({f} < 0 OR {f} > 10) AND ({f} < 20 OR {f} > 30))"

    # Numeric AND range in a single condition
    assert conv("f", ">1 AND <100") == f"({f} > 1 AND {f} < 100)"
    assert conv("f", ">=1 AND <=5") == f"({f} >= 1 AND {f} <= 5)"
    # NOT of an AND-range -> De Morgan (OR of flipped comparisons), parsed before the
    # single-comparison flip (which would otherwise treat ">1 AND <100" as one operand).
    assert conv("f", "NOT >1 AND <100") == f"({f} <= 1 OR {f} >= 100)"
    # AND-range with bare-fraction bounds (consistent with .5 being numeric).
    assert conv("f", ">.5 AND <1") == f"({f} > .5 AND {f} < 1)"
    # A string value that merely contains the word "and" is NOT a numeric AND-range.
    assert conv("f", "red and blue OR 90") == f"{f} = 'red and blue OR 90'"
    # Leading-NOT list with an AND-range member + a plain exclusion (De Morgan + AND).
    assert conv("f", "NOT >1 AND <100, 200") == f"(({f} <= 1 OR {f} >= 100) AND {f} != 200)"

    # Comparisons
    assert conv("f", ">100") == f"{f} > 100"
    assert conv("f", "<=5") == f"{f} <= 5"
    assert conv("f", "<>0") == f"{f} != 0"

    # Wildcards, incl. negated -> NOT LIKE (was != '%foo%')
    assert conv("f", "%foo%") == f"{f} LIKE '%foo%'"
    assert conv("f", "-%foo%") == f"{f} NOT LIKE '%foo%'"
    # Only the "-" dash form negates a wildcard; the word "not" is a literal pattern
    # (Looker negates strings with "-"), so "not %complete%" is a positive LIKE.
    assert conv("f", "not %complete%") == f"{f} LIKE 'not %complete%'"

    # nan/inf/Infinity are NOT numeric filter values -> stay quoted strings (float()
    # accepts them, but Looker only uses inf as an interval bound, handled separately).
    assert conv("f", "nan") == f"{f} = 'nan'"
    assert conv("f", "inf") == f"{f} = 'inf'"
    assert conv("f", "Infinity") == f"{f} = 'Infinity'"
    # Python/float()-only numeric spellings are NOT numeric filter values -> stay quoted
    # (exponent 1e2 would otherwise emit `= 1e2`; only plain decimals are numeric).
    assert conv("f", "1e2") == f"{f} = '1e2'"
    assert conv("f", ".5") == f"{f} = .5"  # but a bare decimal fraction is still numeric

    # Lists
    assert conv("f", "a,b") == f"{f} IN ('a', 'b')"
    assert conv("f", "1,5,9") == f"{f} IN (1, 5, 9)"
    assert conv("f", "-a,-b") == f"{f} NOT IN ('a', 'b')"
    # A single leading NOT negates the whole NUMERIC list
    assert conv("f", "NOT 66, 99, 4") == f"{f} NOT IN (66, 99, 4)"
    # ...but for a STRING list the word "not" is a literal value (Looker negates
    # strings with "-FOO"), so the leading "not" stays on the first value.
    assert conv("f", "not started,pending") == f"{f} IN ('not started', 'pending')"
    # A NON-first "not <string>" is also a literal include (OR'd), not an AND-exclusion.
    assert conv("f", "pending,not started") == f"({f} = 'pending' OR {f} = 'not started')"
    # ...while a word-NOT of a NUMERIC value in a list is a real exclusion (AND).
    assert conv("f", "a,not 5") == f"({f} = 'a' AND {f} != 5)"
    # Mixed-operator list is no longer silently mangled (valid SQL)
    assert conv("f", ">1,<5") == f"({f} > 1 OR {f} < 5)"
    assert conv("f", "%a%,%b%") == f"({f} LIKE '%a%' OR {f} LIKE '%b%')"

    # Interval / range lists -> OR of each part (commas inside brackets preserved)
    assert conv("f", "[0,9],[20,29]") == f"(({f} >= 0 AND {f} <= 9) OR ({f} >= 20 AND {f} <= 29))"
    assert conv("f", "[0,10],20") == f"(({f} >= 0 AND {f} <= 10) OR {f} = 20)"
    # Comma-separated "to"-ranges (no brackets) are numeric ranges, not a string IN.
    assert conv("f", "1 to 10, 20 to 30") == f"(({f} >= 1 AND {f} <= 10) OR ({f} >= 20 AND {f} <= 30))"
    # A non-leading exclusion in a range list is ANDed, not ORed (else it admits everything).
    assert conv("f", "[0,30], NOT 20") == f"(({f} >= 0 AND {f} <= 30) AND {f} != 20)"
    # NOT of a range -> the inverted (outside) condition
    assert conv("f", "NOT 3 to 80.44") == f"({f} < 3 OR {f} > 80.44)"
    # Leading NOT over a SATISFIABLE comparison list negates each clause (De Morgan).
    assert conv("f", "NOT >100, 2, <1") == f"({f} <= 100 AND {f} != 2 AND {f} >= 1)"
    # An IMPOSSIBLE all-negated numeric list (<=1 AND >=100 -> no value) is documented by Looker
    # to select NULLs: emit `IS NULL`, not an always-false AND that also drops NULL rows.
    assert conv("f", "NOT >1, 2, <100") == f"{f} IS NULL"
    assert conv("f", "NOT >=5, <5") == f"{f} IS NULL"  # <5 AND >=5 is empty
    # A single-point intersection (<=5 AND >=5) is still satisfiable -> keep the AND.
    assert conv("f", "NOT >5, <5") == f"({f} <= 5 AND {f} >= 5)"
    # An impossible list that ALSO contains a negated range (OR clause) keeps the AND-join
    # (interval-union impossibility is not evaluated).
    assert conv("f", "NOT >1, <100, [0,10]") == f"({f} <= 1 AND {f} >= 100 AND ({f} < 0 OR {f} > 10))"
    # Single NOT comparison flips the operator; NOT of an interval inverts it
    assert conv("f", "NOT >1") == f"{f} <= 1"
    assert conv("f", "NOT (3,12)") == f"({f} <= 3 OR {f} >= 12)"
    # AND / OR numeric grammar (OR binds loosest)
    assert conv("f", ">1 AND <100") == f"({f} > 1 AND {f} < 100)"
    assert conv("f", ">10 AND <=20 OR 90") == f"(({f} > 10 AND {f} <= 20) OR {f} = 90)"
    # ORed ranges route through range SQL, not string equality
    assert conv("f", "3 to 10 OR 30 to 100") == f"(({f} >= 3 AND {f} <= 10) OR ({f} >= 30 AND {f} <= 100))"
    # A plain string containing "OR" is not misread as an OR filter
    assert conv("f", "cats OR dogs") == f"{f} = 'cats OR dogs'"

    # NOT NULL / NOT EMPTY are null/empty checks (same as -NULL / -EMPTY)
    assert conv("f", "NOT NULL") == f"{f} IS NOT NULL"
    assert conv("f", "NOT EMPTY") == f"({f} IS NOT NULL AND {f} <> '')"

    # Explicit infinity bounds in interval notation -> open-ended comparisons
    assert conv("f", "(500, inf)") == f"{f} > 500"
    assert conv("f", "(-inf, 10]") == f"{f} <= 10"

    # Dash-negation of a DOT-PREFIXED string is a string exclusion, not blocked by the `-.`
    # (only actual negative numbers -5 / -.5 are values, not exclusions).
    assert conv("ext", "-.csv") == "{model}.ext != '.csv'"
    assert conv("f", "-5") == f"{f} = -5"  # negative number, not an exclusion
    assert conv("f", "-.5") == f"{f} = -.5"  # negative bare fraction, not an exclusion

    # A non-leading FRACTIONAL NOT-comparison is classified as an exclusion (AND), not a match.
    assert conv("f", "1, NOT >.5") == f"({f} = 1 AND {f} <= .5)"

    # A dot-prefixed dash exclusion inside a MIXED LIST must be ANDed (excluded), not ORed.
    # single("-.csv") already emits `!= '.csv'`; the classifier must agree so the combiner
    # doesn't OR it in (which would admit almost every value).
    assert conv("ext", "FOO%,-.csv") == "({model}.ext LIKE 'FOO%' AND {model}.ext != '.csv')"

    # A NEGATED interval in a non-leading list position keeps its inner comma (bracket-aware
    # split) and is excluded (AND). Previously the naive split shattered it into "NOT [0"/"10]".
    assert conv("f", "20, NOT [0,10]") == f"({f} = 20 AND ({f} < 0 OR {f} > 10))"
    assert conv("f", "1 to 10, -.csv") == f"(({f} >= 1 AND {f} <= 10) AND {f} != '.csv')"

    # yes/no
    assert conv("f", "yes") == f"{f} = true"
    assert conv("f", "no") == f"{f} = false"


def test_lookml_filter_date_expression_warns(caplog):
    """Untranslated date/interval filters should warn, not silently string-equal."""
    import logging

    adapter = LookMLAdapter()
    with caplog.at_level(logging.WARNING):
        result = adapter._convert_lookml_filter_to_sql("created_date", "last 7 days")
    assert "last 7 days" in result  # value preserved
    assert any("not translated" in rec.getMessage() for rec in caplog.records)

    # Weekday relative date expressions also warn (not silently string-equal)
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        adapter._convert_lookml_filter_to_sql("created_date", "after Monday")
    assert any("not translated" in rec.getMessage() for rec in caplog.records)

    # A comma-separated date OR list warns per date part instead of emitting a plain
    # IN ('today', '7 days ago'), and ORs them (Looker's list-of-alternatives semantics).
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        result = adapter._convert_lookml_filter_to_sql("created_date", "today, 7 days ago")
    assert " OR " in result and "IN (" not in result
    assert sum("not translated" in rec.getMessage() for rec in caplog.records) >= 2


def _parse_lkml(text):
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(text)
        f.flush()
        return LookMLAdapter().parse(Path(f.name))


def test_lookml_self_view_qualified_refs_resolve():
    """${this_view.field} must resolve like ${field}, not leak literal ${...}."""
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: public.orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: amount_x2 { type: number  sql: ${orders.amount} * 2 ;; }
  measure: total { type: sum  sql: ${orders.amount} ;; }
}
"""
    )
    orders = graph.get_model("orders")
    amount_x2 = orders.get_dimension("amount_x2")
    assert "${" not in amount_x2.sql
    assert "{model}.amount" in amount_x2.sql

    total = orders.get_metric("total")
    assert total.agg == "sum"
    assert "${" not in total.sql
    assert "{model}.amount" in total.sql


def test_lookml_self_view_resolved_cross_view_dropped(caplog):
    """Self-view refs resolve; a cross-view ref is unqueryable, so the field is dropped + warned.

    Sidemantic has no inline cross-model column. Leaving the literal ${customers.name} would leak
    it into the model CTE, so any query on the field fails; drop the dimension entirely instead.
    """
    import logging

    with caplog.at_level(logging.WARNING):
        graph = _parse_lkml(
            """
view: orders {
  sql_table_name: public.orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: self_x2 { type: number  sql: ${orders.id} * 2 ;; }
  dimension: cust_name { type: string  sql: ${customers.name} ;; }
}
view: customers {
  sql_table_name: public.customers ;;
  dimension: name { type: string  sql: ${TABLE}.name ;; }
}
"""
        )
    orders = graph.get_model("orders")
    # self-view ref resolves to the model column (no literal left)
    assert orders.get_dimension("self_x2").sql == "({model}.id) * 2"
    # cross-view ref field is dropped (not importable), with a warning
    assert orders.get_dimension("cust_name") is None
    assert any("references another view inline" in rec.getMessage() for rec in caplog.records)


def test_lookml_number_measure_cross_view_dropped():
    """type: number measures resolve self-view refs; a measure with a cross-view ref is dropped."""
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  measure: total { type: sum  sql: ${TABLE}.amount ;; }
  measure: self_ratio { type: number  sql: ${orders.total} / 2 ;; }
  measure: margin_pct { type: number  sql: ${customers.total} / ${orders.total} ;; }
}
view: customers {
  sql_table_name: c ;;
  dimension: total { type: number  sql: ${TABLE}.total ;; }
}
"""
    )
    orders = graph.get_model("orders")
    # self-view measure ref resolves (no literal) and is kept
    assert "${" not in orders.get_metric("self_ratio").sql
    # a measure that references another view inline is dropped, not imported with a leaked literal
    assert orders.get_metric("margin_pct") is None


def test_lookml_cross_view_dimension_group_dropped_and_no_leak_on_compile():
    """A dimension_group with a cross-view base SQL is dropped; surviving fields compile cleanly."""
    from sidemantic.core.semantic_layer import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension_group: cust_created {
    type: time
    timeframes: [date, month]
    sql: ${customers.created_at} ;;
  }
}
view: customers {
  sql_table_name: customers ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: created_at { type: time  sql: ${TABLE}.created_at ;; }
}
"""
    )
    orders = graph.get_model("orders")
    # every timeframe field of the cross-view group is dropped
    assert not any(d.name.startswith("cust_created") for d in orders.dimensions)
    # a surviving field still compiles without a leaked ${...}
    layer = SemanticLayer()
    layer.graph = graph
    sql = layer.compile(dimensions=["orders.amount"])
    assert "${" not in sql


def test_lookml_cross_view_segment_dropped():
    """A view-level filter (segment) with a cross-view ref is dropped, not imported unqueryable.

    Otherwise the unresolved ${customers.active} leaks into the WHERE clause when the segment is
    used, while dimensions/measures with the same leak are dropped. A normal segment is kept.
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  filter: active_customers { type: yesno  sql: ${customers.active} ;; }
  filter: big { type: number  sql: ${TABLE}.amount > 100 ;; }
}
view: customers { sql_table_name: customers ;; dimension: active { type: yesno  sql: ${TABLE}.active ;; } }
"""
    )
    names = {s.name for s in graph.get_model("orders").segments}
    assert "active_customers" not in names  # cross-view segment dropped
    assert "big" in names  # ordinary segment kept


def test_lookml_measure_cross_view_filter_dropped():
    """A measure whose `filters` reference another view inline is dropped, not imported broken.

    filters: [customers.active: "yes"] would become {model}.customers.active in the single-table
    model CTE (no `customers` alias) and fail to query. A self-view filter still imports.
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: cross_filtered { type: sum  sql: ${TABLE}.amount ;; filters: [customers.active: "yes"] }
  measure: self_filtered { type: sum  sql: ${TABLE}.amount ;; filters: [orders.status: "done"] }
  measure: plain { type: sum  sql: ${TABLE}.amount ;; }
}
view: customers { sql_table_name: customers ;; dimension: active { type: yesno  sql: ${TABLE}.active ;; } }
"""
    )
    model = graph.get_model("orders")
    names = {m.name for m in model.metrics}
    assert "cross_filtered" not in names  # cross-view filter -> measure dropped
    assert {"self_filtered", "plain"} <= names  # self-view filter and unfiltered measure kept
    assert model.get_metric("self_filtered").filters == ["{model}.status = 'done'"]


def test_lookml_complete_measure_filter_alias_for_cross_view_dimension_dropped():
    """A complete measure filtered by a LOCAL dimension that is a dropped cross-view alias is dropped.

    filters: [customer_active: "yes"] where customer_active { sql: ${customers.active} } expands to
    the leaked ${customers.active} in the filter; the measure.sql leak check doesn't see filters, so
    the measure must be rejected after the filter rewrite. An unfiltered measure is kept.
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: customer_active { type: yesno  sql: ${customers.active} ;; }
  measure: n { type: number  sql: COUNT(${TABLE}.id) ;; filters: [customer_active: "yes"] }
  measure: plain { type: number  sql: COUNT(${TABLE}.id) ;; }
}
view: customers { sql_table_name: customers ;; dimension: active { type: yesno  sql: ${TABLE}.active ;; } }
"""
    )
    names = {m.name for m in graph.get_model("orders").metrics}
    assert "n" not in names  # filter expands to a cross-view ref -> dropped
    assert "plain" in names


def test_lookml_number_measure_row_level_dimension_expr_skipped():
    """A type: number measure that is a ROW-LEVEL dimension expression (no aggregate) is skipped.

    `${amount} / 2` is not a valid aggregate measure -- as a metric it would return one row
    per input row, not a scalar -- so it is dropped on import (belongs as a dimension).
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: half_amount { type: number  sql: ${orders.amount} / 2 ;; }
}
"""
    )
    assert graph.get_model("orders").get_metric("half_amount") is None  # row-level -> skipped


def test_lookml_measure_referencing_skipped_measure_is_dropped():
    """A measure that references a measure which did NOT survive parsing must be dropped too.

    A row-level helper `bad { sql: ${amount} }` is skipped, but a dependent `outer { sql: ${bad}*2 }`
    still resolved ${bad} to a bare `bad` and imported as `bad * 2` -- compile then failed with
    "Metric bad not found". Drop such dependents (iterating so a dependent of a dependent goes too);
    a dependent of a SURVIVING measure is kept and works.
    """
    import duckdb

    from sidemantic import SemanticLayer

    dropped = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: bad { type: number  sql: ${amount} ;; }
  measure: outer { type: number  sql: ${bad} * 2 ;; }
  measure: outer2 { type: number  sql: ${outer} + 1 ;; }
}
"""
    ).get_model("orders")
    # bad (row-level) is skipped, and its transitive dependents go with it.
    assert dropped.get_metric("bad") is None
    assert dropped.get_metric("outer") is None
    assert dropped.get_metric("outer2") is None

    # A dependent of a SURVIVING measure is kept and compiles.
    kept = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: total { type: sum  sql: ${amount} ;; }
  measure: dbl { type: number  sql: ${total} * 2 ;; }
}
"""
    ).get_model("orders")
    assert kept.get_metric("dbl") is not None
    layer = SemanticLayer(auto_register=False)
    layer.add_model(kept)
    con = duckdb.connect()
    con.execute("create table orders as select 1 id, 10 amount union all select 2, 20")
    assert con.execute(layer.compile(metrics=["orders.dbl"])).fetchall() == [(60,)]


def test_lookml_number_measure_compact_dimension_row_level_skipped():
    """A number measure over a COMPACT dimension with no aggregate is also a row-level expr -> skipped."""
    graph = _parse_lkml(
        """
view: inventory_items {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: cost {}
  measure: half_cost { type: number  sql: ${inventory_items.cost} / 2 ;; }
}
"""
    )
    assert graph.get_model("inventory_items").get_metric("half_cost") is None


def test_lookml_number_measure_mixed_with_raw_dimension_skipped():
    """A number measure dividing an aggregate measure by a RAW dimension column is skipped.

    `${total} / NULLIF(${amount}, 0)` has no valid SQL form (amount is neither grouped
    nor aggregated), so it is dropped on import rather than emitting invalid SQL.
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: total { type: sum  sql: ${TABLE}.amount ;; }
  measure: m { type: number  sql: ${orders.total} / NULLIF(${orders.amount}, 0) ;; }
}
"""
    )
    names = {mt.name for mt in graph.get_model("orders").metrics}
    assert "total" in names  # the clean measure is kept
    assert "m" not in names  # the raw-column mixed measure is dropped


def test_lookml_number_measure_mixed_aggregate_safe_kept_and_executes():
    """A mix where the dimension ref is INSIDE an aggregate is valid and must round-trip.

    `${total} / NULLIF(SUM(${amount}), 0)` has no raw ungrouped column, so the measure
    ref is expanded to its base aggregate over the real column and the whole expression
    is kept as opaque complete SQL that actually executes.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: total { type: sum  sql: ${TABLE}.amount ;; }
  measure: m { type: number  sql: ${orders.total} / NULLIF(SUM(${orders.amount}), 0) ;; }
}
"""
    )
    m = graph.get_model("orders").get_metric("m")
    assert m is not None and m.sql_is_complete is True
    assert "${" not in m.sql  # measure ref expanded to base aggregate, dim ref resolved
    layer = SemanticLayer(auto_register=False)
    layer.add_model(graph.get_model("orders"))
    sql = layer.compile(metrics=["orders.m"])
    con = duckdb.connect()
    con.execute("create table orders as select 1 id, 10 amount union all select 2, 20")
    assert con.execute(sql).fetchall() == [(1.0,)]  # SUM(amount)/SUM(amount) = 1.0


def test_lookml_number_measure_mixed_filtered_base_measure_keeps_filter():
    """Expanding a FILTERED base measure in a mixed expr must keep the filter, not drop it.

    `completed_total` (sum filtered to status='completed') used in
    `${completed_total} / NULLIF(SUM(${amount}), 0)` must compile to a filtered numerator
    (10/40 = 0.25), not an unfiltered SUM(amount)/SUM(amount) = 1.0.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: completed_total { type: sum  sql: ${TABLE}.amount ;; filters: [status: "completed"] }
  measure: m { type: number  sql: ${orders.completed_total} / NULLIF(SUM(${orders.amount}), 0) ;; }
}
"""
    )
    m = graph.get_model("orders").get_metric("m")
    assert m is not None and "completed" in m.sql  # base-measure filter folded in
    layer = SemanticLayer(auto_register=False)
    layer.add_model(graph.get_model("orders"))
    sql = layer.compile(metrics=["orders.m"])
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, status text)")
    con.execute("insert into orders values (1,10,'completed'),(2,30,'pending')")
    assert con.execute(sql).fetchall() == [(0.25,)]  # 10 / (10+30)


def test_lookml_number_measure_ref_to_filtered_complete_measure_keeps_filter():
    """Referencing a FILTERED complete number measure must inline its filter, not drop it.

    `completed_sum` is a type: number inline-aggregate measure filtered to status='completed'.
    `double_sum = ${completed_sum} * 2` must inline the FILTERED aggregate (completed=130, *2 =
    260), not expand to an unfiltered SUM(amount)*2 over all rows (360). A chained reference
    (triple_sum = double_sum + completed_sum) must also stay filtered.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: completed_sum { type: number  sql: SUM(${amount}) ;; filters: [status: "completed"] }
  measure: double_sum { type: number  sql: ${completed_sum} * 2 ;; }
  measure: triple_sum { type: number  sql: ${double_sum} + ${completed_sum} ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("double_sum") is not None  # not dropped
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, status text)")
    con.execute("insert into orders values (1,100,'completed'),(2,50,'pending'),(3,30,'completed')")
    assert con.execute(layer.compile(metrics=["orders.completed_sum"])).fetchall() == [(130,)]
    assert con.execute(layer.compile(metrics=["orders.double_sum"])).fetchall() == [(260,)]  # not 360
    assert con.execute(layer.compile(metrics=["orders.triple_sum"])).fetchall() == [(390,)]  # 260 + 130


def test_lookml_filtered_list_measure_not_cached_for_expansion():
    """A filtered LIST measure the parser skips must not be cached for later expansion.

    ARRAY_LENGTH(LIST(${amount})) / NULLIF(COUNT(*), 0) with filters is unrepresentable (LIST
    keeps NULLs), so _parse_measure skips it. The expansion prepass must apply the same guard: it
    would otherwise fold only the COUNT(*), cache a PARTIALLY filtered SQL, and let a referencing
    measure inline an UNFILTERED LIST numerator over a filtered denominator -- silently wrong.
    An UNFILTERED LIST measure, and its referencer, must still work.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: ratio { type: number  sql: ARRAY_LENGTH(LIST(${amount})) / NULLIF(COUNT(*), 0) ;; filters: [status: "completed"] }
  measure: outer_m { type: number  sql: ${ratio} * 2 ;; }
  measure: unfiltered_ratio { type: number  sql: ARRAY_LENGTH(LIST(${amount})) / NULLIF(COUNT(*), 0) ;; }
  measure: outer_ok { type: number  sql: ${unfiltered_ratio} * 2 ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("ratio") is None  # filtered LIST is unrepresentable
    # The referencer must NOT carry an inlined, partially-filtered copy. It stays a plain derived
    # ref to the skipped measure, which fails LOUDLY rather than returning a wrong number.
    outer = model.get_metric("outer_m")
    assert outer is None or "LIST" not in (outer.sql or ""), outer.sql if outer else None

    # The unfiltered LIST path is unaffected and still executes.
    assert model.get_metric("unfiltered_ratio") is not None
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, status text)")
    con.execute("insert into orders values (1,100,'completed'),(2,50,'pending'),(3,30,'completed')")
    assert con.execute(layer.compile(metrics=["orders.outer_ok"])).fetchall() == [(2.0,)]


def test_lookml_filter_wrapped_windowed_aggregate_is_not_grouped():
    """A FILTER clause between an aggregate and its OVER window must not read as grouped.

    SUM(x) FILTER (WHERE ...) OVER () nests exp.Filter between the SUM and its window, so a
    direct-parent check saw a non-Window parent and treated the raw windowed aggregate as grouped.
    Walk past Filter so it is correctly rejected as ungrouped (aggregate-unsafe).
    """
    is_safe = LookMLAdapter._mixed_is_aggregate_safe
    assert is_safe("SUM({model}.amount) FILTER (WHERE TRUE) OVER ()", lambda rn: False) is False
    assert is_safe("SUM({model}.amount) OVER ()", lambda rn: False) is False
    # A nested aggregate inside a window still groups its column; a plain aggregate is safe.
    assert is_safe("SUM(COUNT({model}.x)) OVER ()", lambda rn: False) is True
    assert is_safe("SUM({model}.a) / COUNT(*)", lambda rn: False) is True


def test_lookml_zero_column_windowed_filtered_measure_dropped():
    """A filtered complete measure whose filter can be applied neither by folding nor column-nulling
    (a zero-column windowed aggregate) is dropped, not imported with ineffective filters."""
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: winct {
    type: number
    sql: COUNT(*) FILTER (WHERE TRUE) OVER () ;;
    filters: [status: "x"]
  }
  measure: colnull { type: number  sql: STDDEV(${TABLE}.amount) ;; filters: [status: "x"] }
}
"""
    )
    names = {m.name for m in graph.get_model("orders").metrics}
    assert "winct" not in names  # filter cannot be applied any way -> dropped
    assert "colnull" in names  # a column-nullable filtered aggregate is kept
    # The helper distinguishes the two.
    assert LookMLAdapter._generator_column_nulling_suffices("COUNT(*) FILTER (WHERE TRUE) OVER ()") is False
    assert LookMLAdapter._generator_column_nulling_suffices("STDDEV({model}.amount)") is True


def test_lookml_hash_with_space_classified_unsafe_like_no_space():
    """`HASH (col)` (space before the paren) must be classified unsafe just like `HASH(col)`.

    A keyed symmetric-distinct aggregate hashes the key; HASH(NULL) is a non-NULL constant, so
    nulling the key does NOT exclude the row -- column-nulling produces garbage. The unsafe check
    must be whitespace-tolerant, otherwise a `HASH (id)` spelling slips through and the filtered
    measure is silently imported with ineffective column-nulling."""
    no_space = "SUM(DISTINCT HASH({model}.id) * 100 + {model}.amount)"
    with_space = "SUM(DISTINCT HASH ({model}.id) * 100 + {model}.amount)"
    # column-nulling is NOT sufficient for either spelling
    assert LookMLAdapter._generator_column_nulling_suffices(no_space) is False
    assert LookMLAdapter._generator_column_nulling_suffices(with_space) is False
    # ...and both are treated as unsafe-to-null, so folding proceeds rather than early-returning None
    assert LookMLAdapter._fold_complete_sql_filters(no_space, ["{model}.status = 'x'"]) is not None
    assert LookMLAdapter._fold_complete_sql_filters(with_space, ["{model}.status = 'x'"]) is not None


def test_lookml_number_measure_case_with_else_folds_filter():
    """A CASE with an ELSE default survives column-nulling, so its filter must be FOLDED.

    The generator filters a complete measure by nulling the columns its SQL reads, relying on the
    aggregate ignoring NULLs. That fails for COUNT(CASE WHEN status='completed' THEN 1 ELSE 0 END):
    nulling `status` only makes the WHEN false, and ELSE 0 is still non-NULL, so COUNT returns
    EVERY row. A CASE with an ELSE must be treated as unsafe-to-null and folded instead.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  dimension: country { type: string  sql: ${TABLE}.country ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: n_else { type: number  sql: COUNT(CASE WHEN ${status} = 'completed' THEN 1 ELSE 0 END) ;; filters: [country: "US"] }
  measure: n_no_else { type: number  sql: COUNT(CASE WHEN ${status} = 'completed' THEN 1 END) ;; filters: [country: "US"] }
  measure: n_if { type: number  sql: COUNT(IF(${status} = 'completed', 1, 0)) ;; filters: [country: "US"] }
  measure: n_if_no_default { type: number  sql: COUNT(IF(${status} = 'completed', 1)) ;; filters: [country: "US"] }
  measure: total { type: number  sql: SUM(${amount}) ;; filters: [country: "US"] }
}
"""
    )
    model = graph.get_model("orders")
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, status text, country text, amount int)")
    con.execute("insert into orders values (1,'completed','US',10),(2,'pending','US',20),(3,'completed','CA',30)")
    # ELSE 0 counts every row unless the filter is folded: 2 US rows, not 3.
    assert con.execute(layer.compile(metrics=["orders.n_else"])).fetchall() == [(2,)]
    # A CASE with no ELSE nulls out naturally: only the US+completed row.
    assert con.execute(layer.compile(metrics=["orders.n_no_else"])).fetchall() == [(1,)]
    # IF(cond, 1, 0) is the same trap: IF(NULL, 1, 0) is 0, so it must fold too.
    assert con.execute(layer.compile(metrics=["orders.n_if"])).fetchall() == [(2,)]
    # IF with no false branch has no default -> nulls out naturally, must NOT over-fold.
    assert con.execute(layer.compile(metrics=["orders.n_if_no_default"])).fetchall() == [(1,)]
    # A plain aggregate still filters correctly via the generator's column-nulling.
    assert con.execute(layer.compile(metrics=["orders.total"])).fetchall() == [(30,)]


def test_lookml_number_measure_filtered_list_aggregate_skipped():
    """A FILTERED LIST(...) measure has no faithful form and must be skipped, not silently wrong.

    LIST keeps NULL inputs, so neither the generator's column-nulling nor a folded CASE excludes
    a row: LIST(CASE WHEN status='completed' THEN amount END) over 3 rows yields [100, NULL, 30],
    so ARRAY_LENGTH still returns 3 and the filter is ignored. Only a dialect-specific
    FILTER (WHERE ...) clause would work. An UNFILTERED LIST, and a filtered NON-list aggregate,
    must both still import.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: n_completed { type: number  sql: ARRAY_LENGTH(LIST(${amount})) ;; filters: [status: "completed"] }
  measure: n_all { type: number  sql: ARRAY_LENGTH(LIST(${amount})) ;; }
  measure: arr_completed { type: number  sql: ARRAY_LENGTH(ARRAY_AGG(${amount})) ;; filters: [status: "completed"] }
  measure: sum_completed { type: number  sql: SUM(${amount}) ;; filters: [status: "completed"] }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("n_completed") is None  # would have silently ignored its filter
    # ARRAY_AGG retains NULLs just like LIST, so a filtered ARRAY_AGG measure is dropped too.
    assert model.get_metric("arr_completed") is None
    assert model.get_metric("n_all") is not None  # unfiltered LIST is fine
    assert model.get_metric("sum_completed") is not None  # a foldable filtered aggregate is fine

    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, status text)")
    con.execute("insert into orders values (1,100,'completed'),(2,50,'pending'),(3,30,'completed')")
    assert con.execute(layer.compile(metrics=["orders.n_all"])).fetchall() == [(3,)]
    assert con.execute(layer.compile(metrics=["orders.sum_completed"])).fetchall() == [(130,)]


def test_lookml_number_measure_list_aggregate_is_scope_safe():
    """A column inside DuckDB's LIST(...) collector is aggregate-scoped, not raw.

    aggregation_detection counts exp.List as an aggregate, so ARRAY_LENGTH(LIST(${amount}))
    takes the complete-SQL path; the safety check must agree or the column reads as raw and the
    valid measure is dropped. A raw column OUTSIDE the LIST must still be rejected.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: n_amounts { type: number  sql: ARRAY_LENGTH(LIST(${amount})) ;; }
  measure: raw_bad { type: number  sql: ARRAY_LENGTH(LIST(${amount})) + ${amount} ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("n_amounts") is not None  # was dropped as a raw ungrouped column
    assert model.get_metric("raw_bad") is None  # raw column outside the LIST is still unsafe
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int)")
    con.execute("insert into orders values (1,100),(2,50),(3,30)")
    assert con.execute(layer.compile(metrics=["orders.n_amounts"])).fetchall() == [(3,)]


def test_lookml_number_measure_zero_column_aggregate_compiles():
    """A complete-SQL measure whose SQL references no columns (bare COUNT(*)) must still compile.

    ``has_inline_agg`` marks ``type: number  sql: COUNT(*)`` as complete, but the complete-SQL CTE
    only projects columns from _complete_sql_columns(), which is empty for COUNT(*). Without a
    fallback the CTE becomes ``SELECT <nothing> FROM orders``, which every engine rejects. The
    generator must emit a constant projection so the outer COUNT(*) still counts source rows.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: cnt { type: number  sql: COUNT(*) ;; }
  measure: cnt_const { type: number  sql: COUNT(DISTINCT 1) ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("cnt") is not None
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, status text)")
    con.execute("insert into orders values (1,'a'),(2,'b'),(3,'a')")
    # Ungrouped COUNT(*) over the CTE must equal the source row count, not raise a syntax error.
    assert con.execute(layer.compile(metrics=["orders.cnt"])).fetchall() == [(3,)]
    # Grouped by a real dimension it still counts per group.
    assert sorted(con.execute(layer.compile(metrics=["orders.cnt"], dimensions=["orders.status"])).fetchall()) == [
        ("a", 2),
        ("b", 1),
    ]
    # A zero-column DISTINCT constant is also valid.
    assert con.execute(layer.compile(metrics=["orders.cnt_const"])).fetchall() == [(1,)]


def test_lookml_number_measure_constant_dimension_is_aggregate_safe():
    """A CONSTANT-valued dimension in a mixed number measure must not read as a raw column.

    `dimension: tax_rate { sql: 0.07 ;; }` plus `tax = ${total} * ${tax_rate}` resolves to
    `SUM(amount) * 0.07` -- valid aggregate SQL with no ungrouped column. Probing the ref as a
    synthetic `t.tax_rate` column wrongly flagged it as raw and dropped the measure.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: tax_rate { type: number  sql: 0.07 ;; }
  measure: total { type: sum  sql: ${amount} ;; }
  measure: tax { type: number  sql: ${total} * ${tax_rate} ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("tax") is not None  # was dropped as an ungrouped column
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int)")
    con.execute("insert into orders values (1,100),(2,50),(3,30)")
    # DuckDB returns a Decimal for the constant-scaled product; compare numerically.
    (tax_value,) = con.execute(layer.compile(metrics=["orders.tax"])).fetchone()
    assert float(tax_value) == pytest.approx(180 * 0.07)  # SUM(amount) * 0.07


def test_lookml_number_measure_quoted_select_identifier_is_not_a_subquery():
    """A quoted IDENTIFIER named after a reserved word must not read as a subquery.

    A column named `select` is written quoted -- ${TABLE}."select", `select`, [select] -- and has
    no subquery, but a bare \\bselect\\b scan matched it and dropped the measure. Every quoted form
    is blanked before scanning; a REAL subquery is still detected.
    """
    conv = LookMLAdapter._has_subquery
    assert not conv('SUM({model}."select")')  # double-quoted (standard / Postgres / DuckDB)
    assert not conv("SUM(`select`)")  # backtick (BigQuery / MySQL)
    assert not conv("SUM([select])")  # bracket (SQL Server)
    assert not conv("SUM(CASE WHEN {model}.s = 'select' THEN {model}.a END)")  # string VALUE
    assert conv("SUM({model}.a) / (SELECT SUM(x) FROM t)")  # real subquery
    assert conv('SUM({model}."col") / (SELECT 1)')  # quoted id AND a real subquery

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  measure: s { type: number  sql: SUM(${TABLE}."select") ;; }
  measure: subq { type: number  sql: SUM(${TABLE}.a) / NULLIF((SELECT SUM(x) FROM t), 0) ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("s") is not None  # was dropped as a phantom subquery
    assert model.get_metric("subq") is None  # a real subquery is still skipped

    # The quoted column must round-trip all the way to a WORKING query -- the complete-SQL CTE
    # projection has to keep the quoting (`"select" AS s__select__cmpl`), not emit a bare `select`.
    import duckdb

    from sidemantic import SemanticLayer

    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    sql = layer.compile(metrics=["orders.s"])
    con = duckdb.connect()
    con.execute('create table orders as select 1 id, 5 "select" union all select 2, 15')
    assert con.execute(sql).fetchall() == [(20,)]  # SUM of the quoted `select` column


def test_lookml_complete_measure_reserved_column_sqlglot_misses_is_quoted():
    """A reserved column sqlglot does NOT auto-quote (e.g. `group`) must still round-trip.

    sqlglot's DuckDB dialect leaves `group` bare as a column, but DuckDB rejects `group AS ...`. The
    CTE projection mirrors the SOURCE quoting instead: a column quoted in the source stays quoted on
    BOTH projection paths (the dedicated __cmpl alias and the metric-filter raw column), while an
    ordinary column is NOT over-quoted (over-quoting would fold-break case-sensitive Postgres names).
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: grp { type: number  sql: ${TABLE}."group" ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: total { type: sum  sql: ${amount} ;; }
  measure: m { type: number  sql: SUM(${grp}) / NULLIF(${total}, 0) ;; }
}
"""
    )
    model = graph.get_model("orders")
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    sql = layer.compile(metrics=["orders.m"])

    assert "group AS group" not in sql  # the reserved column is not projected bare
    assert '"amount" AS' not in sql  # an ordinary column is NOT over-quoted

    con = duckdb.connect()
    con.execute('create table orders as select 1 id, 5 "group", 10 amount union all select 2, 15, 20')
    # SUM("group") = 20, SUM(amount) = 30 -> 20/30.
    assert con.execute(sql).fetchall() == [(20 / 30,)]


def test_lookml_number_measure_dimension_ref_expanding_to_subquery_is_skipped():
    """A dimension ref that EXPANDS to a subquery must be caught after resolution, not just raw.

    The pre-resolution guard only sees `SUM(${target})`; once `target` expands to a scalar subquery
    the complete-SQL builder would rewrite the subquery's OWN columns to this measure's CTE aliases,
    producing wrong correlated SQL. Re-checking the resolved expression skips it; a normal complete
    measure over the same shape is still imported.
    """
    skipped = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: target { type: number  sql: (SELECT target FROM targets WHERE targets.id = ${TABLE}.id) ;; }
  measure: total { type: sum  sql: ${amount} ;; }
  measure: m { type: number  sql: SUM(${target}) / NULLIF(${total}, 0) ;; }
}
"""
    ).get_model("orders")
    assert skipped.get_metric("m") is None  # dim ref expanded to a subquery -> skipped

    kept = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: total { type: sum  sql: ${amount} ;; }
  measure: m { type: number  sql: SUM(${amount}) / NULLIF(${total}, 0) ;; }
}
"""
    ).get_model("orders")
    assert kept.get_metric("m") is not None  # ordinary complete measure still imported


def test_lookml_number_measure_select_in_string_literal_is_not_a_subquery():
    """The word `select` inside a string VALUE must not be mistaken for a subquery.

    `SUM(CASE WHEN ${status} = 'select' THEN ${amount} END)` has no subquery and is a valid
    inline aggregate; a raw \\bselect\\b scan matched the literal and dropped it. A REAL subquery
    must still be skipped.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: sel { type: number  sql: SUM(CASE WHEN ${status} = 'select' THEN ${amount} END) ;; }
  measure: subq { type: number  sql: SUM(${amount}) / NULLIF((SELECT SUM(amount) FROM orders), 0) ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("sel") is not None  # literal 'select' is not a subquery
    assert model.get_metric("subq") is None  # a real subquery is still skipped
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, status text)")
    con.execute("insert into orders values (1,100,'completed'),(2,50,'select'),(3,30,'completed')")
    assert con.execute(layer.compile(metrics=["orders.sel"])).fetchall() == [(50,)]


def test_lookml_number_measure_select_in_comment_is_not_a_subquery():
    """The word `select` inside a SQL COMMENT must not be mistaken for a subquery.

    A comment like `/* select paid rows */ SUM(amount)` has no subquery; leaving the comment text
    in the scan matched `select` and dropped a valid inline aggregate. Comments are blanked with
    quoted tokens, and a real subquery is still skipped.
    """
    conv = LookMLAdapter._has_subquery
    assert conv("/* select paid rows */ SUM(amount)") is False
    assert conv("-- select paid rows\nSUM(amount)") is False
    assert conv("SUM(amount) / (SELECT COUNT(*) FROM t)") is True  # real subquery still detected

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: commented { type: number  sql: /* select paid rows */ SUM(${amount}) / NULLIF(COUNT(*), 0) ;; }
}
"""
    )
    assert graph.get_model("orders").get_metric("commented") is not None  # comment is not a subquery


def test_lookml_number_measure_ref_named_select_is_not_a_subquery():
    """A `${...}` reference to a field named `select` must not look like a subquery.

    The subquery scan runs on the RAW SQL before refs are resolved, so `SUM(${select})` matched
    `select` inside the placeholder and dropped a valid inline aggregate. Placeholders are blanked
    with quotes/comments; a real subquery alongside a ref is still detected.
    """
    conv = LookMLAdapter._has_subquery
    assert conv("SUM(${select})") is False
    assert conv("SUM(${orders.select})") is False
    assert conv("SUM(${select}) / (SELECT COUNT(*) FROM t)") is True  # real subquery still detected

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: select { type: number  sql: ${TABLE}."select" ;; }
  measure: m { type: number  sql: SUM(${select}) / NULLIF(COUNT(*), 0) ;; }
}
"""
    )
    metric = graph.get_model("orders").get_metric("m")
    assert metric is not None  # ${select} ref is not a subquery
    assert '"select"' in metric.sql  # resolved to the quoted column


def test_lookml_number_measure_unsafe_intermediate_not_expandable():
    """A number measure _parse_measure skips as unsafe must not be inlined into a later measure.

    `bad = ${total} + ${amount}` mixes an aggregate measure with a RAW dimension, so it is dropped
    on import. The expansion prepass must apply the same aggregate-safety check, otherwise it
    caches `SUM(amount) + amount` and a later `outer = ${bad} / NULLIF(COUNT(*), 0)` inlines that
    raw ungrouped column and fails on grouped queries -- instead of `outer` being unavailable too.
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: total { type: sum  sql: ${amount} ;; }
  measure: bad { type: number  sql: ${total} + ${amount} ;; }
  measure: outer_m { type: number  sql: ${bad} / NULLIF(COUNT(*), 0) ;; }
}
"""
    )
    model = graph.get_model("orders")
    names = {m.name for m in model.metrics}
    assert "bad" not in names  # mixed aggregate + raw ungrouped column -> unsupported
    assert "outer_m" not in names  # must NOT inline the unsafe intermediate
    assert "total" in names  # the valid base measure is unaffected


def test_lookml_number_measure_expands_chained_derived_ref():
    """A number measure referencing another DERIVED number measure must be kept, not dropped.

    `gross_margin = ${revenue} - ${cost_total}` is itself derived; `avg_margin =
    ${gross_margin} / NULLIF(COUNT(*), 0)` has an inline aggregate, so it needs the complete-SQL
    path. gross_margin must be recursively expandable (else avg_margin is dropped as unexpandable).
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: cost { type: number  sql: ${TABLE}.cost ;; }
  measure: revenue { type: sum  sql: ${amount} ;; }
  measure: cost_total { type: sum  sql: ${cost} ;; }
  measure: gross_margin { type: number  sql: ${revenue} - ${cost_total} ;; }
  measure: avg_margin { type: number  sql: ${gross_margin} / NULLIF(COUNT(*), 0) ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("avg_margin") is not None  # was dropped as unexpandable before
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, cost int)")
    con.execute("insert into orders values (1,100,10),(2,50,20),(3,30,5)")
    # gross_margin = (100+50+30) - (10+20+5) = 180 - 35 = 145; avg_margin = 145 / 3
    assert con.execute(layer.compile(metrics=["orders.gross_margin"])).fetchall() == [(145,)]
    assert con.execute(layer.compile(metrics=["orders.avg_margin"])).fetchall() == [(145 / 3,)]


def test_lookml_number_measure_mixed_filter_clause_kept():
    """A mixed expr using an aggregate FILTER (WHERE ...) clause is aggregate-safe and kept."""
    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: total { type: sum  sql: ${TABLE}.amount ;; }
  measure: m { type: number  sql: ${o.total} / NULLIF(SUM(${o.amount}) FILTER (WHERE ${o.status} = 'completed'), 0) ;; }
}
"""
    )
    m = graph.get_model("o").get_metric("m")
    assert m is not None and m.sql_is_complete is True  # FILTER predicate cols are aggregate-safe


def test_lookml_number_measure_inline_aggregate_cases_execute():
    """A number measure with an INLINE aggregate is opaque/complete and executes correctly.

    Covers a measure ref divided by an inline aggregate (${count}/COUNT(*)) and an
    inline-aggregate-over-dimension measure with filters (SUM(${amount}) filters: ...) --
    both previously mis-routed to the derived path (bare-token / unfiltered SQL).
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: completed_count { type: count  filters: [status: "completed"] }
  measure: rate { type: number  sql: ${o.completed_count} / NULLIF(COUNT(*), 0) ;; }
  measure: completed_amt { type: number  sql: SUM(${o.amount}) ;; filters: [status: "completed"] }
}
"""
    )
    model = graph.get_model("o")
    assert model.get_metric("rate").sql_is_complete is True
    assert model.get_metric("completed_amt").sql_is_complete is True
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, amount int, status text)")
    con.execute("insert into o values (1,10,'completed'),(2,30,'pending')")
    assert con.execute(layer.compile(metrics=["o.rate"])).fetchall() == [(0.5,)]  # 1 completed / 2 rows
    assert con.execute(layer.compile(metrics=["o.completed_amt"])).fetchall() == [(10,)]  # filtered SUM


def test_lookml_number_measure_zero_column_aggregate_filter_applied():
    """A complete number measure whose aggregate has NO foldable column still honors filters.

    The generator filters a complete-SQL measure by nulling the raw columns it references,
    but COUNT(*) has none to null, so the filter would be silently dropped (and a mix like
    COUNT(*)/COUNT(DISTINCT id) would filter inconsistently). The adapter folds the filter
    into every aggregate via CASE WHEN so the result is correct.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: completed_num { type: number  sql: COUNT(*) ;; filters: [status: "completed"] }
  measure: completed_ratio { type: number  sql: COUNT(*) / NULLIF(COUNT(DISTINCT ${id}), 0) ;; filters: [status: "completed"] }
}
"""
    )
    model = graph.get_model("o")
    completed_num = model.get_metric("completed_num")
    # Filter folded INTO the aggregate (not left as a separate, ignored filter).
    assert "CASE WHEN" in completed_num.sql
    assert not completed_num.filters
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, status text)")
    con.execute("insert into o values (1,'completed'),(2,'pending'),(3,'completed'),(4,'cancelled')")
    assert con.execute(layer.compile(metrics=["o.completed_num"])).fetchall() == [(2,)]  # 2 of 4
    assert con.execute(layer.compile(metrics=["o.completed_ratio"])).fetchall() == [(1.0,)]  # 2 completed / 2 distinct


def test_lookml_number_measure_expands_distinct_base_measure():
    """A complete number expr referencing a supported distinct measure must EXPAND it, not drop.

    sum_distinct/average_distinct/etc. are not in _SQL_AGG_FUNC, so they were missing from
    measure_full_sql_lookup and the unexpandable check dropped the whole metric. They now
    expand via _parse_distinct_measure's generated SQL.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: sd_total { type: sum_distinct  sql: ${amount} ;;  sql_distinct_key: ${id} ;; }
  measure: rate { type: number  sql: ${sd_total} / NULLIF(SUM(${amount}), 0) ;; }
}
"""
    )
    model = graph.get_model("o")
    rate = model.get_metric("rate")
    assert rate is not None and rate.sql_is_complete is True  # not dropped as unexpandable
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, amount int)")
    con.execute("insert into o values (1,10),(2,20)")
    # distinct-by-key sum = 30; SUM(amount) = 30 -> 1.0
    assert con.execute(layer.compile(metrics=["o.rate"])).fetchall() == [(1.0,)]


def test_lookml_number_measure_keyed_distinct_ref_with_own_filter_executes():
    """A complete measure's own filter over an expanded KEYED distinct must fold (HASH-safe).

    The keyed symmetric-distinct aggregate hashes the key; nulling the key for excluded rows
    gives HASH(NULL) (a non-NULL constant), so column-nulling leaves garbage. The filter must
    fold into the aggregate args instead.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: country { type: string  sql: ${TABLE}.country ;; }
  measure: sd_total { type: sum_distinct  sql: ${amount} ;;  sql_distinct_key: ${id} ;; }
  measure: us_rate { type: number  sql: ${sd_total} / NULLIF(SUM(${amount}), 0) ;;  filters: [country: "US"] }
}
"""
    )
    model = graph.get_model("o")
    us_rate = model.get_metric("us_rate")
    assert us_rate is not None and not us_rate.filters  # folded into the HASH aggregate, not column-nulled
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, amount int, country text)")
    con.execute("insert into o values (1,10,'US'),(2,20,'US'),(3,1000,'CA')")
    # US distinct-by-key sum = 30; US SUM(amount) = 30 -> 1.0 (the CA 1000 excluded, not garbage)
    assert con.execute(layer.compile(metrics=["o.us_rate"])).fetchall() == [(1.0,)]


def test_lookml_number_measure_count_star_sql_with_filter_expands_validly():
    """A referenced `type: count sql: * ;;` with filters must expand as COUNT(CASE..THEN 1), not THEN *."""
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: completed_ct { type: count  sql: * ;;  filters: [status: "completed"] }
  measure: rate { type: number  sql: ${completed_ct} / NULLIF(COUNT(*), 0) ;; }
}
"""
    )
    model = graph.get_model("o")
    rate = model.get_metric("rate")
    assert rate is not None and "THEN * END" not in (rate.sql or "")  # no invalid star-in-CASE
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, status text)")
    con.execute("insert into o values (1,'completed'),(2,'pending'),(3,'completed'),(4,'x')")
    assert con.execute(layer.compile(metrics=["o.rate"])).fetchall() == [(0.5,)]  # 2 of 4


def test_lookml_number_measure_filtered_distinct_ref_not_silently_unfiltered():
    """A FILTERED distinct base measure can't expand faithfully -> the referencing expr is dropped.

    _parse_distinct_measure doesn't fold the measure's own filters (the keyed symmetric /
    quantile forms have no single predicate slot), so expanding a filtered distinct would
    silently produce an UNfiltered result. Skip it (drop the ref) rather than mislead; an
    UNfiltered distinct still expands.
    """
    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: completed_sd { type: sum_distinct  sql: ${amount} ;;  sql_distinct_key: ${id} ;;  filters: [status: "completed"] }
  measure: rate { type: number  sql: ${completed_sd} / NULLIF(SUM(${amount}), 0) ;; }
  measure: sd_plain { type: sum_distinct  sql: ${amount} ;;  sql_distinct_key: ${id} ;; }
  measure: rate_plain { type: number  sql: ${sd_plain} / NULLIF(SUM(${amount}), 0) ;; }
}
"""
    )
    model = graph.get_model("o")
    assert model.get_metric("rate") is None  # filtered distinct ref dropped (not unfiltered)
    assert model.get_metric("rate_plain") is not None  # unfiltered distinct still expands


def test_lookml_number_measure_own_filter_with_null_predicate_folds():
    """A complete measure's own filter must fold (not null columns) when SQL tests col IS NULL.

    null_status_ct expands to COUNT(CASE WHEN status IS NULL THEN 1 END); applying the
    measure's own country='US' filter by NULLING status would make status IS NULL true for
    non-US rows, inflating the count. The filter must fold into the aggregate predicate.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  dimension: country { type: string  sql: ${TABLE}.country ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: null_status_ct { type: count  filters: [status: "NULL"] }
  measure: us_rate { type: number  sql: ${null_status_ct} / NULLIF(SUM(${amount}), 0) ;;  filters: [country: "US"] }
}
"""
    )
    model = graph.get_model("o")
    us_rate = model.get_metric("us_rate")
    assert us_rate is not None and not us_rate.filters  # own filter folded into the SQL
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, status text, country text, amount int)")
    con.execute("insert into o values (1,NULL,'US',10),(2,'x','US',10),(3,NULL,'CA',10)")
    # numerator = US rows with status NULL = 1 (id1, NOT id3/CA); denom = SUM US amount = 20
    assert con.execute(layer.compile(metrics=["o.us_rate"])).fetchall() == [(0.05,)]


def test_lookml_number_measure_ordered_set_aggregate_kept_and_executes():
    """A number measure using an ordered-set aggregate (WITHIN GROUP) is a valid aggregate.

    sqlglot nests the ORDER BY column under exp.WithinGroup (not exp.AggFunc), so the
    aggregate-safety check must accept it; otherwise the measure is wrongly dropped as a
    'raw ungrouped column'.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: median_amt { type: number  sql: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${amount}) ;; }
}
"""
    )
    model = graph.get_model("o")
    median = model.get_metric("median_amt")
    assert median is not None  # not dropped
    assert median.sql_is_complete is True
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, amount int)")
    con.execute("insert into o values (1,10),(2,20),(3,30)")
    assert con.execute(layer.compile(metrics=["o.median_amt"])).fetchall() == [(20,)]


def test_lookml_number_measure_anonymous_aggregate_filter_folded():
    """A filter over a mix of anonymous aggregate + zero-column aggregate must fold into BOTH.

    PRODUCT(amount) / COUNT(*) with filters: the COUNT(*) forces the fold path; PRODUCT is an
    exp.Anonymous, so without including it the fold would leave PRODUCT over ALL rows while
    COUNT is filtered -> wrong. Both must carry the filter.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: country { type: string  sql: ${TABLE}.country ;; }
  measure: prod_rate { type: number  sql: PRODUCT(${amount}) / NULLIF(COUNT(*), 0) ;;  filters: [country: "US"] }
}
"""
    )
    model = graph.get_model("o")
    prod_rate = model.get_metric("prod_rate")
    assert prod_rate is not None and not prod_rate.filters  # folded into both aggregates
    assert "PRODUCT(CASE WHEN" in prod_rate.sql  # PRODUCT arg wrapped, not left unfiltered
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, amount int, country text)")
    con.execute("insert into o values (1,2,'US'),(2,3,'US'),(3,100,'CA')")
    # US: PRODUCT(2,3)=6 / COUNT(US)=2 -> 3.0 (the CA 100 excluded from BOTH, not just COUNT)
    assert con.execute(layer.compile(metrics=["o.prod_rate"])).fetchall() == [(3.0,)]


def test_lookml_number_measure_anonymous_aggregate_kept_and_executes():
    """A number measure using an anonymous/engine-specific aggregate (PRODUCT) is valid.

    sqlglot parses PRODUCT/ENTROPY/WEIGHTED_AVG etc. as exp.Anonymous (not exp.AggFunc), but
    sidemantic recognizes them as aggregates; the aggregate-safety check must accept their
    column args so the measure imports as complete SQL instead of being dropped as raw.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: prod_amt { type: number  sql: PRODUCT(${amount}) ;; }
}
"""
    )
    model = graph.get_model("o")
    prod = model.get_metric("prod_amt")
    assert prod is not None and prod.sql_is_complete is True
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, amount int)")
    con.execute("insert into o values (1,2),(2,3),(3,4)")
    assert con.execute(layer.compile(metrics=["o.prod_amt"])).fetchall() == [(24,)]  # 2*3*4


def test_lookml_number_measure_ordered_set_aggregate_with_filter_executes():
    """A FILTERED ordered-set aggregate must filter the ORDER BY values, not the constant.

    The zero-column fold must NOT wrap the percentile fraction (PERCENTILE_CONT(CASE ...) is
    a non-constant parameter DuckDB rejects). The ORDER-BY column has a column, so the filter
    is applied by the generator's column-nulling (NULLs are ignored by the percentile).
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: median_done { type: number  sql: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${amount}) ;; filters: [status: "completed"] }
}
"""
    )
    model = graph.get_model("o")
    median = model.get_metric("median_done")
    assert median is not None
    assert "PERCENTILE_CONT(CASE" not in (median.sql or "")  # constant param not wrapped
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    con = duckdb.connect()
    con.execute("create table o(id int, amount int, status text)")
    con.execute("insert into o values (1,10,'completed'),(2,30,'completed'),(3,50,'completed'),(4,1000,'pending')")
    # median of {10,30,50} = 30; the pending 1000 must be excluded
    assert con.execute(layer.compile(metrics=["o.median_done"])).fetchall() == [(30.0,)]


def test_lookml_number_measure_with_subquery_skipped():
    """A number measure containing a scalar subquery is skipped (complete-SQL can't represent it).

    The complete-SQL builder rewrites every parsed column to the measure's CTE raw alias,
    including columns INSIDE the subquery, producing a wrong correlated query -- so skip.
    """
    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: pct_target { type: number  sql: SUM(${amount}) / NULLIF((SELECT SUM(amount) FROM targets), 0) ;; }
  measure: ok_ratio { type: number  sql: SUM(${amount}) / NULLIF(COUNT(*), 0) ;; }
}
"""
    )
    model = graph.get_model("o")
    assert model.get_metric("pct_target") is None  # subquery -> skipped
    assert model.get_metric("ok_ratio") is not None  # ordinary inline-aggregate still kept


def test_lookml_number_measure_mixed_windowed_aggregate_skipped():
    """A mixed expr with a WINDOW aggregate over a raw column is NOT safe -> skipped.

    SUM(x) OVER () runs after grouping, so a raw column there is still ungrouped and would
    be rejected in a grouped SELECT; the measure must be dropped, not imported as invalid.
    """
    graph = _parse_lkml(
        """
view: o {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: total { type: sum  sql: ${TABLE}.amount ;; }
  measure: m { type: number  sql: ${o.total} / NULLIF(SUM(${o.amount}) OVER (), 0) ;; }
}
"""
    )
    assert graph.get_model("o").get_metric("m") is None  # windowed raw column -> skipped


def test_lookml_number_measure_aggregate_nested_in_window_is_kept():
    """A window OVER an already-aggregated column is safe and must be imported.

    In SUM(SUM(x)) OVER () the inner SUM groups the raw column before the window runs, so the
    outer window aggregates a grouped value -- valid in a grouped SELECT. The blanket "any column
    under a window is unsafe" check wrongly dropped this percent-of-total-style measure; only a
    RAW window argument (SUM(x) OVER ()) is unsafe.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: region { type: string  sql: ${TABLE}.region ;; }
  measure: total { type: sum  sql: ${amount} ;; }
  measure: pct_of_total { type: number  sql: ${total} / NULLIF(SUM(SUM(${amount})) OVER (), 0) ;; }
}
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("pct_of_total") is not None  # nested-agg window kept, not dropped

    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    sql = layer.compile(metrics=["orders.pct_of_total"], dimensions=["orders.region"])
    con = duckdb.connect()
    con.execute(
        "create table orders as select 1 id, 10.0 amount, 'e' region "
        "union all select 2, 30, 'w' union all select 3, 60, 'w'"
    )
    # region totals 10 and 90 out of 100 overall.
    assert dict(con.execute(sql).fetchall()) == {"e": 0.1, "w": 0.9}


def test_lookml_filtered_windowed_aggregate_folds_into_inner_aggregate():
    """A filter on a windowed aggregate must fold into its INNER aggregate, not the window arg.

    For COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0) with a filter, wrapping the outer windowed
    SUM's argument put the filter column inside the window, ungrouped -- DuckDB rejected it. The
    inner COUNT(*) is a grouped aggregate that can carry the filter, so fold there; a windowed
    aggregate over a RAW column has nothing to carry it and is dropped rather than mis-filtered.
    """
    import duckdb

    from sidemantic import SemanticLayer

    # The predicate folds into the inner COUNT, never the outer windowed SUM's argument.
    assert LookMLAdapter._fold_complete_sql_filters(
        "COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0)", ["{model}.status = 'x'"]
    ) == (
        "COUNT(CASE WHEN {model}.status = 'x' THEN 1 END) / "
        "NULLIF(SUM(COUNT(CASE WHEN {model}.status = 'x' THEN 1 END)) OVER (), 0)"
    )
    # No inner aggregate to carry the filter -> the fold aborts (returns None).
    assert LookMLAdapter._fold_complete_sql_filters("SUM(amount) OVER ()", ["{model}.status = 'x'"]) is None
    # A FILTER clause nests exp.Filter between the aggregate and its OVER window; the windowed
    # check must still see the window (via the Filter) and abort rather than fold the arg into the
    # window -- COUNT(CASE ...) OVER () would put the filter column ungrouped inside the window.
    assert (
        LookMLAdapter._fold_complete_sql_filters(
            "COUNT(*) FILTER (WHERE TRUE) OVER ()", ["{model}.status = 'x'"], force=True
        )
        is None
    )

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: pct { type: number  sql: COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0) ;; filters: [status: "x"] }
}
"""
    )
    model = graph.get_model("orders")
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    sql = layer.compile(metrics=["orders.pct"])
    con = duckdb.connect()
    con.execute("create table orders as select 1 id, 'x' status union all select 2, 'x' union all select 3, 'y'")
    # 2 rows match the filter; each is its own group, summed over the window = 2, so 2/2 rows -> 1.0.
    assert con.execute(sql).fetchall() == [(1.0,)]


def test_lookml_filtered_multi_column_distinct_folds_around_the_tuple():
    """A filtered multi-column DISTINCT must fold the predicate, not rely on column-nulling.

    Nulling the columns of an excluded row yields the tuple (NULL, NULL), which is NOT a NULL value,
    so COUNT(DISTINCT (a, b)) counts that phantom tuple once and inflates the result by one. Fold
    the predicate around the tuple instead; a single-column distinct stays on the safe nulling path.
    """
    import duckdb

    from sidemantic import SemanticLayer

    # Multi-column distinct (tuple and comma forms) folds; single-column stays on the nulling path.
    assert (
        LookMLAdapter._fold_complete_sql_filters("COUNT(DISTINCT ({model}.uid, {model}.oid))", ["{model}.s = 'x'"])
        == "COUNT(DISTINCT CASE WHEN {model}.s = 'x' THEN ({model}.uid, {model}.oid) END)"
    )
    assert (
        LookMLAdapter._fold_complete_sql_filters("COUNT(DISTINCT {model}.uid, {model}.oid)", ["{model}.s = 'x'"])
        is not None
    )
    assert LookMLAdapter._fold_complete_sql_filters("COUNT(DISTINCT {model}.uid)", ["{model}.s = 'x'"]) is None

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: uid { type: number  sql: ${TABLE}.uid ;; }
  dimension: oid { type: number  sql: ${TABLE}.oid ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: pairs { type: number  sql: COUNT(DISTINCT (${uid}, ${oid})) ;; filters: [status: "completed"] }
}
"""
    )
    model = graph.get_model("orders")
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    sql = layer.compile(metrics=["orders.pairs"])
    con = duckdb.connect()
    con.execute(
        "create table orders as select 1 id, 1 uid, 10 oid, 'completed' status "
        "union all select 2, 2, 20, 'pending' union all select 3, 1, 10, 'completed'"
    )
    # Only the two 'completed' rows, both (1, 10) -> one distinct pair. Nulling would give 2.
    assert con.execute(sql).fetchall() == [(1,)]


def test_lookml_complete_measure_own_filter_resolves_renamed_dimension():
    """A COMPLETE (mixed) measure's OWN filter on a renamed dimension resolves to the real column."""
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: state { type: string  sql: ${TABLE}.status ;; }
  measure: total { type: sum  sql: ${TABLE}.amount ;; }
  measure: m { type: number  sql: ${orders.total} / NULLIF(SUM(${orders.amount}), 0) ;; filters: [state: "completed"] }
}
"""
    )
    m = graph.get_model("orders").get_metric("m")
    assert m is not None and m.sql_is_complete is True
    assert any("status" in f for f in (m.filters or []))  # renamed dim resolved to its column
    assert not any("{model}.state" in f for f in (m.filters or []))  # not the bare dim name
    layer = SemanticLayer(auto_register=False)
    layer.add_model(graph.get_model("orders"))
    sql = layer.compile(metrics=["orders.m"])
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, status text)")
    con.execute("insert into orders values (1,10,'completed'),(2,30,'pending')")
    assert con.execute(sql).fetchall()  # executes (no raw `state` column error)


def test_lookml_measure_filter_strips_self_view_qualifier():
    """A measure filter with a view-qualified field (orders.status) must not double-qualify."""
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: completed_total { type: sum  sql: ${TABLE}.amount ;; filters: [orders.status: "completed"] }
  measure: m { type: number  sql: ${orders.completed_total} / NULLIF(SUM(${orders.amount}), 0) ;; }
}
"""
    )
    m = graph.get_model("orders").get_metric("m")
    assert m is not None and "{model}.status" in m.sql and "orders.status" not in m.sql
    layer = SemanticLayer(auto_register=False)
    layer.add_model(graph.get_model("orders"))
    sql = layer.compile(metrics=["orders.m"])
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, status text)")
    con.execute("insert into orders values (1,10,'completed'),(2,30,'pending')")
    assert con.execute(sql).fetchall() == [(0.25,)]


def test_lookml_number_measure_mixed_filtered_base_measure_resolves_renamed_dimension():
    """Folding a base measure's filter must resolve the filter field through its dimension SQL."""
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  dimension: state { type: string  sql: ${TABLE}.status ;; }
  measure: completed_total { type: sum  sql: ${TABLE}.amount ;; filters: [state: "completed"] }
  measure: m { type: number  sql: ${orders.completed_total} / NULLIF(SUM(${orders.amount}), 0) ;; }
}
"""
    )
    m = graph.get_model("orders").get_metric("m")
    assert m is not None and "status" in m.sql and "{model}.state" not in m.sql  # renamed col resolved
    layer = SemanticLayer(auto_register=False)
    layer.add_model(graph.get_model("orders"))
    sql = layer.compile(metrics=["orders.m"])
    con = duckdb.connect()
    con.execute("create table orders(id int, amount int, status text)")
    con.execute("insert into orders values (1,10,'completed'),(2,30,'pending')")
    assert con.execute(sql).fetchall() == [(0.25,)]  # 10 / (10+30)


def test_lookml_number_measure_mixed_filtered_count_ref_keeps_filter():
    """A filtered `type: count` (no sql) base measure expanded in a mixed expr keeps its filter."""
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: completed_count { type: count  filters: [status: "completed"] }
  measure: rate { type: number  sql: ${orders.completed_count} / NULLIF(COUNT(${orders.id}), 0) ;; }
}
"""
    )
    rate = graph.get_model("orders").get_metric("rate")
    assert rate is not None and "CASE WHEN" in rate.sql  # count filter folded, not bare COUNT(*)
    layer = SemanticLayer(auto_register=False)
    layer.add_model(graph.get_model("orders"))
    sql = layer.compile(metrics=["orders.rate"])
    con = duckdb.connect()
    con.execute("create table orders(id int, status text)")
    con.execute("insert into orders values (1,'completed'),(2,'pending'),(3,'completed'),(4,'pending')")
    assert con.execute(sql).fetchall() == [(0.5,)]  # 2 completed / 4 total


def test_lookml_number_measure_bare_column_dimension_row_level_skipped():
    """A number measure over a bare-column dimension with no aggregate is row-level -> skipped."""
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: o ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: bare { sql: amount ;; }
  measure: m { type: number  sql: ${orders.bare} / 2 ;; }
}
"""
    )
    assert graph.get_model("orders").get_metric("m") is None  # no aggregate -> not a valid measure


def test_lookml_dimension_referencing_compact_dimension_resolves():
    """A dimension whose sql references a COMPACT dimension must resolve, not leak ${name}."""
    graph = _parse_lkml(
        """
view: inventory_items {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: cost {}
  dimension: cost_x2 { sql: ${inventory_items.cost} * 2 ;; }
}
"""
    )
    cost_x2 = graph.get_model("inventory_items").get_dimension("cost_x2")
    assert "${" not in cost_x2.sql
    assert "{model}.cost" in cost_x2.sql


def test_lookml_deep_reference_chain_resolves_fully():
    """Reference chains longer than 10 must resolve fully (no fixed-depth truncation)."""
    dims = "\n".join(f"  dimension: d{i} {{ type: number  sql: ${{d{i - 1}}} + 1 ;; }}" for i in range(1, 13))
    graph = _parse_lkml(
        f"view: v {{\n  sql_table_name: t ;;\n"
        f"  dimension: d0 {{ primary_key: yes  type: number  sql: ${{TABLE}}.x ;; }}\n{dims}\n}}"
    )
    d12 = graph.get_model("v").get_dimension("d12")
    assert "${" not in d12.sql


def test_lookml_self_referential_dimension_terminates():
    """A self-referential dimension must terminate (no infinite loop / runaway expansion)."""
    graph = _parse_lkml(
        "view: v { sql_table_name: t ;; "
        "dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
        "dimension: selfd { type: number  sql: ${selfd} + 1 ;; } }"
    )
    selfd = graph.get_model("v").get_dimension("selfd")
    # Resolution stops at the cycle rather than expanding many levels deep.
    assert selfd.sql.count("+ 1") <= 2


def test_lookml_view_with_unsupported_derived_table_not_defaulted():
    """A view with a derived_table the adapter can't read must NOT get a default physical table."""
    adapter = LookMLAdapter()
    model = adapter._parse_view(
        {
            "name": "ndt_unknown",
            # derived_table with no sql / explore_source the adapter understands
            "derived_table": {"persist_for": "1 hour"},
            "dimensions": [{"name": "id", "type": "number", "sql": "${TABLE}.id"}],
        }
    )
    assert model.table is None  # not fabricated as a physical table named after the view


def test_lookml_time_timeframe_keeps_second_precision():
    """Looker's "time" timeframe keeps to-the-second precision, not hour."""
    graph = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension_group: created {
    type: time
    timeframes: [time, date, minute]
    sql: ${TABLE}.created_at ;;
  }
}
"""
    )
    model = graph.get_model("v")
    assert model.get_dimension("created_time").granularity == "second"
    assert model.get_dimension("created_minute").granularity == "minute"
    assert model.get_dimension("created_date").granularity == "day"


def test_lookml_native_suffix_contradicting_grain_uses_grain():
    """A native dim whose name suffix CONTRADICTS its granularity preserves the GRAIN, not the name.

    created_time at hour granularity must export as [hour] (-> created_hour), not [time]
    (which imports as second), so queries keep grouping by hour.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="created_time", type="time", granularity="hour", sql="ts"),  # suffix `time` != hour
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    grains = {
        d.name: d.granularity for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions if d.type == "time"
    }
    assert grains == {"created_hour": "hour"}  # grain preserved (not silently downgraded to second)


def test_lookml_native_minute15_name_does_not_widen_grain():
    """A native created_minute15 at MINUTE grain must not export [minute15] (15-min buckets).

    The coarse mapping of minute15 equals `minute`, so the old name-suffix inference emitted
    [minute15] -- silently re-bucketing 1-minute data into 15-minute intervals. Inexact
    suffixes are now excluded from name inference, so it exports [minute] (true grain).
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="created_minute15", type="time", granularity="minute", sql="ts"),  # no meta
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    txt = open(out).read()
    assert "minute15" not in txt  # no silent grain widening
    assert "minute" in txt
    # meta-preserved minute15 still round-trips as a 15-min bucket (import path unaffected)
    graph2 = SemanticGraph()
    graph2.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(
                    name="created_minute15",
                    type="time",
                    granularity="minute",
                    sql="ts",
                    meta={"lookml_timeframe": "minute15"},
                ),
            ],
        )
    )
    out2 = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph2, out2)
    assert "minute15" in open(out2).read()


def test_lookml_native_inexact_timeframe_suffix_preserves_grain_not_name():
    """NATIVE inexact-suffix time dims (no meta) preserve the GRAIN, even if the name changes.

    minute15 / minute30 / millisecond / microsecond / time_of_day map MANY-to-one onto a
    coarser-or-finer sidemantic grain, so a native created_minute15 at MINUTE grain must NOT
    export [minute15] (which buckets into 15-min intervals -- a silent grain change). It
    exports at the true grain instead, so the field re-imports renamed (created_minute) but
    with the CORRECT queryable grain. Exact name round-trip for these needs preserved
    meta['lookml_timeframe'] (the import path), covered separately.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    for name, grain in [
        ("created_minute15", "minute"),
        ("created_millisecond", "second"),
        ("created_time_of_day", "hour"),
    ]:
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="v",
                table="t",
                primary_key="id",
                dimensions=[
                    Dimension(name="id", type="numeric", sql="id"),
                    Dimension(name=name, type="time", granularity=grain, sql="ts"),  # no meta
                ],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        grains = [d.granularity for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions if d.type == "time"]
        assert grains == [grain], f"{name}: grain not preserved -> {grains}"


def test_lookml_uncommon_timeframe_suffix_roundtrips():
    """Imported timeframes like millisecond/microsecond round-trip (base derived from stored timeframe)."""
    import tempfile

    graph = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension_group: created {
    type: time
    timeframes: [millisecond, microsecond, date]
    sql: ${TABLE}.created_at ;;
  }
}
"""
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    names = {d.name for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions if d.type == "time"}
    assert {"created_millisecond", "created_microsecond"} <= names
    assert not any(n.endswith("_millisecond_millisecond") or n.endswith("_microsecond_microsecond") for n in names)


def test_lookml_time_grain_roundtrip():
    """time/hour/minute/date grains round-trip through export without grain or suffix corruption."""
    import tempfile

    graph = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension_group: created {
    type: time
    timeframes: [time, hour, minute, date]
    sql: ${TABLE}.created_at ;;
  }
}
"""
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    grains = {
        d.name: d.granularity for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions if d.type == "time"
    }
    assert grains == {
        "created_time": "second",
        "created_hour": "hour",
        "created_minute": "minute",
        "created_date": "day",
    }


def test_lookml_native_second_grain_roundtrips():
    """A second-grain dimension not imported from LookML (no meta) exports as `second`, not `time`."""
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="created_second", type="time", granularity="second", sql="{model}.created_at"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    names = {d.name for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions if d.type == "time"}
    assert "created_second" in names


def test_lookml_same_prefix_time_dims_different_sources_not_merged():
    """Same-prefix time dims backed by different columns must not merge to one group (wrong source)."""
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started_date", type="time", granularity="day", sql="{model}.started_at"),
                Dimension(name="started_second", type="time", granularity="second", sql="{model}.other_at"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    time_dims = [d for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions if d.type == "time"]
    sources = {d.sql for d in time_dims}
    # Each field keeps its own source column (no rewiring to the other group's SQL).
    assert any("started_at" in (s or "") for s in sources)
    assert any("other_at" in (s or "") for s in sources)
    # Split groups get suffix-free, collision-free names (no started_date_date etc.).
    assert not any(d.name.endswith("_date_date") or d.name.endswith("_hour_hour") for d in time_dims)


def test_lookml_collision_subsecond_timeframe_preserves_precision():
    """A millisecond/microsecond collision dim exports DATE_TRUNC at that grain (not second).

    sidemantic stores them as `second`, so the exported SQL must use the LookML timeframe
    grain to keep sub-second precision, and re-import recovers second grain + the timeframe.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(
                    name="started_date", type="time", granularity="day", sql="a", meta={"lookml_timeframe": "date"}
                ),
                Dimension(
                    name="started_millisecond",
                    type="time",
                    granularity="second",
                    sql="b",
                    meta={"lookml_timeframe": "millisecond"},
                ),
            ],
        )
    )
    adapter = LookMLAdapter()
    out = tempfile.mktemp(suffix=".lkml")
    adapter.export(graph, out)
    assert "DATE_TRUNC('millisecond'" in open(out).read()  # sub-second precision kept, not 'second'
    g = adapter.parse(Path(out))
    ms = g.get_model("v").get_dimension("started_millisecond")
    assert ms.granularity == "second" and (ms.meta or {}).get("lookml_timeframe") == "millisecond"
    # second round-trip stable (no nested DATE_TRUNC)
    out2 = tempfile.mktemp(suffix=".lkml")
    adapter.export(g, out2)
    assert "DATE_TRUNC('millisecond', DATE_TRUNC" not in open(out2).read()


def test_lookml_collision_disambiguation_reserves_future_group_fields():
    """Disambiguating a standalone must avoid names a LATER dimension_group will generate.

    `started` + `started_hour` (base `started`) plus `started_2_hour` (base `started_2`): the
    standalone must not disambiguate to `started_2_hour` because the `started_2` group also
    generates it. used_names is pre-seeded with every group's generated fields, so it skips to
    `started_3_hour` -- no duplicate LookML field across the whole view.
    """
    import re
    import tempfile
    from collections import Counter

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started", type="time", granularity="hour", sql="a"),
                Dimension(name="started_hour", type="time", granularity="hour", sql="b"),
                Dimension(name="started_2_hour", type="time", granularity="hour", sql="c"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    names = re.findall(r"\n  dimension: (\w+) \{", text)
    for base, tfs in re.findall(r"dimension_group: (\w+) \{[^}]*?timeframes:\s*\[([^\]]*)\]", text, re.S):
        names += [f"{base}_{tf.strip()}" for tf in tfs.split(",")]
    dups = {n: c for n, c in Counter(names).items() if c > 1}
    assert not dups, f"duplicate field names: {dups}"


def test_lookml_minute_bucket_collision_field_deduplicated():
    """A minute15/30 collision dim must go through the same uniqueness logic (no duplicate names).

    The minute-bucket path previously returned early, bypassing used_names dedup, so a
    suffixless `started` (minute15) colliding with an existing `started_minute15` could emit
    two `started_minute15` fields. It now disambiguates (`started_2_minute15`), still recoverable.
    """
    import re
    import tempfile
    from collections import Counter

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(
                    name="started", type="time", granularity="minute", sql="a", meta={"lookml_timeframe": "minute15"}
                ),
                Dimension(
                    name="started_minute15",
                    type="time",
                    granularity="minute",
                    sql="b",
                    meta={"lookml_timeframe": "minute15"},
                ),
                Dimension(
                    name="started_date", type="time", granularity="day", sql="c", meta={"lookml_timeframe": "date"}
                ),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    names = re.findall(r"\n  dimension: (\w+) \{", text)
    for base, tfs in re.findall(r"dimension_group: (\w+) \{[^}]*?timeframes:\s*\[([^\]]*)\]", text, re.S):
        names += [f"{base}_{tf.strip()}" for tf in tfs.split(",")]
    dups = {n: c for n, c in Counter(names).items() if c > 1}
    assert not dups, f"duplicate field names: {dups}"
    # the disambiguated field is still time-recoverable (ends in a real timeframe suffix)
    g2 = LookMLAdapter().parse(Path(out))
    assert all(d.type == "time" for d in g2.get_model("ev").dimensions if d.name.startswith("started"))


def test_lookml_collision_disambiguated_field_stays_time_recoverable():
    """A disambiguated collision field must stay TIME-recoverable, not become categorical.

    The unrepresentable trio (`started` + `started_hour` + `started_date`, all different SQL)
    forces one field to be renamed for uniqueness. The disambiguator goes into the STEM
    (`started_2_hour`, not `started_hour_2`) so the trailing timeframe is preserved and the
    field re-imports as time@hour, not a categorical dim. Stable across round-trips.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started", type="time", granularity="hour", sql="a"),
                Dimension(name="started_hour", type="time", granularity="hour", sql="b"),
                Dimension(name="started_date", type="time", granularity="day", sql="c"),
            ],
        )
    )
    adapter = LookMLAdapter()
    g = graph
    for _ in range(2):
        out = tempfile.mktemp(suffix=".lkml")
        adapter.export(g, out)
        g = adapter.parse(Path(out))
        kinds = {d.name: d.type for d in g.get_model("ev").dimensions if d.name != "id"}
        # all three time sources stay TIME (none degraded to categorical), 3 distinct names
        assert all(t == "time" for t in kinds.values()), kinds
        assert len(kinds) == 3, kinds


def test_lookml_suffixless_collision_no_dimension_group_field_duplicate():
    """A suffixless dim must not win the group slot and GENERATE a name a sibling standalone owns.

    `started` (hour) + `started_hour` (hour, diff SQL) + `started_date`: if suffixless `started`
    won the dimension_group, the group would generate `started_hour` AND a standalone
    `started_hour` would exist -> a runtime field collision. A suffixed sibling wins the group
    slot instead, so every generated field name (group fields + standalones) is unique.
    """
    import re
    import tempfile
    from collections import Counter

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started", type="time", granularity="hour", sql="a"),
                Dimension(name="started_hour", type="time", granularity="hour", sql="b"),
                Dimension(name="started_date", type="time", granularity="day", sql="c"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    generated = re.findall(r"\n  dimension: (\w+) \{", text)  # standalones
    for base, tfs in re.findall(r"dimension_group: (\w+) \{[^}]*?timeframes:\s*\[([^\]]*)\]", text, re.S):
        generated += [f"{base}_{tf.strip()}" for tf in tfs.split(",")]  # group-generated fields
    dups = {n: c for n, c in Counter(generated).items() if c > 1}
    assert not dups, f"duplicate generated field names: {dups}"


def test_lookml_suffixless_collision_group_field_maps_to_owning_source():
    """The generated group field must map to the source that ORIGINALLY owned that field name.

    `started` (hour, source A) + `started_hour` (hour, source B): the `dimension_group` slot must
    go to the suffixed `started_hour` so the group's generated `started_hour` field comes from
    source B (its own source). If the suffixless `started` won the slot, the group would generate
    `started_hour` from source A while the real `started_hour` got renamed to `started_2_hour` --
    so a round-tripped query for `started_hour` would silently read the WRONG column.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started", type="time", granularity="hour", sql="src_suffixless"),
                Dimension(name="started_hour", type="time", granularity="hour", sql="src_started_hour"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    reloaded = LookMLAdapter().parse(out).get_model("ev")
    by_name = {d.name: d.sql for d in reloaded.dimensions}
    # started_hour must still resolve to its own source, not the suffixless dim's source.
    assert "src_started_hour" in (by_name.get("started_hour") or ""), by_name
    assert "src_suffixless" not in (by_name.get("started_hour") or ""), by_name
    # The suffixless dim's source survives under a distinct, time-recoverable name.
    assert any("src_suffixless" in (s or "") for n, s in by_name.items() if n != "started_hour"), by_name


def test_lookml_suffixless_collision_synth_name_avoids_duplicate():
    """Synthesizing a recoverable suffix must not duplicate an existing field name.

    `started` (hour) alongside an EXISTING `started_hour` and a `started_date` source must not
    emit two `started_hour` blocks; the suffixless one keeps its name (no data-losing dup).
    """
    import re
    import tempfile
    from collections import Counter

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started", type="time", granularity="hour", sql="a"),
                Dimension(name="started_hour", type="time", granularity="hour", sql="b"),  # already exists
                Dimension(name="started_date", type="time", granularity="day", sql="c"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    names = re.findall(r"dimension(?:_group)?: (\w+)", open(out).read())
    dups = {n: c for n, c in Counter(names).items() if c > 1}
    assert not dups, f"duplicate field names emitted: {dups}"


def test_lookml_suffixless_collision_time_dim_stays_time():
    """A suffixless collision time dim must remain a TIME dim across round-trips, not categorical.

    `started` (hour grain) colliding with `started_date` (day, different SQL) can't keep its
    bare name as a recoverable standalone, so the collision export appends the grain timeframe
    (-> `started_hour`) so re-import restores time granularity instead of dropping to a
    categorical dim (which would break time queries/filters on the field).
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started", type="time", granularity="hour", sql="created_at"),  # suffixless
                Dimension(name="started_date", type="time", granularity="day", sql="updated_at"),
            ],
        )
    )
    adapter = LookMLAdapter()
    g = graph
    for _ in range(2):
        out = tempfile.mktemp(suffix=".lkml")
        adapter.export(g, out)
        g = adapter.parse(Path(out))
        times = {d.name: d.granularity for d in g.get_model("ev").dimensions if d.type == "time"}
        # the suffixless dim is recoverable as time@hour (renamed started -> started_hour)
        assert times.get("started_hour") == "hour", times
        assert times.get("started_date") == "day", times


def test_lookml_native_collision_inexact_name_not_widened_on_import():
    """A NATIVE collision dim named *_minute15 (no meta) must not gain false minute15 meta.

    Without meta the collision exporter writes a plain DATE_TRUNC('minute', ...), so import
    must recover grain=minute with NO lookml_timeframe (recording 'minute15' would make the
    next export widen the 1-minute dim into a 15-minute bucket). Stable across round-trips.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started_date", type="time", granularity="day", sql="created_at"),
                # same prefix, DIFFERENT sql -> collision; NO meta
                Dimension(name="started_minute15", type="time", granularity="minute", sql="updated_at"),
            ],
        )
    )
    adapter = LookMLAdapter()
    out = tempfile.mktemp(suffix=".lkml")
    adapter.export(graph, out)
    assert "DATE_TRUNC('minute'" in open(out).read()  # plain minute trunc, not a 15-min bucket
    g = adapter.parse(Path(out))
    m15 = g.get_model("ev").get_dimension("started_minute15")
    assert m15.granularity == "minute"
    assert not (m15.meta or {}).get("lookml_timeframe")  # NO false minute15 meta
    # stable: second round-trip still minute, still no bucket
    out2 = tempfile.mktemp(suffix=".lkml")
    adapter.export(g, out2)
    assert "FLOOR(" not in open(out2).read()  # never widened into a bucket
    m15b = adapter.parse(Path(out2)).get_model("ev").get_dimension("started_minute15")
    assert m15b.granularity == "minute" and not (m15b.meta or {}).get("lookml_timeframe")


def test_lookml_collision_minute15_bucket_roundtrips():
    """A minute15 collision dim exports a faithful 15-min bucket (not minute) and round-trips.

    sidemantic has no 15-min grain (stores minute15 as `minute` + meta), so the standalone
    collision form must emit an explicit N-minute bucket that the importer recovers back to
    grain=minute + meta['lookml_timeframe']='minute15', stable across repeated round-trips.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(
                    name="started_date", type="time", granularity="day", sql="a", meta={"lookml_timeframe": "date"}
                ),
                Dimension(
                    name="started_minute15",
                    type="time",
                    granularity="minute",
                    sql="b",
                    meta={"lookml_timeframe": "minute15"},
                ),
            ],
        )
    )
    adapter = LookMLAdapter()
    out = tempfile.mktemp(suffix=".lkml")
    adapter.export(graph, out)
    text = open(out).read()
    # Faithful 15-minute bucket via PORTABLE FLOOR (not DuckDB-only //), not DATE_TRUNC('minute').
    assert "FLOOR(" in text and " / 15) * 15" in text and "//" not in text
    g = adapter.parse(Path(out))
    m15 = g.get_model("v").get_dimension("started_minute15")
    assert m15.granularity == "minute" and (m15.meta or {}).get("lookml_timeframe") == "minute15"
    # The recovered dim KEEPS the bucket expression (so QUERIES bucket 15-min, not minute).
    assert "FLOOR(" in (m15.sql or "")
    # second round-trip stays stable (no nested bucket)
    out2 = tempfile.mktemp(suffix=".lkml")
    adapter.export(g, out2)
    assert "FLOOR(EXTRACT(MINUTE FROM DATE_TRUNC('hour'" not in open(out2).read()  # no FLOOR(...bucket...)
    m15b = adapter.parse(Path(out2)).get_model("v").get_dimension("started_minute15")
    assert m15b.granularity == "minute" and (m15b.meta or {}).get("lookml_timeframe") == "minute15"


def test_lookml_collision_minute15_query_buckets_by_15_minutes():
    """A round-tripped minute15 collision dim must GROUP queries by 15-minute buckets, not minute."""
    import tempfile

    import duckdb

    from sidemantic import Dimension, Metric, Model, SemanticLayer
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="v",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(
                    name="started_date", type="time", granularity="day", sql="a", meta={"lookml_timeframe": "date"}
                ),
                Dimension(
                    name="started_minute15",
                    type="time",
                    granularity="minute",
                    sql="ts",
                    meta={"lookml_timeframe": "minute15"},
                ),
            ],
            metrics=[Metric(name="cnt", agg="count")],
        )
    )
    adapter = LookMLAdapter()
    out = tempfile.mktemp(suffix=".lkml")
    adapter.export(graph, out)
    g = adapter.parse(Path(out))
    layer = SemanticLayer(auto_register=False)
    for m in g.models.values():
        layer.add_model(m)
    sql = layer.compile(dimensions=["v.started_minute15"], metrics=["v.cnt"])
    con = duckdb.connect()
    con.execute(
        "create table v as select 1 id, TIMESTAMP '2024-01-01 10:07' a, TIMESTAMP '2024-01-01 10:07' ts "
        "union all select 2, TIMESTAMP '2024-01-01 10:11', TIMESTAMP '2024-01-01 10:11' "
        "union all select 3, TIMESTAMP '2024-01-01 10:22', TIMESTAMP '2024-01-01 10:22'"
    )
    import datetime

    rows = sorted(con.execute(sql).fetchall())
    assert rows == [(datetime.datetime(2024, 1, 1, 10, 0), 2), (datetime.datetime(2024, 1, 1, 10, 15), 1)]


def test_lookml_collision_time_dim_multiword_timeframe_suffix_recovered():
    """A collision standalone dim with a MULTI-WORD timeframe suffix (time_of_day) recovers.

    The grain suffix is matched against the longest known LookML timeframe, not just the
    text after the last underscore, so started_time_of_day round-trips as a time dimension
    (gran=hour), not a renamed categorical.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(
                    name="started_date", type="time", granularity="day", sql="a", meta={"lookml_timeframe": "date"}
                ),
                Dimension(
                    name="started_time_of_day",
                    type="time",
                    granularity="hour",
                    sql="b",
                    meta={"lookml_timeframe": "time_of_day"},
                ),
            ],
        )
    )
    adapter = LookMLAdapter()
    # Repeated round-trips must be STABLE: the recovered timeframe is stored in meta so
    # the next export strips the _time_of_day suffix instead of renaming the field.
    g = graph
    for _ in range(3):
        out = tempfile.mktemp(suffix=".lkml")
        adapter.export(g, out)
        g = adapter.parse(Path(out))
        grains = {d.name: d.granularity for d in g.get_model("v").dimensions if d.type == "time"}
        assert grains == {"started_date": "day", "started_time_of_day": "hour"}


def test_lookml_time_dims_grouped_by_effective_sql_not_explicit_sql():
    """Two default-column time dims must not collapse just because both have sql=None.

    `started` and `started_hour` at hour grain with no explicit sql read DIFFERENT columns, but
    grouping on the raw `sql` field saw None for both and merged them into one dimension_group --
    emitting a single field bound to the WRONG column and losing the other dim entirely. Group on
    the EFFECTIVE expression (sql or name). Dims that genuinely share a source still group.
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    def roundtrip(*dims):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="ev",
                table="t",
                primary_key="id",
                dimensions=[Dimension(name="id", type="numeric", sql="id"), *dims],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        reimported = LookMLAdapter().parse(Path(out)).get_model("ev")
        return {d.name: d.sql for d in reimported.dimensions if d.name != "id"}

    # Both default-column: BOTH must survive, and started_hour must read started_hour.
    both_default = roundtrip(
        Dimension(name="started", type="time", granularity="hour"),
        Dimension(name="started_hour", type="time", granularity="hour"),
    )
    assert len(both_default) == 2, both_default
    assert both_default["started_hour"] == "started_hour", both_default  # not the `started` column
    assert any("started" in (sql or "") for name, sql in both_default.items() if name != "started_hour")

    # A genuinely shared source still collapses into one dimension_group (unchanged).
    same_source = roundtrip(
        Dimension(name="started", type="time", granularity="hour", sql="a"),
        Dimension(name="started_hour", type="time", granularity="hour", sql="a"),
    )
    assert same_source == {"started_hour": "a"}, same_source


def test_lookml_imported_dimension_group_without_sql_stays_one_group():
    """An imported `dimension_group` that omits `sql` must round-trip as ONE group.

    Its timeframes all read the same implicit `<base>` column, but each generated Dimension has
    sql=None, so keying them on the effective expression (which falls back to the generated field
    name) read every timeframe as its own source. The group was split into standalone dims backed
    by columns that never existed -- `sql: DATE_TRUNC('week', created_week)`.
    """
    import tempfile

    source = """view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }
  dimension_group: created {
    type: time
    timeframes: [date, week, month]
  }
}
"""
    directory = Path(tempfile.mkdtemp())
    (directory / "orders.view.lkml").write_text(source)
    graph = LookMLAdapter().parse(str(directory / "orders.view.lkml"))

    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    exported = Path(out).read_text()

    # One dimension_group carrying every timeframe, and no phantom-column standalones.
    assert exported.count("dimension_group:") == 1, exported
    assert "timeframes: [date, week, month]" in exported, exported
    assert "created_week" not in exported, exported
    assert "created_month" not in exported, exported
    # The implicit column stays implicit: no invented `sql:` on the group.
    assert "sql: created" not in exported, exported

    reimported = LookMLAdapter().parse(Path(out)).get_model("orders")
    assert sorted((d.name, d.granularity) for d in reimported.dimensions if d.type == "time") == [
        ("created_date", "day"),
        ("created_month", "month"),
        ("created_week", "week"),
    ]


def test_lookml_collision_time_of_day_recovers_timeframe_meta_and_survives_sibling_drop():
    """A collision-exported `time_of_day` standalone must recover its timeframe into meta.

    time_of_day has no finer SQL form -- its collision export is a PLAIN hour DATE_TRUNC named
    `*_time_of_day`. On import the recovery guard must still store `lookml_timeframe=time_of_day`
    (it was wrongly grouped with the exact-form minute-bucket / sub-second timeframes and dropped).
    Without the meta, a later export WITHOUT a colliding sibling loses the timeframe and renames the
    field to `*_hour`. This differs from the multiword test above, where the collision itself masks
    the loss via name preservation.
    """
    import re
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    adapter = LookMLAdapter()
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="ev",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(
                    name="started", type="time", granularity="hour", sql="a", meta={"lookml_timeframe": "time_of_day"}
                ),
                Dimension(name="started_time_of_day", type="time", granularity="hour", sql="b"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    adapter.export(graph, out)
    reimported = adapter.parse(Path(out)).get_model("ev")
    tf = {d.name: (d.meta or {}).get("lookml_timeframe") for d in reimported.dimensions}
    assert tf.get("started_time_of_day") == "time_of_day", tf  # recovered, not dropped

    # Drop the colliding sibling so the time_of_day field exports ALONE: it must keep its name,
    # NOT fall back to the hour timeframe and rename to started_hour.
    reimported.dimensions = [d for d in reimported.dimensions if d.name != "started_hour"]
    solo = SemanticGraph()
    solo.add_model(reimported)
    out2 = tempfile.mktemp(suffix=".lkml")
    adapter.export(solo, out2)
    text2 = open(out2).read()
    names = re.findall(r"\n  dimension: (\w+) \{", text2)
    for base, tfs in re.findall(r"dimension_group: (\w+) \{[^}]*?timeframes:\s*\[([^\]]*)\]", text2, re.S):
        names += [f"{base}_{x.strip()}" for x in tfs.split(",")]
    assert "started_time_of_day" in names, names
    assert "started_hour" not in names, names  # no bogus rename


def test_lookml_same_prefix_time_dims_roundtrip_names_and_grains_losslessly():
    """Collision time dims must round-trip with EXACT names AND granularities.

    Two same-prefix time dims on different sources can't both be dimension_groups, so
    the extra one is exported as a standalone DATE_TRUNC dimension. Both the field name
    and the granularity must survive re-import (no started_2_minute rename, no grain
    loss), and a second round-trip must be stable (no nested DATE_TRUNC).
    """
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id", primary_key=True),
                Dimension(name="started_hour", type="time", granularity="hour", sql="started_at"),
                Dimension(name="started_minute", type="time", granularity="minute", sql="completed_at"),
            ],
        )
    )
    adapter = LookMLAdapter()
    out1 = tempfile.mktemp(suffix=".lkml")
    adapter.export(graph, out1)
    g2 = adapter.parse(Path(out1))
    grains = {d.name: d.granularity for d in g2.get_model("v").dimensions if d.type == "time"}
    assert grains == {"started_hour": "hour", "started_minute": "minute"}

    # Second round-trip is stable and never nests DATE_TRUNC.
    out2 = tempfile.mktemp(suffix=".lkml")
    adapter.export(g2, out2)
    assert "DATE_TRUNC('minute', DATE_TRUNC" not in open(out2).read()
    assert "DATE_TRUNC('hour', DATE_TRUNC" not in open(out2).read()
    grains2 = {d.name: d.granularity for d in adapter.parse(Path(out2)).get_model("v").dimensions if d.type == "time"}
    assert grains2 == {"started_hour": "hour", "started_minute": "minute"}


def test_lookml_handwritten_date_trunc_dimension_not_hijacked():
    """A hand-written DATE_TRUNC dimension must keep its name/type, not be turned into a time group.

    The DATE_TRUNC granularity recovery is only for the collision-export form (name ends
    in _<grain>); an arbitrary-named dim like `created` stays categorical (no rename to
    created_date), and an unsupported dialect grain like 'isoweek' must not crash parsing.
    """
    import tempfile

    # Unsupported grain must not raise a granularity validation error.
    graph = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: w { type: date_time  sql: DATE_TRUNC('isoweek', ${TABLE}.created_at) ;; }
}
"""
    )
    w = graph.get_model("v").get_dimension("w")
    assert w.type == "categorical" and w.granularity is None

    # Arbitrary-named DATE_TRUNC dim keeps its exact name across a round-trip.
    graph2 = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: created { type: date_time  sql: DATE_TRUNC('day', ${TABLE}.created_at) ;; }
}
"""
    )
    assert graph2.get_model("v").get_dimension("created").type == "categorical"
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph2, out)
    names = {d.name for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions}
    assert "created" in names and "created_date" not in names

    # A hand-written dim whose NAME does carry a timeframe suffix (created_date) but also
    # has dimension-level properties (hidden/label) is NOT the collision-export form, so
    # it stays a plain dimension and those properties survive the round-trip.
    graph3 = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: created_date { type: date_time  hidden: yes  label: "Created"  sql: DATE_TRUNC('day', ${TABLE}.created_at) ;; }
}
"""
    )
    assert graph3.get_model("v").get_dimension("created_date").type == "categorical"
    out3 = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph3, out3)
    text3 = open(out3).read()
    assert "created_date" in {d.name for d in LookMLAdapter().parse(Path(out3)).get_model("v").dimensions}
    assert "hidden: yes" in text3 and "Created" in text3  # properties preserved


def test_lookml_split_time_group_names_avoid_existing_bases():
    """A synthetic split-group name must not collide with an existing time-group base."""
    import re
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="started_date", type="time", granularity="day", sql="{model}.a"),
                Dimension(name="started_hour", type="time", granularity="hour", sql="{model}.b"),
                Dimension(name="started_2_hour", type="time", granularity="hour", sql="{model}.c"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    names = re.findall(r"dimension_group: (\w+)", open(out).read())
    assert len(names) == len(set(names)), f"colliding dimension_group names: {names}"


def test_lookml_native_time_named_second_grain_roundtrips():
    """A native second-grain dim named *_time must export as `time` so the name round-trips."""
    import tempfile

    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="v",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="created_time", type="time", granularity="second", sql="{model}.created_at"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    names = {d.name for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions if d.type == "time"}
    assert "created_time" in names  # not renamed to created_second


def test_lookml_time_and_second_timeframes_no_duplicate_export():
    """A group with both `time` and `second` (both -> second grain) must not emit duplicate timeframes."""
    import re
    import tempfile

    graph = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension_group: created {
    type: time
    timeframes: [time, second, hour]
    sql: ${TABLE}.created_at ;;
  }
}
"""
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    timeframes = re.search(r"timeframes:\s*\[([^\]]*)\]", text).group(1)
    parts = [p.strip() for p in timeframes.split(",")]
    assert len(parts) == len(set(parts)), f"duplicate timeframes exported: {parts}"
    # Both `time` and `second` are preserved (no collapse/drop) via the stored
    # original timeframe, so re-import keeps every member.
    names = {d.name for d in LookMLAdapter().parse(Path(out)).get_model("v").dimensions if d.type == "time"}
    assert {"created_time", "created_second", "created_hour"} <= names


def test_lookml_view_without_table_defaults_to_view_name():
    """A view with no sql_table_name/derived_table should default its table to the view name."""
    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        "view: just_fields { "
        "dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
        "measure: c { type: count } }"
    )
    model = graph.get_model("just_fields")
    assert model.table == "just_fields"
    # And it must be a valid, queryable model (previously raised ModelValidationError).
    layer = SemanticLayer()
    layer.add_model(model)
    assert "just_fields" in layer.compile(metrics=["just_fields.c"])


def test_lookml_parse_tableless_view_inside_layer_context():
    """Parsing a tableless view inside a `with SemanticLayer()` block must not crash.

    Auto-registration fires during Model construction; the default table is applied
    after parse, so parsing must suppress registration until models are complete.
    """
    import tempfile

    from sidemantic import SemanticLayer

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(
            "view: just_fields { dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } measure: c { type: count } }"
        )
        f.flush()
        path = Path(f.name)

    with SemanticLayer() as layer:
        LookMLAdapter().parse(path)
    # The model is registered to the context layer with its defaulted table.
    assert layer.graph.get_model("just_fields").table == "just_fields"


def test_lookml_fieldless_ordinary_view_gets_default_table():
    """An ordinary fieldless view (or one with only adapter-ignored fields) still defaults.

    Looker defaults the table name to the view name regardless of parsed fields, so a
    `view: orders {}` must not be left tableless (which would fail CLI load/registration).
    Abstract/unsupported templates are still skipped; this only covers ordinary views.
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write("view: orders {}\nview: just_set { set: foo { fields: [a, b] } }\n")

    layer = SemanticLayer()
    load_from_directory(layer, d)  # must not raise the no-table ModelValidationError
    assert layer.graph.get_model("orders").table == "orders"
    assert layer.graph.get_model("just_set").table == "just_set"


def test_lookml_abstract_base_not_registered_inside_layer_context():
    """An abstract (extension: required) base must NOT be registered inside a `with` layer.

    It's intentionally tableless; deferred registration must skip it (not raise) while the
    concrete child still registers with its defaulted table.
    """
    import tempfile

    from sidemantic import SemanticLayer

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(
            "view: base { extension: required  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "dimension: x { type: number  sql: ${TABLE}.x ;; } } "
            "view: orders { extends: [base]  dimension: y { type: number  sql: ${TABLE}.y ;; } }"
        )
        f.flush()
        path = Path(f.name)

    with SemanticLayer() as layer:
        LookMLAdapter().parse(path)  # must not raise ModelValidationError
    assert "base" not in layer.graph.models  # abstract base skipped
    assert layer.graph.get_model("orders").table == "orders"  # concrete child registered


def test_lookml_directory_load_merges_cross_file_refinement_before_defaulting_table():
    """A CLI directory load must merge `+view` refinements from OTHER files before defaulting.

    `view: base` in one file and `view: +base { extension: required }` in another is a normal
    LookML layout. Parsing each .lkml independently never merged that refinement, so `base` missed
    its abstract marker, got defaulted to a physical table named `base`, and was registered as
    queryable -- validate/queries would silently target a fabricated table.
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    directory = Path(tempfile.mkdtemp())
    (directory / "a.lkml").write_text("view: base {\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n")
    (directory / "b.lkml").write_text("view: +base {\n  extension: required\n}\n")
    (directory / "c.lkml").write_text(
        "view: orders {\n  extends: [base]\n  sql_table_name: raw_orders ;;\n  measure: cnt { type: count }\n}\n"
    )

    layer = SemanticLayer()
    load_from_directory(layer, directory)

    # The refined abstract base stays non-queryable instead of getting a fabricated table.
    assert "base" not in layer.graph.models
    orders = layer.graph.get_model("orders")
    assert orders.table == "raw_orders"
    # Per-file provenance still points at the DEFINING file, not the project root.
    assert getattr(orders, "_source_file", None) == "c.lkml"


def test_lookml_broken_tableless_view_surfaces_error_inside_layer_context():
    """A genuinely-broken tableless view (unresolved extends) must NOT be silently skipped.

    Deferred registration skips only INTENTIONAL templates (extension:required / unsupported
    derived tables). A `view: child { extends: [missing] }` stays tableless because its parent
    can't resolve -- that's a real error, so add_model must still raise rather than drop it.
    """
    import tempfile

    import pytest

    from sidemantic import SemanticLayer
    from sidemantic.validation import ModelValidationError

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(
            "view: child { extends: [missing]  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } }"
        )
        f.flush()
        path = Path(f.name)

    with pytest.raises(ModelValidationError):
        with SemanticLayer():
            LookMLAdapter().parse(path)


def test_lookml_export_unsupported_derived_table_stays_tableless_on_reimport():
    """An unsupported derived_table must round-trip as tableless, not gain a physical table.

    _export_view re-emits the (retained) derived_table with no `sql`, so re-import keeps it
    marked unsupported instead of the implicit-table default assigning `table = <view name>`.
    """
    import tempfile

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write(
            'view: pdt { derived_table: { sql_trigger_value: "SELECT max(id) FROM orders" ;;  persist_for: "24 hours" } '
            "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } }"
        )
    graph = LookMLAdapter().parse(Path(d))
    assert graph.get_model("pdt").table is None
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "derived_table" in text and "sql_trigger_value" in text
    reimported = LookMLAdapter().parse(Path(out))
    m = reimported.get_model("pdt")
    assert m.table is None  # stays tableless, not defaulted to 'pdt'
    assert (m.meta or {}).get("unsupported_derived_table")


def test_lookml_export_abstract_view_stays_tableless_on_reimport():
    """An abstract (extension: required) base must round-trip as tableless, not gain a table.

    _export_view must re-emit `extension: required` so the re-imported base stays non-queryable
    instead of the implicit-table default assigning it a physical table named after the view.
    """
    import tempfile

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write(
            "view: base { extension: required "
            "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "  dimension: x { type: number  sql: ${TABLE}.x ;; } } "
            "view: orders { extends: [base]  dimension: y { type: number  sql: ${TABLE}.y ;; } }"
        )
    graph = LookMLAdapter().parse(Path(d))
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    import re

    assert "extension: required" in re.search(r"view: base \{.*?\n\}", text, re.S).group(0)  # base abstract
    # The concrete child INHERITS extension_required in meta but has its own table; it must NOT
    # re-emit the marker (that would make the usable child abstract on round-trip).
    assert "extension: required" not in re.search(r"view: orders \{.*?\n\}", text, re.S).group(0)
    reimported = LookMLAdapter().parse(Path(out))
    assert reimported.get_model("base").table is None  # base stays tableless, not defaulted
    assert reimported.get_model("orders").table == "orders"  # child stays usable


def test_lookml_abstract_base_does_not_break_directory_load():
    """The CLI path (load_from_directory) must skip the abstract base, not abort the project."""
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write(
            "view: base { extension: required  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "dimension: x { type: number  sql: ${TABLE}.x ;; } } "
            "view: orders { extends: [base]  dimension: y { type: number  sql: ${TABLE}.y ;; } }"
        )

    layer = SemanticLayer()
    load_from_directory(layer, d)  # must not raise ModelValidationError on `base`
    assert "base" not in layer.graph.models  # non-queryable abstract base skipped
    assert layer.graph.get_model("orders").table == "orders"


def test_lookml_drop_metrics_uses_provenance_marker_not_base_ref():
    """Orphan-metric drop keys off the parser-owned `_lookml_template_metric` PROVENANCE marker.

    A metric the LookML parser stamped as coming from a template is dropped; a same-named
    STANDALONE metric from another file (no marker) is preserved -- regardless of whether the
    base ref is qualified or unqualified, and regardless of object identity (refinements
    reconstruct the object but the marker is re-stamped on the graph-registered instance).
    """
    from sidemantic import Metric, Model
    from sidemantic.loaders import _drop_non_registerable_models

    def pop(bm, template_metric=False):
        meta = {"_lookml_template_metric": True} if template_metric else None
        return Metric(
            name="pop",
            type="time_comparison",
            base_metric=bm,
            comparison_type="yoy",
            calculation="difference",
            meta=meta,
        )

    template = Model(name="base", meta={"lookml_template": True})
    orders = Model(name="orders", table="orders", metrics=[Metric(name="total", agg="sum", sql="amt")])

    # A standalone `pop` (NO provenance marker) is preserved, even with an UNqualified base that
    # matches a surviving model's `total` -- and even qualified.
    standalone = pop("total")
    metrics = {"pop": standalone}
    _drop_non_registerable_models({"base": template, "orders": orders}, metrics)
    assert metrics.get("pop") is standalone

    # A template's `pop` (marked) is dropped -- unqualified base, same-named surviving `total`
    # present: the marker (not the base ref) decides.
    metrics2 = {"pop": pop("total", template_metric=True)}
    _drop_non_registerable_models({"base": template, "orders": orders}, metrics2)
    assert "pop" not in metrics2

    # A LATER file can OVERWRITE the template's name with a real model, so NOTHING is dropped
    # this pass -- the marked metric is still orphaned and must go. Keying the cleanup off
    # "something was dropped" left it registered and broke compile with "No models found".
    real_base = Model(name="base", table="real_base", metrics=[Metric(name="cnt", agg="count", sql="id")])
    metrics3 = {"pop": pop("revenue", template_metric=True)}
    kept = _drop_non_registerable_models({"base": real_base, "orders": orders}, metrics3)
    assert "pop" not in metrics3
    assert set(kept) == {"base", "orders"}  # nothing dropped: only the orphaned metric goes


def test_lookml_template_metric_dropped_when_later_file_overwrites_template():
    """A template's orphaned metric must go even when a later file reuses the template's name.

    The template is skipped, but a native file defining a real `base` overwrites it in the model
    dict, so the load drops NOTHING -- and the cleanup, gated on a drop having happened, left the
    template's auto-registered `pop` behind. `compile(metrics=["pop"])` then failed with
    "No models found for query".
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    directory = Path(tempfile.mkdtemp())
    (directory / "base.view.lkml").write_text(
        """view: base {
  extension: required
  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }
  dimension_group: created { type: time  timeframes: [date]  sql: ${TABLE}.created ;; }
  measure: revenue { type: sum  sql: ${TABLE}.amount ;; }
  measure: pop { type: period_over_period  based_on: revenue  based_on_time: created_date  period: year }
}
"""
    )
    # Sorts after the .lkml file, so it overwrites `base` with a real model.
    (directory / "zz_native.py").write_text(
        "from sidemantic import Dimension, Metric, Model\n"
        'model = Model(name="base", table="real_base", primary_key="id",\n'
        '              dimensions=[Dimension(name="id", type="numeric", sql="id")],\n'
        '              metrics=[Metric(name="cnt", agg="count", sql="id")])\n'
    )

    layer = SemanticLayer()
    load_from_directory(layer, directory, strict=False)

    assert layer.graph.models["base"].table == "real_base"  # the real model won
    assert "pop" not in layer.graph.metrics  # the template's orphan did not survive it


def test_lookml_normal_view_graph_metric_dropped_when_model_overwritten():
    """A NORMAL view's graph metric must go when a later file overwrites its model without it.

    A period_over_period on a plain (non-template) view auto-registers graph metric `pop`. The
    project-level LookML load runs before the per-file scan, so a later Python model of the same
    name replaces the model whose measure/time dimension `pop` needs -- but `pop` carries no
    template marker, so it lingered and `compile(["pop"])` failed with "No models found". The
    parser stamps every LookML graph metric with its owner, so the load drops it here; when the
    model is NOT overwritten the metric survives.
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    view = """view: orders {
  sql_table_name: real_orders ;;
  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }
  dimension_group: created { type: time  timeframes: [date]  sql: ${TABLE}.created ;; }
  measure: revenue { type: sum  sql: ${TABLE}.amount ;; }
  measure: pop { type: period_over_period  based_on: revenue  based_on_time: created_date  period: year }
}
"""

    # Overwritten by a later Python model that does not define `pop` -> the orphan is dropped.
    overwritten = Path(tempfile.mkdtemp())
    (overwritten / "orders.view.lkml").write_text(view)
    (overwritten / "zz_native.py").write_text(
        "from sidemantic import Dimension, Metric, Model\n"
        'model = Model(name="orders", table="py_orders", primary_key="id",\n'
        '              dimensions=[Dimension(name="id", type="numeric", sql="id")],\n'
        '              metrics=[Metric(name="cnt", agg="count", sql="id")])\n'
    )
    layer = SemanticLayer()
    load_from_directory(layer, overwritten, strict=False)
    assert layer.graph.models["orders"].table == "py_orders"  # the Python model won
    assert "pop" not in layer.graph.metrics  # its orphan did not survive

    # Not overwritten: the metric is a normal, working graph metric and must be kept.
    intact = Path(tempfile.mkdtemp())
    (intact / "orders.view.lkml").write_text(view)
    layer = SemanticLayer()
    load_from_directory(layer, intact, strict=False)
    assert "pop" in layer.graph.metrics


def test_lookml_included_view_does_not_extend_unincluded_parent():
    """An included view must not inherit fields from a parent no model include reaches.

    A unique view is installed even when un-included (an imperfectly-resolved include must never
    silently drop a view), but loaded is not in-scope: `extends: [base]` resolving against an
    archived `base` merged fields Looker would never expose in that model. The parent is treated
    as absent instead, leaving the child's extends unresolved -- the child still loads.
    """
    import tempfile

    child = "view: orders {\n  extends: [base]\n  sql_table_name: orders ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    parent = "view: base {\n  sql_table_name: base_t ;;\n  dimension: secret { sql: ${TABLE}.secret ;; }\n  measure: leaked { type: sum  sql: ${TABLE}.amount ;; }\n}\n"

    def fields(parent_dir, include):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        (directory / parent_dir).mkdir(exist_ok=True)
        (directory / "orders.model.lkml").write_text(f'include: "{include}"\n')
        (directory / "views" / "orders.view.lkml").write_text(child)
        (directory / parent_dir / "base.view.lkml").write_text(parent)
        graph = LookMLAdapter().parse(str(directory))
        model = graph.get_model("orders")
        return graph, {d.name for d in model.dimensions} | {m.name for m in model.metrics}

    # Parent in archive/: NOT reached by the model's include -> not inherited.
    graph, archived = fields("archive", "/views/orders.view.lkml")
    assert archived == {"id"}, archived
    assert "base" in graph.models  # the un-included view still LOADS; it is just not a parent

    # Parent reached by the include -> inherited as before.
    _, included = fields("views", "/views/*.view.lkml")
    assert {"secret", "leaked"} <= included, included

    # With no include declared anywhere, scoping is off and inheritance is unaffected.
    plain = Path(tempfile.mkdtemp())
    (plain / "orders.view.lkml").write_text(child)
    (plain / "base.view.lkml").write_text(parent)
    model = LookMLAdapter().parse(str(plain)).get_model("orders")
    assert {"secret", "leaked"} <= ({d.name for d in model.dimensions} | {m.name for m in model.metrics})


def test_lookml_implicit_dimension_group_resolves_in_field_references():
    """A ${created_date} reference to a no-sql dimension_group reads the implicit `<group>` column.

    Resolving the group's implicit column on the generated Dimension objects alone was too late:
    the lookup that expands ${ref}s in other fields is built first, so a measure or dimension over
    the timeframe still queried the generated field name -- a column that does not exist -- even
    though a direct query of the timeframe was correct.
    """
    import tempfile

    directory = Path(tempfile.mkdtemp())
    (directory / "orders.view.lkml").write_text(
        """view: orders {
  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }
  dimension_group: created {
    type: time
    timeframes: [date, week]
  }
  measure: cd { type: count_distinct  sql: ${created_date} ;; }
  dimension: is_new { type: yesno  sql: ${created_date} > '2020-01-01' ;; }
}
"""
    )
    model = LookMLAdapter().parse(str(directory / "orders.view.lkml")).get_model("orders")
    fields = {f.name: f.sql for f in [*model.dimensions, *model.metrics]}

    assert fields["cd"] == "({model}.created)"
    assert fields["is_new"] == "({model}.created) > '2020-01-01'"
    # Nothing references the generated field name, which is not a real column.
    assert not [name for name, sql in fields.items() if sql and "created_date" in sql], fields

    # An explicit sql: on the group still wins for references.
    explicit = Path(tempfile.mkdtemp())
    (explicit / "orders.view.lkml").write_text(
        """view: orders {
  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }
  dimension_group: created {
    type: time
    timeframes: [date]
    sql: ${TABLE}.ts ;;
  }
  measure: cd { type: count_distinct  sql: ${created_date} ;; }
}
"""
    )
    referring = LookMLAdapter().parse(str(explicit / "orders.view.lkml")).get_model("orders")
    assert [m.sql for m in referring.metrics if m.name == "cd"] == ["({model}.ts)"]


def test_lookml_implicit_dimension_group_compiles_against_group_column():
    """A dimension_group without `sql` reads Looker's implicit `<group>` column.

    Its generated dimensions carried sql=None, so each fell back to its OWN field name and
    compiled DATE_TRUNC('day', created_date) -- a column that does not exist -- against an
    otherwise valid view. The dims resolve to `created`, and export still re-emits the group
    without inventing a `sql`, since an implicit group is marked rather than inferred from sql.
    """
    import tempfile

    from sidemantic import SemanticLayer

    directory = Path(tempfile.mkdtemp())
    (directory / "orders.view.lkml").write_text(
        """view: orders {
  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }
  dimension_group: created {
    type: time
    timeframes: [date, week]
  }
  measure: cnt { type: count }
}
"""
    )
    graph = LookMLAdapter().parse(str(directory / "orders.view.lkml"))
    assert [d.sql for d in graph.get_model("orders").dimensions if d.name == "created_date"] == ["{model}.created"]

    layer = SemanticLayer()
    layer.graph = graph
    sql = layer.compile(dimensions=["orders.created_date"], metrics=["orders.cnt"])
    assert "DATE_TRUNC('day', created)" in sql, sql
    assert "'day', created_date)" not in sql, sql  # never the generated field name

    # Export still round-trips the group with no invented sql.
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    exported = Path(out).read_text()
    assert exported.count("dimension_group:") == 1, exported
    assert "sql:" not in exported[exported.index("dimension_group:") :], exported


def test_lookml_conflicting_per_model_refinement_order_warns(caplog):
    """Models disagreeing on refinement include order must be surfaced, not silently resolved.

    Looker applies refinements in each model's own include order. One model per view name can
    hold only one result, so a model including z_ref then a_ref and another doing the reverse
    cannot both be served -- the first model file's order silently won.
    """
    import logging
    import tempfile

    base = "view: orders {\n  sql_table_name: real_orders ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    orders_include = 'include: "/views/orders.view.lkml"\n'
    z_then_a = orders_include + 'include: "/views/z_ref.view.lkml"\ninclude: "/views/a_ref.view.lkml"\n'
    a_then_z = orders_include + 'include: "/views/a_ref.view.lkml"\ninclude: "/views/z_ref.view.lkml"\n'

    def parse(*models):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        for name, include in models:
            (directory / f"{name}.model.lkml").write_text(include)
        (directory / "views" / "orders.view.lkml").write_text(base)
        (directory / "views" / "z_ref.view.lkml").write_text("view: +orders {\n  sql_table_name: from_z ;;\n}\n")
        (directory / "views" / "a_ref.view.lkml").write_text("view: +orders {\n  sql_table_name: from_a ;;\n}\n")
        return LookMLAdapter().parse(str(directory))

    def order_conflict(*models):
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
            graph = parse(*models)
        return graph, any("disagree on the include order" in r.getMessage() for r in caplog.records)

    graph, conflicted = order_conflict(("a_first", z_then_a), ("b_second", a_then_z))
    assert conflicted
    assert graph.get_model("orders").table == "from_a"  # first model file's order; still loads

    # Agreeing models are not a conflict.
    _, conflicted = order_conflict(("a", z_then_a), ("b", z_then_a))
    assert not conflicted

    # A model including only SOME of the refinements orders nothing differently.
    _, conflicted = order_conflict(("a", z_then_a), ("b", orders_include + 'include: "/views/a_ref.view.lkml"\n'))
    assert not conflicted

    # A single model has nothing to disagree with.
    _, conflicted = order_conflict(("a", z_then_a))
    assert not conflicted


def test_lookml_refinements_merge_in_include_order_not_pathname_order():
    """Refinements follow the model's include order; the last-included one wins.

    Looker documents that refinements leverage include order, but the tree walk is sorted by
    pathname (for determinism), so a model including z_ref then a_ref applied a_ref FIRST and let
    z_ref win -- leaving the view pointed at the wrong table. Include order now drives the merge,
    and the sorted walk still decides anything the includes do not order.
    """
    import tempfile

    base = "view: orders {\n  sql_table_name: real_orders ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"

    def refinement(table):
        return f"view: +orders {{\n  sql_table_name: {table} ;;\n}}\n"

    def table(model_text, files):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        (directory / "m.model.lkml").write_text(model_text)
        (directory / "views" / "orders.view.lkml").write_text(base)
        for name, content in files.items():
            (directory / "views" / f"{name}.view.lkml").write_text(content)
        return LookMLAdapter().parse(str(directory)).get_model("orders").table

    refs = {"z_ref": refinement("from_z"), "a_ref": refinement("from_a")}

    # Include order wins over pathname order, in both directions.
    assert (
        table(
            'include: "/views/orders.view.lkml"\ninclude: "/views/z_ref.view.lkml"\ninclude: "/views/a_ref.view.lkml"\n',
            refs,
        )
        == "from_a"
    )
    assert (
        table(
            'include: "/views/orders.view.lkml"\ninclude: "/views/a_ref.view.lkml"\ninclude: "/views/z_ref.view.lkml"\n',
            refs,
        )
        == "from_z"
    )

    # A glob cannot express an order, so its matches stay sorted -- the load is deterministic.
    assert table('include: "/views/*.view.lkml"\n', refs) == "from_z"

    # Two refinements in ONE file keep their in-file order.
    assert (
        table(
            'include: "/views/orders.view.lkml"\ninclude: "/views/multi.view.lkml"\n',
            {"multi": refinement("from_first") + refinement("from_second")},
        )
        == "from_second"
    )

    # An included file's refinement lands BEFORE the includer's own: include: brings that content
    # in where it is written, above the includer's definitions.
    nested = Path(tempfile.mkdtemp())
    (nested / "views").mkdir()
    (nested / "m.model.lkml").write_text('include: "/views/orders.view.lkml"\ninclude: "/views/z_ref.view.lkml"\n')
    (nested / "views" / "orders.view.lkml").write_text(base)
    (nested / "views" / "z_ref.view.lkml").write_text('include: "/views/a_ref.view.lkml"\n' + refinement("from_z"))
    (nested / "views" / "a_ref.view.lkml").write_text(refinement("from_a"))
    assert LookMLAdapter().parse(str(nested)).get_model("orders").table == "from_z"

    # A circular include must not hang the ordered walk.
    circular = Path(tempfile.mkdtemp())
    (circular / "views").mkdir()
    (circular / "m.model.lkml").write_text('include: "/views/x.view.lkml"\n')
    (circular / "views" / "x.view.lkml").write_text('include: "/views/y.view.lkml"\n' + base)
    (circular / "views" / "y.view.lkml").write_text('include: "/views/x.view.lkml"\n' + refinement("from_y"))
    assert LookMLAdapter().parse(str(circular)).get_model("orders").table == "from_y"

    # With no include scope at all, the deterministic sorted order still applies.
    plain = Path(tempfile.mkdtemp())
    (plain / "orders.view.lkml").write_text(base)
    (plain / "a_ref.view.lkml").write_text(refinement("from_a"))
    (plain / "z_ref.view.lkml").write_text(refinement("from_z"))
    assert LookMLAdapter().parse(str(plain)).get_model("orders").table == "from_z"


def test_lookml_refinement_selected_by_only_some_models_warns(caplog):
    """A refinement only SOME models include is ambiguous and must be surfaced, not silent.

    Looker resolves each model separately: prod sees the plain view, staging the refined one. A
    graph holds ONE model per view name and cannot hold both, so a staging-only
    `+orders { sql_table_name: staging_orders }` silently repoints the view prod also uses. The
    refinement is still applied -- refusing a valid project, or dropping it and mis-serving the
    model that DID select it, are both worse -- but the load no longer resolves it silently.
    """
    import logging
    import tempfile

    base = "view: orders {\n  sql_table_name: real_orders ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    refinement = "view: +orders {\n  sql_table_name: staging_orders ;;\n}\n"

    def parse(models, refinement_dir="views"):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        (directory / refinement_dir).mkdir(exist_ok=True)
        for name, include in models:
            (directory / f"{name}.model.lkml").write_text(include)
        (directory / "views" / "orders.view.lkml").write_text(base)
        (directory / refinement_dir / "staging_refine.view.lkml").write_text(refinement)
        return LookMLAdapter().parse(str(directory))

    # Only staging includes the refinement, but prod uses the same view -> warn, still apply.
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        graph = parse(
            [
                ("prod", 'include: "/views/orders.view.lkml"\n'),
                ("staging", 'include: "/views/orders.view.lkml"\ninclude: "/views/staging_refine.view.lkml"\n'),
            ]
        )
    assert graph.get_model("orders").table == "staging_orders"  # applied: the project still loads
    assert "prod.model.lkml" in caplog.text
    assert "not included by these models" in caplog.text

    # Every model that uses the view includes the refinement: unambiguous, no warning.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        graph = parse([("prod", 'include: "/views/*.view.lkml"\n'), ("staging", 'include: "/views/*.view.lkml"\n')])
    assert graph.get_model("orders").table == "staging_orders"
    assert caplog.text == ""

    # An un-included refinement is skipped as before -- not applied, and not an ambiguity.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        graph = parse([("prod", 'include: "/views/*.view.lkml"\n')], refinement_dir="archive")
    assert graph.get_model("orders").table == "real_orders"
    assert caplog.text == ""


def test_lookml_extends_parent_scoped_per_model_not_project_wide():
    """A parent selected by ANOTHER model is not in scope for this model's child.

    Checking the parent against the project-wide union of included paths meant that with two
    models -- A including only orders.view.lkml, B including only base.view.lkml -- `orders`
    inherited from `base` anyway, exposing fields from another model's include scope. The parent
    must be visible from the CHILD's own file.
    """
    import tempfile

    child = "view: orders {\n  extends: [base]\n  sql_table_name: orders ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    parent = "view: base {\n  sql_table_name: base_t ;;\n  dimension: secret { sql: ${TABLE}.secret ;; }\n  measure: leaked { type: sum  sql: ${TABLE}.amount ;; }\n}\n"

    def fields(*models):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        for name, include in models:
            (directory / f"{name}.model.lkml").write_text(f'include: "{include}"\n')
        (directory / "views" / "orders.view.lkml").write_text(child)
        (directory / "views" / "base.view.lkml").write_text(parent)
        model = LookMLAdapter().parse(str(directory)).get_model("orders")
        return {d.name for d in model.dimensions} | {m.name for m in model.metrics}

    # Separate scopes: the child's model cannot see the parent -> not inherited.
    assert fields(("a", "/views/orders.view.lkml"), ("b", "/views/base.view.lkml")) == {"id"}

    # One model selecting both still inherits.
    assert {"secret", "leaked"} <= fields(("a", "/views/*.view.lkml"))

    # The child's file reached by a model that also selects the parent: union -> inherited.
    assert {"secret", "leaked"} <= fields(("a", "/views/orders.view.lkml"), ("b", "/views/*.view.lkml"))


def test_lookml_abstract_template_conflicting_with_active_model_is_not_skipped():
    """A template whose name collides with an existing model must not silently take the skip path.

    Skipping tableless templates is right in general, but when the active layer ALREADY defines
    that name the parsed definition was dropped and the pre-existing model silently left in place.
    Such a name must reach add_model so the conflict surfaces. With no collision the template is
    still skipped and the concrete child registers.
    """
    import tempfile

    from sidemantic import Dimension, Model, SemanticLayer

    template = "view: base {\n  extension: required\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(template)
        f.flush()
        collide_path = Path(f.name)

    layer = SemanticLayer()
    layer.add_model(
        Model(
            name="base",
            table="manual_base",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
        )
    )
    # Raises rather than silently keeping manual_base (the exact error depends on which
    # validation fires first; the point is it is surfaced, not swallowed).
    with pytest.raises(Exception):
        with layer:
            LookMLAdapter().parse(collide_path)

    # No collision: the template is still skipped and the concrete child registers.
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(template + "view: orders {\n  extends: [base]\n  sql_table_name: raw_orders ;;\n}\n")
        f.flush()
        ok_path = Path(f.name)
    clean = SemanticLayer()
    with clean:
        LookMLAdapter().parse(ok_path)
    assert "base" not in clean.graph.models
    assert clean.graph.get_model("orders").table == "raw_orders"


def test_lookml_parse_raises_on_duplicate_model_in_active_layer():
    """A model the active layer already defines is a conflict -- deferral must not hide it.

    Deferring registration must not silently skip a name that already exists: parsing a view
    `orders` into a layer that already has a different `orders` model previously raised via
    auto-registration, and must still raise rather than leave the old definition in place.
    """
    import tempfile

    from sidemantic import Dimension, Model, SemanticLayer

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(
            "view: orders {\n  sql_table_name: lookml_orders ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
        )
        f.flush()
        path = Path(f.name)

    layer = SemanticLayer()
    layer.add_model(
        Model(
            name="orders",
            table="manual_orders",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
        )
    )
    with pytest.raises(ValueError, match="already exists"):
        with layer:
            LookMLAdapter().parse(path)


def test_lookml_project_parse_handles_duplicate_views_and_extensionless_includes():
    """An ARCHIVED copy of a view must not fail the load; a real duplicate must still surface.

    Parsing the tree as one project means an archived copy of a view alongside the live one hits
    add_model's duplicate error and fails everything. Includes decide the winner -- but ONLY when
    exactly one copy is included: if both are included (or there are no includes), nothing
    distinguishes them, so the conflict is a real project error and must be raised rather than
    silently loading one at random. LookML also allows the .lkml suffix to be omitted
    (`include: "/views/*.view"`), which must still resolve on disk -- else the included set is
    skewed and stale refinements leak back in.
    """
    import tempfile

    view = "view: orders {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"

    def build(include_pattern, *, duplicate_dir=None, stale_refinement=False):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        (directory / "archive").mkdir()
        if include_pattern:
            (directory / "m.model.lkml").write_text(f'include: "{include_pattern}"\nexplore: orders {{}}\n')
        (directory / "views" / "orders.view.lkml").write_text(view % "real_orders")
        if duplicate_dir:
            (directory / duplicate_dir / "dup.view.lkml").write_text(view % "other_orders")
        if stale_refinement:
            (directory / "archive" / "stale.view.lkml").write_text("view: +orders {\n  sql_table_name: STALE ;;\n}\n")
        return LookMLAdapter().parse(str(directory)).get_model("orders")

    # Exactly one copy included: the archived one is ignored instead of failing the load.
    assert build("/views/*.view.lkml", duplicate_dir="archive").table == "real_orders"
    # Extensionless include patterns still resolve, so the stale refinement stays out.
    assert build("/views/*.view", stale_refinement=True).table == "real_orders"
    assert build("/views/orders.view", stale_refinement=True).table == "real_orders"
    # A view no include matches is NEVER dropped -- include scoping only breaks ties.
    assert build("/nonexistent/*.view.lkml").table == "real_orders"

    # BOTH copies included, or no includes at all: nothing distinguishes them, so the duplicate is
    # a real conflict and must surface rather than silently loading one.
    with pytest.raises(ValueError, match="already exists"):
        build("/views/*.view.lkml", duplicate_dir="views")
    with pytest.raises(ValueError, match="already exists"):
        build(None, duplicate_dir="archive")


def test_lookml_project_parse_resolves_view_with_several_archived_copies():
    """SEVERAL archived copies plus one included copy must still resolve to the included one.

    The decision is made over ALL candidates for a name at once: judging them pairwise as files
    stream in mis-handles two archived copies sorting before the live one, because the first pair
    looks unresolvable before the winner has even been seen.
    """
    import tempfile

    view = "view: orders {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    directory = Path(tempfile.mkdtemp())
    for name in ("aa_old", "ab_old", "views"):
        (directory / name).mkdir()
    (directory / "m.model.lkml").write_text('include: "/views/*.view.lkml"\n')
    (directory / "aa_old" / "o.view.lkml").write_text(view % "arch1")  # both sort BEFORE views/
    (directory / "ab_old" / "o.view.lkml").write_text(view % "arch2")
    (directory / "views" / "o.view.lkml").write_text(view % "real_orders")

    assert LookMLAdapter().parse(str(directory)).get_model("orders").table == "real_orders"


def test_lookml_project_parse_skips_duplicate_views_no_model_includes():
    """Several archived copies of a view that NO model include reaches must not fail the load.

    When scoping is active, a duplicate the includes can distinguish resolves to the included copy.
    But two archived copies with no included rival used to install both and raise
    "Model X already exists" -- failing a valid project over views it never selects. Skip them
    instead; a lone unincluded view (no rival) still loads, so only genuinely-ambiguous dead
    copies are dropped. A real duplicate (both included, or scoping off) still surfaces.
    """
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"

    directory = Path(tempfile.mkdtemp())
    for name in ("views", "archive1", "archive2"):
        (directory / name).mkdir()
    (directory / "m.model.lkml").write_text('include: "/views/orders.view.lkml"\n')
    (directory / "views" / "orders.view.lkml").write_text(view % ("orders", "real_orders"))
    # Two archived copies of `legacy`; no model include reaches either.
    (directory / "archive1" / "legacy.view.lkml").write_text(view % ("legacy", "legacy_v1"))
    (directory / "archive2" / "legacy.view.lkml").write_text(view % ("legacy", "legacy_v2"))

    graph = LookMLAdapter().parse(str(directory))  # must not raise
    assert graph.get_model("orders").table == "real_orders"
    assert "legacy" not in graph.models  # unreachable dead copies dropped, not installed

    # One of the copies IS included: it wins, the other is ignored (unchanged behavior).
    scoped = Path(tempfile.mkdtemp())
    (scoped / "views").mkdir()
    (scoped / "archive").mkdir()
    (scoped / "m.model.lkml").write_text('include: "/views/*.view.lkml"\n')
    (scoped / "views" / "legacy.view.lkml").write_text(view % ("legacy", "live_legacy"))
    (scoped / "archive" / "legacy.view.lkml").write_text(view % ("legacy", "dead_legacy"))
    assert LookMLAdapter().parse(str(scoped)).get_model("legacy").table == "live_legacy"


def test_lookml_include_scoping_follows_closure_from_model_files():
    """Includes are a closure SEEDED from model files, not a flat set of every declaration.

    Reachability from a model file is the rule. A refinement reached THROUGH a model-selected view
    (model -> orders.view -> refine.view) is part of the project and must apply; a stray view
    file's helper include in a directory no model selects must not switch scoping on at all.
    """
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"

    # Reachable through the selected view: the nested refinement applies.
    nested = Path(tempfile.mkdtemp())
    (nested / "views").mkdir()
    (nested / "m.model.lkml").write_text('include: "/views/orders.view.lkml"\n')
    (nested / "views" / "orders.view.lkml").write_text(
        'include: "refine.view.lkml"\n' + (view % ("orders", "real_orders"))
    )
    (nested / "views" / "refine.view.lkml").write_text("view: +orders {\n  sql_table_name: REFINED ;;\n}\n")
    assert LookMLAdapter().parse(str(nested)).get_model("orders").table == "REFINED"

    # NOT reachable from any model (there is none): scoping stays off entirely.
    stray = Path(tempfile.mkdtemp())
    (stray / "helper.view.lkml").write_text(view % ("helper", "h"))
    (stray / "orders.view.lkml").write_text('include: "helper.view.lkml"\n' + (view % ("orders", "real_orders")))
    (stray / "ref.view.lkml").write_text("view: +orders {\n  sql_table_name: REFINED ;;\n}\n")
    assert LookMLAdapter().parse(str(stray)).get_model("orders").table == "REFINED"


def test_lookml_only_model_file_includes_activate_scoping():
    """A VIEW file's helper include must not activate project-wide include scoping.

    `include:` in a view file pulls in a helper; it says nothing about which files the project
    selects. Letting it populate the included set made unrelated sibling refinements (and
    explores) look un-included and silently dropped them from a plain single-directory load.
    """
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"

    directory = Path(tempfile.mkdtemp())
    (directory / "helper.view.lkml").write_text(view % ("helper", "h"))
    (directory / "orders.view.lkml").write_text('include: "helper.view.lkml"\n' + (view % ("orders", "real_orders")))
    (directory / "ref.view.lkml").write_text("view: +orders {\n  sql_table_name: REFINED ;;\n}\n")
    # The sibling refinement is valid and must still apply.
    assert LookMLAdapter().parse(str(directory)).get_model("orders").table == "REFINED"

    # A MODEL file's include still scopes: the stale refinement stays out.
    scoped = Path(tempfile.mkdtemp())
    (scoped / "views").mkdir()
    (scoped / "archive").mkdir()
    (scoped / "m.model.lkml").write_text('include: "/views/*.view.lkml"\n')
    (scoped / "views" / "o.view.lkml").write_text(view % ("orders", "real_orders"))
    (scoped / "archive" / "stale.view.lkml").write_text("view: +orders {\n  sql_table_name: STALE ;;\n}\n")
    assert LookMLAdapter().parse(str(scoped)).get_model("orders").table == "real_orders"


def test_lookml_every_model_file_seeds_include_scoping():
    """A self-contained model file must not be scoped out by an include-based sibling.

    Seeding the include closure only from models that DECLARE `include:` left a model carrying
    its own views and explores unseeded, so once a sibling model switched scoping on, the
    self-contained file looked un-included and its explore segment was silently dropped.
    """
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"

    directory = Path(tempfile.mkdtemp())
    (directory / "views").mkdir()
    (directory / "one.model.lkml").write_text('include: "/views/*.view.lkml"\n')
    (directory / "views" / "orders.view.lkml").write_text(view % ("orders", "real_orders"))
    # Declares no includes: its view and explore live in the file itself.
    (directory / "two.model.lkml").write_text(
        (view % ("local", "local_tbl")) + "explore: local {\n  sql_always_where: ${local.id} > 0 ;;\n}\n"
    )

    graph = LookMLAdapter().parse(str(directory))
    assert [s.name for s in graph.get_model("local").segments] == ["_sql_always_where_local"]
    # The include-based sibling still loads normally.
    assert graph.get_model("orders").table == "real_orders"

    # Model files present but NONE declaring an include leaves scoping off entirely.
    unscoped = Path(tempfile.mkdtemp())
    (unscoped / "m.model.lkml").write_text("explore: orders {}\n")
    (unscoped / "orders.view.lkml").write_text(view % ("orders", "real_orders"))
    (unscoped / "ref.view.lkml").write_text("view: +orders {\n  sql_table_name: REFINED ;;\n}\n")
    assert LookMLAdapter().parse(str(unscoped)).get_model("orders").table == "REFINED"


def test_lookml_partial_explore_filter_kept_and_reported(caplog):
    """An explore's mandatory filter must not silently leak onto models without that explore.

    sql_always_where / always_filter become segments on the single shared base model. When one
    model includes the base view plus a filtered explore sidecar and another includes only the
    base view, the filter attached to the shared model -- exposing a segment Looker would not give
    the second model. One model per view name cannot hold both shapes, so the segment is kept (it
    is opt-in, and dropping it would un-filter the model that wanted it) and the divergence warned.
    """
    import logging
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    sidecar = 'include: "/views/filtered.explore.lkml"\n'

    def parse(models, explore_body):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        for name, include in models:
            (directory / f"{name}.model.lkml").write_text(include)
        (directory / "views" / "orders.view.lkml").write_text(view % ("orders", "real_orders"))
        (directory / "views" / "filtered.explore.lkml").write_text(explore_body)
        return LookMLAdapter().parse(str(directory)).get_model("orders")

    where = "explore: orders {\n  sql_always_where: ${orders.id} > 100 ;;\n}\n"

    # One model includes the filtered explore, another uses the base view without it -> warn, keep.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        model = parse(
            [
                ("orders", 'include: "/views/orders.view.lkml"\n'),
                ("filtered", 'include: "/views/orders.view.lkml"\n' + sidecar),
            ],
            where,
        )
    assert [s.name for s in model.segments] == ["_sql_always_where_orders"]  # kept
    assert "mandatory filter" in caplog.text

    # Every model using the base view includes the explore: no divergence.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        model = parse(
            [
                ("a", 'include: "/views/orders.view.lkml"\n' + sidecar),
                ("b", 'include: "/views/orders.view.lkml"\n' + sidecar),
            ],
            where,
        )
    assert [s.name for s in model.segments] == ["_sql_always_where_orders"]
    assert "mandatory filter" not in caplog.text

    # always_filter diverges the same way.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        parse(
            [
                ("orders", 'include: "/views/orders.view.lkml"\n'),
                ("filtered", 'include: "/views/orders.view.lkml"\n' + sidecar),
            ],
            'explore: orders {\n  always_filter: {\n    filters: [orders.id: "100"]\n  }\n}\n',
        )
    assert "mandatory filter" in caplog.text

    # A partial explore with NO mandatory filter has nothing to leak -> no warning.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        parse(
            [
                ("orders", 'include: "/views/orders.view.lkml"\n'),
                ("filtered", 'include: "/views/orders.view.lkml"\n' + sidecar),
            ],
            'explore: orders {\n  label: "Orders"\n}\n',
        )
    assert "mandatory filter" not in caplog.text


def test_lookml_shared_explore_sidecar_checked_against_one_model_scope(caplog):
    """An explore in a sidecar two models share resolves within ONE model, not their union.

    Giving a shared sidecar the combined view set let `explore: orders { join: customers }` attach
    when an orders model included only `orders` and a customers model only `customers` -- a pair no
    single LookML model can see -- wiring queries to an out-of-scope table. Base view and joins are
    now checked against the same model's scope.
    """
    import logging
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    sidecar = 'include: "/views/shared.explore.lkml"\n'
    join = (
        "explore: orders {\n  join: customers { sql_on: ${orders.id} = ${customers.id} ;; "
        "relationship: many_to_one }\n}\n"
    )

    def relationships(*models):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        for name, include in models:
            (directory / f"{name}.model.lkml").write_text(include)
        for name in ("orders", "customers"):
            (directory / "views" / f"{name}.view.lkml").write_text(view % (name, f"real_{name}"))
        (directory / "views" / "shared.explore.lkml").write_text(join)
        graph = LookMLAdapter().parse(str(directory))
        return [r.name for r in (graph.get_model("orders").relationships or [])]

    # Neither model sees both views: the join belongs to no model.
    assert (
        relationships(
            ("orders", 'include: "/views/orders.view.lkml"\n' + sidecar),
            ("customers", 'include: "/views/customers.view.lkml"\n' + sidecar),
        )
        == []
    )

    # One model seeing both still wires it.
    assert relationships(("both", 'include: "/views/*.view.lkml"\n' + sidecar)) == ["customers"]

    # Two models that both see everything agree: no divergence to report.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        wired = relationships(
            ("a", 'include: "/views/*.view.lkml"\n' + sidecar), ("b", 'include: "/views/*.view.lkml"\n' + sidecar)
        )
    assert wired == ["customers"]
    assert "only some of the models" not in caplog.text

    # One model has the target and another does not: kept for the model that does, and reported.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sidemantic.adapters.lookml"):
        wired = relationships(
            ("a", 'include: "/views/*.view.lkml"\n' + sidecar),
            ("b", 'include: "/views/orders.view.lkml"\n' + sidecar),
        )
    assert wired == ["customers"]
    assert "only some of the models" in caplog.text


def test_lookml_included_explore_sidecar_inherits_model_scope():
    """An explore sidecar included by a model must be scoped like the model file itself.

    Scope was keyed on the file NAME (`.model.lkml`), but explores routinely live in an included
    `orders.explore.lkml` sidecar. That file fell through to the unscoped branch, so its joins
    could wire to any loaded unique view -- including an archived one -- and send queries to a
    stale table. Scope is keyed on reachability instead: a sidecar inherits its includer's scope.
    """
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    join = (
        "explore: orders {\n  join: customers { sql_on: ${orders.id} = ${customers.id} ;; "
        "relationship: many_to_one }\n}\n"
    )

    # Sidecar included by the model; customers.view.lkml is NOT included.
    archived = Path(tempfile.mkdtemp())
    (archived / "archive").mkdir()
    (archived / "orders.model.lkml").write_text('include: "orders.view.lkml"\ninclude: "orders.explore.lkml"\n')
    (archived / "orders.view.lkml").write_text(view % ("orders", "real_orders"))
    (archived / "orders.explore.lkml").write_text(join)
    (archived / "archive" / "customers.view.lkml").write_text(view % ("customers", "archived_cust"))

    graph = LookMLAdapter().parse(str(archived))
    assert [r.name for r in (graph.get_model("orders").relationships or [])] == []
    assert graph.models["customers"].table == "archived_cust"  # loaded, but never joined

    # The same sidecar wires normally when the model includes the join target.
    included = Path(tempfile.mkdtemp())
    (included / "orders.model.lkml").write_text(
        'include: "orders.view.lkml"\ninclude: "customers.view.lkml"\ninclude: "orders.explore.lkml"\n'
    )
    (included / "orders.view.lkml").write_text(view % ("orders", "real_orders"))
    (included / "customers.view.lkml").write_text(view % ("customers", "real_customers"))
    (included / "orders.explore.lkml").write_text(join)
    assert [r.name for r in (LookMLAdapter().parse(str(included)).get_model("orders").relationships or [])] == [
        "customers"
    ]

    # A sidecar two models share is checked against ONE model's scope, never their combined views:
    # neither model below sees both `orders` and `customers`, so the join belongs to no model. See
    # test_lookml_shared_explore_sidecar_checked_against_one_model_scope for the full contract.
    shared = Path(tempfile.mkdtemp())
    (shared / "a.model.lkml").write_text('include: "orders.view.lkml"\ninclude: "shared.explore.lkml"\n')
    (shared / "b.model.lkml").write_text('include: "customers.view.lkml"\ninclude: "shared.explore.lkml"\n')
    (shared / "orders.view.lkml").write_text(view % ("orders", "real_orders"))
    (shared / "customers.view.lkml").write_text(view % ("customers", "real_customers"))
    (shared / "shared.explore.lkml").write_text(join)
    assert [r.name for r in (LookMLAdapter().parse(str(shared)).get_model("orders").relationships or [])] == []


def test_lookml_explore_joins_scoped_to_included_views():
    """A join must not wire to a view the explore's own model file cannot see.

    Scoping only the explore's BASE view left its joins unrestricted: unique un-included views
    are still installed, so a model including just orders.view.lkml could silently wire
    `join: customers` to an archived customers.view.lkml and query the wrong table. A join to a
    NEVER-DEFINED view is still kept -- that dangling relationship is a real error `validate`
    must surface, and dropping it would hide the typo.
    """
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"

    def relationships(explore, include, *, archived=(), views=("orders",)):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        (directory / "archive").mkdir()
        (directory / "orders.model.lkml").write_text(f'include: "{include}"\n' + explore)
        for name in views:
            (directory / "views" / f"{name}.view.lkml").write_text(view % (name, f"real_{name}"))
        for name in archived:
            (directory / "archive" / f"{name}.view.lkml").write_text(view % (name, f"archived_{name}"))
        graph = LookMLAdapter().parse(str(directory))
        return graph, [r.name for r in (graph.get_model("orders").relationships or [])]

    join = (
        "explore: orders {\n  join: customers { sql_on: ${orders.id} = ${customers.id} ;; "
        "relationship: many_to_one }\n}\n"
    )

    # Archived join target: not reached by the model's include -> not wired.
    graph, archived_join = relationships(join, "/views/orders.view.lkml", archived=("customers",))
    assert archived_join == [], archived_join
    assert "customers" in graph.models  # still loaded; just not joinable from this model

    # Included join target still wires.
    _, included_join = relationships(join, "/views/*.view.lkml", views=("orders", "customers"))
    assert included_join == ["customers"]

    # A join's from: target is the view it actually reads, so it is scoped too.
    aliased = (
        "explore: orders {\n  join: c_alias { from: customers  sql_on: ${orders.id} = ${c_alias.id} ;; "
        "relationship: many_to_one }\n}\n"
    )
    _, aliased_join = relationships(aliased, "/views/orders.view.lkml", archived=("customers",))
    assert aliased_join == [], aliased_join

    # A never-defined target is NOT scoped away: validate must still surface it.
    typo = (
        "explore: orders {\n  join: typo_view { sql_on: ${orders.id} = ${typo_view.id} ;; "
        "relationship: many_to_one }\n}\n"
    )
    _, dangling = relationships(typo, "/views/orders.view.lkml")
    assert dangling == ["typo_view"], dangling


def test_lookml_project_parse_ignores_explores_from_unincluded_files():
    """An explore in a model file no include reaches must not mutate the live model.

    Loading the tree as one project runs every file's explores against the loaded views, so an
    archived or alternate model file's joins would silently attach to the live model. A file
    DECLARING includes is part of the project, so the active model file's explores still apply.
    """
    import tempfile

    view = "view: %s {\n  sql_table_name: %s ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    join = (
        "explore: orders {\n  join: customers { sql_on: ${orders.id} = ${customers.id} ;; "
        "relationship: many_to_one }\n}\n"
    )

    def relationships(*, live_explore, archived_explore):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        (directory / "archive").mkdir()
        (directory / "orders.model.lkml").write_text('include: "/views/*.view.lkml"\n' + (join if live_explore else ""))
        (directory / "views" / "orders.view.lkml").write_text(view % ("orders", "real_orders"))
        (directory / "views" / "customers.view.lkml").write_text(view % ("customers", "cust"))
        if archived_explore:
            (directory / "archive" / "old.model.lkml").write_text(join)
        model = LookMLAdapter().parse(str(directory)).get_model("orders")
        return [r.name for r in (model.relationships or [])]

    assert relationships(live_explore=False, archived_explore=True) == []  # archived join must not leak
    assert relationships(live_explore=True, archived_explore=False) == ["customers"]  # live one applies
    assert relationships(live_explore=True, archived_explore=True) == ["customers"]  # and only once


def test_lookml_project_parse_ignores_refinements_from_unincluded_files():
    """A refinement in a file no `include:` reaches must not override a loaded view.

    Parsing the whole tree merges every `view: +...` it finds, so a stale refinement left in e.g.
    archive/ could silently change a model's sql_table_name even though the LookML model only
    includes views/. Only the REFINEMENT merge is scoped -- views still all parse -- and a project
    that declares no includes is unaffected.
    """
    import tempfile

    def build(*, declares_includes, refinement_dir):
        directory = Path(tempfile.mkdtemp())
        (directory / "views").mkdir()
        (directory / "archive").mkdir()
        if declares_includes:
            (directory / "orders.model.lkml").write_text('include: "/views/*.view.lkml"\nexplore: orders {}\n')
        (directory / "views" / "orders.view.lkml").write_text(
            "view: orders {\n  sql_table_name: real_orders ;;\n"
            "  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
        )
        (directory / refinement_dir / "ref.view.lkml").write_text("view: +orders {\n  sql_table_name: REFINED ;;\n}\n")
        return LookMLAdapter().parse(str(directory)).get_model("orders").table

    # Declared includes: an un-included refinement is ignored, an included one still applies.
    assert build(declares_includes=True, refinement_dir="archive") == "real_orders"
    assert build(declares_includes=True, refinement_dir="views") == "REFINED"
    # No includes declared (the common single-directory project): nothing is filtered.
    assert build(declares_includes=False, refinement_dir="archive") == "REFINED"


def test_lookml_refinement_refreshes_graph_level_metrics():
    """Replacing a refined model must refresh its graph-level metrics, not leave a stale one.

    add_model auto-registers a view's graph-level measures (period_over_period ->
    time_comparison) into graph.metrics. A refinement turning that measure into a normal one left
    the ORIGINAL time_comparison registered, so a CLI load exposed a metric no model still defines.
    A refinement that does NOT touch it must leave the graph metric registered.
    """
    import tempfile

    base_view = (
        "view: base {\n  sql_table_name: t ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n"
        "  dimension: created { type: time  timeframes: [date]  sql: ${TABLE}.created ;; }\n"
        "  measure: cnt { type: count }\n"
        "  measure: pop { type: period_over_period  based_on: cnt  period: date  kind: relative_change }\n}\n"
    )

    # Refined into a normal measure -> the stale graph-level metric must be gone.
    refined = Path(tempfile.mkdtemp())
    (refined / "a.lkml").write_text(base_view)
    (refined / "b.lkml").write_text("view: +base {\n  measure: pop { type: count }\n}\n")
    assert "pop" not in LookMLAdapter().parse(str(refined)).metrics

    # An unrelated refinement leaves the graph-level metric registered.
    untouched = Path(tempfile.mkdtemp())
    (untouched / "a.lkml").write_text(base_view)
    (untouched / "b.lkml").write_text("view: +base {\n  label: Refined\n}\n")
    graph = LookMLAdapter().parse(str(untouched))
    assert graph.metrics["pop"].type == "time_comparison"


def test_lookml_refinement_deep_merges_partial_field_properties():
    """A refinement that sets ONE property of a field must not clobber the field's other props.

    `view: +base { dimension: id { label: "ID" } }` only sets a label, so the base field's sql /
    type / primary_key must survive. Merging the PARSED models replaced the field wholesale (the
    parser defaults a bare `dimension:` to a categorical with no sql), so the model silently fell
    back to a default column. Applies to a refinement in the SAME file or another one.
    """
    import tempfile

    base_view = (
        "view: base {\n  sql_table_name: t ;;\n"
        "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }\n"
        "  dimension: amount { type: number  sql: ${TABLE}.amount ;; }\n}\n"
    )
    refinement = 'view: +base {\n  dimension: id { label: "The ID" }\n}\n'

    def check(model):
        by_name = {d.name: d for d in model.dimensions}
        assert by_name["id"].sql == "{model}.id"  # base sql survived
        assert by_name["id"].type == "numeric"  # base type survived
        assert by_name["id"].label == "The ID"  # refinement applied
        assert model.primary_key == "id"  # base primary_key survived
        assert by_name["amount"].sql == "{model}.amount"  # untouched field intact

    # Same file.
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(base_view + refinement)
        f.flush()
        check(LookMLAdapter().parse(Path(f.name)).get_model("base"))

    # Across files (the CLI project-load path).
    directory = Path(tempfile.mkdtemp())
    (directory / "a.lkml").write_text(base_view)
    (directory / "b.lkml").write_text(refinement)
    check(LookMLAdapter().parse(str(directory)).get_model("base"))


def test_lookml_project_parse_merges_refinements_deterministically():
    """Refinements merge in sorted file order, not filesystem traversal order.

    Two refinement files setting the same property must resolve the same way on every load;
    an unsorted rglob would make the winner depend on directory traversal.
    """
    import tempfile

    directory = Path(tempfile.mkdtemp())
    (directory / "a.lkml").write_text(
        "view: base {\n  sql_table_name: t ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    )
    (directory / "m_second.lkml").write_text("view: +base {\n  label: SECOND\n}\n")
    (directory / "b_first.lkml").write_text("view: +base {\n  label: FIRST\n}\n")

    labels = {LookMLAdapter().parse(str(directory)).get_model("base").meta.get("label") for _ in range(5)}
    assert len(labels) == 1, f"nondeterministic refinement merge: {labels}"
    assert labels == {"SECOND"}  # last file in SORTED order wins


def test_lookml_directory_load_non_strict_falls_back_after_project_parse_error():
    """A single malformed .lkml must not drop every valid sibling in a non-strict load.

    LookML is parsed as one project, so a parse error there covers the whole tree. Non-strict
    loading must fall back to the per-file scan (loading what it can, warning about the bad file)
    rather than skipping every .lkml. Strict mode must still raise.
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    directory = Path(tempfile.mkdtemp())
    (directory / "good.lkml").write_text(
        "view: good {\n  sql_table_name: raw_good ;;\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n}\n"
    )
    (directory / "bad.lkml").write_text("view: bad { dimension: x { sql: ${TABLE}.x ;; }\n")  # unclosed brace

    layer = SemanticLayer()
    load_from_directory(layer, directory, strict=False)
    assert "good" in layer.graph.models  # valid sibling survives the bad file

    with pytest.raises(ValueError):
        load_from_directory(SemanticLayer(), directory, strict=True)


def test_lookml_directory_load_keeps_standalone_metric_overwritten_by_template():
    """A directory load must not lose a standalone metric that shares a template metric's name.

    `all_metrics.update(graph.metrics)` overwrites by name, so if a .lkml template's graph metric
    landed AFTER a same-named standalone metric, the marked template metric won and the orphan
    drop popped the name entirely -- silently losing the valid standalone. LookML is now parsed as
    one project BEFORE the per-file scan, so a standalone always wins; an orphan template metric
    with no standalone is still dropped.
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    template = (
        "view: tmpl {\n  extension: required\n  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }\n"
        "  dimension: created { type: time  timeframes: [date]  sql: ${TABLE}.created ;; }\n"
        "  measure: cnt { type: count }\n"
        "  measure: pop { type: period_over_period  based_on: cnt  period: date  kind: relative_change }\n}\n"
    )
    base_model = (
        "models:\n  - name: orders\n    table: raw_orders\n    primary_key: id\n"
        "    dimensions:\n      - name: id\n        type: numeric\n        sql: id\n"
        "    metrics:\n      - name: total\n        agg: sum\n        sql: amount\n"
    )

    # 'z.lkml' sorts AFTER the metric file -- the overwrite-then-drop ordering.
    with_standalone = Path(tempfile.mkdtemp())
    (with_standalone / "z.lkml").write_text(template)
    (with_standalone / "metrics.yml").write_text(
        base_model + "metrics:\n  - name: pop\n    type: ratio\n    numerator: orders.total\n"
        "    denominator: orders.total\n"
    )
    layer = SemanticLayer()
    load_from_directory(layer, with_standalone)
    assert "tmpl" not in layer.graph.models  # abstract template still dropped
    assert "pop" in layer.graph.metrics  # standalone survives the template's same-named metric
    assert layer.graph.metrics["pop"].type == "ratio"  # ...and it IS the standalone

    # With NO standalone, the orphan template metric is still dropped.
    template_only = Path(tempfile.mkdtemp())
    (template_only / "z.lkml").write_text(template)
    (template_only / "m.yml").write_text(base_model)
    layer2 = SemanticLayer()
    load_from_directory(layer2, template_only)
    assert "pop" not in layer2.graph.metrics


def test_lookml_dropped_template_metric_dropped_despite_samename_local_measure():
    """A dropped template's graph metric is removed even if a surviving model has a same-named measure.

    The orphan check matches graph-level metric types (time_comparison/conversion), not bare
    names: a surviving `orders.pop` SIMPLE measure must not keep the template's `pop`
    period_over_period alive (which would expose a metric whose base model is gone).
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write(
            "view: base { extension: required "
            "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "  dimension: amt { type: number  sql: ${TABLE}.amt ;; } "
            "  measure: total { type: sum  sql: ${amt} ;; } "
            "  measure: pop { type: period_over_period  based_on: total  period: year  kind: difference } } "
            "view: orders { sql_table_name: orders ;; "
            "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "  measure: pop { type: count } }"
        )

    layer = SemanticLayer()
    load_from_directory(layer, d)
    assert "base" not in layer.graph.models
    assert "pop" not in layer.graph.metrics  # template graph metric dropped despite orders.pop
    assert layer.graph.get_model("orders").get_metric("pop") is not None  # local measure survives


def test_lookml_dropped_template_graph_metric_not_orphaned():
    """A graph metric (period_over_period) on a dropped template must not linger after CLI load.

    add_model auto-registers time_comparison/conversion measures as graph metrics; when the
    only model carrying it is a skipped extension:required template, the metric must be
    dropped too, else compile/info expose a metric whose base model is missing.
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write(
            "view: base { extension: required "
            "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "  dimension: amt { type: number  sql: ${TABLE}.amt ;; } "
            "  measure: total { type: sum  sql: ${amt} ;; } "
            "  measure: pop { type: period_over_period  based_on: total  period: year  kind: difference } }"
        )

    layer = SemanticLayer()
    load_from_directory(layer, d)  # must not raise
    assert "base" not in layer.graph.models  # template skipped
    assert "pop" not in layer.graph.metrics  # orphaned graph metric dropped too


def test_lookml_registerability_keys_off_parser_marker_not_user_meta():
    """A tableless model with user `extension_required` meta but no parser marker must surface error.

    The skip keys off the parser-owned `lookml_template` marker so a native/other-format
    model that merely carries the public `extension_required` key is still registered (and
    its missing-source error surfaced), not silently dropped.
    """
    from types import SimpleNamespace

    from sidemantic.loaders import _is_registerable_model

    user_meta = SimpleNamespace(table=None, sql=None, dax=None, source_uri=None, meta={"extension_required": True})
    assert _is_registerable_model(user_meta) is True  # surfaces error, not dropped
    template = SimpleNamespace(
        table=None, sql=None, dax=None, source_uri=None, meta={"extension_required": True, "lookml_template": True}
    )
    assert _is_registerable_model(template) is False  # genuine LookML template skipped


def test_lookml_abstract_marker_survives_later_refinement_meta_overwrite():
    """A refinement overwriting meta must not strip the abstract marker the loader keys off.

    `+base { extension: required }` then `+base { label: ... }` leaves base tableless but
    its final meta only has the label; the marker is re-asserted so the CLI loader still
    skips it instead of raising the no-table validation error.
    """
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write(
            "view: base { dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "dimension: x { type: number  sql: ${TABLE}.x ;; } } "
            "view: +base { extension: required } "
            'view: +base { label: "Base" } '
            "view: orders { extends: [base]  dimension: y { type: number  sql: ${TABLE}.y ;; } }"
        )

    layer = SemanticLayer()
    load_from_directory(layer, d)  # must not raise even though `extension_required` was overwritten
    assert "base" not in layer.graph.models
    assert layer.graph.get_model("orders").table == "orders"


def test_lookml_fk_inference_skips_abstract_template_no_dangling_rel():
    """FK inference must not target a skipped abstract template, leaving a dangling relationship."""
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write(
            "view: customer { extension: required  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } } "
            "view: customers { extends: [customer]  sql_table_name: customers ;; "
            "dimension: name { type: string  sql: ${TABLE}.name ;; } } "
            "view: orders { sql_table_name: orders ;; dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "dimension: customer_id { type: number  sql: ${TABLE}.customer_id ;; } }"
        )

    layer = SemanticLayer()
    load_from_directory(layer, d)
    assert "customer" not in layer.graph.models  # abstract template not registered
    # No model carries a relationship pointing at the skipped template.
    for model in layer.graph.models.values():
        for rel in getattr(model, "relationships", []) or []:
            assert rel.name in layer.graph.models, f"dangling relationship to {rel.name}"


def test_lookml_explore_join_to_template_no_dangling_rel():
    """An explore join to a skipped template must not leave a dangling relationship."""
    import tempfile

    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    d = tempfile.mkdtemp()
    with open(Path(d) / "v.lkml", "w") as f:
        f.write(
            "view: customer { extension: required  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } } "
            "view: orders { sql_table_name: orders ;; dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "dimension: customer_id { type: number  sql: ${TABLE}.customer_id ;; } } "
            "explore: orders { join: customer { sql_on: ${orders.customer_id} = ${customer.id} ;; relationship: many_to_one } }"
        )

    layer = SemanticLayer()
    load_from_directory(layer, d)
    assert "customer" not in layer.graph.models
    for model in layer.graph.models.values():
        for rel in getattr(model, "relationships", []) or []:
            assert rel.name in layer.graph.models, f"dangling relationship to {rel.name}"


def test_lookml_active_layer_explore_join_to_template_no_dangling_rel():
    """Same as above but via the ACTIVE-layer parse() path (with SemanticLayer())."""
    import tempfile

    from sidemantic import SemanticLayer

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        f.write(
            "view: customer { extension: required  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } } "
            "view: orders { sql_table_name: orders ;; dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
            "dimension: customer_id { type: number  sql: ${TABLE}.customer_id ;; } } "
            "explore: orders { join: customer { sql_on: ${orders.customer_id} = ${customer.id} ;; relationship: many_to_one } }"
        )
        f.flush()
        path = Path(f.name)

    with SemanticLayer() as layer:
        LookMLAdapter().parse(path)
    assert "customer" not in layer.graph.models
    for model in layer.graph.models.values():
        for rel in getattr(model, "relationships", []) or []:
            assert rel.name in layer.graph.models, f"dangling relationship to {rel.name}"


def test_lookml_extends_tableless_view_keeps_own_table():
    """A child extending a tableless base must default to its OWN name, not inherit the base's default."""
    graph = _parse_lkml(
        "view: base { dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
        "dimension: x { type: number  sql: ${TABLE}.x ;; } } "
        "view: child { extends: [base]  dimension: y { type: number  sql: ${TABLE}.y ;; } }"
    )
    assert graph.get_model("base").table == "base"
    assert graph.get_model("child").table == "child"


def test_lookml_refinement_adds_fields_then_defaults_table():
    """A fieldless base whose fields come from a +refinement still defaults its table after merge."""
    graph = _parse_lkml(
        "view: orders {} view: +orders { dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } }"
    )
    assert graph.get_model("orders").table == "orders"


def test_lookml_filter_only_view_gets_default_table():
    """A view that declares only LookML `filter` fields (-> segments) still defaults its table.

    Such a view is not 'fieldless' -- without the default it fails validation (no table/sql).
    """
    graph = _parse_lkml("view: ff { filter: recent { type: yesno  sql: ${TABLE}.x ;; } }")
    model = graph.get_model("ff")
    assert model.table == "ff"
    assert [s.name for s in model.segments] == ["recent"]


def test_lookml_concrete_child_of_abstract_view_gets_table():
    """A concrete view extending an extension:required base must default to its OWN name, not be treated as abstract."""
    graph = _parse_lkml(
        "view: base { extension: required  "
        "dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } "
        "dimension: x { type: number  sql: ${TABLE}.x ;; } } "
        "view: orders { extends: [base]  dimension: y { type: number  sql: ${TABLE}.y ;; } }"
    )
    assert graph.get_model("base").table is None  # abstract base stays table-less
    assert graph.get_model("orders").table == "orders"  # concrete child defaults to its name


def test_lookml_unsupported_derived_table_marker_survives_refinement():
    """The unsupported-derived_table marker must survive a refinement that carries meta (e.g. label)."""
    graph = _parse_lkml(
        "view: ndt { derived_table: { sql_trigger_value: SELECT CURRENT_DATE() ;; } "
        "dimension: id { type: number  sql: ${TABLE}.id ;; } } "
        'view: +ndt { label: "NDT View" }'
    )
    ndt = graph.get_model("ndt")
    # Not defaulted to a physical table even though the refinement replaced its meta.
    assert ndt.table is None
    assert ndt.sql is None


def test_lookml_child_inheriting_unsupported_derived_table_not_defaulted():
    """A child extending a parent with an unsupported derived_table must stay table-less too."""
    graph = _parse_lkml(
        "view: base_ndt { derived_table: { sql_trigger_value: SELECT 1 ;; } "
        "dimension: id { type: number  sql: ${TABLE}.id ;; } } "
        "view: child_ndt { extends: [base_ndt] }"
    )
    # Inherited the unsupported-derived_table marker via extends -> not defaulted.
    assert graph.get_model("child_ndt").table is None


def test_lookml_refinement_added_extension_required_stays_abstract():
    """A +refinement that adds extension: required must keep the refined view tableless."""
    graph = _parse_lkml(
        "view: base { dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } } "
        "view: +base { extension: required }"
    )
    assert graph.get_model("base").table is None


def test_lookml_abstract_flag_survives_later_refinement():
    """extension: required added by one refinement survives a later refinement that replaces meta."""
    graph = _parse_lkml(
        "view: base { dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } } "
        "view: +base { extension: required } "
        'view: +base { label: "Base" }'
    )
    assert graph.get_model("base").table is None


def test_lookml_child_of_unsupported_dt_with_own_meta_not_defaulted():
    """A child extending an unsupported derived_table base must stay tableless even with its own meta."""
    graph = _parse_lkml(
        "view: base { derived_table: { sql_trigger_value: SELECT 1 ;; } "
        "dimension: id { type: number  sql: ${TABLE}.id ;; } } "
        'view: child { extends: [base]  label: "Child" }'
    )
    assert graph.get_model("child").table is None


def test_lookml_export_unmapped_aggregations_not_count():
    """Unmapped aggregations / complex types must not be silently exported as COUNT."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="id", type="numeric", sql="id")],
        metrics=[
            Metric(name="med", agg="median", sql="amount"),
            Metric(name="sd", agg="stddev", sql="amount"),
            Metric(name="va", agg="variance", sql="amount"),
            Metric(name="cnt", agg="count"),
            Metric(name="sm", agg="sum", sql="amount"),
            Metric(name="cum", type="cumulative", sql="amount"),
        ],
    )
    graph = SemanticGraph()
    graph.add_model(model)

    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()

    # median has a native Looker type
    assert "type: median" in text
    # stddev/variance become type: number with an explicit SQL aggregate
    assert "STDDEV(" in text
    assert "VAR_SAMP(" in text
    # the complex cumulative metric is skipped (no LookML equivalent), not COUNT
    assert "measure: cum" not in text
    # genuine count/sum unchanged
    assert "type: count" in text
    assert "type: sum" in text

    # Round-trip: median survives, none of these come back as a plain count corruption
    reimported = {m.name: m for m in LookMLAdapter().parse(Path(out)).get_model("orders").metrics}
    assert reimported["med"].agg == "median"
    assert "cum" not in reimported  # skipped on export


def test_lookml_export_complex_type_with_agg_skipped():
    """A complex-type metric carrying an agg (e.g. cumulative rolling avg) must be skipped, not exported as a plain measure."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="o",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[Metric(name="roll", type="cumulative", agg="avg", sql="amount")],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: roll" not in text
    assert "type: average" not in text  # not silently downgraded to a plain average


def test_lookml_export_filtered_distinct_stddev_keeps_distinct_outside_case():
    """A filtered DISTINCT stddev/variance must keep DISTINCT OUTSIDE the folded CASE."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
                Dimension(name="amount", type="numeric", sql="amount"),
            ],
            metrics=[
                Metric(name="sd", agg="stddev", sql="DISTINCT {model}.amount", filters=["{model}.status = 'done'"])
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: sd" in text
    assert "STDDEV(DISTINCT CASE WHEN" in text  # DISTINCT outside the CASE
    assert "THEN DISTINCT" not in text  # never DISTINCT inside the CASE (invalid SQL)


def test_lookml_export_filtered_sql_aggregate_folds_filter():
    """A filtered stddev/variance (type: number path) must fold the filter into SQL, not drop it."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="o",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["{model}.status = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    # The single measure folds its filter into the SQL aggregate (the model-qualified
    # ref is resolved to a ${TABLE}-qualified column)...
    assert "STDDEV(CASE WHEN" in text
    assert "(${TABLE}.status) = 'done'" in text
    # ...and is not also emitted as a (non-applied) LookML filters block.
    assert "filters" not in text


def test_lookml_approximate_distinct_preserved_in_post_sql_measure():
    """A post-SQL measure (percent_of_total) over an approximate count_distinct must stay approximate."""
    graph = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  measure: uu { type: count_distinct  sql: ${TABLE}.user_id ;; approximate: yes }
  measure: pct { type: percent_of_total  sql: ${uu} ;; }
}
"""
    )
    pct = graph.get_model("v").get_metric("pct")
    assert "APPROX_COUNT_DISTINCT" in pct.sql
    assert "COUNT(DISTINCT" not in pct.sql


def test_lookml_post_sql_measure_preserves_filtered_base_measure_filter():
    """A post-SQL measure over a FILTERED base must keep the base's filter.

    A percent_of_total over `uu` (count_distinct, approximate, filtered to completed) expanded via
    the bare `<AGG>({model}.uu)` template, which carries no filter -- so the percent was computed
    over every row instead of the filtered population. A filtered base must expand through the
    FILTERED aggregate built in the first pass. An UNFILTERED base keeps the template form.
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: user_id { type: number  sql: ${TABLE}.user_id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: uu { type: count_distinct  approximate: yes  sql: ${user_id} ;; filters: [status: "completed"] }
  measure: pct { type: percent_of_total  sql: ${uu} ;; }
  measure: total { type: sum  sql: ${TABLE}.amount ;; }
  measure: pct_unfiltered { type: percent_of_total  sql: ${total} ;; }
}
"""
    )
    model = graph.get_model("orders")
    pct = model.get_metric("pct")
    # The base's filter is carried into the expansion, over the REAL column, still approximate.
    assert "completed" in pct.sql, pct.sql
    assert "APPROX_COUNT_DISTINCT" in pct.sql and "COUNT(DISTINCT" not in pct.sql, pct.sql
    assert "user_id" in pct.sql, pct.sql
    # An UNFILTERED base is unchanged (still the aggregate template over the measure ref).
    assert "{model}.total" in model.get_metric("pct_unfiltered").sql


def test_lookml_post_sql_measure_expands_untemplated_complete_base():
    """A post-SQL measure over an UNTEMPLATED complete type:number base must expand its full SQL.

    An unfiltered complete base like STDDEV has no native <AGG>({model}.<measure>) template, so the
    percent_of_total resolved it to `{model}.sd` -- but complete measures are projected via
    dedicated raw aliases, so the query referenced a missing `orders_cte.sd` and failed. Expand
    such a base through its full SQL; a native aggregate base keeps the template form.
    """
    import duckdb

    from sidemantic import SemanticLayer

    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: amount { type: number  sql: ${TABLE}.amount ;; }
  measure: sd { type: number  sql: STDDEV(${amount}) ;; }
  measure: total { type: sum  sql: ${amount} ;; }
  measure: pct { type: percent_of_total  sql: ${sd} ;; }
  measure: pct_native { type: percent_of_total  sql: ${total} ;; }
}
"""
    )
    model = graph.get_model("orders")
    # The complete base's full SQL is inlined; the native base keeps its measure-ref template.
    assert "STDDEV" in model.get_metric("pct").sql and "{model}.sd" not in model.get_metric("pct").sql
    assert "{model}.total" in model.get_metric("pct_native").sql

    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    sql = layer.compile(metrics=["orders.pct"])
    con = duckdb.connect()
    con.execute("create table orders as select 1 id, 10.0 amount union all select 2, 30 union all select 3, 60")
    assert con.execute(sql).fetchall() == [(1.0,)]  # single group -> its share is 1.0


def test_lookml_post_sql_measure_with_unresolvable_base_dropped():
    """A percent_of_total over a filtered type:number base whose complete SQL can't be cached is dropped.

    A filtered type: number using a dialect-renamed aggregate (APPROX_COUNT_DISTINCT) has its
    force-fold prepass bail, so the base is not in measure_full_sql_lookup; the ref would fall
    through to a bare {model}.<measure> the complete measure never projects as a column. Drop the
    post-SQL measure instead of emitting a missing-column CTE. A resolvable base still works.
    """
    graph = _parse_lkml(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: approx_ratio {
    type: number
    sql: APPROX_COUNT_DISTINCT(${TABLE}.user_id) / NULLIF(COUNT(*), 0) ;;
    filters: [status: "completed"]
  }
  measure: total { type: sum  sql: ${TABLE}.amount ;; }
  measure: pct_bad { type: percent_of_total  sql: ${approx_ratio} ;; }
  measure: pct_ok { type: percent_of_total  sql: ${total} ;; }
}
"""
    )
    names = {m.name for m in graph.get_model("orders").metrics}
    assert "pct_bad" not in names  # unresolvable base -> dropped
    assert "pct_ok" in names  # resolvable native base -> kept
    assert "approx_ratio" in names  # the base measure itself still imports


def test_lookml_export_running_total_roundtrips():
    """An imported running_total (cumulative + table_calculation meta) round-trips, not dropped."""
    import tempfile

    graph = _parse_lkml(
        """
view: v {
  sql_table_name: t ;;
  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }
  measure: total { type: sum  sql: ${TABLE}.amt ;; }
  measure: rt { type: running_total  sql: ${total} ;; }
}
"""
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    assert "type: running_total" in open(out).read()
    reimported = {m.name: m for m in LookMLAdapter().parse(Path(out)).get_model("v").metrics}
    assert "rt" in reimported
    assert (reimported["rt"].meta or {}).get("table_calculation") == "running_total"


def test_lookml_export_approximate_distinct_preserved():
    """approx_count_distinct must export as count_distinct + approximate: yes and round-trip."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="o",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[Metric(name="uu", agg="approx_count_distinct", sql="user_id")],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "type: count_distinct" in text
    assert "approximate: yes" in text
    reimported = {m.name: m for m in LookMLAdapter().parse(Path(out)).get_model("o").metrics}
    assert reimported["uu"].agg == "approx_count_distinct"


def test_lookml_export_opaque_complete_sql_measure_skipped():
    """An agg-less sql_is_complete measure has no faithful LookML form and is skipped, not exported as broken derived."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="o",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[Metric(name="opaque", agg=None, sql="status_label", sql_is_complete=True)],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    assert "measure: opaque" not in open(out).read()


def test_lookml_export_folded_filter_resolves_dimension_sql():
    """A folded filter must reference the dimension's SQL column, not the bare dimension name."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="o",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["{model}.status = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    # Resolved dimension column is qualified (${TABLE}.) and parenthesized.
    assert "(${TABLE}.order_status) = 'done'" in open(out).read()


def test_lookml_export_folded_filter_resolves_model_qualified_ref():
    """A folded filter qualified by the model's own NAME (orders.status) resolves too."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["orders.status = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "(${TABLE}.order_status) = 'done'" in text
    assert "orders.status" not in text  # model-name prefix was normalized away


def test_lookml_export_complete_aggregate_sql_measure_not_dropped():
    """An opaque COMPLETE aggregate measure (e.g. from Cube) exports as type: number, not dropped."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[Metric(name="total_amt", agg=None, sql="SUM({model}.amount)", sql_is_complete=True)],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: total_amt" in text  # not dropped
    assert "type: number" in text
    assert "SUM(${TABLE}.amount)" in text


def test_lookml_export_string_measure_not_forced_to_number():
    """A non-aggregate (string/yesno/row-level) agg-less measure must NOT export as number.

    Looker measures aggregate; forcing a raw column measure to type: number re-imports as
    a derived metric and crashes ({model}.status read as a metric dep), so it is skipped.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[Metric(name="label", agg=None, sql="{model}.status")],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    assert "measure: label" not in open(out).read()  # skipped, not exported as type: number


def test_lookml_export_complete_aggregate_with_filters_folds_into_aggregate():
    """A complete aggregate measure WITH filters folds them into the aggregate (not a dropped filter)."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
            ],
            metrics=[
                Metric(
                    name="done_amt",
                    agg=None,
                    sql="SUM({model}.amount)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'done'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    # Filter folded INSIDE the aggregate, and no separate (ignored-on-reimport) filters block.
    assert "SUM(CASE WHEN" in text
    assert "(${TABLE}.order_status) = 'done'" in text
    measure_block = text[text.index("measure: done_amt") :]
    measure_block = measure_block[: measure_block.index("}")]
    assert "filters:" not in measure_block


def test_lookml_export_folded_filter_resolves_compact_dimension():
    """A folded filter over a COMPACT (no-sql) dimension resolves to ${TABLE}.col, not <model>.col."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id"), Dimension(name="status", type="categorical")],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["orders.status = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "(${TABLE}.status) = 'done'" in text  # default column, qualified to ${TABLE}
    assert "orders.status" not in text  # not left pointing at a literal `orders` table


def test_lookml_export_folded_filter_resolves_unqualified_dimension():
    """An UNqualified folded filter field (status = 'done') resolves through dimension SQL."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["status = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "(${TABLE}.order_status) = 'done'" in text  # unqualified dim -> its real column
    # 'done' (a string VALUE) must NOT be rewritten even though it's not a dimension.
    assert "'done'" in text


def test_lookml_export_folded_filter_resolves_dimension_inside_function():
    """An unqualified dimension INSIDE a function (LOWER(status)) resolves; quoted value is safe."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
            ],
            # LOWER(status): dim inside a function; and a quoted value equal to a dim name.
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["LOWER(status) = 'status'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "LOWER((${TABLE}.order_status))" in text  # dim resolved inside the function
    assert "= 'status'" in text  # the quoted value was NOT rewritten


def test_lookml_export_folded_filter_leaves_quoted_identifier_untouched():
    """A double-quoted identifier in a folded filter must not be substituted inside."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["\"status\" = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "\"status\" = 'done'" in text  # quoted identifier passes through verbatim
    assert '"(${TABLE}' not in text  # NOT substituted inside the quotes (invalid SQL)


def test_lookml_export_folded_filter_leaves_foreign_qualified_field_untouched():
    """A foreign-qualified field (customers.status) must not have its `status` part rewritten."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["customers.status = 'vip'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "customers.status = 'vip'" in text  # foreign qualifier left intact
    assert "customers.(" not in text  # the `status` part is NOT rewritten into a malformed ref


def test_lookml_export_folded_filter_does_not_rewrite_function_name():
    """A folded filter's SQL function name equal to a dimension name must not be rewritten."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="date", type="time", granularity="day", sql="order_date"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["date(created_at) = '2024-01-01'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "date(created_at)" in text  # function call left intact
    assert "order_date)(" not in text  # the function name is NOT rewritten to a column


def test_lookml_export_folded_filter_does_not_rewrite_template_variable():
    """A folded filter's Liquid/Jinja template variable equal to a dimension name is untouched."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),  # dim named 'status'
                Dimension(name="amount", type="numeric", sql="amount"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="{model}.amount", filters=["{model}.status = {{ status }}"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: sd" in text
    assert "{{ status }}" in text  # template variable left intact
    assert "{{ (${TABLE}" not in text  # NOT rewritten inside the template
    assert "order_status" in text  # the real column operand IS resolved


def test_lookml_export_folded_filter_protects_multiline_liquid_block():
    """A Liquid/Jinja tag that SPANS NEWLINES must be protected whole, not corrupted mid-tag.

    The template patterns used `.*?` without DOTALL, so a `{% ... %}` / `{{ ... }}` spanning a
    newline was not split out as one protected segment, and a bare dimension name on an inner line
    was rewritten to its column -- corrupting the template. Using `[\\s\\S]*?` spans newlines.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="order_status")],
    )
    conds = LookMLAdapter._fold_filter_conds

    # Each tag itself spans a newline: the inner `status` must NOT become `order_status`.
    for predicate in (
        "{% condition\n status %}\n1\n{% endcondition %}",
        "{{\n  status\n}} = 1",
        "{% condition\nstatus\n%} 1 {% endcondition %}",
    ):
        assert "order_status" not in conds([predicate], model), predicate

    # A real bare-dimension filter (no template) still resolves to its column.
    assert conds(["status = 'x'"], model) == "((${TABLE}.order_status) = 'x')"


def test_lookml_export_folded_filter_no_dimensions_does_not_crash():
    """Folding a qualified filter on a model with NO dimensions must not IndexError.

    With no declared dimensions the bare-name alternative is absent from the regex (one group),
    so the callback must read group 2 defensively instead of raising 'no such group'.
    """
    import tempfile

    from sidemantic import Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[],  # no dimensions -> names_alt empty -> single-group regex
            metrics=[Metric(name="sd", agg="stddev", sql="{model}.amount", filters=["{model}.status = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)  # must not raise IndexError
    text = open(out).read()
    assert "measure: sd" in text
    assert "${TABLE}.status" in text  # qualified filter still folded


def test_lookml_export_folded_filter_does_not_rewrite_typed_date_literal():
    """A folded filter's typed date literal (`date '...'`) must not be rewritten as a column."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="date", type="time", granularity="day", sql="order_date"),  # dim named 'date'
                Dimension(name="amount", type="numeric", sql="amount"),
            ],
            metrics=[
                Metric(name="sd", agg="stddev", sql="{model}.amount", filters=["created_at >= date '2024-01-01'"])
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: sd" in text
    assert "date '2024-01-01'" in text  # typed literal left intact
    assert "order_date) '2024" not in text  # NOT rewritten into a column


def test_lookml_export_folded_filter_does_not_rewrite_cast_type():
    """A folded filter's SQL CAST type token equal to a dimension name must not be rewritten."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="date", type="time", granularity="day", sql="order_date"),  # dim named 'date'
                Dimension(name="amount", type="numeric", sql="amount"),
            ],
            metrics=[
                Metric(
                    name="sd", agg="stddev", sql="{model}.amount", filters=["CAST(created_at AS date) = '2024-01-01'"]
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: sd" in text  # not skipped
    assert "CAST(created_at AS date)" in text  # type token intact
    assert "AS (${TABLE}" not in text  # the cast type is NOT rewritten to a column


def test_lookml_export_folded_filter_does_not_rewrite_unquoted_date_part_argument():
    """An UNQUOTED date part passed to a date/time function must not be rewritten as a column.

    BigQuery-style DATE_TRUNC(created_at, month) / DATE_DIFF(a, b, day) pass the part unquoted, so
    on a model with `month`/`day` dimensions it became DATE_TRUNC(..., (${TABLE}.order_month)).
    Protection is gated on the date-part KEYWORD set, so a real column argument of the SAME call
    (created_at) still resolves, and a keyword-named column outside a date function still resolves.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="month", type="time", granularity="month", sql="order_month"),
            Dimension(name="day", type="time", granularity="day", sql="order_day"),
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # The part is protected; the column argument of the same call IS resolved.
    assert conds(["DATE_TRUNC(created_at, month) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC((${TABLE}.created_at), month) = DATE '2024-01-01')"
    )
    assert conds(["DATE_DIFF(created_at, created_at, day) > 1"], model) == (
        "(DATE_DIFF((${TABLE}.created_at), (${TABLE}.created_at), day) > 1)"
    )
    # A keyword-named column used for real is still rewritten (not over-protected).
    assert conds(["month = '2024-01'"], model) == "((${TABLE}.order_month) = '2024-01')"
    assert conds(["UPPER(day) = 'X'"], model) == "(UPPER((${TABLE}.order_day)) = 'X')"


def test_lookml_export_folded_filter_date_part_guard_is_position_aware():
    """Only the date-part ARGUMENT SLOT is protected -- a keyword-named column elsewhere resolves.

    `DATE_TRUNC(date, month)` on a model with BOTH a `date` and a `month` dimension means column
    `date` truncated to part `month`: protecting every keyword inside a date function would leave
    the `date` COLUMN unresolved. Covers both argument conventions -- BigQuery puts the part LAST
    (DATE_TRUNC(value, part), DATE_DIFF(a, b, part)), SQL Server FIRST (DATEDIFF(part, a, b)).
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="date", type="time", granularity="day", sql="order_date"),
            Dimension(name="month", type="time", granularity="month", sql="order_month"),
            Dimension(name="day", type="time", granularity="day", sql="order_day"),
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # BigQuery: part is the LAST arg -- the `date` COLUMN in slot 0 must still resolve.
    assert conds(["DATE_TRUNC(date, month) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC((${TABLE}.order_date), month) = DATE '2024-01-01')"
    )
    assert conds(["DATE_DIFF(created_at, date, day) > 1"], model) == (
        "(DATE_DIFF((${TABLE}.created_at), (${TABLE}.order_date), day) > 1)"
    )
    # SQL Server: part is the FIRST arg; the trailing columns still resolve.
    assert conds(["DATEDIFF(day, created_at, date) > 1"], model) == (
        "(DATEDIFF(day, (${TABLE}.created_at), (${TABLE}.order_date)) > 1)"
    )
    # The unspaced TIMESTAMPADD/TIMESTAMPDIFF aliases also take the part first.
    assert conds(["TIMESTAMPADD(day, 1, created_at) > 1"], model) == (
        "(TIMESTAMPADD(day, 1, (${TABLE}.created_at)) > 1)"
    )
    assert conds(["TIMESTAMPDIFF(day, created_at, date) > 1"], model) == (
        "(TIMESTAMPDIFF(day, (${TABLE}.created_at), (${TABLE}.order_date)) > 1)"
    )
    # The two TRUNC spellings differ: underscored DATE_TRUNC is BigQuery's (value, part), while
    # the unspaced DATETRUNC is SQL Server's (part, value).
    assert conds(["DATETRUNC(month, created_at) = DATE '2024-01-01'"], model) == (
        "(DATETRUNC(month, (${TABLE}.created_at)) = DATE '2024-01-01')"
    )
    # A function with NO bare date-part argument must not protect its slots: time_bucket takes an
    # INTERVAL, so a keyword-named COLUMN in its last slot is still resolved.
    assert conds(["time_bucket(INTERVAL '5 minutes', date) = 1"], model) == (
        "(time_bucket(INTERVAL '5 minutes', (${TABLE}.order_date)) = 1)"
    )
    # DATE_TRUNC has NO fixed part position -- BigQuery is (value, part), Snowflake is (part, expr)
    # -- so it is disambiguated by CONTENT: whichever argument is a date-part keyword is the part.
    assert conds(["DATE_TRUNC(month, created_at) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC(month, (${TABLE}.created_at)) = DATE '2024-01-01')"
    )  # Snowflake order
    # When BOTH arguments are keywords (a model with `date` AND `month` dimensions) neither order
    # is decisive, so coarseness decides: truncation goes finer -> coarser, so `month` is the part
    # and `date` the column -- the SAME reading under either dialect's argument order.
    assert conds(["DATE_TRUNC(date, month) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC((${TABLE}.order_date), month) = DATE '2024-01-01')"
    )
    assert conds(["DATE_TRUNC(month, date) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC(month, (${TABLE}.order_date)) = DATE '2024-01-01')"
    )
    # Postgres/DuckDB quote the part. The quoted token is already protected from rewriting, but it
    # must still be RECOGNISED as the part -- otherwise the `date` COLUMN looks like the only
    # keyword and is left unresolved.
    assert conds(["DATE_TRUNC('month', date) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC('month', (${TABLE}.order_date)) = DATE '2024-01-01')"
    )
    assert conds(["DATE_TRUNC(\"month\", date) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC(\"month\", (${TABLE}.order_date)) = DATE '2024-01-01')"
    )


def test_lookml_export_folded_filter_trunc_date_expression_protects_part():
    """A TRUNC over a date EXPRESSION (not a bare dimension) is still the date overload.

    TRUNC is numeric-overloaded (TRUNC(number, scale)), so its part slot is only guarded when the
    value is date-typed. The old guard only accepted a value that was EXACTLY a time dimension name,
    so TRUNC(CAST({model}.created_at AS DATE), month) on a model that also has a `month` dimension
    left `month` looking like a plain column: it got rewritten to the dimension SQL, corrupting the
    exported unit. A value that references a time dimension, or is wrapped in an explicit date cast,
    must be recognised as the date overload -- while a genuinely numeric TRUNC(amount, month) still
    resolves `month` to its column.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
            Dimension(name="month", type="time", granularity="month", sql="order_month"),
            Dimension(name="amount", type="numeric", sql="amount"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # CAST(... AS DATE) value: `month` stays the PART; the cast's `created_at` still resolves.
    assert conds(["TRUNC(CAST({model}.created_at AS DATE), month) = DATE '2024-01-01'"], model) == (
        "(TRUNC(CAST((${TABLE}.created_at) AS DATE), month) = DATE '2024-01-01')"
    )
    # Postgres `::date` shorthand is recognised the same way.
    assert conds(["TRUNC({model}.created_at::date, month) = DATE '2024-01-01'"], model) == (
        "(TRUNC((${TABLE}.created_at)::date, month) = DATE '2024-01-01')"
    )
    # A genuinely numeric TRUNC (value is a numeric dimension, no date cast) is untouched: `month`
    # is a scale COLUMN and must resolve to its dimension SQL, not be protected as a part.
    assert conds(["TRUNC({model}.amount, month) > 5"], model) == (
        "(TRUNC((${TABLE}.amount), (${TABLE}.order_month)) > 5)"
    )


def test_lookml_export_folded_filter_date_diff_part_position_is_content_based():
    """date_diff's part is at EITHER end depending on dialect, so decide by content, not position.

    BigQuery is DATE_DIFF(end, start, part) (part LAST); DuckDB is date_diff(part, start, end) (part
    FIRST). A fixed position leaves the DuckDB spelling's first-argument unit unprotected, so a model
    with a `day` dimension rewrites the unit to (${TABLE}.order_day). Whichever END argument is a
    date-part keyword is the part; the other columns still resolve.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="date", type="time", granularity="day", sql="order_date"),
            Dimension(name="day", type="time", granularity="day", sql="order_day"),
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
            Dimension(name="start_at", type="time", granularity="day", sql="started_at"),
            Dimension(name="end_at", type="time", granularity="day", sql="ended_at"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # DuckDB: part FIRST -- `day` must stay a unit, and start/end columns still resolve.
    assert conds(["date_diff(day, start_at, end_at) > 1"], model) == (
        "(date_diff(day, (${TABLE}.started_at), (${TABLE}.ended_at)) > 1)"
    )
    # BigQuery: part LAST -- unchanged; the `date` COLUMN in the middle still resolves.
    assert conds(["DATE_DIFF(created_at, date, day) > 1"], model) == (
        "(DATE_DIFF((${TABLE}.created_at), (${TABLE}.order_date), day) > 1)"
    )
    # Both ends look like keywords (a `date` dimension at the far end): the genuine UNIT `day` wins,
    # so the DuckDB unit is protected and the `date` column resolves.
    assert conds(["date_diff(day, created_at, date) > 1"], model) == (
        "(date_diff(day, (${TABLE}.created_at), (${TABLE}.order_date)) > 1)"
    )


def test_lookml_export_folded_filter_equal_rank_date_trunc_uses_trunc_unit_as_part():
    """When both DATE_TRUNC args share coarseness, the truncation UNIT is the part.

    DATE_TRUNC(day, date) has both args at rank 7, so the coarseness tie-break gave no signal and
    fell back to a fixed position -- picking `date` as the part and leaving the real `date` column
    unresolved (or rewriting `day`). A DATE_TRUNC part must be a truncation unit, and only `day`
    is one (`date`/`dow`/`doy` are extraction-only), so `day` is the part and `date` resolves.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw",
        primary_key="id",
        dimensions=[
            Dimension(name="date", type="time", granularity="day", sql="order_date"),
            Dimension(name="day", type="time", granularity="day", sql="order_day"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # Snowflake order (part first): day is the part, date resolves to its column.
    assert conds(["DATE_TRUNC(day, date) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC(day, (${TABLE}.order_date)) = DATE '2024-01-01')"
    )
    # BigQuery order (part last): still day is the part, date resolves.
    assert conds(["DATE_TRUNC(date, day) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC((${TABLE}.order_date), day) = DATE '2024-01-01')"
    )


def test_lookml_export_folded_filter_protects_sql_server_datepart_first_functions():
    """SQL Server functions taking the datepart FIRST must protect that part token.

    DATENAME / DATEDIFF_BIG / DATE_BUCKET were absent from the datepart-position map, so on a model
    with a `day` dimension a folded filter's DATENAME(day, created_at) rewrote `day` to
    (${TABLE}.order_day) -- and the stddev/complete-aggregate export paths suppress the filters
    block, so the exported LookML SQL was invalid. The trailing column arguments still resolve.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="day", type="time", granularity="day", sql="order_day"),
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
            Dimension(name="start_at", type="time", granularity="day", sql="started_at"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # The datepart token is protected; the column argument(s) of the SAME call still resolve.
    assert conds(["DATENAME(day, created_at) = 'Monday'"], model) == (
        "(DATENAME(day, (${TABLE}.created_at)) = 'Monday')"
    )
    assert conds(["DATEDIFF_BIG(day, start_at, created_at) > 1"], model) == (
        "(DATEDIFF_BIG(day, (${TABLE}.started_at), (${TABLE}.created_at)) > 1)"
    )
    assert conds(["DATE_BUCKET(day, 1, created_at) > 1"], model) == ("(DATE_BUCKET(day, 1, (${TABLE}.created_at)) > 1)")
    # A keyword-named column used for real (outside a date function) is still rewritten.
    assert conds(["UPPER(day) = 'X'"], model) == "(UPPER((${TABLE}.order_day)) = 'X')"


def test_lookml_export_folded_filter_protects_snowflake_timeadd_and_trunc_parts():
    """Snowflake TIMEADD/TIMEDIFF (datepart first) and TRUNC/TRUNCATE (datepart last) protect the part.

    Without these aliases a folded filter's TIMEADD(day, 1, x) or TRUNC(x, month) rewrote the unit
    to a same-named dimension's SQL. Numeric TRUNC(n, 2) is unaffected -- the trailing 2 is not a
    date part.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="day", type="time", granularity="day", sql="order_day"),
            Dimension(name="month", type="time", granularity="month", sql="order_month"),
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
            Dimension(name="n", type="numeric", sql="num_col"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    assert conds(["TIMEADD(day, 1, created_at) > 1"], model) == "(TIMEADD(day, 1, (${TABLE}.created_at)) > 1)"
    assert conds(["TIMEDIFF(day, created_at, created_at) > 1"], model) == (
        "(TIMEDIFF(day, (${TABLE}.created_at), (${TABLE}.created_at)) > 1)"
    )
    # TRUNC(expr, part): the LAST arg is the part -> month protected, created_at resolves.
    assert conds(["TRUNC(created_at, month) = DATE '2024-01-01'"], model) == (
        "(TRUNC((${TABLE}.created_at), month) = DATE '2024-01-01')"
    )
    # Numeric TRUNC(n, 2): the trailing 2 is not a date part, so the real column resolves.
    assert conds(["TRUNC(n, 2) > 5"], model) == "(TRUNC((${TABLE}.num_col), 2) > 5)"
    # Two-argument LAST_DAY(expr, part): the part is protected; single-argument LAST_DAY is unaffected.
    assert conds(["LAST_DAY(created_at, month) = DATE '2024-01-31'"], model) == (
        "(LAST_DAY((${TABLE}.created_at), month) = DATE '2024-01-31')"
    )
    assert conds(["LAST_DAY(created_at) = DATE '2024-01-31'"], model) == (
        "(LAST_DAY((${TABLE}.created_at)) = DATE '2024-01-31')"
    )


def test_lookml_export_folded_filter_numeric_trunc_scale_column_resolves():
    """TRUNC's numeric overload must not protect a scale column named like a date part.

    TRUNC(amount, month) where amount is numeric is TRUNC(number, scale), so `month` is a scale
    COLUMN and must resolve to its dimension SQL -- only a date/time value argument makes the last
    arg a date part (TRUNC(created_at, month)).
    """
    from sidemantic import Dimension, Model

    numeric = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="amount", type="numeric", sql="amt"),
            Dimension(name="month", type="numeric", sql="scale_col"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # Numeric value arg -> `month` is a scale column, resolved (not protected as a part).
    assert conds(["TRUNC(amount, month) > 0"], numeric) == "(TRUNC((${TABLE}.amt), (${TABLE}.scale_col)) > 0)"

    dated = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
            Dimension(name="month", type="time", granularity="month", sql="order_month"),
        ],
    )
    # Time value arg -> `month` is the date part, protected.
    assert conds(["TRUNC(created_at, month) = DATE '2024-01-01'"], dated) == (
        "(TRUNC((${TABLE}.created_at), month) = DATE '2024-01-01')"
    )
    # The QUALIFIED form ({model}.created_at) must still be recognized as a time value.
    assert conds(["TRUNC({model}.created_at, month) = DATE '2024-01-01'"], dated) == (
        "(TRUNC((${TABLE}.created_at), month) = DATE '2024-01-01')"
    )
    # And a qualified numeric value still resolves the scale column.
    assert conds(["TRUNC({model}.amount, month) > 0"], numeric) == ("(TRUNC((${TABLE}.amt), (${TABLE}.scale_col)) > 0)")


def test_lookml_export_folded_filter_iso_date_trunc_units_resolve_value_column():
    """DATE_TRUNC ISO units (isoweek/isoyear) are recognized so the value column still resolves."""
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="week", type="time", granularity="week", sql="order_week"),
            Dimension(name="year", type="time", granularity="year", sql="order_year"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    assert conds(["DATE_TRUNC(week, isoweek) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC((${TABLE}.order_week), isoweek) = DATE '2024-01-01')"
    )
    assert conds(["DATE_TRUNC(year, isoyear) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC((${TABLE}.order_year), isoyear) = DATE '2024-01-01')"
    )


def test_lookml_export_folded_filter_quoted_date_trunc_unit_resolves_value_column():
    """A QUOTED DATE_TRUNC unit (part) is decided by its quotes, so a same-named value column resolves.

    DATE_TRUNC('week', week) on a model with a `week` dimension: the quoted 'week' is the part and
    the bare `week` is the value column. Stripping quotes made both look like the unit, and the
    tie-break picked the value column as the part, leaving it unresolved. Quoting decides the part.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="week", type="time", granularity="week", sql="order_week"),
            Dimension(name="date", type="time", granularity="day", sql="order_date"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # Postgres/DuckDB order (quoted part first): the value column resolves, the quoted part stays.
    assert conds(["DATE_TRUNC('week', week) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC('week', (${TABLE}.order_week)) = DATE '2024-01-01')"
    )
    assert conds(["DATE_TRUNC('month', date) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC('month', (${TABLE}.order_date)) = DATE '2024-01-01')"
    )
    # BigQuery order (unquoted, value first) is unaffected: date resolves, month is the part.
    assert conds(["DATE_TRUNC(date, month) = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC((${TABLE}.order_date), month) = DATE '2024-01-01')"
    )
    # A DOUBLE-quoted second argument is a quoted IDENTIFIER, not a string-literal part, so it must
    # NOT be treated as the part -- the real `month` part token stays protected (Snowflake style).
    assert conds(["DATE_TRUNC(month, \"date\") = DATE '2024-01-01'"], model) == (
        "(DATE_TRUNC(month, \"date\") = DATE '2024-01-01')"
    )


def test_lookml_export_folded_filter_protects_sql_server_datepart_abbreviations():
    """SQL Server datepart ABBREVIATIONS (dd, mm, d, ...) in a part slot must be protected.

    DATEADD/DATEDIFF accept abbreviated dateparts, but they were absent from the keyword set, so on
    a model with a `dd`/`mm` dimension a folded filter's DATEADD(dd, 1, created_at) rewrote `dd` to
    (${TABLE}.order_dd), producing invalid SQL. They are protected only in the part slot, so a
    same-named column used elsewhere still resolves.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="dd", type="time", granularity="day", sql="order_dd"),
            Dimension(name="mm", type="time", granularity="month", sql="order_mm"),
            Dimension(name="d", type="time", granularity="day", sql="order_d"),
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # Abbreviated part is protected; the trailing column arguments resolve. Covers two-letter and
    # single-letter forms and the datepart-first DATENAME.
    assert conds(["DATEADD(dd, 1, created_at) > 1"], model) == "(DATEADD(dd, 1, (${TABLE}.created_at)) > 1)"
    assert conds(["DATEDIFF(mm, created_at, created_at) > 1"], model) == (
        "(DATEDIFF(mm, (${TABLE}.created_at), (${TABLE}.created_at)) > 1)"
    )
    assert conds(["DATEADD(d, 1, created_at) > 1"], model) == "(DATEADD(d, 1, (${TABLE}.created_at)) > 1)"
    assert conds(["DATENAME(dd, created_at) = 'x'"], model) == "(DATENAME(dd, (${TABLE}.created_at)) = 'x')"
    # A same-named column used outside a date function is still rewritten.
    assert conds(["mm = '2024-01'"], model) == "((${TABLE}.order_mm) = '2024-01')"


def test_lookml_export_folded_filter_date_part_scan_is_quote_aware():
    """A `)` inside a string literal must not break the date-part call scan.

    The backward scan that finds the enclosing DATE_TRUNC counted the `)` inside `= ')'` as syntax
    and lost the call, so the `month` date-part token was rewritten to the `month` dimension SQL.
    The scan is now quote-aware, so the part token stays intact and the real column still resolves.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="id", type="numeric", sql="id"),
            Dimension(name="month", type="time", granularity="month", sql="order_month"),
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    out = conds(["DATE_TRUNC(CASE WHEN label = ')' THEN created_at END, month) = DATE '2024-01-01'"], model)
    assert ", month)" in out  # date-part token untouched despite the ')' inside the string
    assert "order_month" not in out  # not rewritten to the same-named dimension
    assert "(${TABLE}.created_at)" in out  # the real column argument still resolves
    # A same-named `month` column used as a real operand still resolves.
    assert conds(["month = '2024-01'"], model) == "((${TABLE}.order_month) = '2024-01')"


def test_lookml_export_folded_filter_protects_multi_word_cast_type():
    """Every token of a multi-word cast type (double precision) is protected, not just the first."""
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="id", type="numeric", sql="id"),
            Dimension(name="precision", type="numeric", sql="precision_col"),
            Dimension(name="amount", type="numeric", sql="amt"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    assert conds(["cast(amount as double precision) > 0"], model) == "(cast((${TABLE}.amt) as double precision) > 0)"
    # The `::` form is protected across the whole multi-word type too.
    assert conds(["amount::double precision > 0"], model) == "((${TABLE}.amt)::double precision > 0)"
    # A same-named `precision` column used as a real operand still resolves.
    assert conds(["precision > 5"], model) == "((${TABLE}.precision_col) > 5)"


def test_lookml_export_dialect_renamed_aggregate_skipped():
    """A filtered complete metric whose aggregate sqlglot would RENAME on export is skipped, not mangled.

    Serializing the fold rewrites APPROX_COUNT_DISTINCT to APPROX_DISTINCT, which DuckDB rejects, so
    the fold bails and the measure is skipped rather than exported with an invalid function name. A
    plain SUM/COUNT (no renamed aggregate) still folds and exports.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    # The fold bails (returns None) on a renamed aggregate ONLY for a FORCE caller (export/inline,
    # which skips the measure on None), but folds a stable one.
    assert (
        LookMLAdapter._fold_complete_sql_filters(
            "APPROX_COUNT_DISTINCT({model}.user_id) / COUNT(*)", ["{model}.status = 'x'"], force=True
        )
        is None
    )
    assert (
        LookMLAdapter._fold_complete_sql_filters("SUM({model}.amount) / COUNT(*)", ["{model}.status = 'x'"], force=True)
        is not None
    )
    # The IMPORT caller (force=False) falls back to column-nulling on None, which would drop the
    # filter on a zero-column COUNT(*) denominator, so it must NOT bail on a rename -- it folds,
    # filtering BOTH the numerator and the denominator.
    _imported = LookMLAdapter._fold_complete_sql_filters(
        "APPROX_COUNT_DISTINCT({model}.user_id) / NULLIF(COUNT(*), 0)", ["{model}.status = 'completed'"]
    )
    assert _imported is not None
    assert "COUNT(CASE" in _imported  # denominator folded, not left over all rows

    def _export(sql, name):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="orders",
                table="t",
                primary_key="id",
                dimensions=[
                    Dimension(name="id", type="numeric", sql="id"),
                    Dimension(name="status", type="categorical", sql="status"),
                ],
                metrics=[Metric(name=name, agg=None, sql=sql, sql_is_complete=True, filters=["{model}.status = 'x'"])],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        return open(out).read()

    assert "measure: au" not in _export("APPROX_COUNT_DISTINCT({model}.user_id) / COUNT(*)", "au")  # skipped
    assert "measure: ratio" in _export("SUM({model}.amount) / COUNT(*)", "ratio")  # ordinary fold exports


def test_lookml_export_folded_filter_keeps_boolean_null_literals():
    """A bare true/false/null in a folded filter stays a literal even when a dimension shares the name.

    SQL requires a real column named `true` to be quoted, so a bare token is always the literal.
    Rewriting it to a same-named dimension's SQL silently changed the predicate
    (`status = true` -> `status = (${TABLE}.is_active)`). The literal must survive; a genuine
    same-named column used as an operand (LHS) still resolves.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status_col"),
            Dimension(name="true", type="boolean", sql="is_active"),
            Dimension(name="false", type="boolean", sql="is_closed"),
            Dimension(name="null", type="categorical", sql="missing_col"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # RHS literals are preserved, not rewritten to the same-named dimension SQL.
    assert conds(["{model}.status = true"], model) == "((${TABLE}.status_col) = true)"
    assert conds(["{model}.status = false"], model) == "((${TABLE}.status_col) = false)"
    assert conds(["{model}.status IS NOT null"], model) == "((${TABLE}.status_col) IS NOT null)"
    # Case-insensitive: TRUE / NULL keywords are literals too.
    assert conds(["{model}.status = TRUE"], model) == "((${TABLE}.status_col) = TRUE)"


def test_lookml_export_folded_filter_keeps_sql_operator_keywords():
    """Bare SQL operators/keywords (or/and/in/between) stay operators even when a dimension shares the name.

    An unquoted `or` is always the operator, never a column, so a folded filter must not rewrite it
    to a same-named `or` dimension's SQL (which produced invalid `... 'done' (${TABLE}.or_col) ...`).
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status_col"),
            Dimension(name="created_at", type="time", sql="created_col"),
            Dimension(name="or", type="categorical", sql="or_col"),
            Dimension(name="and", type="categorical", sql="and_col"),
            Dimension(name="in", type="categorical", sql="in_col"),
            Dimension(name="from", type="categorical", sql="from_col"),
            Dimension(name="as", type="categorical", sql="as_col"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    assert conds(["status = 'done' or status = 'paid'"], model) == (
        "((${TABLE}.status_col) = 'done' or (${TABLE}.status_col) = 'paid')"
    )
    assert conds(["status in ('a', 'b') and status = 'c'"], model) == (
        "((${TABLE}.status_col) in ('a', 'b') and (${TABLE}.status_col) = 'c')"
    )
    # Grammar keywords inside an expression (EXTRACT ... FROM, CAST ... AS) stay keywords; the real
    # column argument still resolves.
    assert conds(["extract(day from created_at) = 5"], model) == ("(extract(day from (${TABLE}.created_col)) = 5)")
    assert conds(["cast(created_at as date) = '2024-01-01'"], model) == (
        "(cast((${TABLE}.created_col) as date) = '2024-01-01')"
    )


def test_lookml_export_ordered_set_aggregate_with_filter_skipped():
    """A filtered ordered-set aggregate can't fold (ORDER BY would land inside the CASE) -> skipped.

    Folding wraps only the aggregate ARGUMENT in CASE, so SUM(x ORDER BY y) would become
    SUM(CASE WHEN ... THEN x ORDER BY y END) -- ORDER BY buried inside the CASE, malformed LookML
    SQL. _complete_sql_fold_is_safe must reject it so the measure is skipped, not emitted broken.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    assert LookMLAdapter._complete_sql_fold_is_safe("SUM({model}.amount ORDER BY {model}.created_at)") is False
    # A plain aggregate (no ORDER BY) is still foldable.
    assert LookMLAdapter._complete_sql_fold_is_safe("SUM({model}.amount) / COUNT({model}.id)") is True
    # A NULL-retaining array collector (LIST/ARRAY_AGG) is NOT foldable -- a folded CASE leaves the
    # excluded row as a NULL element, so the measure must be skipped on export (as import does).
    assert LookMLAdapter._complete_sql_fold_is_safe("ARRAY_LENGTH(LIST({model}.amount))") is False
    assert LookMLAdapter._complete_sql_fold_is_safe("ARRAY_LENGTH(ARRAY_AGG({model}.amount))") is False

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(
                    name="oa",
                    agg=None,
                    sql="SUM({model}.amount ORDER BY {model}.created_at)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'x'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: oa" not in text  # skipped, not emitted with ORDER BY inside the CASE
    assert "ORDER BY ${TABLE}.created_at END" not in text  # never the malformed inside-CASE form
    # A WITHIN GROUP ordered-set aggregate IS foldable (the folder targets the ORDER BY value), so
    # it must NOT be rejected -- filtered percentile/median metrics still export.
    assert (
        LookMLAdapter._complete_sql_fold_is_safe("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {model}.amount)") is True
    )


def test_lookml_export_template_only_folded_filter_skipped():
    """A folded filter that is ONLY a Liquid template leaves no real column -> skip the measure.

    sqlglot cannot parse a Liquid segment, and the column check treats a parse failure as
    "has columns", so COUNT(*) with filters: ["{{ user_filter }}"] folded to
    COUNT(CASE WHEN ({{ user_filter }}) THEN 1 END) slipped past the zero-column guard and
    exported a type: number with no real column. Templates are neutralised before parsing; a
    template COMBINED with a real column still exports.
    """
    import re
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    def export_measure(filters):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="o",
                table="t",
                primary_key="id",
                dimensions=[
                    Dimension(name="id", type="numeric", sql="id"),
                    Dimension(name="status", type="categorical", sql="status"),
                ],
                metrics=[Metric(name="c", agg=None, sql="COUNT(*)", sql_is_complete=True, filters=filters)],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        m = re.search(r"measure: c \{.*?\n  \}", open(out).read(), re.S)
        return m.group(0) if m else None

    assert export_measure(["{{ user_filter }}"]) is None  # template-only -> no real column
    assert export_measure(["{model}.status = 'x'"]) is not None  # real column filter still exports
    assert export_measure(["{model}.status = 'x' AND {{ f }}"]) is not None  # template + column

    # The helper itself: a template is not a column, but a genuine column alongside one is.
    assert not LookMLAdapter._aggregate_references_column("COUNT(CASE WHEN ({{ user_filter }}) THEN 1 END)")
    assert LookMLAdapter._aggregate_references_column("COUNT(CASE WHEN (status = 'x' AND {{ f }}) THEN 1 END)")


def test_lookml_export_folded_filter_does_not_rewrite_table_qualifier():
    """A foreign table QUALIFIER that matches a dimension name must not be rewritten.

    On an `orders` model that also has a `customers` dimension, the filter
    `customers.status = 'vip'` qualifies another table. The lookbehind only guards the field
    AFTER a dot, so `customers` (before the dot) still matched and produced
    `(${TABLE}.customer_id).status = 'vip'`. A bare name followed by a dot is a qualifier, not a
    column; a same-named dimension WITHOUT a dot is still a column.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="customers", type="numeric", sql="customer_id"),
            Dimension(name="status", type="categorical", sql="order_status"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # Foreign qualifier left intact (neither the qualifier nor its field rewritten).
    assert conds(["customers.status = 'vip'"], model) == "(customers.status = 'vip')"
    # The same name used as a real column (no dot) IS still rewritten.
    assert conds(["customers = 5"], model) == "((${TABLE}.customer_id) = 5)"
    # Own-model and {model} qualifiers still resolve.
    assert conds(["orders.status = 'done'"], model) == "((${TABLE}.order_status) = 'done')"
    assert conds(["{model}.status = 'done'"], model) == "((${TABLE}.order_status) = 'done')"


def test_lookml_export_folded_filter_does_not_rewrite_date_part_keyword():
    """A folded filter's date-part / interval-unit keyword equal to a dimension must not be rewritten.

    With a `day` dimension, EXTRACT(day FROM ...) and INTERVAL 7 day contain the SQL keyword `day`
    in a non-column position. Rewriting it to the dimension SQL emits invalid LookML such as
    EXTRACT((${TABLE}.order_day) FROM ...); the keyword must be protected while genuine column uses
    (and the extract SOURCE column) are still rewritten.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="id", type="numeric", sql="id"),
            Dimension(name="day", type="time", granularity="day", sql="order_day"),  # dim named 'day'
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # EXTRACT part keyword protected; the FROM source column IS rewritten.
    assert conds(["EXTRACT(day FROM created_at) = 1"], model) == "(EXTRACT(day FROM (${TABLE}.created_at)) = 1)"
    # INTERVAL unit keyword protected -- both bare and quoted-number spellings.
    assert conds(["created_at >= CURRENT_DATE - INTERVAL 7 day"], model) == (
        "((${TABLE}.created_at) >= CURRENT_DATE - INTERVAL 7 day)"
    )
    assert conds(["created_at >= CURRENT_DATE - INTERVAL '7' day"], model) == (
        "((${TABLE}.created_at) >= CURRENT_DATE - INTERVAL '7' day)"
    )
    # A genuine column use of the same name IS still rewritten (not over-protected).
    assert conds(["day = '2024-01-01'"], model) == "((${TABLE}.order_day) = '2024-01-01')"
    assert conds(["LOWER(day) = 'x'"], model) == "(LOWER((${TABLE}.order_day)) = 'x')"


def test_lookml_export_scalar_wrapped_aggregate_filter_folds_into_inner():
    """A scalar-wrapped aggregate with filters (ABS(SUM(x))) folds into the INNER aggregate.

    The complete-SQL folder wraps the filter around SUM's argument -- ABS(SUM(CASE WHEN ... THEN
    amount END)) -- which is faithful, so the measure exports. It must NEVER push the CASE around
    the nested aggregate itself (ABS(CASE WHEN ... THEN SUM(amount) END)), which would be wrong.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(
                    name="aw",
                    agg=None,
                    sql="ABS(SUM({model}.amount))",
                    sql_is_complete=True,
                    filters=["{model}.status = 'x'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: aw" in text  # exports: the filter folds into the inner SUM
    assert "ABS(CASE WHEN" not in text  # never push CASE around the nested aggregate
    assert "SUM(CASE WHEN" in text  # the CASE wraps SUM's argument, correctly


def test_lookml_export_multi_column_distinct_filter_skipped():
    """A multi-column COUNT(DISTINCT a, b) with filters can't fold to one CASE -> skipped, not malformed."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(
                    name="du",
                    agg=None,
                    sql="COUNT(DISTINCT {model}.a, {model}.b)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'x'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    assert "measure: du" not in open(out).read()  # skipped, no malformed `THEN a, b END`


def test_lookml_export_multi_input_aggregate_filter_skipped():
    """A filtered two-input aggregate (CORR(x, y)) can't fold consistently -> skipped, not mis-filtered.

    The folder wraps only the aggregate's first argument, so CORR(CASE WHEN ... THEN x END, y) would
    filter x but leave y over all rows -- a wrong statistic. _complete_sql_fold_is_safe rejects it.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    assert LookMLAdapter._complete_sql_fold_is_safe("CORR({model}.x, {model}.y)") is False
    assert LookMLAdapter._complete_sql_fold_is_safe("COVAR_POP({model}.x, {model}.y)") is False
    assert LookMLAdapter._complete_sql_fold_is_safe("SUM({model}.amount) / COUNT(*)") is True

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(
                    name="c",
                    agg=None,
                    sql="CORR({model}.x, {model}.y)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'x'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    assert "measure: c" not in open(out).read()  # skipped rather than filtering only the first input


def test_lookml_export_aggregate_with_scalar_subquery_skipped():
    """An agg-less complete measure carrying a scalar subquery is skipped, not silently lost on re-import.

    SUM({model}.amount) / (SELECT COUNT(*) FROM other) would export as type: number, but the import
    side's _parse_measure rejects subquery SQL, so the measure vanishes on re-import. Skip it on
    export so the round-trip stays consistent rather than dropping a measure the adapter emitted.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[
                Metric(name="sq", agg=None, sql="SUM({model}.amount) / (SELECT COUNT(*) FROM other)"),
                Metric(name="total", agg="sum", sql="{model}.amount"),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: sq" not in text  # subquery measure skipped rather than emitted-then-dropped
    assert "measure: total" in text  # ordinary aggregates still export


def test_lookml_export_complete_sql_with_raw_column_outside_aggregate_skipped():
    """An aggregate expression with a raw column OUTSIDE any aggregate is skipped, not lost on re-import.

    SUM({model}.amount) + {model}.tax_rate is not a valid grouped measure (tax_rate is ungrouped);
    the import side's _mixed_is_aggregate_safe rejects it, so a type: number export would be dropped
    on re-import. Skip on export to keep the round-trip consistent; a pure aggregate still exports.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[
                Metric(name="mixed", agg=None, sql="SUM({model}.amount) + {model}.tax_rate", sql_is_complete=True),
                Metric(name="ratio", agg=None, sql="SUM({model}.amount) / COUNT(*)", sql_is_complete=True),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: mixed" not in text  # raw column outside aggregate -> skipped
    assert "measure: ratio" in text  # fully-aggregated expression still exports


def test_lookml_export_folded_filter_keeps_constant_dimension_sql_unqualified():
    """A folded filter over a constant-SQL dimension keeps the literal unqualified, not ${TABLE}.1."""
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status_col"),
            Dimension(name="flag", type="numeric", sql="1"),  # constant-valued dimension
            Dimension(name="active", type="boolean", sql="TRUE"),
            Dimension(name="today", type="time", sql="CURRENT_DATE"),  # nullary SQL constant
            Dimension(name="ts", type="time", sql="current_timestamp"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # A constant/literal resolved SQL is parenthesized WITHOUT a table qualifier.
    assert conds(["{model}.flag = 1"], model) == "((1) = 1)"
    assert conds(["{model}.active = true"], model) == "((TRUE) = true)"
    # A nullary SQL constant (CURRENT_DATE / current_timestamp) is a value, not a column.
    assert conds(["{model}.today = CURRENT_DATE"], model) == "((CURRENT_DATE) = CURRENT_DATE)"
    assert conds(["{model}.ts > current_timestamp"], model) == "((current_timestamp) > current_timestamp)"
    # A genuine column still gets qualified.
    assert conds(["{model}.status = 'x'"], model) == "((${TABLE}.status_col) = 'x')"


def test_lookml_export_folded_filter_leaves_backtick_identifier_untouched():
    """A folded filter's backtick/bracket-quoted identifier must not be rewritten inside the quotes."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),  # dim named 'status'
                Dimension(name="amount", type="numeric", sql="amount"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="{model}.amount", filters=["`status` = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: sd" in text
    assert "`status`" in text  # backtick identifier intact
    assert "`(${TABLE}" not in text  # not rewritten inside the backticks


def test_lookml_export_folded_filter_model_placeholder_in_string_literal_preserved():
    """A `{model}` inside a string LITERAL is a value, not a placeholder, so it must not become ${TABLE}."""
    from sidemantic import Dimension, Model

    model = Model(
        name="orders",
        table="t",
        primary_key="id",
        dimensions=[
            Dimension(name="label", type="categorical", sql="label_col"),
            Dimension(name="status", type="categorical", sql="status_col"),
        ],
    )
    conds = LookMLAdapter._fold_filter_conds
    # The literal value keeps `{model}`; a real placeholder outside the quotes is converted.
    assert conds(["{model}.label = '{model}.status'"], model) == "((${TABLE}.label_col) = '{model}.status')"
    assert conds(["{model}.label = {model}.status"], model) == "((${TABLE}.label_col) = (${TABLE}.status_col))"


def test_lookml_export_string_literal_paren_in_aggregate_arg_folds():
    """A valid aggregate whose arg has a paren inside a STRING LITERAL must fold, not be rejected."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(
                    name="du",
                    agg=None,
                    sql="COUNT(DISTINCT CONCAT({model}.a, ')'))",  # literal ')' must not break depth scan
                    sql_is_complete=True,
                    filters=["{model}.status = 'x'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: du" in text  # folded, not skipped
    assert "DISTINCT CASE WHEN" in text


def test_lookml_export_normalizes_all_modifier_for_roundtrip():
    """An explicit ALL modifier must be normalized away so the exported measure round-trips.

    ALL is the DEFAULT aggregate modifier and changes nothing, but sqlglot cannot parse
    COUNT(ALL x), so a type: number measure exported with it is DROPPED on re-import. Strip it on
    export; DISTINCT (which is NOT a no-op) and a column named `all` must survive untouched.
    """
    import re
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    def roundtrip(expr):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="o",
                table="t",
                primary_key="id",
                dimensions=[
                    Dimension(name="id", type="numeric", sql="id"),
                    Dimension(name="amount", type="numeric", sql="amount"),
                ],
                metrics=[Metric(name="c", agg=None, sql=expr, sql_is_complete=True)],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        text = open(out).read()
        block = re.search(r"measure: c \{.*?\n  \}", text, re.S)
        reimported = "c" in {m.name for m in LookMLAdapter().parse(Path(out)).get_model("o").metrics}
        return (block.group(0) if block else None), reimported

    for expr in ("COUNT(ALL {model}.id)", "SUM(ALL {model}.amount)"):
        block, kept = roundtrip(expr)
        assert block and "ALL" not in block, block  # normalized away
        assert kept, f"{expr} must survive the round-trip"

    # DISTINCT is not a no-op and must be preserved; a plain aggregate is unchanged.
    block, kept = roundtrip("COUNT(DISTINCT {model}.id)")
    assert block and "DISTINCT" in block and kept
    block, kept = roundtrip("SUM({model}.amount)")
    assert block and kept

    # `(ALL ` inside a STRING LITERAL is data, not a modifier -- it must be left intact so the
    # metric still includes the same rows, while a real ALL modifier in the same SQL is stripped.
    block, kept = roundtrip("SUM(ALL CASE WHEN {model}.amount > 0 THEN 1 ELSE 0 END) || '(ALL x)'")
    assert block and "(ALL x)" in block and "SUM(ALL" not in block, block


def test_lookml_export_strips_leading_all_modifier_in_stddev_argument():
    """A LEADING ALL on a stddev/variance argument must be stripped, not wrapped as STDDEV(ALL x).

    The stddev/variance export wraps the metric SQL as STDDEV(<arg>). When the arg itself carries an
    explicit ALL (Metric(agg='stddev', sql='ALL {model}.amount')), emitting STDDEV(ALL amount) is
    unparseable, so the re-imported type: number measure is dropped. The ALL is the default modifier
    and must be normalized away before wrapping."""
    import re
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="amount", type="numeric", sql="amount"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="ALL {model}.amount")],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    block = re.search(r"measure: sd \{.*?\n  \}", text, re.S)
    assert block and "ALL" not in block.group(0), block  # STDDEV(${TABLE}.amount), not STDDEV(ALL ...)
    reimported = {m.name for m in LookMLAdapter().parse(Path(out)).get_model("o").metrics}
    assert "sd" in reimported  # survives the round-trip


def test_lookml_export_sample_aggregate_complete_measures_survive():
    """Agg-less complete measures using sample-aggregate aliases must export, not be dropped.

    The agg-detection at the export gate ran on the raw {model} SQL. sqlglot happens to parse
    {model}.col as a struct so VAR_SAMP/STDDEV_SAMP are detected today, but if that ever failed the
    regex fallback (which omits var_samp/stddev_samp) would skip the measure. Neutralizing {model}
    first (as the sibling call sites do) keeps detection on sqlglot's accurate path.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    def exports(sql):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="orders",
                table="t",
                primary_key="id",
                dimensions=[
                    Dimension(name="id", type="numeric", sql="id"),
                    Dimension(name="amount", type="numeric", sql="amount"),
                ],
                metrics=[Metric(name="m", agg=None, sql=sql, sql_is_complete=True)],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        text = open(out).read()
        reimported = "m" in {x.name for x in LookMLAdapter().parse(Path(out)).get_model("orders").metrics}
        return "measure: m" in text and reimported

    for sql in (
        "VAR_SAMP({model}.amount)",
        "STDDEV_SAMP({model}.amount)",
        "VARIANCE({model}.amount)",
        "STDDEV_POP({model}.amount)",
        "VAR_POP({model}.amount)",
        # An explicit ALL modifier makes sqlglot fail; detection must run on the ALL-stripped
        # col_sql, not the raw metric.sql, so these are not dropped.
        "VAR_SAMP(ALL {model}.amount)",
        "STDDEV_SAMP(ALL {model}.amount)",
    ):
        assert exports(sql), sql


def test_lookml_export_zero_column_stddev_skipped():
    """A stddev/variance over a CONSTANT also emits type: number -> needs the zero-column guard.

    STDDEV(1) references no column, so re-importing builds a metric over an empty model CTE --
    the same failure the agg-less path already guards. A filter needn't add a column either, so
    the folded SQL is re-checked. Column-based and native aggregates are unaffected.
    """
    import re
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    def export_measure(**metric_kwargs):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="o",
                table="t",
                primary_key="id",
                dimensions=[
                    Dimension(name="id", type="numeric", sql="id"),
                    Dimension(name="status", type="categorical", sql="status"),
                    Dimension(name="amount", type="numeric", sql="amount"),
                ],
                metrics=[Metric(name="c", **metric_kwargs)],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        m = re.search(r"measure: c \{.*?\n  \}", open(out).read(), re.S)
        return m.group(0) if m else None

    assert export_measure(agg="stddev", sql="1") is None  # STDDEV(1) -> no column
    assert export_measure(agg="variance", sql="1") is None
    assert export_measure(agg="stddev", sql="1", filters=["1 = 1"]) is None  # filter adds no column
    # A real column still exports, filtered or not.
    assert "type: number" in (export_measure(agg="stddev", sql="{model}.amount") or "")
    assert "type: number" in (
        export_measure(agg="stddev", sql="{model}.amount", filters=["{model}.status = 'x'"]) or ""
    )
    # A natively-mapped aggregate is untouched.
    assert "type: sum" in (export_measure(agg="sum", sql="{model}.amount") or "")


def test_lookml_export_folded_zero_column_aggregate_skipped():
    """A filter that adds NO column must not sneak a zero-column aggregate past the guard.

    The zero-column check runs before folding, but a filter needn't reference a column: COUNT(*)
    with `1 = 1` folds to COUNT(CASE WHEN (1 = 1) THEN 1 END), which STILL references none and
    re-imports as a metric over an empty model CTE. Re-check the folded SQL. A filter that does
    reference a column must still export.
    """
    import re
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    def export_measure(expr, filters):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="o",
                table="t",
                primary_key="id",
                dimensions=[
                    Dimension(name="id", type="numeric", sql="id"),
                    Dimension(name="status", type="categorical", sql="status"),
                ],
                metrics=[Metric(name="c", agg=None, sql=expr, sql_is_complete=True, filters=filters)],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        m = re.search(r"measure: c \{.*?\n  \}", open(out).read(), re.S)
        return m.group(0) if m else None

    # Folds to COUNT(CASE WHEN (1 = 1) THEN 1 END) -> still zero-column -> skipped.
    assert export_measure("COUNT(*)", ["1 = 1"]) is None
    # A filter that DOES reference a column still exports (the folded CASE reads it).
    block = export_measure("COUNT(*)", ["{model}.status = 'x'"])
    assert block and "COUNT(CASE WHEN" in block and "THEN 1 END" in block


def test_lookml_export_aggregate_order_by_not_folded_into_case():
    """An aggregate-local ORDER BY belongs to the aggregate call, not the CASE result.

    SUM(amount ORDER BY created_at) must NOT fold to
    SUM(CASE WHEN ... THEN amount ORDER BY created_at END) (malformed); bail so the caller skips.
    An ORDER BY inside a string literal or nested parens is not a top-level one.
    """
    from sidemantic import Dimension, Model

    model = Model(
        name="o",
        table="t",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )
    filters = ["{model}.status = 'x'"]
    assert (
        LookMLAdapter._fold_filters_into_aggregate("SUM({model}.amount ORDER BY {model}.created_at)", filters, model)
        is None
    )
    # A plain aggregate still folds normally.
    folded = LookMLAdapter._fold_filters_into_aggregate("SUM({model}.amount)", filters, model)
    assert folded and folded.startswith("SUM(CASE WHEN")

    # The detector itself: only a TOP-LEVEL, unquoted ORDER BY counts.
    assert LookMLAdapter._has_top_level_order_by("{model}.a ORDER BY {model}.b")
    assert LookMLAdapter._has_top_level_order_by("{model}.a order by {model}.b")  # case-insensitive
    assert not LookMLAdapter._has_top_level_order_by("'a order by b'")  # string literal
    assert not LookMLAdapter._has_top_level_order_by("F({model}.a, ' order by ')")  # literal in a call
    assert not LookMLAdapter._has_top_level_order_by("{model}.reorder_by_date")  # word boundary


def test_lookml_export_all_modifier_stays_outside_folded_case():
    """COUNT(ALL x) must fold as COUNT(ALL CASE ... END), not COUNT(CASE ... THEN ALL x END).

    ALL is an aggregate MODIFIER like DISTINCT, not a row expression, so wrapping it inside the
    CASE emits malformed SQL while the separate filters block is suppressed. A column actually
    NAMED `all` must still be treated as a plain argument.
    """
    import duckdb

    from sidemantic import Dimension, Model

    model = Model(
        name="o",
        table="t",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )
    folded = LookMLAdapter._fold_filters_into_aggregate("COUNT(ALL {model}.user_id)", ["{model}.status = 'x'"], model)
    assert folded is not None
    assert folded.startswith("COUNT(ALL CASE WHEN"), folded  # modifier outside the CASE
    assert "THEN ALL " not in folded, folded  # NOT the malformed generic wrapper

    # The folded SQL must actually execute.
    con = duckdb.connect()
    con.execute("create table t as select * from (values (1,'x'),(2,'y'),(3,'x')) v(user_id,status)")
    runnable = folded.replace("${TABLE}.", "").replace("{model}.", "")
    assert con.execute(f"select {runnable} from t").fetchone() == (2,)

    # A column literally named `all` is a plain argument, not a modifier.
    plain = LookMLAdapter._fold_filters_into_aggregate("COUNT({model}.all)", ["{model}.status = 'x'"], model)
    assert plain is not None and "COUNT(ALL CASE" not in plain, plain

    # Multi-column ALL has no single CASE result -> bail so the caller skips it.
    assert (
        LookMLAdapter._fold_filters_into_aggregate("COUNT(ALL {model}.a, {model}.b)", ["{model}.status = 'x'"], model)
        is None
    )


def test_lookml_export_parenthesized_distinct_filter_folds():
    """COUNT(DISTINCT(x)) (parenthesized, no space) must fold its filter, not emit malformed SQL."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(
                    name="du",
                    agg=None,
                    sql="COUNT(DISTINCT({model}.uid))",
                    sql_is_complete=True,
                    filters=["{model}.status = 'x'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: du" in text
    assert "DISTINCT CASE WHEN" in text  # folded as DISTINCT over a CASE
    assert "THEN DISTINCT(" not in text  # NOT the malformed generic-wrapper output


def test_lookml_export_delimited_distinct_filter_folds_not_skipped():
    """A single-arg DISTINCT containing a comma STRING LITERAL must fold, not be mis-rejected.

    COUNT(DISTINCT a || ',' || b) is one column; the arity check must ignore the comma
    inside the string literal (quote-aware split) instead of treating it as multi-column.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(
                    name="composite_distinct",
                    agg=None,
                    sql="COUNT(DISTINCT {model}.a || ',' || {model}.b)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'x'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    txt = open(out).read()
    assert "measure: composite_distinct" in txt  # folded, NOT skipped
    assert "DISTINCT CASE WHEN" in txt  # filter folded inside the single-column DISTINCT


def test_lookml_export_filtered_multi_aggregate_measure_folds_and_roundtrips():
    """A filtered complete measure with MULTIPLE aggregates must export, folding each aggregate.

    SUM(a) / NULLIF(COUNT(*), 0) is not a single outer FUNC(arg), so _fold_filters_into_aggregate
    bailed and the measure was skipped. The complete-SQL folder wraps EVERY aggregate's argument in
    the filter, so it exports and round-trips to the correct filtered value; a renamed filter
    dimension resolves to its column.
    """
    import tempfile

    import duckdb

    from sidemantic import Dimension, Metric, Model, SemanticLayer
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="amount", type="numeric", sql="amount"),
                Dimension(name="status", type="categorical", sql="order_status"),  # renamed column
            ],
            metrics=[
                Metric(
                    name="avg_completed",
                    agg=None,
                    sql="SUM({model}.amount) / NULLIF(COUNT(*), 0)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'completed'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    reimported = LookMLAdapter().parse(Path(out)).get_model("orders")
    assert reimported.get_metric("avg_completed") is not None  # not skipped

    layer = SemanticLayer(auto_register=False)
    layer.add_model(reimported)
    con = duckdb.connect()
    con.execute(
        "create table orders as select 1 id, 10 amount, 'completed' order_status "
        "union all select 2, 30, 'pending' union all select 3, 20, 'completed'"
    )
    # completed rows: amount 10 + 20 = 30 over 2 rows -> 15.0.
    assert con.execute(layer.compile(metrics=["orders.avg_completed"])).fetchall() == [(15.0,)]


def test_lookml_export_multi_arg_aggregate_filter_skipped():
    """A multi-argument aggregate WEIGHTED_AVG(price, qty) with filters skips, not malformed CASE."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(
                    name="wavg",
                    agg=None,
                    sql="WEIGHTED_AVG({model}.price, {model}.qty)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'x'"],
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    assert "measure: wavg" not in open(out).read()  # skipped, no malformed `THEN price, qty END`


def test_lookml_export_count_constant_uses_native_count_type():
    """COUNT over any NON-NULL constant is a native row count and exports as type: count.

    COUNT(1)/COUNT(0), plus COUNT(TRUE), COUNT('x'), COUNT(1.0), COUNT(.5) all count every row.
    Exporting them as type: number would re-import as a zero-column complete-SQL metric whose
    query hits an empty model CTE (SELECT FROM ...). Every OTHER zero-column aggregate that is NOT
    a plain row count -- COUNT(NULL), COUNT(DISTINCT 1), SUM(1), MAX('x') -- has no faithful native
    form, so it is SKIPPED (not emitted as a broken type: number).
    """
    import re
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    def export_measure(expr):
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="o",
                table="t",
                primary_key="id",
                dimensions=[Dimension(name="id", type="numeric", sql="id")],
                metrics=[Metric(name="c", agg=None, sql=expr, sql_is_complete=True)],
            )
        )
        out = tempfile.mktemp(suffix=".lkml")
        LookMLAdapter().export(graph, out)
        m = re.search(r"measure: c \{.*?\n  \}", open(out).read(), re.S)
        return m.group(0) if m else None

    for expr in ("COUNT(1)", "COUNT(0)", "COUNT(TRUE)", "COUNT('x')", "COUNT(1.0)", "COUNT(.5)"):
        block = export_measure(expr)
        assert block and "type: count" in block and expr not in block, f"{expr} -> {block}"

    # Zero-column aggregates that are NOT plain row counts have no round-trippable form -> skipped.
    for expr in ("COUNT(NULL)", "COUNT(DISTINCT 1)", "SUM(1)", "MAX('x')"):
        assert export_measure(expr) is None, f"{expr} should be skipped, not exported"

    # An explicit ALL modifier is the default and does not change the count, so COUNT(ALL <const>)
    # is the same native row count. (sqlglot cannot parse ALL, so the column check strips it --
    # otherwise every ALL form fell back to "has columns" and COUNT(ALL NULL) exported broken.)
    for expr in ("COUNT(ALL 1)", "COUNT(ALL TRUE)", "COUNT(ALL 'x')"):
        block = export_measure(expr)
        assert block and "type: count" in block, f"{expr} -> {block}"
    assert export_measure("COUNT(ALL NULL)") is None  # still not a row count
    assert "type: number" in (export_measure("COUNT(ALL {model}.id)") or "")  # a real column stays


def test_lookml_export_spaced_count_star_maps_to_native_count():
    """A spaced `COUNT (*)` complete aggregate must still export as native type: count."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[Metric(name="c", agg=None, sql="COUNT (*)", sql_is_complete=True)],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    import re

    block = re.search(r"measure: c \{.*?\}", open(out).read(), re.S)
    assert block and "type: count" in block.group(0)  # native count, not a number over empty CTE


def test_lookml_export_count_star_and_distinct_filters_fold_validly():
    """COUNT(*) / COUNT(DISTINCT x) complete aggregates with filters fold to valid SQL that runs."""
    import tempfile

    import duckdb

    from sidemantic import Dimension, Metric, Model, SemanticLayer
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
            ],
            metrics=[
                Metric(name="cnt", agg=None, sql="COUNT(*)", sql_is_complete=True, filters=["{model}.status = 'done'"]),
                Metric(
                    name="du",
                    agg=None,
                    sql="COUNT(DISTINCT {model}.user_id)",
                    sql_is_complete=True,
                    filters=["{model}.status = 'done'"],
                ),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "COUNT(CASE WHEN" in text and "THEN 1 END)" in text  # COUNT(*) -> THEN 1
    assert "COUNT(DISTINCT CASE WHEN" in text  # DISTINCT stays outside the CASE

    layer = SemanticLayer(auto_register=False)
    for m in LookMLAdapter().parse(Path(out)).models.values():
        layer.add_model(m)
    con = duckdb.connect()
    con.execute("create table orders(id int, user_id int, order_status text)")
    con.execute("insert into orders values (1,7,'done'),(2,7,'open'),(3,8,'done')")
    assert con.execute(layer.compile(metrics=["orders.cnt"])).fetchall() == [(2,)]
    assert con.execute(layer.compile(metrics=["orders.du"])).fetchall() == [(2,)]


def test_lookml_export_bare_count_star_uses_native_count_type():
    """A bare COUNT(*) complete aggregate exports as native type: count (round-trips + runs).

    type: number would re-import as a derived metric over an empty CTE (SELECT FROM ...),
    which the compiler rejects; native type: count counts rows and round-trips cleanly.
    """
    import tempfile

    import duckdb

    from sidemantic import Dimension, Metric, Model, SemanticLayer
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[Metric(name="cnt", agg=None, sql="COUNT(*)", sql_is_complete=True)],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    assert "type: count" in open(out).read()
    reimported = LookMLAdapter().parse(Path(out))
    assert reimported.get_model("orders").get_metric("cnt").agg == "count"
    layer = SemanticLayer(auto_register=False)
    for m in reimported.models.values():
        layer.add_model(m)
    con = duckdb.connect()
    con.execute("create table orders as select 1 id union all select 2")
    assert con.execute(layer.compile(metrics=["orders.cnt"])).fetchall() == [(2,)]


def test_lookml_export_running_total_cross_view_ref_not_double_wrapped():
    """A cumulative metric whose sql is a single already-braced cross-view ref exports as-is (no ${${}}).

    Such a metric is no longer importable from LookML (a cross-view ref is dropped at parse time,
    see test_lookml_cross_view_reference), but it can still arrive via the Python API, so the export
    path must pass the single ${...} ref straight through rather than re-wrap it into ${${...}}.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[
                Metric(
                    name="rt",
                    type="cumulative",
                    sql="${other_view.total}",
                    meta={"table_calculation": "running_total"},
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "${${" not in text
    assert "sql: ${other_view.total}" in text


def test_lookml_export_running_total_braced_ref_plus_expression_skipped():
    """A running_total whose sql is a braced cross-view ref PLUS more must be skipped, not exported.

    `${other.total} + tax` contains `${` so a substring check would wrongly accept it and emit
    a malformed `sql: ${other.total} + tax` (the local `tax` ref already lost its braces). Only
    a string that is EXACTLY one `${...}` reference may pass through.
    """
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[
                Metric(
                    name="rt",
                    type="cumulative",
                    sql="${other.total} + tax",
                    meta={"table_calculation": "running_total"},
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: rt" not in text  # not a single ref -> skipped
    assert "${other.total} + tax" not in text  # never emit the malformed mixed expression


def test_lookml_export_folded_filter_does_not_rewrite_schema_qualified_ref():
    """A folded filter's schema-qualified own-model ref must not match the model-name suffix."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="status", type="categorical", sql="order_status"),
                Dimension(name="amount", type="numeric", sql="amount"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="{model}.amount", filters=["schema.orders.status = 'done'"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: sd" in text
    assert "schema.orders.status" in text  # schema-qualified ref left intact
    assert "schema.(${TABLE}" not in text  # not mangled into a column substitution


def test_lookml_export_running_total_expression_skipped():
    """A running_total over an EXPRESSION (not a single base measure ref) is skipped, not malformed."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
            metrics=[
                Metric(name="total", agg="sum", sql="{model}.amt"),
                Metric(name="rt", type="cumulative", sql="total + tax", meta={"table_calculation": "running_total"}),
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    assert "measure: rt" not in text  # expression isn't a valid running_total base -> skipped
    assert "${total + tax}" not in text  # never emit a malformed field reference


def test_lookml_export_multiple_folded_filters_parenthesized():
    """Each folded filter is parenthesized so a filter containing OR isn't broken by AND precedence."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="o",
            primary_key="id",
            dimensions=[Dimension(name=n, type="numeric", sql=n) for n in ("id", "a", "b", "c")],
            metrics=[
                Metric(
                    name="sd", agg="stddev", sql="amount", filters=["{model}.a = 1 OR {model}.b = 1", "{model}.c = 1"]
                )
            ],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    text = open(out).read()
    # The OR filter is grouped before being AND-joined with the second filter.
    assert "((${TABLE}.a) = 1 OR (${TABLE}.b) = 1) AND ((${TABLE}.c) = 1)" in text


def test_lookml_export_folded_filter_parenthesizes_expression_dimension():
    """A folded filter on a dimension whose SQL is an expression must be parenthesized."""
    import tempfile

    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="o",
            table="o",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="eligible", type="categorical", sql="{model}.amount > 10 OR {model}.special"),
            ],
            metrics=[Metric(name="sd", agg="stddev", sql="amount", filters=["{model}.eligible = false"])],
        )
    )
    out = tempfile.mktemp(suffix=".lkml")
    LookMLAdapter().export(graph, out)
    assert "(${TABLE}.amount > 10 OR ${TABLE}.special) = false" in open(out).read()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
