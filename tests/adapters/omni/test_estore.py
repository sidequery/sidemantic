"""Tests for estore-analytics Omni fixtures.

Fixtures sourced from vbalalian/estore-analytics (MIT license).
Tests parsing of real-world Omni views with advanced features:
bin_boundaries, all_values, sample_values, format, filtered measures,
computed measures, synonyms, custom SQL dims, funnel measures, ratio measures,
hierarchical categories, RFM scores, and SCD Type 2 snapshots.
"""

from pathlib import Path

import pytest

from sidemantic.adapters.omni import OmniAdapter

ESTORE_DIR = Path("tests/fixtures/omni/estore")


@pytest.fixture
def estore_graph():
    """Parse the estore fixtures into a semantic graph."""
    adapter = OmniAdapter()
    return adapter.parse(ESTORE_DIR)


# =============================================================================
# BASIC PARSING: verify all views load with correct model/dim/metric counts
# =============================================================================


def test_estore_loads_all_views(estore_graph):
    """All 6 estore view files parse into models."""
    model_names = sorted(estore_graph.models.keys())
    assert len(model_names) == 6
    # Names come from file stems (dim_users.view.yaml -> dim_users.view)
    for expected in [
        "dim_categories.view",
        "dim_products.view",
        "dim_user_rfm.view",
        "dim_users.view",
        "fct_events.view",
        "fct_sessions.view",
    ]:
        assert expected in model_names, f"Missing model: {expected}"


def test_estore_dim_users_dimensions(estore_graph):
    """dim_users has 24 dimensions including custom SQL and bin_boundaries."""
    model = estore_graph.models["dim_users.view"]
    dim_names = [d.name for d in model.dimensions]

    # Basic dims
    assert "user_id" in dim_names
    assert "activity_status" in dim_names
    assert "is_churned" in dim_names
    assert "has_purchase_history" in dim_names
    assert "total_revenue" in dim_names
    assert "purchase_count" in dim_names

    # Bin boundary dims
    assert "avg_order_value_bin" in dim_names
    assert "total_revenue_bin" in dim_names

    # Custom SQL dim
    assert "customer_lifecycle_stage" in dim_names
    assert "rfm_segment_inclusive" in dim_names

    assert len(model.dimensions) == 24


def test_estore_dim_users_measures(estore_graph):
    """dim_users has 16 measures including filtered and computed."""
    model = estore_graph.models["dim_users.view"]
    metric_names = [m.name for m in model.metrics]

    # Standard aggregates
    assert "count" in metric_names
    assert "total_sessions" in metric_names
    assert "total_events" in metric_names
    assert "sum_revenue" in metric_names
    assert "avg_revenue_per_user" in metric_names

    # Filtered measures
    assert "churned_user_count" in metric_names
    assert "purchaser_count" in metric_names

    # Computed / ratio measures
    assert "churn_rate" in metric_names
    assert "purchaser_rate" in metric_names

    assert len(model.metrics) == 16


def test_estore_fct_events_dimensions(estore_graph):
    """fct_events has 16 dimensions including event metadata."""
    model = estore_graph.models["fct_events.view"]
    dim_names = [d.name for d in model.dimensions]

    assert "event_id" in dim_names
    assert "event_time" in dim_names
    assert "event_date" in dim_names
    assert "event_type" in dim_names
    assert "user_id" in dim_names
    assert "product_id" in dim_names
    assert "brand" in dim_names
    assert "price" in dim_names
    assert "revenue" in dim_names
    assert "is_purchase" in dim_names
    assert "is_cart_add" in dim_names
    assert "is_view" in dim_names

    assert len(model.dimensions) == 16


def test_estore_fct_events_measures(estore_graph):
    """fct_events has 13 measures including count_distinct and filtered."""
    model = estore_graph.models["fct_events.view"]
    metric_names = [m.name for m in model.metrics]

    # count_distinct measures
    assert "unique_users" in metric_names
    assert "unique_products" in metric_names
    assert "unique_sessions" in metric_names

    # Filtered measures
    assert "purchase_count" in metric_names
    assert "cart_add_count" in metric_names
    assert "view_count" in metric_names

    # Computed / ratio measures
    assert "purchase_rate" in metric_names
    assert "cart_add_rate" in metric_names

    assert len(model.metrics) == 13


def test_estore_fct_sessions_dimensions(estore_graph):
    """fct_sessions has 19 dimensions including custom SQL dims."""
    model = estore_graph.models["fct_sessions.view"]
    dim_names = [d.name for d in model.dimensions]

    # Funnel dim
    assert "funnel_stage" in dim_names

    # Custom SQL dims
    assert "is_converting_session" in dim_names
    assert "session_quality_tier" in dim_names
    assert "session_length_bucket" in dim_names

    assert len(model.dimensions) == 19


def test_estore_fct_sessions_measures(estore_graph):
    """fct_sessions has 10 measures including funnel and ratio measures."""
    model = estore_graph.models["fct_sessions.view"]
    metric_names = [m.name for m in model.metrics]

    # Funnel measures
    assert "sessions_reached_view" in metric_names
    assert "sessions_reached_cart" in metric_names
    assert "sessions_reached_purchase" in metric_names

    # Ratio measures
    assert "conversion_rate" in metric_names
    assert "view_to_cart_rate" in metric_names
    assert "cart_to_purchase_rate" in metric_names
    assert "revenue_per_session" in metric_names
    assert "avg_revenue_per_purchasing_session" in metric_names

    assert len(model.metrics) == 10


def test_estore_dim_products(estore_graph):
    """dim_products has brand/category dims and a count measure."""
    model = estore_graph.models["dim_products.view"]
    dim_names = [d.name for d in model.dimensions]

    assert "brand" in dim_names
    assert "category_code" in dim_names
    assert "category_lvl_1" in dim_names
    assert "category_lvl_2" in dim_names
    assert "product_id" in dim_names
    assert len(model.dimensions) == 7
    assert len(model.metrics) == 1


def test_estore_dim_categories(estore_graph):
    """dim_categories has hierarchical category levels."""
    model = estore_graph.models["dim_categories.view"]
    dim_names = [d.name for d in model.dimensions]

    assert "category_lvl_1" in dim_names
    assert "category_lvl_2" in dim_names
    assert "category_lvl_3" in dim_names
    assert "category_lvl_4" in dim_names
    assert "raw_category_id" in dim_names
    assert len(model.dimensions) == 6
    assert len(model.metrics) == 1


def test_estore_dim_user_rfm(estore_graph):
    """dim_user_rfm has RFM score dimensions."""
    model = estore_graph.models["dim_user_rfm.view"]
    dim_names = [d.name for d in model.dimensions]

    assert "recency_score" in dim_names
    assert "frequency_score" in dim_names
    assert "monetary_score" in dim_names
    assert "rfm_score" in dim_names
    assert "rfm_segment" in dim_names
    assert "user_id" in dim_names
    assert len(model.dimensions) == 6
    assert len(model.metrics) == 1


# =============================================================================
# TABLE REFERENCES
# =============================================================================


def test_estore_table_references(estore_graph):
    """All estore views have schema.table_name format."""
    expected_tables = {
        "dim_users.view": "omni_dbt_marts.dim_users",
        "fct_events.view": "omni_dbt_marts.fct_events",
        "fct_sessions.view": "omni_dbt_marts.fct_sessions",
        "dim_products.view": "omni_dbt_marts.dim_products",
        "dim_categories.view": "omni_dbt_marts.dim_categories",
        "dim_user_rfm.view": "omni_dbt_marts.dim_user_rfm",
    }
    for model_name, expected_table in expected_tables.items():
        model = estore_graph.models[model_name]
        assert model.table == expected_table, f"{model_name}: expected table={expected_table}, got {model.table}"


# =============================================================================
# PRIMARY KEYS
# =============================================================================


def test_estore_primary_keys(estore_graph):
    """Views with primary_key: true are detected."""
    # fct_events has event_id as primary key
    events = estore_graph.models["fct_events.view"]
    assert events.primary_key == "event_id"

    # dim_products has product_id as primary key
    products = estore_graph.models["dim_products.view"]
    assert products.primary_key == "product_id"

    # dim_categories has raw_category_id as primary key
    categories = estore_graph.models["dim_categories.view"]
    assert categories.primary_key == "raw_category_id"


# =============================================================================
# MEASURE AGGREGATION TYPES
# =============================================================================


def test_estore_count_distinct_measures(estore_graph):
    """count_distinct aggregation is correctly parsed."""
    events = estore_graph.models["fct_events.view"]

    unique_users = events.get_metric("unique_users")
    assert unique_users.agg == "count_distinct"
    assert unique_users.sql == "user_id"  # ${omni_dbt_marts__fct_events.user_id} -> user_id

    unique_products = events.get_metric("unique_products")
    assert unique_products.agg == "count_distinct"

    unique_sessions = events.get_metric("unique_sessions")
    assert unique_sessions.agg == "count_distinct"


def test_estore_filtered_measures(estore_graph):
    """Filtered measures parse the filter conditions."""
    events = estore_graph.models["fct_events.view"]

    purchase_count = events.get_metric("purchase_count")
    assert purchase_count.agg == "count"
    assert purchase_count.filters is not None
    assert len(purchase_count.filters) > 0

    cart_add_count = events.get_metric("cart_add_count")
    assert cart_add_count.agg == "count"
    assert cart_add_count.filters is not None

    view_count = events.get_metric("view_count")
    assert view_count.agg == "count"
    assert view_count.filters is not None


def test_estore_dim_users_filtered_measures(estore_graph):
    """dim_users filtered measures (churned_user_count, purchaser_count)."""
    users = estore_graph.models["dim_users.view"]

    churned = users.get_metric("churned_user_count")
    assert churned.agg == "count"
    assert churned.filters is not None

    purchasers = users.get_metric("purchaser_count")
    assert purchasers.agg == "count"
    assert purchasers.filters is not None


# =============================================================================
# DERIVED / COMPUTED MEASURES (no aggregate_type, just SQL)
# =============================================================================


def test_estore_derived_measures(estore_graph):
    """Computed measures without aggregate_type parse as derived."""
    events = estore_graph.models["fct_events.view"]

    purchase_rate = events.get_metric("purchase_rate")
    assert purchase_rate.sql is not None
    assert "is_purchase" in purchase_rate.sql

    users = estore_graph.models["dim_users.view"]

    churn_rate = users.get_metric("churn_rate")
    assert churn_rate.sql is not None


def test_estore_sessions_ratio_measures(estore_graph):
    """fct_sessions ratio measures have SQL expressions."""
    sessions = estore_graph.models["fct_sessions.view"]

    conversion_rate = sessions.get_metric("conversion_rate")
    assert conversion_rate.sql is not None
    assert "purchase_count" in conversion_rate.sql

    view_to_cart = sessions.get_metric("view_to_cart_rate")
    assert view_to_cart.sql is not None

    cart_to_purchase = sessions.get_metric("cart_to_purchase_rate")
    assert cart_to_purchase.sql is not None


# =============================================================================
# SQL REFERENCE CLEANUP
# =============================================================================


def test_estore_sql_references_cleaned(estore_graph):
    """${view.field} references are cleaned to just field names."""
    events = estore_graph.models["fct_events.view"]

    sum_revenue = events.get_metric("sum_revenue")
    # ${omni_dbt_marts__fct_events.revenue} should become just "revenue"
    assert sum_revenue.sql == "revenue"
    assert "${" not in sum_revenue.sql


def test_estore_custom_sql_dims_cleaned(estore_graph):
    """Custom SQL dimensions have ${view.field} references cleaned."""
    sessions = estore_graph.models["fct_sessions.view"]

    converting = sessions.get_dimension("is_converting_session")
    assert converting.sql is not None
    # Should not have raw ${} references
    assert "${" not in converting.sql


# =============================================================================
# DESCRIPTIONS
# =============================================================================


def test_estore_model_descriptions(estore_graph):
    """Models pick up description from view YAML."""
    events = estore_graph.models["fct_events.view"]
    assert events.description is not None
    assert "events" in events.description.lower()

    sessions = estore_graph.models["fct_sessions.view"]
    assert sessions.description is not None
    assert "session" in sessions.description.lower()


def test_estore_dimension_descriptions(estore_graph):
    """Dimensions carry their descriptions through."""
    events = estore_graph.models["fct_events.view"]
    event_type = events.get_dimension("event_type")
    assert event_type.description is not None
    assert "event" in event_type.description.lower()


def test_estore_metric_labels(estore_graph):
    """Metric labels are preserved."""
    events = estore_graph.models["fct_events.view"]

    sum_rev = events.get_metric("sum_revenue")
    assert sum_rev.label == "Total Revenue"

    unique_users = events.get_metric("unique_users")
    assert unique_users.label == "Unique Users"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
