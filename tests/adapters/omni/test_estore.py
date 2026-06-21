"""Tests for estore-analytics Omni fixtures.

Fixtures sourced from vbalalian/estore-analytics (MIT license).
Tests parsing of a real-world Omni export with advanced features:
- a global ``relationships.yaml`` file (bare top-level list of joins),
- ``*.topic.yaml`` topic files (base view + nested joins),
- ``*.view.yaml`` views named by Omni's ``{schema}__{table_name}`` convention,
- dimension/measure metadata: bin_boundaries, all_values, sample_values, format,
  synonyms, filtered measures, computed measures, custom SQL dims, funnel
  measures, ratio measures, hierarchical categories, and RFM scores.
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
    # Views are named by Omni's reference convention {schema}__{table_name}
    # (e.g. dim_users.view.yaml in schema omni_dbt_marts -> omni_dbt_marts__dim_users).
    for expected in [
        "omni_dbt_marts__dim_categories",
        "omni_dbt_marts__dim_products",
        "omni_dbt_marts__dim_user_rfm",
        "omni_dbt_marts__dim_users",
        "omni_dbt_marts__fct_events",
        "omni_dbt_marts__fct_sessions",
    ]:
        assert expected in model_names, f"Missing model: {expected}"


def test_estore_dim_users_dimensions(estore_graph):
    """dim_users has 24 dimensions including custom SQL and bin_boundaries."""
    model = estore_graph.models["omni_dbt_marts__dim_users"]
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
    model = estore_graph.models["omni_dbt_marts__dim_users"]
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
    model = estore_graph.models["omni_dbt_marts__fct_events"]
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
    model = estore_graph.models["omni_dbt_marts__fct_events"]
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
    model = estore_graph.models["omni_dbt_marts__fct_sessions"]
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
    model = estore_graph.models["omni_dbt_marts__fct_sessions"]
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
    model = estore_graph.models["omni_dbt_marts__dim_products"]
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
    model = estore_graph.models["omni_dbt_marts__dim_categories"]
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
    model = estore_graph.models["omni_dbt_marts__dim_user_rfm"]
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
        "omni_dbt_marts__dim_users": "omni_dbt_marts.dim_users",
        "omni_dbt_marts__fct_events": "omni_dbt_marts.fct_events",
        "omni_dbt_marts__fct_sessions": "omni_dbt_marts.fct_sessions",
        "omni_dbt_marts__dim_products": "omni_dbt_marts.dim_products",
        "omni_dbt_marts__dim_categories": "omni_dbt_marts.dim_categories",
        "omni_dbt_marts__dim_user_rfm": "omni_dbt_marts.dim_user_rfm",
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
    events = estore_graph.models["omni_dbt_marts__fct_events"]
    assert events.primary_key == "event_id"

    # dim_products has product_id as primary key
    products = estore_graph.models["omni_dbt_marts__dim_products"]
    assert products.primary_key == "product_id"

    # dim_categories has raw_category_id as primary key
    categories = estore_graph.models["omni_dbt_marts__dim_categories"]
    assert categories.primary_key == "raw_category_id"


# =============================================================================
# MEASURE AGGREGATION TYPES
# =============================================================================


def test_estore_count_distinct_measures(estore_graph):
    """count_distinct aggregation is correctly parsed."""
    events = estore_graph.models["omni_dbt_marts__fct_events"]

    unique_users = events.get_metric("unique_users")
    assert unique_users.agg == "count_distinct"
    assert unique_users.sql == "user_id"  # ${omni_dbt_marts__fct_events.user_id} -> user_id

    unique_products = events.get_metric("unique_products")
    assert unique_products.agg == "count_distinct"

    unique_sessions = events.get_metric("unique_sessions")
    assert unique_sessions.agg == "count_distinct"


def test_estore_filtered_measures(estore_graph):
    """Filtered measures parse the filter conditions."""
    events = estore_graph.models["omni_dbt_marts__fct_events"]

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
    users = estore_graph.models["omni_dbt_marts__dim_users"]

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
    events = estore_graph.models["omni_dbt_marts__fct_events"]

    purchase_rate = events.get_metric("purchase_rate")
    assert purchase_rate.sql is not None
    assert "is_purchase" in purchase_rate.sql

    users = estore_graph.models["omni_dbt_marts__dim_users"]

    churn_rate = users.get_metric("churn_rate")
    assert churn_rate.sql is not None


def test_estore_sessions_ratio_measures(estore_graph):
    """fct_sessions ratio measures have SQL expressions."""
    sessions = estore_graph.models["omni_dbt_marts__fct_sessions"]

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
    events = estore_graph.models["omni_dbt_marts__fct_events"]

    sum_revenue = events.get_metric("sum_revenue")
    # ${omni_dbt_marts__fct_events.revenue} should become just "revenue"
    assert sum_revenue.sql == "revenue"
    assert "${" not in sum_revenue.sql


def test_estore_custom_sql_dims_cleaned(estore_graph):
    """Custom SQL dimensions have ${view.field} references cleaned."""
    sessions = estore_graph.models["omni_dbt_marts__fct_sessions"]

    converting = sessions.get_dimension("is_converting_session")
    assert converting.sql is not None
    # Should not have raw ${} references
    assert "${" not in converting.sql


# =============================================================================
# DESCRIPTIONS
# =============================================================================


def test_estore_model_descriptions(estore_graph):
    """Models pick up description from view YAML."""
    events = estore_graph.models["omni_dbt_marts__fct_events"]
    assert events.description is not None
    assert "events" in events.description.lower()

    sessions = estore_graph.models["omni_dbt_marts__fct_sessions"]
    assert sessions.description is not None
    assert "session" in sessions.description.lower()


def test_estore_dimension_descriptions(estore_graph):
    """Dimensions carry their descriptions through."""
    events = estore_graph.models["omni_dbt_marts__fct_events"]
    event_type = events.get_dimension("event_type")
    assert event_type.description is not None
    assert "event" in event_type.description.lower()


def test_estore_metric_labels(estore_graph):
    """Metric labels are preserved."""
    events = estore_graph.models["omni_dbt_marts__fct_events"]

    sum_rev = events.get_metric("sum_revenue")
    assert sum_rev.label == "Total Revenue"

    unique_users = events.get_metric("unique_users")
    assert unique_users.label == "Unique Users"


# =============================================================================
# RELATIONSHIPS (global relationships.yaml — bare top-level list of joins)
# =============================================================================


def test_estore_relationships_parsed(estore_graph):
    """The global relationships.yaml is parsed into model relationships.

    Before this fix the estore export yielded 6 models but 0 relationships
    because the adapter only read a nested ``relationships:`` key inside
    model.yaml. Omni now ships a bare top-level list in relationships.yaml.
    """
    all_rels = []
    for model in estore_graph.models.values():
        for rel in model.relationships:
            all_rels.append((model.name, rel.name, rel.type))

    # 4 joins defined in relationships.yaml
    assert ("omni_dbt_marts__fct_events", "omni_dbt_marts__dim_products", "many_to_one") in all_rels
    assert ("omni_dbt_marts__fct_events", "omni_dbt_marts__dim_users", "many_to_one") in all_rels
    assert ("omni_dbt_marts__dim_users", "omni_dbt_marts__dim_user_rfm", "one_to_one") in all_rels
    assert ("omni_dbt_marts__fct_sessions", "omni_dbt_marts__dim_users", "many_to_one") in all_rels


def test_estore_relationship_keys_from_on_sql(estore_graph):
    """Foreign and primary keys are extracted from on_sql."""
    events = estore_graph.models["omni_dbt_marts__fct_events"]

    to_products = next(r for r in events.relationships if r.name == "omni_dbt_marts__dim_products")
    assert to_products.foreign_key == "product_id"
    assert to_products.primary_key == "product_id"

    to_users = next(r for r in events.relationships if r.name == "omni_dbt_marts__dim_users")
    assert to_users.foreign_key == "user_id"
    assert to_users.primary_key == "user_id"


def test_estore_relationship_metadata(estore_graph):
    """join_type / reversible metadata is preserved on relationships."""
    events = estore_graph.models["omni_dbt_marts__fct_events"]
    to_products = next(r for r in events.relationships if r.name == "omni_dbt_marts__dim_products")
    assert to_products.metadata is not None
    assert to_products.metadata["join_type"] == "always_left"
    assert to_products.metadata["reversible"] is False


def test_estore_one_to_one_relationship(estore_graph):
    """dim_users -> dim_user_rfm is a one_to_one relationship."""
    users = estore_graph.models["omni_dbt_marts__dim_users"]
    rfm = next(r for r in users.relationships if r.name == "omni_dbt_marts__dim_user_rfm")
    assert rfm.type == "one_to_one"


# =============================================================================
# TOPICS (*.topic.yaml — base view + nested joins)
# =============================================================================


def test_estore_topics_parsed(estore_graph):
    """All three topic files are recorded on graph.topics.

    Before this fix topics were never read (parse only globbed views/*.yaml).
    """
    topics = {t["name"]: t for t in estore_graph.topics}
    assert set(topics) == {"events", "customers", "sessions"}


def test_estore_topic_base_views_and_labels(estore_graph):
    """Topics expose base_view and label."""
    topics = {t["name"]: t for t in estore_graph.topics}

    assert topics["events"]["base_view"] == "omni_dbt_marts__fct_events"
    assert topics["events"]["label"] == "Events"

    assert topics["customers"]["base_view"] == "omni_dbt_marts__dim_users"
    assert topics["customers"]["label"] == "Customers"

    assert topics["sessions"]["base_view"] == "omni_dbt_marts__fct_sessions"
    assert topics["sessions"]["label"] == "Sessions"


def test_estore_topic_nested_joins_flattened(estore_graph):
    """Nested joins in a topic are flattened into the joined_views list."""
    topics = {t["name"]: t for t in estore_graph.topics}

    # Events topic: dim_users -> dim_user_rfm (nested), dim_products
    events_joins = set(topics["events"]["joined_views"])
    assert "omni_dbt_marts__dim_users" in events_joins
    assert "omni_dbt_marts__dim_user_rfm" in events_joins  # nested under dim_users
    assert "omni_dbt_marts__dim_products" in events_joins

    # Sessions topic: dim_users -> dim_user_rfm (nested)
    sessions_joins = set(topics["sessions"]["joined_views"])
    assert "omni_dbt_marts__dim_users" in sessions_joins
    assert "omni_dbt_marts__dim_user_rfm" in sessions_joins


# =============================================================================
# DIMENSION / MEASURE METADATA
# =============================================================================


def test_estore_dimension_format(estore_graph):
    """Dimension format is captured."""
    users = estore_graph.models["omni_dbt_marts__dim_users"]
    assert users.get_dimension("total_revenue").format == "currency"
    assert users.get_dimension("user_id").format == "ID"


def test_estore_dimension_bin_boundaries(estore_graph):
    """bin_boundaries are preserved in dimension metadata."""
    users = estore_graph.models["omni_dbt_marts__dim_users"]
    aov_bin = users.get_dimension("avg_order_value_bin")
    assert aov_bin.metadata is not None
    assert aov_bin.metadata["bin_boundaries"] == [50, 100, 200, 400]


def test_estore_dimension_all_values(estore_graph):
    """all_values are preserved in dimension metadata."""
    users = estore_graph.models["omni_dbt_marts__dim_users"]
    status = users.get_dimension("activity_status")
    assert status.metadata is not None
    assert status.metadata["all_values"] == ["active", "declining", "at_risk", "churned", "prospect"]


def test_estore_dimension_sample_values(estore_graph):
    """sample_values are preserved in dimension metadata."""
    users = estore_graph.models["omni_dbt_marts__dim_users"]
    flag = users.get_dimension("data_quality_flag")
    assert flag.metadata is not None
    assert flag.metadata["sample_values"] == ["missing_sessions", "anomalous_session_ratio"]


def test_estore_measure_format_and_synonyms(estore_graph):
    """Measure format and synonyms are captured."""
    events = estore_graph.models["omni_dbt_marts__fct_events"]
    sum_revenue = events.get_metric("sum_revenue")
    assert sum_revenue.format == "BIGUSDCURRENCY_2"
    assert sum_revenue.metadata is not None
    assert sum_revenue.metadata["synonyms"] == ["sales"]
    assert sum_revenue.metadata["aggregate_type"] == "sum"


def test_estore_average_aggregate_mapped(estore_graph):
    """Omni 'average' aggregate_type maps to avg."""
    users = estore_graph.models["omni_dbt_marts__dim_users"]
    assert users.get_metric("avg_revenue_per_user").agg == "avg"


def test_estore_boolean_filter_value(estore_graph):
    """A measure filter on a boolean field renders TRUE/FALSE, not quoted."""
    users = estore_graph.models["omni_dbt_marts__dim_users"]
    purchasers = users.get_metric("purchaser_count")
    assert purchasers.filters == ["has_purchase_history = TRUE"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
