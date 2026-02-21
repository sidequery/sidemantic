"""Tests for Hex adapter - advanced fixture parsing.

Tests real-world Hex features: base_sql_query, func_calc, func_sql,
semi-additive measures, visibility settings, median/stddev aggregations,
expr_calc dimensions, and display names.
"""

import pytest

from sidemantic.adapters.hex import HexAdapter

# =============================================================================
# SAAS ANALYTICS FIXTURE TESTS
# =============================================================================


class TestSaasAnalyticsParsing:
    """Tests for the saas_analytics.yml fixture with advanced Hex features."""

    @pytest.fixture
    def graph(self):
        adapter = HexAdapter()
        return adapter.parse("tests/fixtures/hex/saas_analytics.yml")

    @pytest.fixture
    def model(self, graph):
        return graph.models["saas_analytics"]

    def test_model_loads(self, graph):
        """Fixture parses without errors and produces a model."""
        assert "saas_analytics" in graph.models

    def test_model_uses_base_sql_query(self, model):
        """Model with base_sql_query stores SQL, not table."""
        assert model.sql is not None
        assert "SELECT" in model.sql
        assert "subscriptions" in model.sql
        assert model.table is None

    def test_model_description(self, model):
        """Model description is preserved."""
        assert model.description is not None
        assert "SaaS" in model.description

    def test_dimension_count(self, model):
        """All dimensions are parsed."""
        assert len(model.dimensions) == 12

    def test_dimension_names(self, model):
        """Key dimensions are present by name."""
        dim_names = [d.name for d in model.dimensions]
        assert "id" in dim_names
        assert "customer_id" in dim_names
        assert "plan" in dim_names
        assert "mrr" in dim_names
        assert "arr" in dim_names
        assert "costs" in dim_names
        assert "started_at" in dim_names
        assert "ended_at" in dim_names
        assert "is_active" in dim_names
        assert "segment" in dim_names
        assert "subscription_quarter" in dim_names
        assert "tenure_months" in dim_names

    def test_dimension_type_mapping(self, model):
        """Dimension types are correctly mapped."""
        assert model.get_dimension("id").type == "numeric"
        assert model.get_dimension("plan").type == "categorical"
        assert model.get_dimension("mrr").type == "numeric"
        assert model.get_dimension("started_at").type == "time"
        assert model.get_dimension("ended_at").type == "time"
        assert model.get_dimension("is_active").type == "categorical"  # boolean -> categorical
        assert model.get_dimension("segment").type == "categorical"
        assert model.get_dimension("subscription_quarter").type == "time"

    def test_expr_sql_dimension(self, model):
        """Dimensions with expr_sql have SQL expressions."""
        arr_dim = model.get_dimension("arr")
        assert arr_dim.sql is not None
        assert "mrr" in arr_dim.sql
        assert "12" in arr_dim.sql

    def test_expr_calc_dimension(self, model):
        """Dimensions with expr_calc have SQL expressions."""
        tenure = model.get_dimension("tenure_months")
        assert tenure.sql is not None
        assert "DATEDIFF" in tenure.sql or "datediff" in tenure.sql.lower()

    def test_boolean_dimension_with_expr_sql(self, model):
        """Boolean dimension with expr_sql preserves expression."""
        is_active = model.get_dimension("is_active")
        assert is_active.sql is not None
        assert "NULL" in is_active.sql

    def test_timestamp_granularity(self, model):
        """Timestamp dimensions get appropriate granularity defaults."""
        started_at = model.get_dimension("started_at")
        assert started_at.granularity == "hour"

    def test_primary_key(self, model):
        """Unique dimension is used as primary key."""
        assert model.primary_key == "id"

    def test_measure_count(self, model):
        """All measures are parsed."""
        assert len(model.metrics) == 16

    def test_standard_func_measures(self, model):
        """Standard func measures have correct aggregation types."""
        assert model.get_metric("total_mrr").agg == "sum"
        assert model.get_metric("subscription_count").agg == "count"
        assert model.get_metric("unique_customers").agg == "count_distinct"
        assert model.get_metric("avg_mrr").agg == "avg"

    def test_func_sum_with_of(self, model):
        """func: sum + of: column stores the column reference."""
        total_mrr = model.get_metric("total_mrr")
        assert total_mrr.agg == "sum"
        assert total_mrr.sql == "mrr"

    def test_func_count_distinct_with_of(self, model):
        """func: count_distinct + of: column works."""
        unique = model.get_metric("unique_customers")
        assert unique.agg == "count_distinct"
        assert unique.sql == "customer_id"

    def test_func_calc_measure(self, model):
        """func_calc measures are parsed as derived metrics."""
        profit = model.get_metric("profit")
        assert profit.type == "derived"
        assert profit.sql is not None
        assert "total_mrr" in profit.sql
        assert "total_costs" in profit.sql

    def test_func_calc_arpu(self, model):
        """func_calc with division is parsed as derived."""
        arpu = model.get_metric("arpu")
        assert arpu.type == "derived"
        assert arpu.sql is not None

    def test_func_sql_measure(self, model):
        """func_sql measures are parsed as derived with full SQL."""
        gross_margin = model.get_metric("gross_margin")
        assert gross_margin.type == "derived"
        assert gross_margin.sql is not None
        assert "SUM" in gross_margin.sql
        assert "NULLIF" in gross_margin.sql

    def test_func_sql_adjusted_arr(self, model):
        """func_sql with arithmetic is parsed correctly."""
        adjusted = model.get_metric("adjusted_arr")
        assert adjusted.type == "derived"
        assert adjusted.sql is not None
        assert "0.95" in adjusted.sql

    def test_measure_with_filter(self, model):
        """Measures with filters preserve filter information."""
        active_subs = model.get_metric("active_subscriptions")
        assert active_subs.filters is not None
        assert len(active_subs.filters) > 0

    def test_measure_with_inline_filter(self, model):
        """Measures with inline expr_sql filters are parsed."""
        enterprise = model.get_metric("enterprise_mrr")
        assert enterprise.filters is not None
        assert len(enterprise.filters) > 0

    def test_median_aggregation(self, model):
        """Median func is parsed (mapped to closest aggregation)."""
        median_mrr = model.get_metric("median_mrr")
        # Median maps to avg in current adapter since there's no median agg
        assert median_mrr is not None
        assert median_mrr.agg is not None

    def test_stddev_aggregation(self, model):
        """Stddev func is parsed (mapped to closest aggregation)."""
        stddev = model.get_metric("mrr_stddev")
        assert stddev is not None
        assert stddev.agg is not None

    @pytest.mark.xfail(reason="semi_additive not yet supported in adapter")
    def test_semi_additive_measure(self, model):
        """Semi-additive measures preserve semi_additive setting."""
        current_mrr = model.get_metric("current_mrr")
        # Semi-additive measures should have non_additive_dimension or similar
        assert current_mrr.non_additive_dimension is not None

    def test_count_if_aggregation(self, model):
        """count_if func is parsed as conditional count."""
        hv = model.get_metric("high_value_count")
        assert hv.agg == "count"
        assert hv.filters is not None

    def test_relation(self, model):
        """Relations are parsed."""
        rel_names = [r.name for r in model.relationships]
        assert "customers" in rel_names

    def test_measure_descriptions(self, model):
        """Measure descriptions are preserved."""
        total_mrr = model.get_metric("total_mrr")
        assert total_mrr.description is not None
        assert "monthly" in total_mrr.description.lower()


# =============================================================================
# PRODUCT EVENTS FIXTURE TESTS
# =============================================================================


class TestProductEventsParsing:
    """Tests for the product_events.yml fixture with visibility and event tracking."""

    @pytest.fixture
    def graph(self):
        adapter = HexAdapter()
        return adapter.parse("tests/fixtures/hex/product_events.yml")

    @pytest.fixture
    def model(self, graph):
        return graph.models["product_events"]

    def test_model_loads(self, graph):
        """Fixture parses without errors."""
        assert "product_events" in graph.models

    def test_model_has_table(self, model):
        """Model with base_sql_table stores table reference."""
        assert model.table == "analytics.product_events"

    def test_dimension_count(self, model):
        """All dimensions are parsed."""
        assert len(model.dimensions) == 10

    def test_dimension_names(self, model):
        """Key dimensions are present."""
        dim_names = [d.name for d in model.dimensions]
        assert "id" in dim_names
        assert "user_id" in dim_names
        assert "event_name" in dim_names
        assert "event_timestamp" in dim_names
        assert "event_date" in dim_names
        assert "session_id" in dim_names
        assert "device_type" in dim_names
        assert "raw_payload" in dim_names
        assert "is_conversion" in dim_names
        assert "page_url" in dim_names

    def test_date_dimension(self, model):
        """Date dimension has correct type and granularity."""
        event_date = model.get_dimension("event_date")
        assert event_date.type == "time"
        assert event_date.granularity == "day"

    def test_timestamp_dimension(self, model):
        """Timestamp dimension has correct type."""
        event_ts = model.get_dimension("event_timestamp")
        assert event_ts.type == "time"
        assert event_ts.granularity == "hour"

    def test_other_type_dimension(self, model):
        """Dimension with type 'other' is mapped to categorical."""
        raw = model.get_dimension("raw_payload")
        assert raw.type == "categorical"

    def test_expr_calc_boolean_dimension(self, model):
        """Boolean dimension with expr_calc has SQL expression."""
        is_conv = model.get_dimension("is_conversion")
        assert is_conv.sql is not None
        assert "purchase_completed" in is_conv.sql

    def test_expr_sql_date_dimension(self, model):
        """Date dimension with expr_sql has SQL expression."""
        event_date = model.get_dimension("event_date")
        assert event_date.sql is not None
        assert "DATE" in event_date.sql

    def test_measure_count(self, model):
        """All measures are parsed."""
        assert len(model.metrics) == 9

    def test_standard_measures(self, model):
        """Standard aggregation measures are correct."""
        assert model.get_metric("total_events").agg == "count"
        assert model.get_metric("unique_users").agg == "count_distinct"
        assert model.get_metric("min_event_time").agg == "min"
        assert model.get_metric("max_event_time").agg == "max"

    def test_count_distinct_measure(self, model):
        """Count distinct measures reference correct column."""
        unique_users = model.get_metric("unique_users")
        assert unique_users.sql == "user_id"

    def test_func_sql_conversion_rate(self, model):
        """Complex func_sql measure is parsed as derived."""
        conv_rate = model.get_metric("conversion_rate")
        assert conv_rate.type == "derived"
        assert conv_rate.sql is not None
        assert "CASE" in conv_rate.sql or "SUM" in conv_rate.sql

    def test_func_calc_events_per_session(self, model):
        """func_calc measure is parsed as derived."""
        eps = model.get_metric("events_per_session")
        assert eps.type == "derived"
        assert eps.sql is not None

    def test_measure_with_dimension_filter(self, model):
        """Measure with dimension reference filter works."""
        conv = model.get_metric("conversion_count")
        assert conv.filters is not None
        assert len(conv.filters) > 0

    @pytest.mark.xfail(reason="semi_additive not yet supported in adapter")
    def test_semi_additive_daily_active_users(self, model):
        """Semi-additive DAU measure preserves non-additivity."""
        dau = model.get_metric("daily_active_users")
        assert dau.non_additive_dimension is not None

    @pytest.mark.xfail(reason="visibility not yet mapped to model metadata")
    def test_dimension_visibility(self, model):
        """Visibility settings are preserved on dimensions."""
        # Visibility would be stored in dimension meta
        session_dim = model.get_dimension("session_id")
        assert session_dim.meta is not None
        assert session_dim.meta.get("visibility") == "internal"

    @pytest.mark.xfail(reason="visibility not yet mapped to metric metadata")
    def test_measure_visibility(self, model):
        """Visibility settings are preserved on measures."""
        unique_sessions = model.get_metric("unique_sessions")
        assert unique_sessions.meta is not None
        assert unique_sessions.meta.get("visibility") == "internal"

    def test_multiple_relations(self, model):
        """Multiple relations are parsed."""
        rel_names = [r.name for r in model.relationships]
        assert "users" in rel_names
        assert "sessions" in rel_names

    def test_relation_types(self, model):
        """Relations have correct types."""
        users_rel = next(r for r in model.relationships if r.name == "users")
        assert users_rel.type == "many_to_one"

    def test_relation_join_sql(self, model):
        """Relations have foreign keys from join_sql."""
        users_rel = next(r for r in model.relationships if r.name == "users")
        assert users_rel.foreign_key == "user_id"


# =============================================================================
# MULTI-FIXTURE DIRECTORY PARSE
# =============================================================================


class TestHexDirectoryParse:
    """Tests for parsing the entire hex fixtures directory with new fixtures."""

    @pytest.fixture
    def graph(self):
        adapter = HexAdapter()
        return adapter.parse("tests/fixtures/hex/")

    def test_all_models_loaded(self, graph):
        """All fixture files produce models."""
        assert "orders" in graph.models
        assert "users" in graph.models
        assert "organizations" in graph.models
        assert "saas_analytics" in graph.models
        assert "product_events" in graph.models
        assert "inventory" in graph.models
        assert "employees" in graph.models
        assert "support_tickets" in graph.models
        assert "page_views" in graph.models

    def test_total_model_count(self, graph):
        """All 9 fixture files produce 9 models."""
        assert len(graph.models) == 9

    def test_cross_model_measure_reference(self, graph):
        """Organizations model has cross-model measure referencing users."""
        orgs = graph.models["organizations"]
        arr = orgs.get_metric("arr")
        assert arr is not None
        assert arr.sql is not None
        # The 'of' field references users.annual_seat_price
        assert "users" in arr.sql or "annual_seat_price" in arr.sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
