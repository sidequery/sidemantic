"""Tests for MetricFlow adapter with real-world fixture patterns.

These tests use fixtures derived from the official MetricFlow test suite and
real-world dbt semantic layer projects. Tests are permissive: they verify
parsing succeeds without errors and check key structural properties like
model/metric counts, names, dimension types, and aggregation mappings.

Fixtures sourced from:
- MetricFlow official test helpers (bookings_source, accounts_source, SCD manifest,
  multi_hop_join_manifest, user_sm_source)
- https://github.com/dbt-labs/metricflow/tree/main/metricflow-semantics/
"""

from pathlib import Path

import pytest

from sidemantic.adapters.metricflow import MetricFlowAdapter

FIXTURES = Path("tests/fixtures/metricflow")


# =============================================================================
# Bookings source: many aggregation types, percentile agg_params, partitioned dims
# =============================================================================


class TestBookingsSource:
    """Tests for the bookings_source fixture (MetricFlow canonical example)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "bookings_source.yml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_model_exists(self, graph):
        """The bookings_source semantic model is imported."""
        assert "bookings_source" in graph.models

    def test_model_table_from_ref(self, graph):
        """Table is extracted from model: ref('fct_bookings')."""
        model = graph.models["bookings_source"]
        assert model.table == "fct_bookings"

    def test_measure_count(self, graph):
        """All 14 measures are imported."""
        model = graph.models["bookings_source"]
        assert len(model.metrics) == 14

    def test_measure_names(self, graph):
        """All measure names are present."""
        model = graph.models["bookings_source"]
        names = {m.name for m in model.metrics}
        expected = {
            "bookings",
            "instant_bookings",
            "booking_value",
            "max_booking_value",
            "min_booking_value",
            "bookers",
            "average_booking_value",
            "booking_payments",
            "referred_bookings",
            "median_booking_value",
            "booking_value_p99",
            "discrete_booking_value_p99",
            "approximate_continuous_booking_value_p99",
            "approximate_discrete_booking_value_p99",
        }
        assert names == expected

    def test_sum_boolean_maps_to_sum(self, graph):
        """sum_boolean aggregation maps to sum."""
        model = graph.models["bookings_source"]
        instant = model.get_metric("instant_bookings")
        assert instant.agg == "sum"

    def test_median_aggregation(self, graph):
        """median aggregation is preserved."""
        model = graph.models["bookings_source"]
        median = model.get_metric("median_booking_value")
        assert median.agg == "median"

    def test_percentile_aggregation_fallback(self, graph):
        """percentile aggregation falls through to default (sum) since not in type_mapping."""
        model = graph.models["bookings_source"]
        p99 = model.get_metric("booking_value_p99")
        assert p99.agg == "sum"

    def test_count_distinct(self, graph):
        """count_distinct aggregation is mapped correctly."""
        model = graph.models["bookings_source"]
        bookers = model.get_metric("bookers")
        assert bookers.agg == "count_distinct"

    def test_max_min_aggregations(self, graph):
        """max and min aggregations are mapped correctly."""
        model = graph.models["bookings_source"]
        assert model.get_metric("max_booking_value").agg == "max"
        assert model.get_metric("min_booking_value").agg == "min"

    def test_average_maps_to_avg(self, graph):
        """MetricFlow 'average' maps to sidemantic 'avg'."""
        model = graph.models["bookings_source"]
        assert model.get_metric("average_booking_value").agg == "avg"

    def test_count_aggregation(self, graph):
        """count aggregation is mapped correctly."""
        model = graph.models["bookings_source"]
        assert model.get_metric("referred_bookings").agg == "count"

    def test_dimension_count(self, graph):
        """All 4 dimensions are imported."""
        model = graph.models["bookings_source"]
        assert len(model.dimensions) == 4

    def test_time_dimensions(self, graph):
        """Time dimensions have correct type and granularity."""
        model = graph.models["bookings_source"]
        ds = model.get_dimension("ds")
        assert ds.type == "time"
        assert ds.granularity == "day"

        paid_at = model.get_dimension("paid_at")
        assert paid_at.type == "time"
        assert paid_at.granularity == "day"

    def test_partitioned_dimension_parses(self, graph):
        """Partitioned dimension (is_partition: true) parses without error."""
        model = graph.models["bookings_source"]
        ds_part = model.get_dimension("ds_partitioned")
        assert ds_part is not None
        assert ds_part.type == "time"
        assert ds_part.granularity == "day"

    def test_categorical_dimension(self, graph):
        """Categorical dimension is parsed."""
        model = graph.models["bookings_source"]
        is_instant = model.get_dimension("is_instant")
        assert is_instant.type == "categorical"

    def test_foreign_entities_as_relationships(self, graph):
        """Foreign entities create many_to_one relationships."""
        model = graph.models["bookings_source"]
        rel_names = {r.name for r in model.relationships}
        assert "listing" in rel_names
        assert "guest" in rel_names
        assert "host" in rel_names
        for r in model.relationships:
            assert r.type == "many_to_one"

    def test_primary_entity_shorthand_not_handled(self, graph):
        """primary_entity shorthand (outside entities list) defaults pk to 'id'."""
        model = graph.models["bookings_source"]
        assert model.primary_key == "id"

    def test_default_time_dimension(self, graph):
        """defaults.agg_time_dimension is captured."""
        model = graph.models["bookings_source"]
        assert model.default_time_dimension == "ds"

    def test_graph_metric_count(self, graph):
        """All 5 graph-level metrics are parsed."""
        assert len(graph.metrics) == 5

    def test_simple_metric(self, graph):
        """Simple metric is parsed as untyped with measure reference."""
        bookings = graph.get_metric("bookings")
        assert bookings.type is None
        assert bookings.sql == "bookings"

    def test_derived_metric(self, graph):
        """Derived metric preserves expression."""
        fees = graph.get_metric("booking_fees_per_booker")
        assert fees.type == "derived"
        assert fees.sql == "booking_value * 0.05 / bookers"

    def test_ratio_metric(self, graph):
        """Ratio metric has numerator and denominator."""
        ratio = graph.get_metric("bookings_per_booker")
        assert ratio.type == "ratio"
        assert ratio.numerator == "bookings"
        assert ratio.denominator == "bookers"

    def test_ratio_metric_with_filter_alias(self, graph):
        """Ratio metric with filter and alias on numerator parses correctly."""
        ratio = graph.get_metric("instant_booking_value_ratio")
        assert ratio.type == "ratio"
        assert ratio.numerator == "booking_value"
        assert ratio.denominator == "booking_value"


# =============================================================================
# Accounts source: non_additive_dimension with window_choice and window_groupings
# =============================================================================


class TestAccountsSource:
    """Tests for the accounts_source fixture (balance-type semi-additive measures)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "accounts_source.yml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_model_exists(self, graph):
        """The accounts_source semantic model is imported."""
        assert "accounts_source" in graph.models

    def test_measure_count(self, graph):
        """All 4 measures are imported."""
        model = graph.models["accounts_source"]
        assert len(model.metrics) == 4

    def test_non_additive_dimension_basic(self, graph):
        """Non-additive dimension name is captured (window_choice: min)."""
        model = graph.models["accounts_source"]
        first_day = model.get_metric("total_account_balance_first_day")
        assert first_day.non_additive_dimension == "ds"

    def test_non_additive_dimension_with_window_groupings(self, graph):
        """Non-additive dimension with window_groupings parses (name is captured)."""
        model = graph.models["accounts_source"]
        by_user = model.get_metric("current_account_balance_by_user")
        assert by_user.non_additive_dimension == "ds"

    def test_non_additive_dimension_monthly(self, graph):
        """Non-additive dimension on a monthly time dimension."""
        model = graph.models["accounts_source"]
        monthly = model.get_metric("total_account_balance_first_day_of_month")
        assert monthly.non_additive_dimension == "ds_month"

    def test_plain_balance_has_no_non_additive(self, graph):
        """Plain account_balance measure has no non_additive_dimension."""
        model = graph.models["accounts_source"]
        balance = model.get_metric("account_balance")
        assert balance.non_additive_dimension is None

    def test_dual_time_dimensions(self, graph):
        """Both ds (day) and ds_month (month) time dimensions are parsed."""
        model = graph.models["accounts_source"]
        ds = model.get_dimension("ds")
        assert ds.type == "time"
        assert ds.granularity == "day"

        ds_month = model.get_dimension("ds_month")
        assert ds_month.type == "time"
        assert ds_month.granularity == "month"
        assert ds_month.sql == "ds_month"

    def test_categorical_dimension(self, graph):
        """Categorical dimension is parsed."""
        model = graph.models["accounts_source"]
        acct_type = model.get_dimension("account_type")
        assert acct_type.type == "categorical"

    def test_foreign_entity(self, graph):
        """Foreign entity creates a relationship."""
        model = graph.models["accounts_source"]
        assert len(model.relationships) == 1
        user_rel = model.relationships[0]
        assert user_rel.name == "user"
        assert user_rel.type == "many_to_one"
        assert user_rel.foreign_key == "user_id"

    def test_default_agg_time_dimension(self, graph):
        """defaults.agg_time_dimension is captured."""
        model = graph.models["accounts_source"]
        assert model.default_time_dimension == "ds"


# =============================================================================
# SCD Type II: validity_params, natural/unique entity types
# =============================================================================


class TestSCDTypeII:
    """Tests for the SCD Type II fixture (validity_params, natural/unique entities)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "scd_type_ii.yml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_all_models_imported(self, graph):
        """All 3 semantic models are imported."""
        assert "listings" in graph.models
        assert "primary_accounts" in graph.models
        assert "companies" in graph.models

    def test_listings_dimensions(self, graph):
        """Listings model has all 5 dimensions."""
        model = graph.models["listings"]
        assert len(model.dimensions) == 5

    def test_validity_params_dimensions_parse(self, graph):
        """Dimensions with validity_params parse and preserve type/granularity/sql."""
        model = graph.models["listings"]

        window_start = model.get_dimension("window_start")
        assert window_start is not None
        assert window_start.type == "time"
        assert window_start.granularity == "day"
        assert window_start.sql == "active_from"

        window_end = model.get_dimension("window_end")
        assert window_end is not None
        assert window_end.type == "time"
        assert window_end.granularity == "day"
        assert window_end.sql == "active_to"

    def test_primary_accounts_validity_params(self, graph):
        """Primary accounts SCD dimensions also parse correctly."""
        model = graph.models["primary_accounts"]

        primary_from = model.get_dimension("primary_from")
        assert primary_from is not None
        assert primary_from.type == "time"
        assert primary_from.sql == "set_as_primary"

        primary_to = model.get_dimension("primary_to")
        assert primary_to is not None
        assert primary_to.type == "time"
        assert primary_to.sql == "removed_as_primary"

    def test_natural_entity_creates_one_relationship(self, graph):
        """Listings has 1 foreign rel (user), natural entity (listing) is not a rel."""
        model = graph.models["listings"]
        rel_names = {r.name for r in model.relationships}
        assert "user" in rel_names
        assert len(model.relationships) == 1

    def test_natural_entity_only_model_has_no_relationships(self, graph):
        """Primary accounts has only a natural entity (user), zero relationships."""
        model = graph.models["primary_accounts"]
        assert len(model.relationships) == 0

    def test_unique_entity_not_relationship(self, graph):
        """Unique entity (type: unique) does not create a relationship."""
        model = graph.models["companies"]
        rel_names = {r.name for r in model.relationships}
        assert "user" not in rel_names
        assert len(model.relationships) == 0

    def test_companies_primary_key(self, graph):
        """Companies model uses company_id from type: primary entity."""
        model = graph.models["companies"]
        assert model.primary_key == "company_id"

    def test_primary_entity_shorthand_not_handled(self, graph):
        """primary_entity shorthand on listings/primary_accounts defaults pk to 'id'."""
        assert graph.models["listings"].primary_key == "id"
        assert graph.models["primary_accounts"].primary_key == "id"

    def test_categorical_dimensions(self, graph):
        """Categorical dimensions on SCD models parse correctly."""
        listings = graph.models["listings"]
        assert listings.get_dimension("country").type == "categorical"
        assert listings.get_dimension("is_lux").type == "categorical"
        assert listings.get_dimension("capacity").type == "categorical"

    def test_table_from_ref(self, graph):
        """Table names are extracted from ref() syntax."""
        assert graph.models["listings"].table == "dim_listings"
        assert graph.models["primary_accounts"].table == "dim_primary_accounts"
        assert graph.models["companies"].table == "dim_companies"


# =============================================================================
# Multi-hop joins: bridge tables, 3+ hop entity chains, dimension-only models
# =============================================================================


class TestMultiHopJoins:
    """Tests for the multi-hop join fixture (bridge tables, dimension-only models)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "multi_hop_joins.yml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_all_models_imported(self, graph):
        """All 4 semantic models are imported."""
        assert len(graph.models) == 4
        assert "account_month_txns" in graph.models
        assert "bridge_table" in graph.models
        assert "customer_table" in graph.models
        assert "third_hop_table" in graph.models

    def test_model_with_measures(self, graph):
        """account_month_txns has a measure (txn_count)."""
        model = graph.models["account_month_txns"]
        assert len(model.metrics) == 1
        txn = model.get_metric("txn_count")
        assert txn.agg == "sum"

    def test_bridge_table_no_measures(self, graph):
        """bridge_table has no measures (dimension/entity-only model)."""
        model = graph.models["bridge_table"]
        assert len(model.metrics) == 0

    def test_customer_table_no_measures(self, graph):
        """customer_table has no measures."""
        model = graph.models["customer_table"]
        assert len(model.metrics) == 0

    def test_third_hop_no_measures(self, graph):
        """third_hop_table has no measures."""
        model = graph.models["third_hop_table"]
        assert len(model.metrics) == 0

    def test_bridge_table_entities(self, graph):
        """bridge_table has primary (account_id) and foreign (customer_id) entities."""
        model = graph.models["bridge_table"]
        assert model.primary_key == "account_id"
        rel_names = {r.name for r in model.relationships}
        assert "customer_id" in rel_names

    def test_bridge_table_relationship_type(self, graph):
        """Foreign entity on bridge_table creates many_to_one relationship."""
        model = graph.models["bridge_table"]
        customer_rel = next(r for r in model.relationships if r.name == "customer_id")
        assert customer_rel.type == "many_to_one"

    def test_dimension_only_models(self, graph):
        """Models with only dimensions (no measures) parse correctly."""
        customer = graph.models["customer_table"]
        assert len(customer.dimensions) == 2
        assert customer.get_dimension("customer_name").type == "categorical"
        assert customer.get_dimension("customer_atomic_weight").type == "categorical"

        third = graph.models["third_hop_table"]
        assert len(third.dimensions) == 1
        assert third.get_dimension("value").type == "categorical"

    def test_time_dimension(self, graph):
        """account_month_txns has a time dimension."""
        model = graph.models["account_month_txns"]
        ds = model.get_dimension("ds")
        assert ds.type == "time"
        assert ds.granularity == "day"

    def test_categorical_dimension_on_txn_model(self, graph):
        """account_month_txns has a categorical dimension."""
        model = graph.models["account_month_txns"]
        acct_month = model.get_dimension("account_month")
        assert acct_month.type == "categorical"

    def test_default_agg_time_dimension(self, graph):
        """Only account_month_txns defines a default agg_time_dimension."""
        assert graph.models["account_month_txns"].default_time_dimension == "ds"
        assert graph.models["bridge_table"].default_time_dimension is None
        assert graph.models["customer_table"].default_time_dimension is None

    def test_graph_metric(self, graph):
        """Graph-level metric txn_count is parsed."""
        assert "txn_count" in graph.metrics
        txn = graph.get_metric("txn_count")
        assert txn.type is None  # Simple -> untyped
        assert txn.sql == "txn_count"

    def test_table_from_ref(self, graph):
        """Table names extracted from ref() syntax."""
        assert graph.models["account_month_txns"].table == "account_month_txns"
        assert graph.models["bridge_table"].table == "bridge_table"
        assert graph.models["customer_table"].table == "customer_table"
        assert graph.models["third_hop_table"].table == "third_hop_table"


# =============================================================================
# Sub-daily granularities: second/minute/hour + cumulative/derived
# =============================================================================


class TestSubDailyGranularities:
    """Tests for the sub-daily granularity fixture (second/minute/hour time dimensions)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "sub_daily_granularities.yml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_model_exists(self, graph):
        """The users_ds_source semantic model is imported."""
        assert "users_ds_source" in graph.models

    def test_dimension_count(self, graph):
        """All 7 dimensions are imported."""
        model = graph.models["users_ds_source"]
        assert len(model.dimensions) == 7

    def test_day_granularity(self, graph):
        """Standard day granularity dimensions parse correctly."""
        model = graph.models["users_ds_source"]
        ds = model.get_dimension("ds")
        assert ds.type == "time"
        assert ds.granularity == "day"

        created = model.get_dimension("created_at")
        assert created.type == "time"
        assert created.granularity == "day"

    def test_partitioned_dimension(self, graph):
        """Partitioned dimension parses without error."""
        model = graph.models["users_ds_source"]
        ds_part = model.get_dimension("ds_partitioned")
        assert ds_part is not None
        assert ds_part.type == "time"
        assert ds_part.granularity == "day"

    def test_second_granularity(self, graph):
        """second time granularity is preserved."""
        model = graph.models["users_ds_source"]
        dim = model.get_dimension("bio_added_ts")
        assert dim.type == "time"
        assert dim.granularity == "second"

    def test_minute_granularity(self, graph):
        """minute time granularity is preserved."""
        model = graph.models["users_ds_source"]
        dim = model.get_dimension("last_login_ts")
        assert dim.type == "time"
        assert dim.granularity == "minute"

    def test_hour_granularity(self, graph):
        """hour time granularity is preserved."""
        model = graph.models["users_ds_source"]
        dim = model.get_dimension("archived_at")
        assert dim.type == "time"
        assert dim.granularity == "hour"

    def test_categorical_dimension(self, graph):
        """Categorical dimension parses correctly."""
        model = graph.models["users_ds_source"]
        home = model.get_dimension("home_state")
        assert home.type == "categorical"

    def test_measures(self, graph):
        """Both measures are imported with correct aggregation."""
        model = graph.models["users_ds_source"]
        assert len(model.metrics) == 2

        new_users = model.get_metric("new_users")
        assert new_users.agg == "sum"
        assert new_users.sql == "1"

        archived = model.get_metric("archived_users")
        assert archived.agg == "sum"
        assert archived.sql == "1"

    def test_default_agg_time_dimension(self, graph):
        """defaults.agg_time_dimension is captured."""
        model = graph.models["users_ds_source"]
        assert model.default_time_dimension == "created_at"

    def test_primary_entity(self, graph):
        """Primary entity from entities list sets the primary key."""
        model = graph.models["users_ds_source"]
        assert model.primary_key == "user_id"

    def test_cumulative_window_metric(self, graph):
        """Cumulative metric with sub-daily window parses correctly."""
        assert "subdaily_cumulative_window_metric" in graph.metrics
        metric = graph.get_metric("subdaily_cumulative_window_metric")
        assert metric.type == "cumulative"
        assert metric.window == "3 hours"

    def test_derived_offset_window_metric(self, graph):
        """Derived metric with offset_window parses correctly."""
        assert "subdaily_offset_window_metric" in graph.metrics
        metric = graph.get_metric("subdaily_offset_window_metric")
        assert metric.type == "derived"
        assert metric.sql == "archived_users"

    def test_derived_offset_grain_to_date_metric(self, graph):
        """Derived metric with offset_to_grain parses correctly."""
        assert "subdaily_offset_grain_to_date_metric" in graph.metrics
        metric = graph.get_metric("subdaily_offset_grain_to_date_metric")
        assert metric.type == "derived"
        assert metric.sql == "archived_users"

    def test_metric_count(self, graph):
        """All 3 graph-level metrics are imported."""
        assert len(graph.metrics) == 3


# =============================================================================
# xfail tests for unsupported features
# =============================================================================


class TestUnsupportedFeatures:
    """Tests for MetricFlow features not yet supported by sidemantic."""

    @pytest.mark.xfail(
        reason="Dimension model does not support 'millisecond' granularity",
        raises=Exception,
    )
    def test_millisecond_granularity(self):
        """Parsing a dimension with millisecond granularity fails validation."""
        adapter = MetricFlowAdapter()
        adapter.parse(FIXTURES / "sub_daily_millisecond.yml")

    @pytest.mark.xfail(
        reason="Metric model does not support sub-daily grain_to_date (hour)",
        raises=Exception,
    )
    def test_subdaily_grain_to_date(self):
        """Parsing a cumulative metric with grain_to_date: hour fails validation."""
        adapter = MetricFlowAdapter()
        adapter.parse(FIXTURES / "sub_daily_grain_to_date_hour.yml")


# =============================================================================
# Simple manifest metrics: 85+ metrics from MetricFlow canonical test suite
# =============================================================================


class TestSimpleManifestMetrics:
    """Tests for the full simple_manifest metrics fixture (Apache 2.0, dbt-labs/metricflow).

    Covers: offset_window, offset_to_grain, nested derived, fill_nulls_with,
    conversion with constant_properties, metric-level time_granularity,
    metric filters referencing other metrics, percentile flavors, median,
    sum_boolean, period_agg, join_to_timespine, alias reuse.
    """

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "simple_manifest_metrics.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_model_exists(self, graph):
        """The bookings_source semantic model is imported."""
        assert "bookings_source" in graph.models

    def test_measure_count(self, graph):
        """All 14 measures are imported from the semantic model."""
        model = graph.models["bookings_source"]
        assert len(model.metrics) == 14

    def test_simple_metrics_parsed(self, graph):
        """Simple metrics are parsed as untyped with measure references."""
        bookings = graph.get_metric("bookings")
        assert bookings is not None
        assert bookings.type is None
        assert bookings.sql == "bookings"

        bookers = graph.get_metric("bookers")
        assert bookers is not None
        assert bookers.type is None

    def test_simple_metric_with_filter(self, graph):
        """Simple metric with Dimension filter parses with filter captured."""
        instant_bv = graph.get_metric("instant_booking_value")
        assert instant_bv is not None
        assert instant_bv.type is None
        assert instant_bv.filters is not None
        assert len(instant_bv.filters) > 0

    def test_simple_metric_with_entity_filter(self, graph):
        """Simple metric with Entity filter parses correctly."""
        non_null = graph.get_metric("booking_value_for_non_null_listing_id")
        assert non_null is not None
        assert non_null.filters is not None

    def test_fill_nulls_with_measure_dict(self, graph):
        """Simple metric with fill_nulls_with as dict measure parses."""
        metric = graph.get_metric("bookings_fill_nulls_with_0_without_time_spine")
        assert metric is not None
        assert metric.type is None

    def test_join_to_timespine_measure_dict(self, graph):
        """Simple metric with join_to_timespine parses."""
        metric = graph.get_metric("bookings_join_to_time_spine")
        assert metric is not None

    def test_metric_level_time_granularity(self, graph):
        """Metric with time_granularity field parses (field not captured but no error)."""
        metric = graph.get_metric("largest_listing")
        assert metric is not None

    def test_metric_filter_referencing_other_metric(self, graph):
        """Metric with filter referencing another metric via Metric() parses."""
        metric = graph.get_metric("active_listings")
        assert metric is not None
        assert metric.filters is not None
        assert "Metric(" in metric.filters[0]

    def test_cumulative_with_period_agg(self, graph):
        """Cumulative metric with period_agg parses correctly."""
        t2mr = graph.get_metric("trailing_2_months_revenue")
        assert t2mr is not None
        assert t2mr.type == "cumulative"
        assert t2mr.window == "2 month"

    def test_cumulative_all_time(self, graph):
        """Cumulative metric with no window (all time) parses."""
        bat = graph.get_metric("bookings_all_time")
        assert bat is not None
        assert bat.type == "cumulative"
        assert bat.window is None

    def test_cumulative_grain_to_date(self, graph):
        """Cumulative metric with grain_to_date parses."""
        mtd = graph.get_metric("revenue_mtd")
        assert mtd is not None
        assert mtd.type == "cumulative"
        assert mtd.grain_to_date == "month"

    def test_cumulative_with_filter(self, graph):
        """Cumulative metric with filter parses."""
        metric = graph.get_metric("trailing_2_months_revenue_with_filter")
        assert metric is not None
        assert metric.type == "cumulative"
        assert metric.filters is not None

    def test_derived_basic(self, graph):
        """Basic derived metric parses with expression."""
        fees = graph.get_metric("booking_fees")
        assert fees is not None
        assert fees.type == "derived"
        assert fees.sql == "booking_value * 0.05"

    def test_derived_multi_input(self, graph):
        """Derived metric with multiple inputs parses."""
        fees_per = graph.get_metric("booking_fees_per_booker")
        assert fees_per is not None
        assert fees_per.type == "derived"
        assert "bookers" in fees_per.sql

    def test_derived_with_offset_window(self, graph):
        """Derived metric with offset_window on input parses."""
        growth = graph.get_metric("bookings_growth_2_weeks")
        assert growth is not None
        assert growth.type == "derived"

    def test_derived_offset_to_grain(self, graph):
        """Derived metric with offset_to_grain parses."""
        metric = graph.get_metric("bookings_all_time_at_start_of_month")
        assert metric is not None
        assert metric.type == "derived"

    def test_derived_nested(self, graph):
        """Nested derived metric (derived referencing derived) parses."""
        nested = graph.get_metric("booking_value_sub_instant_add_10")
        assert nested is not None
        assert nested.type == "derived"
        assert "booking_value_sub_instant" in nested.sql

    def test_derived_with_alias(self, graph):
        """Derived metric with alias on input parses."""
        pct = graph.get_metric("non_referred_bookings_pct")
        assert pct is not None
        assert pct.type == "derived"
        assert "ref_bookings" in pct.sql

    def test_derived_with_filtered_input(self, graph):
        """Derived metric with filter on input metric parses."""
        delayed = graph.get_metric("double_counted_delayed_bookings")
        assert delayed is not None
        assert delayed.type == "derived"

    def test_derived_mom_yoy(self, graph):
        """Month-over-month and year-over-year derived metrics parse."""
        mom = graph.get_metric("bookings_mom")
        assert mom is not None
        assert mom.type == "derived"

        yoy = graph.get_metric("bookings_yoy")
        assert yoy is not None
        assert yoy.type == "derived"

    def test_derived_offset_once_twice(self, graph):
        """Nested offset (offset_once -> offset_twice) parses."""
        once = graph.get_metric("bookings_offset_once")
        assert once is not None
        assert once.type == "derived"

        twice = graph.get_metric("bookings_offset_twice")
        assert twice is not None
        assert twice.type == "derived"

    def test_derived_shared_aliases(self, graph):
        """Derived metrics with shared alias names parse independently."""
        a = graph.get_metric("derived_shared_alias_1a")
        assert a is not None
        assert a.type == "derived"
        assert "shared_alias" in a.sql

        b = graph.get_metric("derived_shared_alias_2")
        assert b is not None

    def test_derived_fill_nulls(self, graph):
        """Derived metrics with fill_nulls inputs parse."""
        metric = graph.get_metric("bookings_growth_2_weeks_fill_nulls_with_0")
        assert metric is not None
        assert metric.type == "derived"

    def test_ratio_basic(self, graph):
        """Basic ratio metric parses with numerator and denominator."""
        ratio = graph.get_metric("bookings_per_booker")
        assert ratio is not None
        assert ratio.type == "ratio"
        assert ratio.numerator == "bookings"
        assert ratio.denominator == "bookers"

    def test_ratio_with_filter(self, graph):
        """Ratio metric with filter on numerator parses."""
        ratio = graph.get_metric("instant_booking_fraction_of_max_value")
        assert ratio is not None
        assert ratio.type == "ratio"

    def test_ratio_with_alias(self, graph):
        """Ratio metric with alias on numerator parses."""
        ratio = graph.get_metric("instant_booking_value_ratio")
        assert ratio is not None
        assert ratio.type == "ratio"
        assert ratio.numerator == "booking_value"
        assert ratio.denominator == "booking_value"

    def test_ratio_with_metric_filter(self, graph):
        """Ratio metric with Metric() filter parses."""
        ratio = graph.get_metric("popular_listing_bookings_per_booker")
        assert ratio is not None
        assert ratio.type == "ratio"
        assert ratio.filters is not None

    def test_conversion_metrics_skipped(self, graph):
        """Conversion metrics are skipped (unsupported type)."""
        assert "visit_buy_conversion_rate_7days" not in graph.metrics
        assert "visit_buy_conversion_rate" not in graph.metrics
        assert "visit_buy_conversions" not in graph.metrics
        assert "visit_buy_conversion_rate_by_session" not in graph.metrics

    def test_total_metric_count(self, graph):
        """Verify total number of parsed metrics (simple + cumulative + derived + ratio, excluding conversion)."""
        # Conversion metrics are skipped, so we count only supported types
        assert len(graph.metrics) >= 50


# =============================================================================
# Simple manifest buys_source: conversion tracking semantic model
# =============================================================================


class TestSimpleManifestBuysSource:
    """Tests for the buys_source fixture (Apache 2.0, dbt-labs/metricflow)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "simple_manifest_buys_source.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_model_exists(self, graph):
        """The buys_source semantic model is imported."""
        assert "buys_source" in graph.models

    def test_measure_count(self, graph):
        """All 3 measures are imported."""
        model = graph.models["buys_source"]
        assert len(model.metrics) == 3

    def test_count_aggregation(self, graph):
        """Count aggregation on buys measure."""
        model = graph.models["buys_source"]
        buys = model.get_metric("buys")
        assert buys.agg == "count"
        assert buys.sql == "1"

    def test_count_distinct_aggregation(self, graph):
        """Count distinct on buyers measure."""
        model = graph.models["buys_source"]
        buyers = model.get_metric("buyers")
        assert buyers.agg == "count_distinct"
        assert buyers.sql == "user_id"

    def test_monthly_agg_time_dimension(self, graph):
        """buys_month measure uses ds_month as agg_time_dimension."""
        model = graph.models["buys_source"]
        buys_month = model.get_metric("buys_month")
        assert buys_month is not None

    def test_dual_time_dimensions(self, graph):
        """Day and month time dimensions coexist."""
        model = graph.models["buys_source"]
        ds = model.get_dimension("ds")
        assert ds.type == "time"
        assert ds.granularity == "day"

        ds_month = model.get_dimension("ds_month")
        assert ds_month.type == "time"
        assert ds_month.granularity == "month"

    def test_foreign_entities(self, graph):
        """Foreign entities create relationships."""
        model = graph.models["buys_source"]
        rel_names = {r.name for r in model.relationships}
        assert "user" in rel_names
        assert "session_id" in rel_names


# =============================================================================
# Simple manifest saved queries: saved queries with order_by, limit, exports
# =============================================================================


class TestSimpleManifestSavedQueries:
    """Tests for the saved_queries fixture (Apache 2.0, dbt-labs/metricflow)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "simple_manifest_saved_queries.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors (saved_queries key is ignored gracefully)."""
        assert graph is not None

    def test_model_exists(self, graph):
        """The placeholder semantic model is imported."""
        assert "sales_for_saved_queries" in graph.models

    def test_saved_queries_not_in_metrics(self, graph):
        """saved_queries are not parsed into graph.metrics (not yet supported)."""
        assert "p0_booking" not in graph.metrics
        assert "p0_booking_with_order_by_and_limit" not in graph.metrics


# =============================================================================
# SCD listings: validity_params from upstream test suite
# =============================================================================


class TestSCDListings:
    """Tests for the scd_listings fixture (Apache 2.0, dbt-labs/metricflow)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "scd_listings.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_model_exists(self, graph):
        """The listings semantic model is imported."""
        assert "listings" in graph.models

    def test_validity_params_start(self, graph):
        """Dimension with validity_params is_start parses with expr."""
        model = graph.models["listings"]
        window_start = model.get_dimension("window_start")
        assert window_start.type == "time"
        assert window_start.granularity == "day"
        assert window_start.sql == "active_from"

    def test_validity_params_end(self, graph):
        """Dimension with validity_params is_end parses with expr."""
        model = graph.models["listings"]
        window_end = model.get_dimension("window_end")
        assert window_end.type == "time"
        assert window_end.sql == "active_to"

    def test_categorical_dimensions(self, graph):
        """All 3 categorical dimensions parse."""
        model = graph.models["listings"]
        for dim_name in ("country", "is_lux", "capacity"):
            assert model.get_dimension(dim_name).type == "categorical"

    def test_natural_entity(self, graph):
        """Natural entity (listing) does not create a foreign relationship."""
        model = graph.models["listings"]
        rel_names = {r.name for r in model.relationships}
        assert "listing" not in rel_names

    def test_foreign_entity(self, graph):
        """Foreign entity (user) creates many_to_one relationship."""
        model = graph.models["listings"]
        user_rel = next(r for r in model.relationships if r.name == "user")
        assert user_rel.type == "many_to_one"
        assert user_rel.foreign_key == "user_id"


# =============================================================================
# SCD metrics: metrics on SCD dimensions
# =============================================================================


class TestSCDMetrics:
    """Tests for the scd_metrics fixture (Apache 2.0, dbt-labs/metricflow)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "scd_metrics.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_simple_metric(self, graph):
        """Simple metric on SCD model parses."""
        bookings = graph.get_metric("bookings")
        assert bookings is not None
        assert bookings.type is None
        assert bookings.sql == "bookings"

    def test_metric_with_scd_dimension_filter(self, graph):
        """Metric with filter on SCD dimension (capacity) parses."""
        family = graph.get_metric("family_bookings")
        assert family is not None
        assert family.filters is not None
        assert "capacity" in family.filters[0]

    def test_metric_with_boolean_or_scd_filter(self, graph):
        """Metric with boolean OR filter on SCD dimension parses."""
        lux = graph.get_metric("potentially_lux_bookings")
        assert lux is not None
        assert lux.filters is not None
        assert "is_lux" in lux.filters[0]


# =============================================================================
# Ambiguous resolution manifest: mixed month/year grain
# =============================================================================


class TestAmbiguousResolutionManifest:
    """Tests for the ambiguous_resolution_manifest fixture (Apache 2.0, dbt-labs/metricflow)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "ambiguous_resolution_manifest.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_monthly_model(self, graph):
        """Monthly measures source with month-grain time dimension."""
        assert "monthly_measures_source" in graph.models
        model = graph.models["monthly_measures_source"]
        assert len(model.metrics) == 2
        ct = model.get_dimension("creation_time")
        assert ct.type == "time"
        assert ct.granularity == "month"

    def test_yearly_model(self, graph):
        """Yearly measure source with year-grain time dimension."""
        assert "yearly_measure_source" in graph.models
        model = graph.models["yearly_measure_source"]
        assert len(model.metrics) == 1
        ct = model.get_dimension("creation_time")
        assert ct.granularity == "year"

    def test_simple_metrics(self, graph):
        """Simple metrics on monthly and yearly measures parse."""
        assert "monthly_metric_0" in graph.metrics
        assert "monthly_metric_1" in graph.metrics
        assert "yearly_metric_0" in graph.metrics

    def test_derived_same_grain(self, graph):
        """Derived metric combining same-grain (monthly) parents."""
        metric = graph.get_metric("derived_metric_with_same_parent_time_grains")
        assert metric.type == "derived"
        assert "monthly_metric_0" in metric.sql
        assert "monthly_metric_1" in metric.sql

    def test_derived_different_grains(self, graph):
        """Derived metric combining different-grain (month + year) parents."""
        metric = graph.get_metric("derived_metric_with_different_parent_time_grains")
        assert metric.type == "derived"
        assert "monthly_metric_0" in metric.sql
        assert "yearly_metric_0" in metric.sql

    def test_nested_derived_heterogeneous(self, graph):
        """Nested derived from heterogeneous-grain derived metric."""
        metric = graph.get_metric("metric_derived_from_heterogeneous_derived_metric")
        assert metric.type == "derived"

    def test_nested_derived_homogeneous(self, graph):
        """Nested derived from homogeneous-grain derived metric."""
        metric = graph.get_metric("metric_derived_from_homogeneous_derived_metric")
        assert metric.type == "derived"

    def test_cumulative_non_day_grain(self, graph):
        """Cumulative metric on monthly measure with 2-month window."""
        metric = graph.get_metric("accumulate_last_2_months_metric")
        assert metric.type == "cumulative"
        assert metric.window == "2 months"

    def test_metric_level_time_granularity(self, graph):
        """Metric with time_granularity: quarter parses."""
        metric = graph.get_metric("simple_metric_with_default_time_granularity")
        assert metric is not None

    def test_derived_with_time_granularity(self, graph):
        """Derived metric with time_granularity: year parses."""
        metric = graph.get_metric("derived_metric_with_time_granularity")
        assert metric.type == "derived"

    def test_derived_with_filtered_input(self, graph):
        """Derived metric with TimeDimension filter on input parses."""
        metric = graph.get_metric("derived_metric_with_common_filtered_metric_0")
        assert metric.type == "derived"

    def test_total_metric_count(self, graph):
        """All metrics from the ambiguous resolution manifest are parsed."""
        # 5 simple + 9 derived + 1 cumulative + 1 derived w/o time_gran = 16
        assert len(graph.metrics) == 16


# =============================================================================
# Extended date manifest: monthly grain, cross-grain ratios
# =============================================================================


class TestExtendedDateManifest:
    """Tests for the extended_date_manifest fixture (Apache 2.0, dbt-labs/metricflow)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "extended_date_manifest.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_all_models(self, graph):
        """All 3 semantic models are imported."""
        assert "monthly_bookings_source" in graph.models
        assert "extended_bookings_source" in graph.models
        assert "listings_extended_source" in graph.models

    def test_monthly_grain_model(self, graph):
        """Monthly bookings source has month-grain time dimension."""
        model = graph.models["monthly_bookings_source"]
        ds = model.get_dimension("ds")
        assert ds.type == "time"
        assert ds.granularity == "month"

    def test_daily_grain_model(self, graph):
        """Extended bookings source has day-grain time dimension."""
        model = graph.models["extended_bookings_source"]
        ds = model.get_dimension("ds")
        assert ds.type == "time"
        assert ds.granularity == "day"

    def test_listings_dimension_with_expr(self, graph):
        """Listings extended source dimension has expr."""
        model = graph.models["listings_extended_source"]
        ds = model.get_dimension("ds")
        assert ds.sql == "listing_creation_ds"

    def test_cumulative_daily_window(self, graph):
        """Cumulative metric with 7-day window on daily measure."""
        metric = graph.get_metric("weekly_bookers")
        assert metric.type == "cumulative"
        assert metric.window == "7 days"

    def test_derived_offset_on_monthly(self, graph):
        """Derived metric with offset_window on monthly measure."""
        metric = graph.get_metric("bookings_last_month")
        assert metric.type == "derived"

    def test_cumulative_monthly_window(self, graph):
        """Cumulative metric with 3-month window on monthly measure."""
        metric = graph.get_metric("trailing_3_months_bookings")
        assert metric.type == "cumulative"
        assert metric.window == "3 month"

    def test_cross_grain_ratio(self, graph):
        """Ratio metric with daily numerator and monthly denominator."""
        metric = graph.get_metric("monthly_bookings_to_daily_bookings")
        assert metric.type == "ratio"
        assert metric.numerator == "bookings"
        assert metric.denominator == "bookings_monthly"

    def test_foreign_entities_across_models(self, graph):
        """Foreign entity linking monthly bookings to listings."""
        model = graph.models["monthly_bookings_source"]
        rel_names = {r.name for r in model.relationships}
        assert "listing" in rel_names


# =============================================================================
# Cyclic join manifest: bidirectional entity links
# =============================================================================


class TestCyclicJoinManifest:
    """Tests for the cyclic_join_manifest fixture (Apache 2.0, dbt-labs/metricflow)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "cyclic_join_manifest.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_both_models(self, graph):
        """Both listings_latest and listings_latest_cyclic are imported."""
        assert "listings_latest" in graph.models
        assert "listings_latest_cyclic" in graph.models

    def test_listings_latest_measures(self, graph):
        """listings_latest has a sum measure."""
        model = graph.models["listings_latest"]
        listings = model.get_metric("listings")
        assert listings.agg == "sum"
        assert listings.sql == "1"

    def test_listings_latest_dimensions(self, graph):
        """listings_latest has time and categorical dimensions."""
        model = graph.models["listings_latest"]
        ds = model.get_dimension("ds")
        assert ds.type == "time"
        assert ds.sql == "created_at"

        country = model.get_dimension("country_latest")
        assert country.type == "categorical"
        assert country.sql == "country"

    def test_cyclic_model_no_measures(self, graph):
        """listings_latest_cyclic has no measures (dimension-only)."""
        model = graph.models["listings_latest_cyclic"]
        assert len(model.metrics) == 0

    def test_cyclic_model_dimension(self, graph):
        """listings_latest_cyclic has a categorical dimension with expr."""
        model = graph.models["listings_latest_cyclic"]
        cap = model.get_dimension("capacity_latest")
        assert cap.type == "categorical"
        assert cap.sql == "capacity"

    def test_unique_entity_not_relationship(self, graph):
        """Unique entity (cyclic_entity) does not create a foreign relationship."""
        model = graph.models["listings_latest"]
        rel_names = {r.name for r in model.relationships}
        assert "cyclic_entity" not in rel_names

    def test_primary_key(self, graph):
        """Both models have listing_id as primary key."""
        assert graph.models["listings_latest"].primary_key == "listing_id"
        assert graph.models["listings_latest_cyclic"].primary_key == "listing_id"


# =============================================================================
# Name edge case manifest: metric=dimension and metric=entity name collisions
# =============================================================================


class TestNameEdgeCaseManifest:
    """Tests for the name_edge_case_manifest fixture (Apache 2.0, dbt-labs/metricflow)."""

    @pytest.fixture()
    def graph(self):
        adapter = MetricFlowAdapter()
        return adapter.parse(FIXTURES / "name_edge_case_manifest.yaml")

    def test_parse_succeeds(self, graph):
        """Fixture parses without errors."""
        assert graph is not None

    def test_model_exists(self, graph):
        """The bookings_source semantic model is imported."""
        assert "bookings_source" in graph.models

    def test_measure(self, graph):
        """booking_value measure is parsed."""
        model = graph.models["bookings_source"]
        bv = model.get_metric("booking_value")
        assert bv.agg == "sum"

    def test_metric_same_name_as_dimension(self, graph):
        """Metric named 'homonymous_metric_and_dimension' coexists with dimension of same name."""
        metric = graph.get_metric("homonymous_metric_and_dimension")
        assert metric is not None
        assert metric.type is None  # Simple -> untyped

        model = graph.models["bookings_source"]
        dim = model.get_dimension("homonymous_metric_and_dimension")
        assert dim is not None
        assert dim.type == "categorical"

    def test_metric_same_name_as_entity(self, graph):
        """Metric named 'homonymous_metric_and_entity' parses alongside entity of same name."""
        metric = graph.get_metric("homonymous_metric_and_entity")
        assert metric is not None
        assert metric.type is None

    def test_dummy_expressions(self, graph):
        """Dimension and entity with string literal expressions parse."""
        model = graph.models["bookings_source"]
        dim = model.get_dimension("homonymous_metric_and_dimension")
        assert dim.sql == "'dummy_dimension_value'"
