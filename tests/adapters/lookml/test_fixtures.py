"""Tests for real-world LookML fixtures.

These tests parse actual LookML patterns found in production Looker projects.
They are intentionally permissive: parse without errors, check counts, verify key names.
Features the adapter does not yet support are marked with pytest.mark.xfail.
"""

from pathlib import Path

import pytest

from sidemantic.adapters.lookml import LookMLAdapter

FIXTURES_DIR = Path("tests/fixtures/lookml")


# =============================================================================
# REDSHIFT ADMIN BLOCK
# Source: looker-open-source/blocks_redshift_admin
# Multi-view file, derived tables, Liquid HTML, datagroup_trigger,
# distribution/sortkeys, yesno, count_distinct, derived measures
# =============================================================================


class TestRedshiftAdmin:
    """Tests for the Redshift Admin block fixture."""

    @pytest.fixture(autouse=True)
    def setup(self):
        adapter = LookMLAdapter()
        self.graph = adapter.parse(FIXTURES_DIR / "realworld_redshift_admin.view.lkml")

    def test_parses_all_five_views(self):
        """All 5 views in the multi-view file should be parsed as models."""
        expected = {
            "redshift_db_space",
            "redshift_etl_errors",
            "redshift_data_loads",
            "redshift_queries",
            "redshift_tables",
        }
        assert expected == set(self.graph.models.keys())

    def test_derived_tables_have_sql(self):
        """All views use derived_table, so table should be None and sql should exist."""
        for model_name in self.graph.models:
            model = self.graph.get_model(model_name)
            assert model.table is None, f"{model_name} should have no table (derived)"
            assert model.sql is not None, f"{model_name} should have derived table SQL"

    def test_redshift_db_space_dimensions(self):
        model = self.graph.get_model("redshift_db_space")
        assert len(model.dimensions) == 5
        assert model.get_dimension("table") is not None
        assert model.get_dimension("schema") is not None
        assert model.get_dimension("megabytes").type == "numeric"
        assert model.get_dimension("rows").type == "numeric"
        # table_stem has complex CASE/regex SQL referencing ${table}
        assert model.get_dimension("table_stem") is not None

    def test_redshift_db_space_measures(self):
        model = self.graph.get_model("redshift_db_space")
        assert model.get_metric("total_megabytes").agg == "sum"
        assert model.get_metric("total_rows").agg == "sum"
        assert model.get_metric("total_tables").agg == "count_distinct"

    def test_redshift_etl_errors_time_dimensions(self):
        """dimension_group type:time with timeframes [time, date]."""
        model = self.graph.get_model("redshift_etl_errors")
        assert model.get_dimension("error_time") is not None
        assert model.get_dimension("error_time").type == "time"
        assert model.get_dimension("error_time").granularity == "hour"
        assert model.get_dimension("error_date") is not None
        assert model.get_dimension("error_date").type == "time"
        assert model.get_dimension("error_date").granularity == "day"

    def test_redshift_etl_errors_string_dimensions(self):
        model = self.graph.get_model("redshift_etl_errors")
        for dim_name in ["file_name", "column_name", "column_data_type", "error_reason"]:
            dim = model.get_dimension(dim_name)
            assert dim is not None, f"Missing dimension: {dim_name}"
            assert dim.type == "categorical"

    def test_redshift_data_loads_derived_measures(self):
        """type:string and type:number measures become derived."""
        model = self.graph.get_model("redshift_data_loads")
        # most_recent_load is type:string -> derived
        most_recent = model.get_metric("most_recent_load")
        assert most_recent is not None
        # hours_since_last_load is type:number -> derived
        hours = model.get_metric("hours_since_last_load")
        assert hours is not None
        assert hours.type == "derived"

    def test_redshift_queries_primary_key(self):
        model = self.graph.get_model("redshift_queries")
        assert model.primary_key == "query"

    def test_redshift_queries_yesno_dimension(self):
        """type:yesno maps to categorical."""
        model = self.graph.get_model("redshift_queries")
        was_queued = model.get_dimension("was_queued")
        assert was_queued is not None
        assert was_queued.type == "categorical"

    def test_redshift_queries_time_group_many_timeframes(self):
        """dimension_group with 8 timeframes (including raw, which is skipped)."""
        model = self.graph.get_model("redshift_queries")
        # Raw is skipped, so 7 time dims expected from start group
        time_dims = [d for d in model.dimensions if d.name.startswith("start_")]
        assert len(time_dims) >= 5  # at least date, hour, minute, second, day_of_week

    def test_redshift_queries_derived_measures(self):
        model = self.graph.get_model("redshift_queries")
        # percent_queued and time_executing_per_query are type:number -> derived
        assert model.get_metric("percent_queued").type == "derived"
        assert model.get_metric("time_executing_per_query").type == "derived"

    def test_redshift_tables_yesno_and_group_labels(self):
        """encoded is type:yesno, several dims use group_label."""
        model = self.graph.get_model("redshift_tables")
        encoded = model.get_dimension("encoded")
        assert encoded is not None
        assert encoded.type == "categorical"  # yesno -> categorical

    def test_redshift_tables_measures(self):
        model = self.graph.get_model("redshift_tables")
        assert model.get_metric("count").agg == "count"
        assert model.get_metric("total_rows").agg == "sum"
        assert model.get_metric("total_size").agg == "sum"


# =============================================================================
# GOOGLE ADS BIGQUERY TRANSFER BLOCK
# Source: looker/app-marketing-google-ads-transfer-bigquery
# extension:required, extends chains, case dimensions, yesno, link blocks
# =============================================================================


class TestGoogleAds:
    """Tests for the Google Ads BigQuery Transfer fixture."""

    @pytest.fixture(autouse=True)
    def setup(self):
        adapter = LookMLAdapter()
        self.graph = adapter.parse(FIXTURES_DIR / "realworld_google_ads.view.lkml")

    def test_parses_all_views(self):
        expected = {
            "hour_base",
            "transformations_base",
            "ad_impressions_adapter",
            "ad_impressions_campaign_adapter",
            "ad_impressions_ad_group_adapter",
            "keyword_adapter",
        }
        assert expected == set(self.graph.models.keys())

    def test_extension_required_base_views_parsed(self):
        """Views with extension:required should still be parsed as models."""
        hour = self.graph.get_model("hour_base")
        assert hour is not None
        assert len(hour.dimensions) == 1
        assert hour.get_dimension("hour_of_day").type == "numeric"

    def test_transformations_base_case_dimensions(self):
        """case: dimensions with when: blocks should parse as categorical."""
        model = self.graph.get_model("transformations_base")
        assert model.get_dimension("ad_network_type").type == "categorical"
        assert model.get_dimension("device_type").type == "categorical"

    def test_ad_impressions_adapter_table_and_dims(self):
        model = self.graph.get_model("ad_impressions_adapter")
        assert model.table == "adwords.AccountBasicStats"
        assert model.get_dimension("cost").type == "numeric"
        assert model.get_dimension("clicks").type == "numeric"
        assert model.get_dimension("impressions").type == "numeric"

    def test_ad_impressions_adapter_time_group(self):
        model = self.graph.get_model("ad_impressions_adapter")
        assert model.get_dimension("date_date") is not None
        assert model.get_dimension("date_week") is not None
        assert model.get_dimension("date_month") is not None
        assert model.get_dimension("date_quarter") is not None
        assert model.get_dimension("date_year") is not None

    def test_ad_impressions_adapter_measures(self):
        model = self.graph.get_model("ad_impressions_adapter")
        assert model.get_metric("total_impressions").agg == "sum"
        assert model.get_metric("total_clicks").agg == "sum"
        assert model.get_metric("total_cost").agg == "sum"
        # Derived ratio measures
        assert model.get_metric("average_cpc").type == "derived"
        assert model.get_metric("click_through_rate").type == "derived"

    def test_extends_chain_views_parsed(self):
        """Views in the extends chain should each be parsed independently."""
        campaign = self.graph.get_model("ad_impressions_campaign_adapter")
        assert campaign.table == "adwords.CampaignBasicStats"
        assert campaign.get_dimension("campaign_id") is not None
        assert campaign.get_dimension("campaign_name") is not None

        ad_group = self.graph.get_model("ad_impressions_ad_group_adapter")
        assert ad_group.table == "adwords.AdGroupBasicStats"
        assert ad_group.get_dimension("ad_group_id") is not None

    def test_keyword_adapter_yesno_and_case(self):
        kw = self.graph.get_model("keyword_adapter")
        assert kw.primary_key == "criterion_id"
        # yesno dimension
        assert kw.get_dimension("is_negative").type == "categorical"
        # case dimension
        assert kw.get_dimension("bidding_strategy_type").type == "categorical"

    def test_keyword_adapter_grouped_url_dimensions(self):
        """Dimensions with group_label should still parse."""
        kw = self.graph.get_model("keyword_adapter")
        assert kw.get_dimension("criteria_destination_url") is not None
        assert kw.get_dimension("final_url") is not None


# =============================================================================
# DATATONIC DATE COMPARISON
# Source: teamdatatonic/looker-date-comparison
# extension:required, dimension_group type:duration, filter fields, parameter blocks
# =============================================================================


class TestDateComparison:
    """Tests for the date comparison fixture."""

    @pytest.fixture(autouse=True)
    def setup(self):
        adapter = LookMLAdapter()
        self.graph = adapter.parse(FIXTURES_DIR / "realworld_date_comparison.view.lkml")

    def test_parses_single_view(self):
        assert "_date_comparison" in self.graph.models
        assert len(self.graph.models) == 1

    def test_duration_dimension_group(self):
        """dimension_group type:duration should produce numeric dimensions."""
        model = self.graph.get_model("_date_comparison")
        dur_dim = model.get_dimension("in_period_days")
        assert dur_dim is not None
        assert dur_dim.type == "numeric"
        assert "DATE_DIFF" in dur_dim.sql

    def test_time_dimension_group(self):
        """date_in_period dimension_group type:time with 5 timeframes."""
        model = self.graph.get_model("_date_comparison")
        assert model.get_dimension("date_in_period_date") is not None
        assert model.get_dimension("date_in_period_week") is not None
        assert model.get_dimension("date_in_period_month") is not None
        assert model.get_dimension("date_in_period_quarter") is not None
        assert model.get_dimension("date_in_period_year") is not None

    def test_regular_dimensions(self):
        model = self.graph.get_model("_date_comparison")
        assert model.get_dimension("period") is not None
        assert model.get_dimension("period").type == "categorical"
        assert model.get_dimension("day_in_period").type == "numeric"
        assert model.get_dimension("order_for_period").type == "numeric"

    def test_filter_fields_without_sql_not_segments(self):
        """filter: fields with no sql should NOT become segments."""
        model = self.graph.get_model("_date_comparison")
        # These filters have no sql key, so the adapter correctly skips them
        assert len(model.segments) == 0

    def test_total_dimension_count(self):
        """Should have duration + time group + regular dimensions."""
        model = self.graph.get_model("_date_comparison")
        # 1 duration dim + 5 time dims + 5 regular dims = 11
        assert len(model.dimensions) == 11


# =============================================================================
# PYLOOKML KITCHEN SINK
# Source: looker-open-source/pylookml kitchenSink
# action blocks, filter fields with suggestions, tier/yesno/median/count_distinct,
# derived measures, drill_fields with set refs, link blocks, set blocks
# =============================================================================


class TestPyLookMLKitchenSink:
    """Tests for the PyLookML kitchen sink fixture."""

    @pytest.fixture(autouse=True)
    def setup(self):
        adapter = LookMLAdapter()
        self.graph = adapter.parse(FIXTURES_DIR / "realworld_pylookml_kitchen_sink.view.lkml")

    def test_parses_single_view(self):
        assert "order_items" in self.graph.models
        assert len(self.graph.models) == 1

    def test_table_name_and_primary_key(self):
        model = self.graph.get_model("order_items")
        assert model.table == "ecomm.order_items"
        assert model.primary_key == "id"

    def test_dimension_count(self):
        """Should have many dimensions including time groups."""
        model = self.graph.get_model("order_items")
        # 8 regular dims + ~10 created timeframes + ~3 shipped timeframes = ~21
        assert len(model.dimensions) >= 18

    def test_tier_dimension(self):
        """type:tier maps to categorical."""
        model = self.graph.get_model("order_items")
        tier_dim = model.get_dimension("item_gross_margin_percentage_tier")
        assert tier_dim is not None
        assert tier_dim.type == "categorical"

    def test_yesno_dimension(self):
        model = self.graph.get_model("order_items")
        assert model.get_dimension("is_returned").type == "categorical"

    def test_numeric_dimensions(self):
        model = self.graph.get_model("order_items")
        assert model.get_dimension("sale_price").type == "numeric"
        assert model.get_dimension("gross_margin").type == "numeric"
        assert model.get_dimension("days_to_process").type == "numeric"

    def test_created_time_group(self):
        """dimension_group with 11 timeframes (raw skipped)."""
        model = self.graph.get_model("order_items")
        assert model.get_dimension("created_date") is not None
        assert model.get_dimension("created_date").granularity == "day"
        assert model.get_dimension("created_week") is not None
        assert model.get_dimension("created_month") is not None
        assert model.get_dimension("created_year") is not None
        assert model.get_dimension("created_time") is not None
        assert model.get_dimension("created_time").granularity == "hour"

    def test_shipped_time_group(self):
        model = self.graph.get_model("order_items")
        assert model.get_dimension("shipped_date") is not None
        assert model.get_dimension("shipped_week") is not None
        assert model.get_dimension("shipped_month") is not None

    def test_measure_count_distinct(self):
        model = self.graph.get_model("order_items")
        assert model.get_metric("count").agg == "count_distinct"
        assert model.get_metric("order_count").agg == "count_distinct"

    def test_measure_median(self):
        model = self.graph.get_model("order_items")
        assert model.get_metric("median_sale_price").agg == "median"

    def test_measure_sum(self):
        model = self.graph.get_model("order_items")
        assert model.get_metric("total_sale_price").agg == "sum"
        assert model.get_metric("total_gross_margin").agg == "sum"

    def test_derived_measures(self):
        """type:number measures become derived."""
        model = self.graph.get_model("order_items")
        pct = model.get_metric("total_gross_margin_percentage")
        assert pct is not None
        assert pct.type == "derived"
        assert pct.sql is not None

        avg_spend = model.get_metric("average_spend_per_user")
        assert avg_spend is not None
        assert avg_spend.type == "derived"

    def test_first_purchase_count_distinct(self):
        model = self.graph.get_model("order_items")
        fpc = model.get_metric("first_purchase_count")
        assert fpc is not None
        assert fpc.agg == "count_distinct"

    def test_dimension_reference_resolution(self):
        """gross_margin references sale_price, should have resolved SQL."""
        model = self.graph.get_model("order_items")
        gm = model.get_dimension("gross_margin")
        assert gm is not None
        assert gm.sql is not None
        # Should contain resolved sale_price SQL ({model}.sale_price)
        assert "{model}" in gm.sql


# =============================================================================
# BIGQUERY THELOOK SESSIONS
# Source: looker/bq_thelook
# Native derived tables (explore_source), column mappings, derived_column,
# tier dimension, set blocks, block-style filters on measures, compact syntax
# =============================================================================


class TestBQTheLookSessions:
    """Tests for the BigQuery TheLook sessions fixture."""

    @pytest.fixture(autouse=True)
    def setup(self):
        adapter = LookMLAdapter()
        self.graph = adapter.parse(FIXTURES_DIR / "realworld_bq_thelook_sessions.view.lkml")

    def test_parses_all_views(self):
        expected = {"event_sessions", "user_order_facts", "bq_thelook_users", "bq_thelook_order_items"}
        assert expected == set(self.graph.models.keys())

    def test_event_sessions_is_derived_table(self):
        model = self.graph.get_model("event_sessions")
        assert model.table is None
        assert model.sql is not None

    def test_event_sessions_dimensions(self):
        model = self.graph.get_model("event_sessions")
        assert model.primary_key == "session_id"
        assert model.get_dimension("session_id") is not None
        assert model.get_dimension("event_types") is not None
        assert model.get_dimension("user_id") is not None
        assert model.get_dimension("session_sequence").type == "numeric"

    def test_event_sessions_tier_dimension(self):
        model = self.graph.get_model("event_sessions")
        tier = model.get_dimension("session_length_tiered")
        assert tier is not None
        assert tier.type == "categorical"  # tier -> categorical

    def test_event_sessions_time_group(self):
        model = self.graph.get_model("event_sessions")
        assert model.get_dimension("session_date") is not None
        assert model.get_dimension("session_date").type == "time"

    def test_event_sessions_filtered_measures(self):
        """Block-style filters: { field: x value: y }."""
        model = self.graph.get_model("event_sessions")
        cart = model.get_metric("count_sessions_with_cart")
        assert cart is not None
        assert cart.agg == "count"
        assert cart.filters is not None
        assert len(cart.filters) == 1
        assert "Cart" in cart.filters[0]

        purchases = model.get_metric("count_sessions_with_purchases")
        assert purchases is not None
        assert purchases.filters is not None
        assert "Purchase" in purchases.filters[0]

    def test_event_sessions_basic_measure(self):
        model = self.graph.get_model("event_sessions")
        assert model.get_metric("count_sessions").agg == "count"

    def test_user_order_facts_derived_table(self):
        model = self.graph.get_model("user_order_facts")
        assert model.table is None
        assert model.sql is not None

    def test_user_order_facts_dimensions(self):
        model = self.graph.get_model("user_order_facts")
        assert model.get_dimension("user_id") is not None
        assert model.get_dimension("lifetime_revenue").type == "numeric"
        assert model.get_dimension("lifetime_number_of_orders").type == "numeric"
        assert model.get_dimension("lifetime_product_categories") is not None
        assert model.get_dimension("lifetime_brands") is not None

    def test_bq_thelook_users_compact_syntax(self):
        """Views with compact inline dimension syntax."""
        model = self.graph.get_model("bq_thelook_users")
        assert model.table == "thelook_web_analytics.users"
        assert model.primary_key == "id"
        assert model.get_dimension("age").type == "numeric"
        assert model.get_dimension("city") is not None
        assert model.get_dimension("email") is not None
        assert model.get_dimension("created_date") is not None
        assert model.get_metric("count").agg == "count"

    def test_bq_thelook_order_items(self):
        model = self.graph.get_model("bq_thelook_order_items")
        assert model.table == "thelook_web_analytics.order_items"
        assert model.get_dimension("sale_price").type == "numeric"
        assert model.get_metric("total_revenue").agg == "sum"
        assert model.get_metric("order_count").agg == "count_distinct"


# =============================================================================
# SNOWFLAKE ACS GEO MAP
# Source: llooker/datablocks-acs
# Derived table with persist_for, type:location, map_layer_name, link blocks,
# suggest_persist_for, drill_fields on dimensions, group_label, value_format_name
# =============================================================================


class TestSnowflakeGeo:
    """Tests for the Snowflake ACS geo map fixture."""

    @pytest.fixture(autouse=True)
    def setup(self):
        adapter = LookMLAdapter()
        self.graph = adapter.parse(FIXTURES_DIR / "realworld_snowflake_geo.view.lkml")

    def test_parses_single_view(self):
        assert "sf_logrecno_bg_map" in self.graph.models
        assert len(self.graph.models) == 1

    def test_derived_table_with_complex_sql(self):
        model = self.graph.get_model("sf_logrecno_bg_map")
        assert model.table is None
        assert model.sql is not None
        assert "SELECT" in model.sql.upper()
        assert "JOIN" in model.sql.upper()
        assert "GROUP BY" in model.sql.upper()

    def test_primary_key(self):
        model = self.graph.get_model("sf_logrecno_bg_map")
        assert model.primary_key == "row_id"

    def test_dimension_count(self):
        model = self.graph.get_model("sf_logrecno_bg_map")
        assert len(model.dimensions) == 13

    def test_location_dimension_parsed(self):
        """type:location should be parsed (currently as categorical)."""
        model = self.graph.get_model("sf_logrecno_bg_map")
        loc = model.get_dimension("block_group_centroid")
        assert loc is not None
        # The adapter maps location to categorical since there's no special location type
        assert loc.type == "categorical"

    def test_numeric_lat_lng_dimensions(self):
        model = self.graph.get_model("sf_logrecno_bg_map")
        assert model.get_dimension("latitude").type == "numeric"
        assert model.get_dimension("longitude").type == "numeric"

    def test_string_dimensions(self):
        model = self.graph.get_model("sf_logrecno_bg_map")
        for dim_name in ["stusab", "state_name", "county_name", "tract", "blkgrp"]:
            dim = model.get_dimension(dim_name)
            assert dim is not None, f"Missing dimension: {dim_name}"

    def test_measures(self):
        model = self.graph.get_model("sf_logrecno_bg_map")
        assert model.get_metric("sq_miles_land").agg == "sum"
        assert model.get_metric("sq_miles_water").agg == "sum"
        assert model.get_metric("count").agg == "count"

    def test_derived_table_sql_contains_snowflake_functions(self):
        """SQL should contain Snowflake-specific functions."""
        model = self.graph.get_model("sf_logrecno_bg_map")
        assert "SPLIT_PART" in model.sql.upper()
        assert "COALESCE" in model.sql.upper()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
