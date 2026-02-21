from pathlib import Path

import pytest
from pydantic import ValidationError

from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter
from sidemantic.adapters.bsl import BSLAdapter
from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.gooddata import GoodDataAdapter, GoodDataParseError
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.holistics import HolisticsAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.malloy import MalloyAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.osi import OSIAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.snowflake import SnowflakeAdapter
from sidemantic.adapters.superset import SupersetAdapter
from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter

ADDED_FIXTURE_CASES = [
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_date.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_geography.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_product.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/dataset_fact_internet_sales.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/dataset_user_country_mapping.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/dimension_date.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/dimension_geography.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/dimension_product.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/metric_orderquantity.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/metric_salesamount.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/model_internet_sales.yml"),
    (AtScaleSMLAdapter, "tests/fixtures/atscale_sml_kitchen_sink/row_security_country.yml"),
    (BSLAdapter, "tests/fixtures/bsl/flights.yml"),
    (BSLAdapter, "tests/fixtures/bsl/ga_sessions.yaml"),
    (BSLAdapter, "tests/fixtures/bsl/healthcare.yml"),
    (BSLAdapter, "tests/fixtures/bsl/nyc_taxi.yml"),
    (BSLAdapter, "tests/fixtures/bsl/yaml_example_filter.yaml"),
    (CubeAdapter, "tests/fixtures/cube/case_switch_ownership.yaml"),
    (CubeAdapter, "tests/fixtures/cube/custom_calendar.yml"),
    (CubeAdapter, "tests/fixtures/cube/folders.yml"),
    (CubeAdapter, "tests/fixtures/cube/pre_aggregation_types.yaml"),
    (CubeAdapter, "tests/fixtures/cube/rbac_customers.yaml"),
    (CubeAdapter, "tests/fixtures/cube/rbac_policy_overlap.yaml"),
    (CubeAdapter, "tests/fixtures/cube/rbac_views.yaml"),
    (CubeAdapter, "tests/fixtures/cube/switch_dimension.yml"),
    (CubeAdapter, "tests/fixtures/cube/visitors_geo_subquery.yaml"),
    (GoodDataAdapter, "tests/fixtures/gooddata/ecommerce_demo_ldm.json"),
    (HexAdapter, "tests/fixtures/hex/employees.yml"),
    (HexAdapter, "tests/fixtures/hex/inventory.yml"),
    (HexAdapter, "tests/fixtures/hex/page_views.yml"),
    (HexAdapter, "tests/fixtures/hex/support_tickets.yml"),
    (HolisticsAdapter, "tests/fixtures/holistics_kitchen_sink/events.model.aml"),
    (HolisticsAdapter, "tests/fixtures/holistics_kitchen_sink/imports.aml"),
    (HolisticsAdapter, "tests/fixtures/holistics_kitchen_sink/role_playing.aml"),
    (HolisticsAdapter, "tests/fixtures/holistics_kitchen_sink/transactions.dataset.aml"),
    (HolisticsAdapter, "tests/fixtures/holistics_kitchen_sink/user_sessions.model.aml"),
    (LookMLAdapter, "tests/fixtures/lookml/ga360_ga_block.view.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/lkml2cube_constants.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/lkml_liquid_templating.model.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/lkml_model_all_fields.model.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/lkml_ndt_bind_filters.model.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/lkml_parameter_join.model.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/lkml_view_all_fields.view.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/node_lookml_refinement_merging.model.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/node_lookml_refinement_sequencing.model.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/pylookml_aggregate_tables.model.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/pylookml_manifest.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/pylookml_sql_preamble.view.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/segment_attribution_conversion.view.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/segment_attribution_manifest.lkml"),
    (LookMLAdapter, "tests/fixtures/lookml/segment_attribution_model.model.lkml"),
    (MalloyAdapter, "tests/fixtures/malloy/bigquery_jobs.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/bigquery_jobs_config.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/ecommerce_malloydata.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/flights_cube.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/flights_docs.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/flights_docs_airports.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/ga4.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/ga4_config.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/hackernews.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/hackernews_faang.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/test_persist.malloy"),
    (MalloyAdapter, "tests/fixtures/malloy/the_met.malloy"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/ambiguous_resolution_manifest.yaml"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/cyclic_join_manifest.yaml"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/extended_date_manifest.yaml"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/name_edge_case_manifest.yaml"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/scd_listings.yaml"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/scd_metrics.yaml"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/simple_manifest_buys_source.yaml"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/simple_manifest_metrics.yaml"),
    (MetricFlowAdapter, "tests/fixtures/metricflow/simple_manifest_saved_queries.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/model.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/relationships.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/snapshots/snap_user_rfm.view.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/topics/Customers.topic.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/topics/Events.topic.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/topics/sessions.topic.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/views/dim_categories.view.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/views/dim_products.view.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/views/dim_user_rfm.view.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/views/dim_users.view.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/views/fct_events.view.yaml"),
    (OmniAdapter, "tests/fixtures/omni/estore/views/fct_sessions.view.yaml"),
    (OSIAdapter, "tests/fixtures/osi/mdb_member_knowledge.yaml"),
    (OSIAdapter, "tests/fixtures/osi/mdb_movies.yaml"),
    (OSIAdapter, "tests/fixtures/osi/tpcds_osi_official.yaml"),
    (RillAdapter, "tests/fixtures/rill/ad_bids_advanced.yaml"),
    (RillAdapter, "tests/fixtures/rill/ad_bids_policy.yaml"),
    (RillAdapter, "tests/fixtures/rill/bids_canvas.yaml"),
    (RillAdapter, "tests/fixtures/rill/bids_explore.yaml"),
    (RillAdapter, "tests/fixtures/rill/metrics_annotations.yaml"),
    (RillAdapter, "tests/fixtures/rill/metrics_geospatial.yaml"),
    (RillAdapter, "tests/fixtures/rill/metrics_null_filling.yaml"),
    (RillAdapter, "tests/fixtures/rill/nyc_trips_dashboard.yaml"),
    (RillAdapter, "tests/fixtures/rill/query_log_metrics.yaml"),
    (SnowflakeAdapter, "tests/fixtures/snowflake/customer_loyalty_metrics.yaml"),
    (SnowflakeAdapter, "tests/fixtures/snowflake/ecommerce_analytics.yaml"),
    (SnowflakeAdapter, "tests/fixtures/snowflake/supply_chain.yaml"),
    (SnowflakeAdapter, "tests/fixtures/snowflake/support_tickets.yaml"),
    (SupersetAdapter, "tests/fixtures/superset/birth_france_by_region.yaml"),
    (SupersetAdapter, "tests/fixtures/superset/fcc_new_coder_survey.yaml"),
    (SupersetAdapter, "tests/fixtures/superset/international_sales.yaml"),
    (SupersetAdapter, "tests/fixtures/superset/project_management.yaml"),
    (SupersetAdapter, "tests/fixtures/superset/sales_dashboard.yaml"),
    (SupersetAdapter, "tests/fixtures/superset/usa_birth_names.yaml"),
    (SupersetAdapter, "tests/fixtures/superset/video_game_sales.yaml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_customer.table.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_lineitem.table.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_liveboard.liveboard.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_nation.table.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_orders.table.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_part.table.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_partsupp.table.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_region.table.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_supplier.table.tml"),
    (ThoughtSpotAdapter, "tests/fixtures/thoughtspot/tpch_worksheet.worksheet.tml"),
]

ADDED_FIXTURE_EXPECTED_FAILURE_CASES = [
    (
        GoodDataAdapter,
        "tests/fixtures/gooddata/ecommerce_demo_analytics.json",
        GoodDataParseError,
    ),
    (
        GoodDataAdapter,
        "tests/fixtures/gooddata/sdk_declarative_analytics_model.json",
        GoodDataParseError,
    ),
    (
        GoodDataAdapter,
        "tests/fixtures/gooddata/sdk_declarative_ldm.json",
        ValidationError,
    ),
    (
        GoodDataAdapter,
        "tests/fixtures/gooddata/sdk_declarative_ldm_with_sql_dataset.json",
        ValidationError,
    ),
]


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_path"),
    ADDED_FIXTURE_CASES,
    ids=[Path(path).name for _, path in ADDED_FIXTURE_CASES],
)
def test_added_fixture_parses(adapter_cls, fixture_path):
    graph = adapter_cls().parse(fixture_path)
    assert graph is not None


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_path", "exception_cls"),
    ADDED_FIXTURE_EXPECTED_FAILURE_CASES,
    ids=[Path(path).name for _, path, _ in ADDED_FIXTURE_EXPECTED_FAILURE_CASES],
)
def test_added_fixture_expected_failures(adapter_cls, fixture_path, exception_cls):
    with pytest.raises(exception_cls):
        adapter_cls().parse(fixture_path)
