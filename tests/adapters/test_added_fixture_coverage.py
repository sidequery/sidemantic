import re
from copy import deepcopy
from datetime import date
from functools import cache
from numbers import Number
from pathlib import Path

import duckdb
import pytest
import sqlglot
from pydantic import ValidationError
from sqlglot import exp

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
from sidemantic.sql.generator import SQLGenerator

ALLOWED_RELATIONSHIP_TYPES = {"many_to_one", "one_to_many", "one_to_one", "many_to_many"}
UNQUOTED_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

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

ADDED_EXPECTED_EMPTY_GRAPH_FIXTURES = {
    "tests/fixtures/atscale_sml_kitchen_sink/model_internet_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/row_security_country.yml",
    "tests/fixtures/cube/rbac_views.yaml",
    "tests/fixtures/holistics_kitchen_sink/transactions.dataset.aml",
    "tests/fixtures/lookml/lkml_model_all_fields.model.lkml",
    "tests/fixtures/lookml/lkml_parameter_join.model.lkml",
    "tests/fixtures/lookml/pylookml_aggregate_tables.model.lkml",
    "tests/fixtures/lookml/pylookml_manifest.lkml",
    "tests/fixtures/lookml/pylookml_sql_preamble.view.lkml",
    "tests/fixtures/lookml/segment_attribution_manifest.lkml",
    "tests/fixtures/lookml/segment_attribution_model.model.lkml",
    "tests/fixtures/omni/estore/model.yaml",
    "tests/fixtures/omni/estore/relationships.yaml",
    "tests/fixtures/rill/bids_canvas.yaml",
    "tests/fixtures/rill/bids_explore.yaml",
    "tests/fixtures/rill/nyc_trips_dashboard.yaml",
    "tests/fixtures/thoughtspot/tpch_liveboard.liveboard.tml",
}

ADDED_EXPECTED_LOW_SIGNAL_FIXTURES = {
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_date.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_geography.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_product.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_fact_internet_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_user_country_mapping.yml",
    "tests/fixtures/lookml/node_lookml_refinement_sequencing.model.lkml",
    "tests/fixtures/malloy/bigquery_jobs_config.malloy",
    "tests/fixtures/malloy/ecommerce_malloydata.malloy",
    "tests/fixtures/malloy/flights_cube.malloy",
    "tests/fixtures/malloy/ga4_config.malloy",
    "tests/fixtures/omni/estore/topics/Customers.topic.yaml",
    "tests/fixtures/omni/estore/topics/Events.topic.yaml",
    "tests/fixtures/omni/estore/topics/sessions.topic.yaml",
}

ADDED_EXPECTED_NO_COMPILE_QUERY_FIXTURES = {
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_date.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_geography.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_product.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_fact_internet_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_user_country_mapping.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dimension_date.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dimension_geography.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dimension_product.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/metric_orderquantity.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/metric_salesamount.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/model_internet_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/row_security_country.yml",
    "tests/fixtures/cube/rbac_views.yaml",
    "tests/fixtures/holistics_kitchen_sink/role_playing.aml",
    "tests/fixtures/holistics_kitchen_sink/transactions.dataset.aml",
    "tests/fixtures/lookml/ga360_ga_block.view.lkml",
    "tests/fixtures/lookml/lkml_model_all_fields.model.lkml",
    "tests/fixtures/lookml/lkml_parameter_join.model.lkml",
    "tests/fixtures/lookml/node_lookml_refinement_merging.model.lkml",
    "tests/fixtures/lookml/node_lookml_refinement_sequencing.model.lkml",
    "tests/fixtures/lookml/pylookml_aggregate_tables.model.lkml",
    "tests/fixtures/lookml/pylookml_manifest.lkml",
    "tests/fixtures/lookml/pylookml_sql_preamble.view.lkml",
    "tests/fixtures/lookml/segment_attribution_manifest.lkml",
    "tests/fixtures/lookml/segment_attribution_model.model.lkml",
    "tests/fixtures/malloy/bigquery_jobs.malloy",
    "tests/fixtures/malloy/bigquery_jobs_config.malloy",
    "tests/fixtures/malloy/ecommerce_malloydata.malloy",
    "tests/fixtures/malloy/flights_cube.malloy",
    "tests/fixtures/malloy/ga4.malloy",
    "tests/fixtures/malloy/ga4_config.malloy",
    "tests/fixtures/metricflow/scd_metrics.yaml",
    "tests/fixtures/omni/estore/model.yaml",
    "tests/fixtures/omni/estore/relationships.yaml",
    "tests/fixtures/omni/estore/snapshots/snap_user_rfm.view.yaml",
    "tests/fixtures/omni/estore/topics/Customers.topic.yaml",
    "tests/fixtures/omni/estore/topics/Events.topic.yaml",
    "tests/fixtures/omni/estore/topics/sessions.topic.yaml",
    "tests/fixtures/omni/estore/views/dim_categories.view.yaml",
    "tests/fixtures/omni/estore/views/dim_products.view.yaml",
    "tests/fixtures/omni/estore/views/dim_user_rfm.view.yaml",
    "tests/fixtures/omni/estore/views/dim_users.view.yaml",
    "tests/fixtures/omni/estore/views/fct_events.view.yaml",
    "tests/fixtures/omni/estore/views/fct_sessions.view.yaml",
    "tests/fixtures/rill/bids_canvas.yaml",
    "tests/fixtures/rill/bids_explore.yaml",
    "tests/fixtures/rill/nyc_trips_dashboard.yaml",
    "tests/fixtures/thoughtspot/tpch_liveboard.liveboard.tml",
}

ADDED_EXPECTED_NO_EXECUTION_QUERY_FIXTURES = {
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_date.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_geography.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_product.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_fact_internet_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_user_country_mapping.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/model_internet_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/row_security_country.yml",
    "tests/fixtures/cube/rbac_views.yaml",
    "tests/fixtures/holistics_kitchen_sink/transactions.dataset.aml",
    "tests/fixtures/lookml/lkml_model_all_fields.model.lkml",
    "tests/fixtures/lookml/lkml_parameter_join.model.lkml",
    "tests/fixtures/lookml/lkml_view_all_fields.view.lkml",
    "tests/fixtures/lookml/node_lookml_refinement_sequencing.model.lkml",
    "tests/fixtures/lookml/pylookml_aggregate_tables.model.lkml",
    "tests/fixtures/lookml/pylookml_manifest.lkml",
    "tests/fixtures/lookml/pylookml_sql_preamble.view.lkml",
    "tests/fixtures/lookml/segment_attribution_conversion.view.lkml",
    "tests/fixtures/lookml/segment_attribution_manifest.lkml",
    "tests/fixtures/lookml/segment_attribution_model.model.lkml",
    "tests/fixtures/malloy/bigquery_jobs_config.malloy",
    "tests/fixtures/malloy/ecommerce_malloydata.malloy",
    "tests/fixtures/malloy/flights_cube.malloy",
    "tests/fixtures/malloy/ga4_config.malloy",
    "tests/fixtures/metricflow/scd_metrics.yaml",
    "tests/fixtures/omni/estore/model.yaml",
    "tests/fixtures/omni/estore/relationships.yaml",
    "tests/fixtures/omni/estore/topics/Customers.topic.yaml",
    "tests/fixtures/omni/estore/topics/Events.topic.yaml",
    "tests/fixtures/omni/estore/topics/sessions.topic.yaml",
    "tests/fixtures/rill/bids_canvas.yaml",
    "tests/fixtures/rill/bids_explore.yaml",
    "tests/fixtures/rill/nyc_trips_dashboard.yaml",
    "tests/fixtures/thoughtspot/tpch_liveboard.liveboard.tml",
    "tests/fixtures/thoughtspot/tpch_worksheet.worksheet.tml",
}


def _graph_stats(graph) -> dict[str, int]:
    return {
        "models": len(graph.models),
        "model_dimensions": sum(len(model.dimensions) for model in graph.models.values()),
        "model_metrics": sum(len(model.metrics) for model in graph.models.values()),
        "model_relationships": sum(len(model.relationships) for model in graph.models.values()),
        "model_segments": sum(len(model.segments) for model in graph.models.values()),
        "graph_metrics": len(graph.metrics),
    }


def _assert_graph_integrity(fixture_path: str, graph) -> None:
    stats = _graph_stats(graph)

    if fixture_path in ADDED_EXPECTED_EMPTY_GRAPH_FIXTURES:
        assert stats == {
            "models": 0,
            "model_dimensions": 0,
            "model_metrics": 0,
            "model_relationships": 0,
            "model_segments": 0,
            "graph_metrics": 0,
        }, f"{fixture_path}: expected empty graph, got {stats}"
        return

    assert stats["models"] + stats["graph_metrics"] > 0, (
        f"{fixture_path}: parser produced no semantic entities ({stats})"
    )

    semantic_signal = (
        stats["model_dimensions"]
        + stats["model_metrics"]
        + stats["model_relationships"]
        + stats["model_segments"]
        + stats["graph_metrics"]
    )
    if fixture_path not in ADDED_EXPECTED_LOW_SIGNAL_FIXTURES:
        assert semantic_signal > 0, f"{fixture_path}: expected semantic signal, got only skeletal models ({stats})"

    for model_name, model in graph.models.items():
        assert model_name, f"{fixture_path}: model has empty name"

        dimension_names = [dimension.name for dimension in model.dimensions]
        metric_names = [metric.name for metric in model.metrics]
        segment_names = [segment.name for segment in model.segments]

        assert len(dimension_names) == len(set(dimension_names)), f"{fixture_path}:{model_name} duplicate dimensions"
        assert len(metric_names) == len(set(metric_names)), f"{fixture_path}:{model_name} duplicate metrics"
        assert len(segment_names) == len(set(segment_names)), f"{fixture_path}:{model_name} duplicate segments"

        for relationship in model.relationships:
            assert relationship.type in ALLOWED_RELATIONSHIP_TYPES, (
                f"{fixture_path}:{model_name} invalid relationship type {relationship.type}"
            )


@cache
def _parse_graph(adapter_cls: type, fixture_path: str):
    return adapter_cls().parse(fixture_path)


def _pick_compile_query(graph):
    for model in graph.models.values():
        if not (model.table or model.sql):
            continue
        if "." in model.name:
            continue

        simple_metrics = [
            metric
            for metric in model.metrics
            if metric.name
            and "." not in metric.name
            and metric.agg
            and metric.type not in {"cumulative", "time_comparison", "conversion", "ratio"}
        ]
        if simple_metrics:
            return {"metrics": [f"{model.name}.{simple_metrics[0].name}"], "dimensions": []}

        simple_dimensions = [
            dimension for dimension in model.dimensions if dimension.name and "." not in dimension.name
        ]
        if simple_dimensions:
            return {"metrics": [], "dimensions": [f"{model.name}.{simple_dimensions[0].name}"]}
    return None


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _normalize_identifier(identifier: str, fallback: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_]+", "_", identifier).strip("_")
    if not normalized:
        normalized = fallback
    if normalized[0].isdigit():
        normalized = f"_{normalized}"
    return normalized


def _extract_sql_columns(sql_expr: str | None) -> tuple[set[str], bool]:
    if not sql_expr or "{" in sql_expr:
        return set(), False
    try:
        parsed = sqlglot.parse_one(sql_expr, read="duckdb")
    except Exception:
        return set(), False

    columns = set()
    for column in parsed.find_all(exp.Column):
        if not column.name or column.name == "*":
            return set(), False
        if column.table:
            return set(), False
        columns.add(column.name)
    return columns, True


def _parse_table_reference(table_name: str) -> tuple[str | None, str | None, str] | None:
    if not table_name or "{" in table_name or "(" in table_name:
        return None

    try:
        table = sqlglot.parse_one(table_name, into=exp.Table, read="duckdb")
    except Exception:
        table = None

    if isinstance(table, exp.Table) and table.name:
        catalog = table.catalog or None
        schema = table.db or None
        name = table.name
    else:
        stripped = table_name.strip().strip("`").strip('"')
        if not stripped:
            return None
        parts = [part.strip().strip("`").strip('"') for part in stripped.split(".") if part.strip()]
        if not parts:
            return None
        if len(parts) == 1:
            catalog = None
            schema = None
            name = parts[0]
        elif len(parts) == 2:
            catalog = None
            schema = parts[0]
            name = parts[1]
        else:
            catalog = "__".join(parts[:-2])
            schema = parts[-2]
            name = parts[-1]

    if not name:
        return None
    name_parts = [part for part in [catalog, schema, name] if part is not None]
    if any("/" in part or "\\" in part for part in name_parts):
        return None
    if any(not UNQUOTED_SQL_IDENTIFIER_RE.match(part) for part in name_parts):
        return None
    return catalog, schema, name


def _normalize_table_reference(table_name: str) -> tuple[str | None, str | None, str] | None:
    if not table_name or "{" in table_name or "(" in table_name:
        return None
    stripped = table_name.strip().strip("`").strip('"')
    if not stripped:
        return None
    normalized_name = _normalize_identifier(stripped, fallback="table")
    return None, None, f"exec_{normalized_name}"


def _execution_table_parts(table_name: str) -> tuple[tuple[str | None, str | None, str], bool] | None:
    parsed = _parse_table_reference(table_name)
    if parsed is not None:
        return parsed, False

    normalized = _normalize_table_reference(table_name)
    if normalized is not None:
        return normalized, True
    return None


def _table_parts_to_reference(table_parts: tuple[str | None, str | None, str]) -> str:
    catalog, schema, table = table_parts
    return ".".join(part for part in [catalog, schema, table] if part is not None)


def _execution_model_name(model_name: str) -> tuple[str, bool]:
    if UNQUOTED_SQL_IDENTIFIER_RE.match(model_name):
        return model_name, False
    normalized = _normalize_identifier(model_name, fallback="model")
    return f"exec_model_{normalized}", True


def _execution_table_for_model(model) -> tuple[tuple[str | None, str | None, str], bool]:
    if model.table:
        table_info = _execution_table_parts(model.table)
        if table_info is not None:
            return table_info

    normalized = _normalize_identifier(model.name, fallback="table")
    return (None, None, f"exec_{normalized}"), True


def _pick_execution_query(graph):
    for model in graph.models.values():
        if any(field.sql_expr and "${" in field.sql_expr for field in [*model.metrics, *model.dimensions]):
            continue
        execution_table_parts, requires_table_override = _execution_table_for_model(model)
        execution_model_name, requires_model_override = _execution_model_name(model.name)
        requires_sql_override = bool(model.sql)

        primary_keys = model.primary_key if isinstance(model.primary_key, list) else [model.primary_key]
        if any(not isinstance(pk, str) or not pk.strip() for pk in primary_keys):
            continue
        base_column_types = {pk: "INTEGER" for pk in primary_keys}

        def pick_metric_candidate():
            for metric in model.metrics:
                if not metric.name or "." in metric.name:
                    continue
                if not metric.agg or metric.type in {"cumulative", "time_comparison", "conversion", "ratio"}:
                    continue
                if metric.agg not in {"count", "count_distinct"} and metric.sql_expr and "'" in metric.sql_expr:
                    continue

                column_types = dict(base_column_types)
                if metric.agg == "count" and (metric.sql is None or metric.sql.strip() in {"", "*"}):
                    if not column_types:
                        column_types["id"] = "INTEGER"
                    return {
                        "model_name": model.name,
                        "execution_model_name": execution_model_name,
                        "query_kind": "metric",
                        "field_name": metric.name,
                        "metric_agg": metric.agg,
                        "table_name": model.table,
                        "execution_table_parts": execution_table_parts,
                        "execution_table_ref": _table_parts_to_reference(execution_table_parts),
                        "requires_table_override": requires_table_override,
                        "requires_model_override": requires_model_override,
                        "requires_sql_override": requires_sql_override,
                        "metrics": [f"{execution_model_name}.{metric.name}"],
                        "dimensions": [],
                        "column_types": column_types,
                    }

                metric_columns, ok = _extract_sql_columns(metric.sql_expr)
                if not ok:
                    continue
                for column in metric_columns or {"id"}:
                    column_types.setdefault(column, "DOUBLE")
                return {
                    "model_name": model.name,
                    "execution_model_name": execution_model_name,
                    "query_kind": "metric",
                    "field_name": metric.name,
                    "metric_agg": metric.agg,
                    "table_name": model.table,
                    "execution_table_parts": execution_table_parts,
                    "execution_table_ref": _table_parts_to_reference(execution_table_parts),
                    "requires_table_override": requires_table_override,
                    "requires_model_override": requires_model_override,
                    "requires_sql_override": requires_sql_override,
                    "metrics": [f"{execution_model_name}.{metric.name}"],
                    "dimensions": [],
                    "column_types": column_types,
                }
            return None

        def pick_dimension_candidate():
            for dimension in model.dimensions:
                if not dimension.name or "." in dimension.name:
                    continue
                if dimension.sql_expr and "'" in dimension.sql_expr:
                    continue
                dimension_columns, ok = _extract_sql_columns(dimension.sql_expr)
                if not ok:
                    continue
                column_types = dict(base_column_types)
                if dimension.type == "time":
                    dimension_type = "DATE"
                elif dimension.type == "numeric":
                    dimension_type = "DOUBLE"
                elif dimension.type == "boolean":
                    dimension_type = "BOOLEAN"
                else:
                    dimension_type = "VARCHAR"
                for column in dimension_columns or {"id"}:
                    column_types.setdefault(column, dimension_type)
                return {
                    "model_name": model.name,
                    "execution_model_name": execution_model_name,
                    "query_kind": "dimension",
                    "field_name": dimension.name,
                    "dimension_type": dimension.type,
                    "table_name": model.table,
                    "execution_table_parts": execution_table_parts,
                    "execution_table_ref": _table_parts_to_reference(execution_table_parts),
                    "requires_table_override": requires_table_override,
                    "requires_model_override": requires_model_override,
                    "requires_sql_override": requires_sql_override,
                    "metrics": [],
                    "dimensions": [f"{execution_model_name}.{dimension.name}"],
                    "column_types": column_types,
                }
            return None

        if requires_sql_override:
            candidate = pick_dimension_candidate() or pick_metric_candidate()
        else:
            candidate = pick_metric_candidate() or pick_dimension_candidate()
        if candidate:
            return candidate
    return None


def _materialize_execution_table(conn: duckdb.DuckDBPyConnection, query_spec: dict) -> None:
    table_parts = query_spec.get("execution_table_parts") or _parse_table_reference(query_spec["table_name"])
    assert table_parts is not None, f"Unsupported execution table reference: {query_spec['table_name']}"
    catalog_name, schema_name, table_name = table_parts

    if catalog_name:
        quoted_catalog = _quote_identifier(catalog_name)
        conn.execute(f"ATTACH ':memory:' AS {quoted_catalog}")
        if schema_name:
            quoted_schema = _quote_identifier(schema_name)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_catalog}.{quoted_schema}")
            table_ref = f"{quoted_catalog}.{quoted_schema}.{_quote_identifier(table_name)}"
        else:
            table_ref = f"{quoted_catalog}.{_quote_identifier(table_name)}"
    elif schema_name:
        quoted_schema = _quote_identifier(schema_name)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
        table_ref = f"{quoted_schema}.{_quote_identifier(table_name)}"
    else:
        table_ref = _quote_identifier(table_name)

    column_types = query_spec["column_types"]
    column_defs = ", ".join(
        f"{_quote_identifier(column)} {column_type}" for column, column_type in sorted(column_types.items())
    )
    conn.execute(f"CREATE TABLE {table_ref} ({column_defs})")

    insert_columns = ", ".join(_quote_identifier(column) for column in sorted(column_types))
    values = []
    for column in sorted(column_types):
        column_type = column_types[column]
        if column_type == "DOUBLE":
            values.append("1.0")
        elif column_type == "INTEGER":
            values.append("1")
        elif column_type == "DATE":
            values.append("DATE '2024-01-01'")
        elif column_type == "BOOLEAN":
            values.append("TRUE")
        else:
            values.append("'x'")
    conn.execute(f"INSERT INTO {table_ref} ({insert_columns}) VALUES ({', '.join(values)})")


def _prepare_graph_for_execution(graph, query_spec: dict):
    if not (
        query_spec.get("requires_table_override")
        or query_spec.get("requires_model_override")
        or query_spec.get("requires_sql_override")
    ):
        return graph

    graph_for_query = deepcopy(graph)
    source_model_name = query_spec["model_name"]
    execution_model_name = query_spec["execution_model_name"]

    model = graph_for_query.models[source_model_name]
    if query_spec.get("requires_table_override"):
        model.table = query_spec["execution_table_ref"]
    if query_spec.get("requires_sql_override"):
        model.sql = f"SELECT * FROM {query_spec['execution_table_ref']}"
    if query_spec.get("requires_model_override"):
        existing = graph_for_query.models.get(execution_model_name)
        assert existing is None or execution_model_name == source_model_name, (
            f"Execution model alias collision for {execution_model_name}"
        )
        graph_for_query.models.pop(source_model_name)
        model.name = execution_model_name
        graph_for_query.models[execution_model_name] = model

    return graph_for_query


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_path"),
    ADDED_FIXTURE_CASES,
    ids=[Path(path).name for _, path in ADDED_FIXTURE_CASES],
)
def test_added_fixtures_parse_with_graph_contracts(adapter_cls, fixture_path):
    graph = _parse_graph(adapter_cls, fixture_path)
    assert graph is not None, f"{fixture_path}: parse returned None"
    _assert_graph_integrity(fixture_path, graph)


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_path", "exception_cls"),
    ADDED_FIXTURE_EXPECTED_FAILURE_CASES,
    ids=[Path(path).name for _, path, _ in ADDED_FIXTURE_EXPECTED_FAILURE_CASES],
)
def test_added_fixtures_raise_expected_parse_failures(adapter_cls, fixture_path, exception_cls):
    with pytest.raises(exception_cls):
        adapter_cls().parse(fixture_path)


def test_added_fixtures_generate_sql_for_compile_candidates():
    parseable_cases = [
        (adapter_cls, fixture_path)
        for adapter_cls, fixture_path in ADDED_FIXTURE_CASES
        if fixture_path not in {path for _, path, _ in ADDED_FIXTURE_EXPECTED_FAILURE_CASES}
    ]

    attempted_queries = 0
    compiled_queries = 0
    compile_failures = {}
    no_compile_candidate = []

    for adapter_cls, fixture_path in parseable_cases:
        graph = _parse_graph(adapter_cls, fixture_path)
        query = _pick_compile_query(graph)
        if not query:
            no_compile_candidate.append(fixture_path)
            continue

        attempted_queries += 1
        try:
            sql = SQLGenerator(graph).generate(
                metrics=query["metrics"],
                dimensions=query["dimensions"],
                limit=5,
            )
            assert "SELECT" in sql.upper()
            assert "FROM" in sql.upper()
            compiled_queries += 1
        except Exception as exc:
            compile_failures[fixture_path] = exc

    assert set(no_compile_candidate) == ADDED_EXPECTED_NO_COMPILE_QUERY_FIXTURES, (
        "Added fixture compile-candidate set drifted.\n"
        f"Expected no-candidate fixtures: {sorted(ADDED_EXPECTED_NO_COMPILE_QUERY_FIXTURES)}\n"
        f"Actual no-candidate fixtures: {sorted(no_compile_candidate)}"
    )

    minimum_attempts = max(1, len(parseable_cases) // 2)
    assert attempted_queries >= minimum_attempts, (
        f"Expected at least {minimum_attempts} compileable added fixtures, got {attempted_queries}"
    )
    assert not compile_failures, "Unexpected compile failures in added fixtures:\n" + "\n".join(
        f"{path}: {type(exc).__name__}({exc})" for path, exc in sorted(compile_failures.items())
    )
    assert compiled_queries == attempted_queries


def test_added_fixtures_execute_sql_for_execution_candidates():
    parseable_cases = [
        (adapter_cls, fixture_path)
        for adapter_cls, fixture_path in ADDED_FIXTURE_CASES
        if fixture_path not in {path for _, path, _ in ADDED_FIXTURE_EXPECTED_FAILURE_CASES}
    ]

    attempted_executions = 0
    executed_queries = 0
    execution_failures = {}
    no_execution_candidate = []

    for adapter_cls, fixture_path in parseable_cases:
        graph = _parse_graph(adapter_cls, fixture_path)
        query_spec = _pick_execution_query(graph)
        if not query_spec:
            no_execution_candidate.append(fixture_path)
            continue

        attempted_executions += 1
        conn = duckdb.connect(":memory:")
        try:
            _materialize_execution_table(conn, query_spec)
            graph_for_query = _prepare_graph_for_execution(graph, query_spec)

            sql = SQLGenerator(graph_for_query).generate(
                metrics=query_spec["metrics"],
                dimensions=query_spec["dimensions"],
                limit=5,
                skip_default_time_dimensions=True,
            )
            result = conn.execute(sql)
            rows = result.fetchall()
            assert rows is not None
            assert result.description is not None
            column_names = [column[0] for column in result.description]
            assert column_names == [query_spec["field_name"]], (
                f"{fixture_path}: expected output column {query_spec['field_name']}, got {column_names}"
            )
            assert len(rows) == 1, f"{fixture_path}: expected one result row from synthetic execution, got {len(rows)}"

            value = rows[0][0]
            if query_spec["query_kind"] == "metric":
                metric_agg = query_spec["metric_agg"]
                if metric_agg in {"count", "count_distinct"}:
                    assert value == 1, f"{fixture_path}: expected {metric_agg}=1 from one synthetic row, got {value}"
                elif metric_agg in {"stddev", "variance"}:
                    assert value is None or isinstance(value, Number), (
                        f"{fixture_path}: expected nullable numeric for {metric_agg}, got {type(value).__name__}"
                    )
                else:
                    assert isinstance(value, Number), (
                        f"{fixture_path}: expected numeric metric result for {metric_agg}, got {type(value).__name__}"
                    )
            else:
                dimension_type = query_spec["dimension_type"]
                if dimension_type == "time":
                    assert isinstance(value, (date, Number, str)), (
                        f"{fixture_path}: expected time-like result for time dimension, got {type(value).__name__}"
                    )
                elif dimension_type == "numeric":
                    assert isinstance(value, Number), (
                        f"{fixture_path}: expected numeric result for numeric dimension, got {type(value).__name__}"
                    )
                elif dimension_type == "boolean":
                    assert isinstance(value, bool), (
                        f"{fixture_path}: expected bool result for boolean dimension, got {type(value).__name__}"
                    )
                else:
                    assert isinstance(value, (str, Number, bool, date)), (
                        f"{fixture_path}: expected scalar result for categorical dimension, got {type(value).__name__}"
                    )
            executed_queries += 1
        except Exception as exc:
            execution_failures[fixture_path] = exc
        finally:
            conn.close()

    assert set(no_execution_candidate) == ADDED_EXPECTED_NO_EXECUTION_QUERY_FIXTURES, (
        "Added fixture execution-candidate set drifted.\n"
        f"Expected no-execution fixtures: {sorted(ADDED_EXPECTED_NO_EXECUTION_QUERY_FIXTURES)}\n"
        f"Actual no-execution fixtures: {sorted(no_execution_candidate)}"
    )

    minimum_attempts = max(1, len(parseable_cases) // 4)
    assert attempted_executions >= minimum_attempts, (
        f"Expected at least {minimum_attempts} executable added fixtures, got {attempted_executions}"
    )
    assert not execution_failures, "Unexpected execution failures in added fixtures:\n" + "\n".join(
        f"{path}: {type(exc).__name__}({exc})" for path, exc in sorted(execution_failures.items())
    )
    assert executed_queries == attempted_executions
