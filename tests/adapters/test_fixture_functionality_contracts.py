from __future__ import annotations

from collections import Counter
from functools import cache
from pathlib import Path

import duckdb
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
from sidemantic.sql.generator import SQLGenerator
from tests.adapters.test_added_fixture_coverage import (
    _assert_compiled_query_contract,
    _assert_execution_result_contract,
    _materialize_execution_table,
    _pick_execution_query,
    _prepare_graph_for_execution,
)

ALLOWED_RELATIONSHIP_TYPES = {"many_to_one", "one_to_many", "one_to_one", "many_to_many"}

ADAPTER_FIXTURE_ROOTS = [
    ("atscale_sml", AtScaleSMLAdapter, {".yml", ".yaml"}),
    ("atscale_sml_kitchen_sink", AtScaleSMLAdapter, {".yml", ".yaml"}),
    ("bsl", BSLAdapter, {".yml", ".yaml"}),
    ("cube", CubeAdapter, {".yml", ".yaml"}),
    ("gooddata", GoodDataAdapter, {".json"}),
    ("hex", HexAdapter, {".yml", ".yaml"}),
    ("holistics", HolisticsAdapter, {".aml"}),
    ("holistics_kitchen_sink", HolisticsAdapter, {".aml"}),
    ("lookml", LookMLAdapter, {".lkml"}),
    ("malloy", MalloyAdapter, {".malloy"}),
    ("metricflow", MetricFlowAdapter, {".yml", ".yaml"}),
    ("omni", OmniAdapter, {".yml", ".yaml"}),
    ("osi", OSIAdapter, {".yml", ".yaml"}),
    ("rill", RillAdapter, {".yml", ".yaml"}),
    ("snowflake", SnowflakeAdapter, {".yml", ".yaml"}),
    ("superset", SupersetAdapter, {".yml", ".yaml"}),
    ("thoughtspot", ThoughtSpotAdapter, {".tml"}),
]

EXPECTED_PARSE_FAILURES = {
    "tests/fixtures/gooddata/ecommerce_demo_analytics.json": GoodDataParseError,
    "tests/fixtures/gooddata/sdk_declarative_analytics_model.json": GoodDataParseError,
    "tests/fixtures/gooddata/sdk_declarative_ldm.json": ValidationError,
    "tests/fixtures/gooddata/sdk_declarative_ldm_with_sql_dataset.json": ValidationError,
    "tests/fixtures/metricflow/sub_daily_grain_to_date_hour.yml": ValidationError,
    "tests/fixtures/metricflow/sub_daily_millisecond.yml": ValidationError,
}

EXPECTED_COMPILE_FAILURES = {}

EXPECTED_EMPTY_GRAPH_FIXTURES = {
    "tests/fixtures/atscale_sml/catalog.yml",
    "tests/fixtures/atscale_sml/connection_demo.yml",
    "tests/fixtures/atscale_sml/metric_avg_order_value.yml",
    "tests/fixtures/atscale_sml/model_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/atscale.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/connection_core.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/metric_avg_order_value.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/model_internet_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/model_orders.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/model_returns.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/row_security_country.yml",
    "tests/fixtures/cube/rbac_views.yaml",
    "tests/fixtures/holistics/ecommerce.dataset.aml",
    "tests/fixtures/holistics/relationships.aml",
    "tests/fixtures/holistics_kitchen_sink/constants.aml",
    "tests/fixtures/holistics_kitchen_sink/extensions.aml",
    "tests/fixtures/holistics_kitchen_sink/kitchen_sink.dataset.aml",
    "tests/fixtures/holistics_kitchen_sink/relationships.aml",
    "tests/fixtures/holistics_kitchen_sink/transactions.dataset.aml",
    "tests/fixtures/lookml/ecommerce_explores.lkml",
    "tests/fixtures/lookml/kitchen_sink_explores.lkml",
    "tests/fixtures/lookml/lkml_model_all_fields.model.lkml",
    "tests/fixtures/lookml/lkml_parameter_join.model.lkml",
    "tests/fixtures/lookml/orders.explore.lkml",
    "tests/fixtures/lookml/pylookml_aggregate_tables.model.lkml",
    "tests/fixtures/lookml/pylookml_manifest.lkml",
    "tests/fixtures/lookml/pylookml_sql_preamble.view.lkml",
    "tests/fixtures/lookml/segment_attribution_manifest.lkml",
    "tests/fixtures/lookml/segment_attribution_model.model.lkml",
    "tests/fixtures/omni/estore/model.yaml",
    "tests/fixtures/omni/estore/relationships.yaml",
    "tests/fixtures/omni/model.yaml",
    "tests/fixtures/rill/bids_canvas.yaml",
    "tests/fixtures/rill/bids_explore.yaml",
    "tests/fixtures/rill/nyc_trips_dashboard.yaml",
    "tests/fixtures/thoughtspot/tpch_liveboard.liveboard.tml",
}

EXPECTED_LOW_SIGNAL_FIXTURES = {
    "tests/fixtures/atscale_sml/dataset_dim_customers.yml",
    "tests/fixtures/atscale_sml/dataset_dim_regions.yml",
    "tests/fixtures/atscale_sml/dataset_fact_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_customers.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_date.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_dates.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_geography.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_product.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_dim_promos.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_fact_internet_sales.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_fact_orders.yml",
    "tests/fixtures/atscale_sml_kitchen_sink/dataset_fact_returns.yml",
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

NON_EXECUTION_REASON_ALLOWED_ADAPTERS = {
    "metadata_only_no_models": {
        "AtScaleSMLAdapter",
        "CubeAdapter",
        "HolisticsAdapter",
        "LookMLAdapter",
        "MetricFlowAdapter",
        "OmniAdapter",
        "RillAdapter",
        "ThoughtSpotAdapter",
    },
    "source_fragments_without_fields": {"AtScaleSMLAdapter", "MalloyAdapter"},
    "semantic_only_no_sources": {"AtScaleSMLAdapter", "LookMLAdapter", "MalloyAdapter", "OmniAdapter"},
    "complex_or_nonportable_sql_fields": {"LookMLAdapter"},
}


def _discover_fixture_cases() -> list[tuple[type, str]]:
    root = Path("tests/fixtures")
    cases = []
    for fixture_dir, adapter_cls, extensions in ADAPTER_FIXTURE_ROOTS:
        fixture_root = root / fixture_dir
        if not fixture_root.exists():
            continue
        for fixture_path in sorted(p for p in fixture_root.rglob("*") if p.is_file() and p.suffix in extensions):
            cases.append((adapter_cls, fixture_path.as_posix()))
    return cases


ALL_FIXTURE_CASES = _discover_fixture_cases()
PARSEABLE_FIXTURE_CASES = [
    (adapter_cls, fixture_path)
    for adapter_cls, fixture_path in ALL_FIXTURE_CASES
    if fixture_path not in EXPECTED_PARSE_FAILURES
]


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
            metric = simple_metrics[0]
            return {
                "metrics": [f"{model.name}.{metric.name}"],
                "dimensions": [],
                "query_kind": "metric",
                "field_name": metric.name,
                "metric_agg": metric.agg,
            }

        simple_dimensions = [
            dimension for dimension in model.dimensions if dimension.name and "." not in dimension.name
        ]
        if simple_dimensions:
            dimension = simple_dimensions[0]
            return {
                "metrics": [],
                "dimensions": [f"{model.name}.{dimension.name}"],
                "query_kind": "dimension",
                "field_name": dimension.name,
            }
    return None


def _assert_graph_contracts(fixture_path: str, graph) -> None:
    assert graph is not None, f"{fixture_path}: parse returned None"

    stats = {
        "models": len(graph.models),
        "model_dimensions": sum(len(model.dimensions) for model in graph.models.values()),
        "model_metrics": sum(len(model.metrics) for model in graph.models.values()),
        "model_relationships": sum(len(model.relationships) for model in graph.models.values()),
        "model_segments": sum(len(model.segments) for model in graph.models.values()),
        "graph_metrics": len(graph.metrics),
    }

    if fixture_path in EXPECTED_EMPTY_GRAPH_FIXTURES:
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
    if fixture_path not in EXPECTED_LOW_SIGNAL_FIXTURES:
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


def _classify_non_execution_fixture(graph) -> str:
    if not graph.models:
        return "metadata_only_no_models"

    models = list(graph.models.values())
    if not any((model.table or model.sql) for model in models):
        return "semantic_only_no_sources"

    if all(len(model.dimensions) == 0 and len(model.metrics) == 0 for model in models):
        return "source_fragments_without_fields"

    all_fields = [field for model in models for field in [*model.metrics, *model.dimensions]]
    if all_fields and all(
        field.sql_expr and ("${" in field.sql_expr or "{%" in field.sql_expr) for field in all_fields
    ):
        return "templated_fields_only"

    return "complex_or_nonportable_sql_fields"


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_path"),
    ALL_FIXTURE_CASES,
    ids=[fixture_path for _, fixture_path in ALL_FIXTURE_CASES],
)
def test_fixture_parse_and_graph_contracts(adapter_cls, fixture_path):
    expected_exception = EXPECTED_PARSE_FAILURES.get(fixture_path)
    if expected_exception:
        with pytest.raises(expected_exception):
            adapter_cls().parse(fixture_path)
        return

    graph = _parse_graph(adapter_cls, fixture_path)
    _assert_graph_contracts(fixture_path, graph)


def test_fixture_query_compilation_coverage():
    compile_failures = {}
    compiled_queries = 0
    attempted_queries = 0

    for adapter_cls, fixture_path in PARSEABLE_FIXTURE_CASES:
        graph = _parse_graph(adapter_cls, fixture_path)
        query = _pick_compile_query(graph)
        if not query:
            continue

        attempted_queries += 1
        try:
            sql = SQLGenerator(graph).generate(
                metrics=query["metrics"],
                dimensions=query["dimensions"],
                limit=5,
            )
            _assert_compiled_query_contract(sql, query, fixture_path)
            compiled_queries += 1
        except Exception as exc:
            compile_failures[fixture_path] = exc

    assert attempted_queries > 0, "No fixture produced a compileable query candidate"

    expected_failure_paths = set(EXPECTED_COMPILE_FAILURES)
    actual_failure_paths = set(compile_failures)
    assert actual_failure_paths == expected_failure_paths, (
        "Unexpected fixture compile failures.\n"
        f"Expected: {sorted(expected_failure_paths)}\n"
        f"Actual: {sorted(actual_failure_paths)}"
    )

    for fixture_path, expected_exception in EXPECTED_COMPILE_FAILURES.items():
        assert isinstance(compile_failures[fixture_path], expected_exception), (
            f"{fixture_path} raised {type(compile_failures[fixture_path]).__name__}, "
            f"expected {expected_exception.__name__}"
        )

    minimum_compile_attempts = 180
    assert attempted_queries >= minimum_compile_attempts, (
        f"Expected at least {minimum_compile_attempts} fixture compile candidates, got {attempted_queries}"
    )

    minimum_compile_successes = minimum_compile_attempts
    assert compiled_queries >= minimum_compile_successes, (
        f"Expected at least {minimum_compile_successes} compiled fixture queries, got {compiled_queries} "
        f"(attempted {attempted_queries})"
    )


def test_non_executable_fixture_reason_contracts():
    reason_counts: Counter[str] = Counter()

    for adapter_cls, fixture_path in PARSEABLE_FIXTURE_CASES:
        graph = _parse_graph(adapter_cls, fixture_path)
        if _pick_execution_query(graph):
            continue

        reason = _classify_non_execution_fixture(graph)
        reason_counts[reason] += 1

        if reason == "templated_fields_only":
            # No current fixtures should be blocked solely by templating after candidate-selection hardening.
            raise AssertionError(f"{fixture_path}: expected at least one non-templated fallback execution candidate")

        expected_adapters = NON_EXECUTION_REASON_ALLOWED_ADAPTERS[reason]
        assert adapter_cls.__name__ in expected_adapters, (
            f"{fixture_path}: reason={reason} unexpected for adapter {adapter_cls.__name__}; "
            f"expected adapters={sorted(expected_adapters)}"
        )

        if reason == "complex_or_nonportable_sql_fields":
            # Ensure complex cases still have compile-time functionality coverage.
            assert _pick_compile_query(graph) is not None, (
                f"{fixture_path}: non-executable complex fixture should still have a compile candidate"
            )


def test_fixture_query_execution_coverage():
    execution_failures = {}
    attempted_executions = 0
    executed_queries = 0
    adapters_with_execution = set()

    for adapter_cls, fixture_path in PARSEABLE_FIXTURE_CASES:
        graph = _parse_graph(adapter_cls, fixture_path)
        query_spec = _pick_execution_query(graph)
        if not query_spec:
            continue

        attempted_executions += 1
        adapters_with_execution.add(adapter_cls.__name__)
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

            assert result.description is not None
            column_names = [column[0] for column in result.description]
            assert column_names == [query_spec["field_name"]], (
                f"{fixture_path}: expected output column {query_spec['field_name']}, got {column_names}"
            )
            _assert_execution_result_contract(query_spec, rows, fixture_path)

            executed_queries += 1
        except Exception as exc:
            execution_failures[fixture_path] = exc
        finally:
            conn.close()

    assert attempted_executions > 0, "No fixture produced an executable query candidate"

    minimum_execution_attempts = 210
    assert attempted_executions >= minimum_execution_attempts, (
        f"Expected at least {minimum_execution_attempts} executable fixture queries, got {attempted_executions}"
    )

    total_adapters = len({adapter_cls.__name__ for adapter_cls, _ in PARSEABLE_FIXTURE_CASES})
    minimum_adapter_execution_coverage = 15
    assert len(adapters_with_execution) >= minimum_adapter_execution_coverage, (
        f"Expected execution coverage in at least {minimum_adapter_execution_coverage}/{total_adapters} adapters, "
        f"got {len(adapters_with_execution)}"
    )

    assert not execution_failures, "Unexpected execution failures in fixture contracts:\n" + "\n".join(
        f"{path}: {type(exc).__name__}({exc})" for path, exc in sorted(execution_failures.items())
    )
    assert executed_queries == attempted_executions
