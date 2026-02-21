from __future__ import annotations

from functools import cache
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
from sidemantic.sql.generator import SQLGenerator

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
            return {"metrics": [f"{model.name}.{simple_metrics[0].name}"], "dimensions": []}

        simple_dimensions = [
            dimension for dimension in model.dimensions if dimension.name and "." not in dimension.name
        ]
        if simple_dimensions:
            return {"metrics": [], "dimensions": [f"{model.name}.{simple_dimensions[0].name}"]}
    return None


def _assert_graph_contracts(fixture_path: str, graph) -> None:
    assert graph is not None, f"{fixture_path}: parse returned None"

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
            assert "SELECT" in sql.upper()
            assert "FROM" in sql.upper()
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

    minimum_compile_successes = max(1, len(PARSEABLE_FIXTURE_CASES) // 2)
    assert compiled_queries >= minimum_compile_successes, (
        f"Expected at least {minimum_compile_successes} compiled fixture queries, got {compiled_queries} "
        f"(attempted {attempted_queries})"
    )
