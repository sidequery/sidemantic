"""Run shared contracts with fallback forbidden and direct Rust helper probes."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType

import duckdb
import pytest

import sidemantic
import sidemantic.core.semantic_graph as semantic_graph_module
import sidemantic.core.semantic_layer as semantic_layer_module
import sidemantic.sql.generator as sql_generator_module
import sidemantic.sql.query_rewriter as query_rewriter_module
import tests.core.test_auto_dimensions as auto_dimension_tests
import tests.core.test_symmetric_aggregates as symmetric_aggregate_tests
import tests.dates.test_integration as date_integration_tests
import tests.joins.test_composite_key_joins as composite_join_tests
import tests.joins.test_many_to_many_joins as many_to_many_join_tests
import tests.joins.test_multi_hop_joins as multi_hop_join_tests
import tests.joins.test_rails_joins as rails_join_tests
import tests.metrics.test_cumulative as cumulative_metric_tests
import tests.metrics.test_default_time_dimension as default_time_dimension_tests
import tests.metrics.test_derived as derived_metric_tests
import tests.metrics.test_filters as metric_filter_tests
import tests.optimizations.test_pre_aggregations as pre_aggregation_tests
import tests.optimizations.test_preagg_recommender as preagg_recommender_tests
import tests.optimizations.test_predicate_pushdown as predicate_pushdown_tests
import tests.queries.test_basic as basic_query_tests
import tests.queries.test_count_distinct_and_segments as count_distinct_segment_tests
import tests.queries.test_sql_rewriter as sql_rewriter_tests
import tests.queries.test_ungrouped_queries as ungrouped_query_tests
import tests.queries.test_view_generation as view_generation_tests
import tests.templates.test_jinja_integration as jinja_integration_tests
import tests.test_catalog as catalog_tests
import tests.test_foreign_key_dimensions as foreign_key_dimension_tests
import tests.test_hierarchies as hierarchy_tests
import tests.test_metadata_fields as metadata_field_tests
import tests.test_preaggregation_bugs as preaggregation_bug_tests
import tests.test_segments as segment_tests
import tests.test_semantic_graph_errors as semantic_graph_error_tests
import tests.test_sql_generation_security as sql_security_tests
import tests.test_validation as validation_tests
import tests.test_with_data as data_query_tests
import tests.widget.test_widget_examples as widget_example_tests
from tests.rust_layer_adapter import (
    RustQueryRewriterAdapter,
    RustRuntimeSemanticLayer,
    RustSemanticGraphDirectAdapter,
    RustSemanticLayerAdapter,
    RustSQLGeneratorAdapter,
    rust_build_symmetric_aggregate_sql,
    rust_needs_symmetric_aggregate,
)


@dataclass(frozen=True)
class PythonTestParityCase:
    module: ModuleType
    func: Callable[..., object]
    nodeid: str
    fixture_names: tuple[str, ...]
    owner: type[object] | None = None


PYTHON_TEST_PARITY_MODULES = [
    validation_tests,
    symmetric_aggregate_tests,
    segment_tests,
    basic_query_tests,
    data_query_tests,
    auto_dimension_tests,
    date_integration_tests,
    composite_join_tests,
    many_to_many_join_tests,
    multi_hop_join_tests,
    rails_join_tests,
    cumulative_metric_tests,
    derived_metric_tests,
    default_time_dimension_tests,
    metric_filter_tests,
    pre_aggregation_tests,
    preagg_recommender_tests,
    predicate_pushdown_tests,
    count_distinct_segment_tests,
    sql_rewriter_tests,
    view_generation_tests,
    ungrouped_query_tests,
    jinja_integration_tests,
    foreign_key_dimension_tests,
    catalog_tests,
    preaggregation_bug_tests,
    sql_security_tests,
    hierarchy_tests,
    metadata_field_tests,
    semantic_graph_error_tests,
    widget_example_tests,
]


DIRECT_RUST_GRAPH_PARITY_NODEIDS = {
    "tests.joins.test_composite_key_joins::test_semantic_graph_composite_pk_adjacency",
    "tests.joins.test_composite_key_joins::test_semantic_graph_single_pk_still_works",
    "tests.test_semantic_graph_errors::test_add_duplicate_model",
    "tests.test_semantic_graph_errors::test_add_duplicate_metric",
    "tests.test_semantic_graph_errors::test_add_duplicate_table_calculation",
    "tests.test_semantic_graph_errors::test_get_nonexistent_model",
    "tests.test_semantic_graph_errors::test_get_nonexistent_metric",
    "tests.test_semantic_graph_errors::test_get_nonexistent_table_calculation",
    "tests.test_semantic_graph_errors::test_find_path_nonexistent_from_model",
    "tests.test_semantic_graph_errors::test_find_path_nonexistent_to_model",
    "tests.test_semantic_graph_errors::test_find_path_same_model",
    "tests.test_semantic_graph_errors::test_find_path_no_relationship",
    "tests.test_semantic_graph_errors::test_auto_register_time_comparison_metric",
    "tests.test_semantic_graph_errors::test_no_auto_register_regular_metrics",
    "tests.test_semantic_graph_errors::test_adjacency_not_built_on_add",
    "tests.test_semantic_graph_errors::test_adjacency_built_on_find_path",
}


DIRECT_RUST_FUNCTION_PARITY_MODULES = {
    "tests.core.test_symmetric_aggregates",
}


DIRECT_RUST_SQL_GENERATOR_PARITY_NODEIDS = {
    "tests.queries.test_view_generation::test_generate_view_creates_valid_sql",
    "tests.queries.test_view_generation::test_view_can_be_queried",
    "tests.queries.test_view_generation::test_view_name_sql_injection_rejected",
    "tests.queries.test_view_generation::test_view_name_with_spaces_rejected",
    "tests.queries.test_view_generation::test_join_view_against_other_tables",
    "tests.test_foreign_key_dimensions::test_foreign_key_as_dimension_no_join",
    "tests.test_foreign_key_dimensions::test_foreign_key_dimension_with_join",
    "tests.test_sql_generation_security::test_count_fanout_uses_column_reference",
    "tests.widget.test_widget_examples::test_notebook_metric_sql_generates",
    "tests.widget.test_widget_examples::test_auto_model_metrics_generate_without_dependency_errors",
}


SUPPORTED_SIGNATURES = {
    ("con",),
    ("layer",),
    ("layer", "orders_db"),
    ("layer", "three_table_chain"),
    ("orders_db", "layer"),
    ("semantic_layer",),
    ("test_db", "semantic_layer"),
    ("three_table_chain", "layer"),
    ("timeseries_db", "layer"),
}


def _python_test_parity_functions(module: ModuleType) -> list[pytest.ParameterSet]:
    tests: list[pytest.ParameterSet] = []
    for name, value in vars(module).items():
        if name.startswith("test_") and callable(value):
            signature = inspect.signature(value)
            fixture_names = tuple(signature.parameters)
            nodeid = f"{module.__name__}::{name}"
            if (
                fixture_names in SUPPORTED_SIGNATURES
                or _is_direct_rust_graph_test(nodeid, fixture_names)
                or _is_direct_rust_function_test(nodeid, fixture_names)
                or _is_direct_rust_sql_generator_test(nodeid, fixture_names)
            ):
                tests.append(
                    pytest.param(
                        PythonTestParityCase(module=module, func=value, nodeid=nodeid, fixture_names=fixture_names),
                        id=nodeid,
                    )
                )
        elif inspect.isclass(value) and name.startswith("Test"):
            tests.extend(_python_test_parity_methods(module, value))
    return tests


def _python_test_parity_methods(module: ModuleType, owner: type[object]) -> list[pytest.ParameterSet]:
    tests: list[pytest.ParameterSet] = []
    for method_name, method in vars(owner).items():
        if not method_name.startswith("test_") or not callable(method):
            continue
        signature = inspect.signature(method)
        fixture_names = tuple(name for name in signature.parameters if name != "self")
        nodeid = f"{module.__name__}::{owner.__name__}::{method_name}"
        if (
            fixture_names in SUPPORTED_SIGNATURES
            or _is_direct_rust_graph_test(nodeid, fixture_names)
            or _is_direct_rust_function_test(nodeid, fixture_names)
            or _is_direct_rust_sql_generator_test(nodeid, fixture_names)
        ):
            tests.append(
                pytest.param(
                    PythonTestParityCase(
                        module=module,
                        func=method,
                        nodeid=nodeid,
                        fixture_names=fixture_names,
                        owner=owner,
                    ),
                    id=nodeid,
                )
            )
    return tests


def _is_direct_rust_graph_test(nodeid: str, fixture_names: tuple[str, ...]) -> bool:
    return fixture_names == () and nodeid in DIRECT_RUST_GRAPH_PARITY_NODEIDS


def _is_direct_rust_function_test(nodeid: str, fixture_names: tuple[str, ...]) -> bool:
    module_name = nodeid.split("::", 1)[0]
    return module_name in DIRECT_RUST_FUNCTION_PARITY_MODULES and fixture_names in {(), ("conn",)}


def _is_direct_rust_sql_generator_test(nodeid: str, fixture_names: tuple[str, ...]) -> bool:
    return fixture_names == () and nodeid in DIRECT_RUST_SQL_GENERATOR_PARITY_NODEIDS


PYTHON_LAYER_PARITY_CASES = [
    *[test for module in PYTHON_TEST_PARITY_MODULES for test in _python_test_parity_functions(module)],
]


@pytest.mark.parametrize("case", PYTHON_LAYER_PARITY_CASES)
def test_existing_python_layer_contract_matches_pure_rust(case: PythonTestParityCase, monkeypatch):
    """Exercise the public Rust engine; direct helper cases keep their Rust adapters."""
    _patch_semantic_layer_constructors(monkeypatch, case)
    fixtures: dict[str, object] = {}
    try:
        fixtures = _fixture_values(case)
        _call_python_test_case(case, fixtures)
    finally:
        _close_fixture_values(fixtures)


def _patch_semantic_layer_constructors(monkeypatch, case: PythonTestParityCase) -> None:
    module = case.module
    direct_helper = (
        _is_direct_rust_graph_test(case.nodeid, case.fixture_names)
        or _is_direct_rust_sql_generator_test(case.nodeid, case.fixture_names)
        or _is_direct_rust_function_test(case.nodeid, case.fixture_names)
    )
    layer_class = RustSemanticLayerAdapter if direct_helper else RustRuntimeSemanticLayer
    monkeypatch.setattr(sidemantic, "SemanticLayer", layer_class)
    monkeypatch.setattr(semantic_layer_module, "SemanticLayer", layer_class)
    if hasattr(module, "SemanticLayer"):
        monkeypatch.setattr(module, "SemanticLayer", layer_class)
    if not direct_helper:
        # Some shared bodies instantiate QueryRewriter directly from the layer
        # graph; keep those calls on Rust too, not its legacy Python default.
        monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "1")
        monkeypatch.setenv("SIDEMANTIC_RS_NO_FALLBACK", "1")
        return

    # Keep the direct probes on their original pure-Rust graph/compiler harness.
    # Public runtime host services are covered separately above.
    monkeypatch.setattr(semantic_graph_module, "SemanticGraph", RustSemanticGraphDirectAdapter)
    monkeypatch.setattr(sql_generator_module, "SQLGenerator", RustSQLGeneratorAdapter)
    monkeypatch.setattr(query_rewriter_module, "QueryRewriter", RustQueryRewriterAdapter)
    if hasattr(module, "SemanticGraph"):
        monkeypatch.setattr(module, "SemanticGraph", RustSemanticGraphDirectAdapter)
    if hasattr(module, "SQLGenerator"):
        monkeypatch.setattr(module, "SQLGenerator", RustSQLGeneratorAdapter)
    if hasattr(module, "QueryRewriter"):
        monkeypatch.setattr(module, "QueryRewriter", RustQueryRewriterAdapter)
    if hasattr(module, "build_symmetric_aggregate_sql"):
        monkeypatch.setattr(module, "build_symmetric_aggregate_sql", rust_build_symmetric_aggregate_sql)
    if hasattr(module, "needs_symmetric_aggregate"):
        monkeypatch.setattr(module, "needs_symmetric_aggregate", rust_needs_symmetric_aggregate)


def _fixture_values(case: PythonTestParityCase) -> dict[str, object]:
    values: dict[str, object] = {}
    for fixture_name in case.fixture_names:
        if fixture_name == "layer":
            if case.module is auto_dimension_tests:
                values["db"] = _call_fixture(case.module, "db")
                values[fixture_name] = _call_fixture(case.module, "layer", values["db"])
            else:
                values[fixture_name] = RustRuntimeSemanticLayer(auto_register=False)
        elif fixture_name == "test_db":
            values[fixture_name] = _call_fixture(case.module, "test_db")
        elif fixture_name == "semantic_layer":
            values[fixture_name] = _semantic_layer_fixture(case.module, values)
        elif fixture_name == "orders_db":
            values[fixture_name] = _call_fixture(case.module, "orders_db")
        elif fixture_name == "con":
            values[fixture_name] = _call_fixture(case.module, "con")
        elif fixture_name == "conn":
            values[fixture_name] = duckdb.connect(":memory:")
        elif fixture_name == "three_table_chain":
            values[fixture_name] = _call_fixture(case.module, "three_table_chain")
        elif fixture_name == "timeseries_db":
            values[fixture_name] = _call_fixture(case.module, "timeseries_db")
        else:
            raise AssertionError(f"unsupported Python test parity fixture: {fixture_name}")
    return values


def _call_python_test_case(case: PythonTestParityCase, fixtures: dict[str, object]) -> None:
    if case.owner is None:
        case.func(**{name: fixtures[name] for name in case.fixture_names})
        return

    instance = case.owner()
    case.func(instance, **{name: fixtures[name] for name in case.fixture_names})


def _call_fixture(module: ModuleType, name: str, *args: object) -> object:
    fixture = getattr(module, name)
    factory = getattr(fixture, "__wrapped__", fixture)
    return factory(*args)


def _semantic_layer_fixture(module: ModuleType, values: dict[str, object]) -> object:
    fixture = getattr(module, "semantic_layer")
    factory = getattr(fixture, "__wrapped__", fixture)
    fixture_params = tuple(inspect.signature(factory).parameters)
    args = []
    for fixture_name in fixture_params:
        if fixture_name == "test_db":
            args.append(values.setdefault("test_db", _call_fixture(module, "test_db")))
        else:
            raise AssertionError(f"unsupported semantic_layer fixture dependency: {fixture_name}")
    return factory(*args)


def _close_fixture_values(values: dict[str, object]) -> None:
    seen: set[int] = set()
    for value in values.values():
        value_id = id(value)
        if value_id in seen:
            continue
        seen.add(value_id)
        close = getattr(value, "close", None)
        if callable(close):
            close()
