"""Every export target returns the native numbers after re-import, or names what it lost.

One small native model is exported to each registered export format, read back
with that format's importer, and queried on DuckDB. A target passes only when

(a) the re-imported model returns the same rows as the native model, or
(b) the export fidelity report names each construct that did not survive, the
    export and the CLI do not claim a lossless result, and the model without
    those constructs still returns the native rows.

``EXPECTED_LOSSES`` is the matrix of what each format cannot carry. An export
that silently changes a number, or reports a loss that is not declared here,
fails.
"""

from __future__ import annotations

import copy
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest
import yaml
from typer.testing import CliRunner

from sidemantic import SemanticLayer
from sidemantic.cli import app
from sidemantic.fidelity import ConversionFidelityError, ExportReport, capture_export_report
from sidemantic.formats import (
    OutputKind,
    export_semantic_graph,
    get_semantic_format,
    load_semantic_source,
    semantic_formats,
)

RUNNER = CliRunner()

NATIVE_MODEL = {
    "models": [
        {
            "name": "orders",
            "table": "orders",
            "primary_key": "order_id",
            "dimensions": [
                {"name": "status", "type": "categorical", "sql": "status"},
                {"name": "created_at", "type": "time", "sql": "created_at", "granularity": "day"},
            ],
            "metrics": [
                {"name": "order_count", "agg": "count"},
                {"name": "revenue", "agg": "sum", "sql": "amount"},
                {"name": "unique_customers", "agg": "count_distinct", "sql": "customer_id"},
                {"name": "completed_revenue", "agg": "sum", "sql": "amount", "filters": ["status = 'complete'"]},
            ],
            "relationships": [{"name": "customers", "type": "many_to_one", "foreign_key": "customer_id"}],
        },
        {
            "name": "customers",
            "table": "customers",
            "primary_key": "customer_id",
            "dimensions": [
                {"name": "region", "type": "categorical", "sql": "region"},
                {"name": "signup_date", "type": "time", "sql": "signup_date", "granularity": "day"},
            ],
            "metrics": [{"name": "customer_count", "agg": "count"}],
        },
    ],
    "metrics": [
        {
            "name": "completion_rate",
            "type": "ratio",
            "numerator": "orders.completed_revenue",
            "denominator": "orders.revenue",
        },
        {"name": "revenue_per_customer", "type": "derived", "sql": "orders.revenue / orders.unique_customers"},
    ],
}

# Constructs a format may be unable to carry, by the key used in ``Loss.construct``.
ORDER_METRICS = ("revenue", "completed_revenue", "order_count", "unique_customers")
JOIN = "orders -> customers"
GRAPH_METRIC_OPERANDS = {
    "completion_rate": ("orders.completed_revenue", "orders.revenue"),
    "revenue_per_customer": ("orders.revenue", "orders.unique_customers"),
}


@dataclass(frozen=True)
class Loss:
    """One construct a format does not carry, and how its export says so."""

    feature: str  # slug of the export diagnostic that reports the loss
    names: str  # text the diagnostic must contain to identify the construct
    construct: str | None  # model construct that does not survive; None when query results are unaffected
    reason: str  # why the format cannot carry it
    blocks: bool = True  # blocked export (nothing published) vs. published with a review-level loss


def no_graph_metrics(feature: str, reason: str) -> tuple[Loss, ...]:
    return tuple(Loss(feature, name, name, reason) for name in GRAPH_METRIC_OPERANDS)


def no_keys(reason: str) -> tuple[Loss, ...]:
    return tuple(Loss("primary_key", key, None, reason, blocks=False) for key in ("order_id", "customer_id"))


EXPECTED_LOSSES: dict[str, tuple[Loss, ...]] = {
    "atscale-sml": (
        *no_graph_metrics("graph_metrics", "a cross-model calculation is an MDX metric_calc, which is not written"),
        Loss(
            "metric_filter",
            "orders.completed_revenue",
            "orders.completed_revenue",
            "an SML metric aggregates a dataset column and has no filter; a calculated column is not written",
        ),
    ),
    "bsl": no_graph_metrics("graph_metrics", "a BSL measure belongs to one model file"),
    "cube": no_graph_metrics("graph_metric", "a Cube measure belongs to one cube"),
    "gooddata": (
        *no_graph_metrics("graph_metrics", "an LDM has no metrics; calculations are MAQL"),
        Loss(
            "metric_aggregation",
            "orders.order_count",
            "orders.order_count",
            "an LDM fact is a physical column; a row count is a MAQL metric",
        ),
        Loss(
            "metric_aggregation",
            "orders.unique_customers",
            "orders.unique_customers",
            "an LDM fact imports as SUM; a distinct count is a MAQL metric",
        ),
        Loss(
            "metric_filter",
            "orders.completed_revenue",
            "orders.completed_revenue",
            "an LDM fact has no filter; a filtered measure is a MAQL metric",
        ),
        Loss(
            "metric_aggregation",
            "customers.customer_count",
            "customers.customer_count",
            "an LDM fact is a physical column; a row count is a MAQL metric",
        ),
    ),
    "hex": no_graph_metrics("graph_metrics", "a Hex measure belongs to one model"),
    "holistics": no_graph_metrics("graph_metrics", "an AML measure belongs to one model"),
    "lookml": no_graph_metrics("graph_metric", "a LookML measure belongs to one view"),
    "malloy": no_graph_metrics("graph_metrics", "a Malloy measure belongs to one source"),
    "metricflow": (),
    "omni": no_graph_metrics("graph_metrics", "an Omni measure belongs to one view"),
    "ossie": (
        Loss(
            "ossie.synthesis.runtime_extension_required",
            "runtime extension",
            None,
            "Ossie core has no time granularity or implicit keys; they ride in a Sidemantic extension",
            blocks=False,
        ),
    ),
    "rill": (
        *no_graph_metrics("graph_metrics", "a Rill measure belongs to one metrics view"),
        Loss("relationship", "orders.customers", JOIN, "a metrics view reads one table and has no joins", blocks=False),
        *no_keys("a metrics view has no key"),
    ),
    "sidemantic": (),
    "snowflake": (),
    "superset": (
        *no_graph_metrics("graph_metrics", "a Superset metric belongs to one dataset"),
        Loss("relationship", "orders.customers", JOIN, "a dataset reads one table and has no joins", blocks=False),
        *no_keys("a dataset has no key"),
    ),
    "thoughtspot": (
        *no_graph_metrics("graph_metrics", "a TML formula belongs to one worksheet"),
        Loss(
            "metric_filter",
            "orders.completed_revenue",
            "orders.completed_revenue",
            "a TML column has no filter; a conditional formula (sum_if) is not written",
        ),
        *no_keys("TML has no primary-key property"),
    ),
    "tmdl": (
        *no_graph_metrics("graph_metrics", "a TMDL measure needs a home table"),
        Loss(
            "sql_to_dax",
            "orders.revenue",
            "orders.revenue",
            "DAX aggregates a declared column; amount is not an exported dimension",
        ),
        Loss(
            "sql_to_dax",
            "orders.unique_customers",
            "orders.unique_customers",
            "DAX aggregates a declared column; customer_id is not an exported dimension",
        ),
        Loss(
            "metric_modifiers",
            "filters completed_revenue",
            "orders.completed_revenue",
            "SQL-to-DAX does not translate filtered measures",
        ),
        *no_keys("isKey is a column property and the key columns are not exported dimensions"),
    ),
}

# Lightdash and Sigma go through the official Ossie converters, which accept
# only portable Ossie core. The reference model is refused before the vendor
# CLI runs; each entry is text the refusal must contain.
BRIDGE_REFUSALS: dict[str, tuple[str, ...]] = {
    target: (
        "orders.created_at has native state not represented in Ossie core: granularity",
        "Declared key field 'order_id' does not exist uniquely in dataset 'orders'",
        "Relationship from 'orders' to 'customers' has no declared edge identity",
    )
    for target in ("lightdash", "sigma")
}

OPTIONAL_DEPENDENCY = {"lookml": "lkml", "ossie": "jsonschema", "lightdash": "jsonschema", "sigma": "jsonschema"}
ADAPTER_OPTIONS = {"lightdash": {"expression_dialect": "ANSI_SQL"}, "sigma": {"expression_dialect": "ANSI_SQL"}}
EXPORT_OPTIONS = {"ossie": {"scope_name": "commerce", "expression_dialect": "ANSI_SQL"}}
CLI_OPTIONS = {
    "ossie": ["--ossie-scope", "commerce", "--ossie-expression-dialect", "ANSI_SQL"],
    "lightdash": ["--ossie-expression-dialect", "ANSI_SQL"],
    "sigma": ["--ossie-expression-dialect", "ANSI_SQL"],
}
EXPORT_TARGETS = sorted(spec.name for spec in semantic_formats() if spec.supports_export)


@pytest.fixture(scope="module")
def database(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("roundtrip") / "data.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("create table customers (customer_id integer, region varchar, signup_date date)")
        connection.execute(
            "insert into customers values (1, 'EU', '2024-01-01'), (2, 'US', '2024-01-05'), (3, 'EU', '2024-02-01')"
        )
        connection.execute(
            "create table orders "
            "(order_id integer, customer_id integer, status varchar, amount double, created_at date)"
        )
        connection.execute(
            "insert into orders values (1, 1, 'complete', 100, '2024-03-01'), "
            "(2, 3, 'pending', 50, '2024-03-02'), (3, 2, 'complete', 100, '2024-03-02')"
        )
    return path


def native_model(dropped: frozenset[str]) -> dict:
    """The reference model without the named constructs."""
    data = copy.deepcopy(NATIVE_MODEL)
    for model in data["models"]:
        model["metrics"] = [m for m in model["metrics"] if f"{model['name']}.{m['name']}" not in dropped]
        relationships = [r for r in model.get("relationships", []) if f"{model['name']} -> {r['name']}" not in dropped]
        if relationships:
            model["relationships"] = relationships
        else:
            model.pop("relationships", None)
    data["metrics"] = [m for m in data["metrics"] if m["name"] not in dropped]
    if not data["metrics"]:
        del data["metrics"]
    return data


def native_graph(directory: Path, dropped: frozenset[str]):
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / "native.yml"
    source.write_text(yaml.safe_dump(native_model(dropped), sort_keys=False))
    return source, load_semantic_source(source, source_format="sidemantic")


def reference_queries(dropped: frozenset[str]) -> list[tuple[list[str], list[str]]]:
    """Queries over everything that survives ``dropped``."""
    order_metrics = [f"orders.{name}" for name in ORDER_METRICS if f"orders.{name}" not in dropped]
    graph_metrics = [name for name in GRAPH_METRIC_OPERANDS if name not in dropped]
    queries = []
    if order_metrics:
        queries.append((order_metrics, ["orders.status"]))
    if "customers.customer_count" not in dropped:
        queries.append((["customers.customer_count"], ["customers.region"]))
    if JOIN not in dropped:
        if order_metrics:
            queries.append((order_metrics, ["customers.region"]))
        if graph_metrics:
            queries.append((graph_metrics, ["customers.region"]))
    elif graph_metrics:
        queries.append((graph_metrics, ["orders.status"]))
    return queries


def query_rows(graph, database: Path, dropped: frozenset[str]) -> list[list[tuple]]:
    layer = SemanticLayer(connection=f"duckdb:///{database}", auto_register=False)
    for model in graph.models.values():
        # Importers for formats with a designated time column (Rill, ThoughtSpot)
        # set a default time dimension. How queries group by that default is
        # covered by the query tests; it is cleared here so this test compares
        # the same grouping on both sides of the round trip.
        model.default_time_dimension = None
        model.default_grain = None
        layer.add_model(model)
    for metric in graph.metrics.values():
        if metric.name not in layer.graph.metrics:
            layer.add_metric(metric)
    results = []
    for metrics, dimensions in reference_queries(dropped):
        rows = layer.query(metrics=metrics, dimensions=dimensions, order_by=dimensions).fetchall()
        results.append([tuple(round(float(v), 6) if isinstance(v, (int, float)) else v for v in row) for row in rows])
    layer.adapter.close()
    return results


def output_path(directory: Path, target: str) -> Path:
    spec = get_semantic_format(target)
    if spec.output_kind == OutputKind.FILE:
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"model{spec.extensions[0]}"
    # Formats that write one file per model select that layout from a directory.
    (directory / "project").mkdir(parents=True, exist_ok=True)
    return directory / "project"


def export(graph, directory: Path, target: str) -> tuple[Path | None, ExportReport]:
    """Export through the shared conversion path; the path is None when publication was blocked."""
    destination = output_path(directory, target)
    with capture_export_report() as report:
        try:
            export_semantic_graph(
                graph,
                destination,
                target_format=target,
                adapter_options=ADAPTER_OPTIONS.get(target),
                export_options=EXPORT_OPTIONS.get(target),
            )
        except ConversionFidelityError:
            assert report.is_blocked
            assert not any(destination.iterdir()) if destination.is_dir() else not destination.exists()
            return None, report
    assert not report.is_blocked
    return destination, report


def reported(report: ExportReport) -> list[tuple[str, str]]:
    """``(slug, text)`` for every loss in an export report."""
    notes = [(note.construct, f"{note.detail} {note.location or ''}") for note in report.notes]
    features = [
        (feature.feature, f"{feature.detail or ''} {feature.location or ''}")
        for feature in report.features
        if feature.status != "exact"
    ]
    return notes + features


def is_reported(report: ExportReport, loss: Loss) -> bool:
    return any(slug == loss.feature and loss.names in text for slug, text in reported(report))


def assert_round_trip(exported: Path, target: str, database: Path, directory: Path, dropped: frozenset[str]):
    """The re-imported export answers every surviving query like the native model does."""
    _, native = native_graph(directory / "expected", dropped)
    reimported = load_semantic_source(exported, source_format=target, adapter_options=ADAPTER_OPTIONS.get(target))
    assert query_rows(reimported, database, dropped) == query_rows(native, database, dropped)


def test_every_export_target_declares_its_losses():
    assert sorted({*EXPECTED_LOSSES, *BRIDGE_REFUSALS}) == EXPORT_TARGETS
    assert not set(EXPECTED_LOSSES) & set(BRIDGE_REFUSALS)


def test_reference_model_returns_reference_rows(database, tmp_path):
    _, graph = native_graph(tmp_path, frozenset())
    by_status, customer_count, by_region, graph_metrics = query_rows(graph, database, frozenset())
    # orders.revenue, orders.completed_revenue, orders.order_count, orders.unique_customers
    assert by_region == [("EU", 150.0, 100.0, 2.0, 2.0), ("US", 100.0, 100.0, 1.0, 1.0)]
    assert by_status == [("complete", 200.0, 200.0, 2.0, 2.0), ("pending", 50.0, None, 1.0, 1.0)]
    assert customer_count == [("EU", 2.0), ("US", 1.0)]
    # completion_rate, revenue_per_customer
    assert graph_metrics == [("EU", pytest.approx(2 / 3), 75.0), ("US", 1.0, 100.0)]


@pytest.mark.parametrize("target", sorted(EXPECTED_LOSSES))
def test_export_round_trips_or_reports_each_loss(target, database, tmp_path):
    if target in OPTIONAL_DEPENDENCY:
        pytest.importorskip(OPTIONAL_DEPENDENCY[target])
    losses = EXPECTED_LOSSES[target]
    blocking = [loss for loss in losses if loss.blocks]
    lost = frozenset(loss.construct for loss in losses if loss.construct)
    everything = frozenset()

    # 1. The full model: published only when nothing blocks, and never called
    #    lossless when something is lost.
    source, graph = native_graph(tmp_path / "full", everything)
    exported, report = export(graph, tmp_path / "full", target)
    assert (exported is None) == bool(blocking)
    assert report.is_ready == (not losses)
    if exported is not None:
        assert_round_trip(exported, target, database, tmp_path / "full", lost)

    # 2. The CLI says the same thing.
    destination = output_path(tmp_path / "cli", target)
    result = RUNNER.invoke(
        app,
        [
            "convert",
            str(source),
            "--from",
            "sidemantic",
            "--to",
            target,
            "--output",
            str(destination),
            "--force",
            *CLI_OPTIONS.get(target, []),
        ],
    )
    assert result.exit_code == (1 if blocking else 0), result.output
    if blocking:
        assert "Export readiness: blocked" in result.stderr
        assert any(loss.names in result.stderr for loss in blocking)
        assert "Converted" not in result.stderr
    elif losses:
        assert "Export readiness: review_required" in result.stderr
        assert all(loss.names in result.stderr for loss in losses)
    else:
        assert "Export readiness: ready (no reported losses)" in result.stderr
    assert ("no reported losses" in result.stderr) == (not losses)

    # 3. Each declared loss is reported for exactly the construct it names.
    for index, loss in enumerate(losses):
        kept = {loss.construct, *GRAPH_METRIC_OPERANDS.get(loss.construct, ())}
        _, graph = native_graph(tmp_path / f"loss{index}", lost - kept)
        _, report = export(graph, tmp_path / f"loss{index}", target)
        assert is_reported(report, loss), (loss, reported(report))

    # 4. Without the declared losses the export is published, reports nothing
    #    undeclared, and returns the native rows.
    if blocking:
        _, graph = native_graph(tmp_path / "surviving", lost)
        exported, report = export(graph, tmp_path / "surviving", target)
        assert exported is not None, reported(report)
        review_level = [loss for loss in losses if not loss.blocks]
        for slug, text in reported(report):
            assert any(slug == loss.feature and loss.names in text for loss in review_level), (slug, text)
        assert_round_trip(exported, target, database, tmp_path / "surviving", lost)


@pytest.mark.parametrize("target", sorted(BRIDGE_REFUSALS))
def test_official_bridge_refuses_the_reference_model_by_name(target, tmp_path):
    pytest.importorskip(OPTIONAL_DEPENDENCY[target])
    source, graph = native_graph(tmp_path / "full", frozenset())
    exported, report = export(graph, tmp_path / "full", target)
    assert exported is None
    refusal = " ".join(text for slug, text in reported(report) if slug == "official_converter_semantics")
    for construct in BRIDGE_REFUSALS[target]:
        assert construct in refusal

    destination = output_path(tmp_path / "cli", target)
    arguments = ["convert", str(source), "--from", "sidemantic", "--to", target, "--output", str(destination)]
    result = RUNNER.invoke(app, [*arguments, "--force", *CLI_OPTIONS[target]])
    assert result.exit_code == 1, result.output
    assert "Export readiness: blocked" in result.stderr
    assert "no reported losses" not in result.stderr
    assert all(construct in result.stderr for construct in BRIDGE_REFUSALS[target])


@pytest.mark.parametrize("target", ["holistics", "omni"])
def test_directory_auto_detection_keeps_exported_relationships(target, database, tmp_path):
    """Joins live in a project-level file that format auto-detection must attach."""
    from sidemantic.loaders import load_from_directory

    lost = frozenset(loss.construct for loss in EXPECTED_LOSSES[target] if loss.construct)
    _, graph = native_graph(tmp_path / "native", lost)
    exported, _ = export(graph, tmp_path / "native", target)
    project = tmp_path / "project"
    shutil.copytree(exported, project)

    layer = SemanticLayer(connection=f"duckdb:///{database}", auto_register=False)
    load_from_directory(layer, project)
    relationship = layer.graph.models["orders"].relationships[0]
    assert (relationship.name, relationship.foreign_key, relationship.primary_key) == (
        "customers",
        "customer_id",
        "customer_id",
    )
    rows = layer.query(metrics=["orders.revenue"], dimensions=["customers.region"], order_by=["customers.region"])
    assert rows.fetchall() == [("EU", 150.0), ("US", 100.0)]
    layer.adapter.close()
