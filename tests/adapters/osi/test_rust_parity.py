"""Differential parity: pure-Python OSIAdapter vs sidemantic-rs.

Runs every OSI fixture through both implementations and asserts the resulting
semantic graphs (and re-exported OSI documents) are equivalent. Skipped when the
``sidemantic_rs`` extension is not built.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

pytest.importorskip("sidemantic_rs")

from sidemantic.adapters.osi import OSIAdapter as PyOSIAdapter
from sidemantic.rust_bridge import export_osi_with_rust, load_osi_graph_with_rust

FIXTURE_DIR = Path("tests/fixtures/osi")
FIXTURES = sorted(FIXTURE_DIR.glob("*.yaml"))


def _cols(single, columns) -> list[str]:
    if columns:
        return list(columns)
    if single is None:
        return []
    return [single] if isinstance(single, str) else list(single)


def _eff_type(metric) -> str:
    """Effective metric type.

    Python's OSIAdapter leaves derived/simple metrics' ``type`` as ``None`` and
    relies on ``agg``/``sql`` to disambiguate; sidemantic-rs records the explicit
    type. Normalize both to compare semantics rather than representation.
    """
    if metric.type:
        return metric.type
    return "simple" if metric.agg else "derived"


def _canonical_graph(graph) -> dict:
    models = {}
    for name, model in graph.models.items():
        models[name] = {
            "table": model.table,
            "primary_key": list(model.primary_key_columns),
            "unique_keys": model.unique_keys,
            "default_time_dimension": model.default_time_dimension,
            "description": model.description,
            "meta": model.meta,
            "dimensions": {
                dim.name: {
                    "type": dim.type,
                    "sql": dim.sql,
                    "granularity": dim.granularity,
                    "label": dim.label,
                    "meta": dim.meta,
                }
                for dim in model.dimensions
            },
            "relationships": {
                rel.name: {
                    "type": rel.type,
                    "foreign_key": _cols(rel.foreign_key, getattr(rel, "foreign_key_columns", None)),
                    "primary_key": _cols(rel.primary_key, getattr(rel, "primary_key_columns", None)),
                    "metadata": rel.metadata,
                }
                for rel in model.relationships
            },
            "metrics": {
                metric.name: {"agg": metric.agg, "sql": metric.sql, "type": _eff_type(metric)}
                for metric in model.metrics
            },
        }
    metrics = {
        name: {
            "agg": metric.agg,
            "sql": metric.sql,
            "type": _eff_type(metric),
            "meta": metric.meta,
            "description": metric.description,
        }
        for name, metric in graph.metrics.items()
    }
    return {"models": models, "metrics": metrics, "metadata": graph.metadata or {}}


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda p: p.name)
def test_osi_import_parity(fixture):
    """Rust import of each fixture matches the Python adapter."""
    python_graph = PyOSIAdapter().parse(str(fixture))
    rust_graph = load_osi_graph_with_rust(str(fixture))
    assert _canonical_graph(rust_graph) == _canonical_graph(python_graph)


def _by_name(items) -> dict:
    return {item.get("name"): item for item in (items or [])}


def _canonical_osi_doc(data: dict) -> dict:
    semantic_model = (data.get("semantic_model") or [{}])[0]
    datasets = {}
    for dataset in semantic_model.get("datasets") or []:
        entry = dict(dataset)
        entry["fields"] = _by_name(dataset.get("fields"))
        datasets[dataset.get("name")] = entry
    return {
        "version": data.get("version"),
        "name": semantic_model.get("name"),
        "description": semantic_model.get("description"),
        "datasets": datasets,
        "relationships": _by_name(semantic_model.get("relationships")),
        "metrics": _by_name(semantic_model.get("metrics")),
    }


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda p: p.name)
def test_osi_export_parity(fixture, tmp_path):
    """Rust export of a Python-parsed graph matches the Python adapter's export."""
    graph = PyOSIAdapter().parse(str(fixture))

    python_path = tmp_path / "python.yaml"
    PyOSIAdapter().export(graph, python_path)
    python_doc = yaml.safe_load(python_path.read_text())
    rust_doc = yaml.safe_load(export_osi_with_rust(graph))

    assert _canonical_osi_doc(rust_doc) == _canonical_osi_doc(python_doc)


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda p: p.name)
def test_osi_roundtrip_parity(fixture, tmp_path):
    """Export via Rust, re-import via both adapters; graphs stay equivalent."""
    graph = PyOSIAdapter().parse(str(fixture))
    rust_yaml = export_osi_with_rust(graph)
    out = tmp_path / "rust.yaml"
    out.write_text(rust_yaml)

    python_graph = PyOSIAdapter().parse(str(out))
    rust_graph = load_osi_graph_with_rust(str(out))
    assert _canonical_graph(rust_graph) == _canonical_graph(python_graph)


def test_osi_export_resolves_model_inheritance(tmp_path):
    """A graph with unresolved Model.extends exports identically via both adapters."""
    from sidemantic.core.dimension import Dimension
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    base = Model(
        name="base_orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )
    child = Model(name="orders", extends="base_orders", primary_key="order_id")
    graph = SemanticGraph()
    graph.add_model(base)
    graph.add_model(child)
    assert graph.models["orders"].table is None  # unresolved before export

    python_path = tmp_path / "python.yaml"
    PyOSIAdapter().export(graph, python_path)
    python_doc = yaml.safe_load(python_path.read_text())
    rust_doc = yaml.safe_load(export_osi_with_rust(graph))

    assert _canonical_osi_doc(rust_doc) == _canonical_osi_doc(python_doc)
    # The child dataset must carry the inherited source and field.
    orders = {d["name"]: d for d in rust_doc["semantic_model"][0]["datasets"]}["orders"]
    assert orders["source"] == "orders"
    assert [field["name"] for field in orders["fields"]] == ["status"]
