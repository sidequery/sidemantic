"""Focused complexity and compatibility coverage for project loading/validation."""

import os
from types import SimpleNamespace

import pytest
import yaml

from sidemantic import Metric, SemanticLayer, load_from_directory
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.validation import _check_circular_dependencies
from sidemantic.validation_runner import _find_orphaned_models
from sidemantic.yaml_compat import safe_load, safe_load_all


def test_directory_load_reuses_one_pruned_walk(tmp_path, monkeypatch):
    (tmp_path / "models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: id
        type: numeric
    metrics:
      - name: count
        agg: count
"""
    )
    ignored = tmp_path / ".venv" / "models"
    ignored.mkdir(parents=True)
    (ignored / "broken.yml").write_text("models: [")

    real_walk = os.walk
    walked_roots = []

    def counted_walk(root, *args, **kwargs):
        walked_roots.append(root)
        return real_walk(root, *args, **kwargs)

    monkeypatch.setattr(os, "walk", counted_walk)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert set(layer.graph.models) == {"orders"}
    assert walked_roots == [tmp_path]


def test_yaml_compat_uses_safe_c_loader_when_available():
    from sidemantic import yaml_compat

    assert safe_load("answer: 42") == {"answer": 42}
    assert list(safe_load_all("a: 1\n---\nb: 2\n")) == [{"a": 1}, {"b": 2}]
    if hasattr(yaml, "CSafeLoader"):
        assert yaml_compat._SafeLoader is yaml.CSafeLoader
    with pytest.raises(yaml.constructor.ConstructorError):
        safe_load("!!python/object/apply:builtins.eval ['1 + 1']")


def test_orphan_detection_walks_each_relationship_collection_once():
    class CountingRelationships(list):
        iterations = 0

        def __iter__(self):
            type(self).iterations += 1
            return super().__iter__()

    model_count = 4_000
    models = {f"model_{index}": SimpleNamespace(relationships=CountingRelationships()) for index in range(model_count)}
    models["model_0"].relationships.append(SimpleNamespace(name="model_1"))

    orphaned = _find_orphaned_models(models)

    assert orphaned == [f"model_{index}" for index in range(2, model_count)]
    assert CountingRelationships.iterations == model_count


def test_derived_cycle_validation_is_iterative_and_memoized(monkeypatch):
    graph = SemanticGraph()
    metric_count = 1_200
    for index in range(metric_count):
        graph.add_metric(Metric(name=f"metric_{index}", type="derived", sql=f"metric_{index + 1}"))

    real_get_dependencies = Metric.get_dependencies
    dependency_scans = 0

    def counted_dependencies(self, *args, **kwargs):
        nonlocal dependency_scans
        dependency_scans += 1
        return real_get_dependencies(self, *args, **kwargs)

    monkeypatch.setattr(Metric, "get_dependencies", counted_dependencies)

    for metric in graph.metrics.values():
        assert _check_circular_dependencies(metric, graph, set()) is None

    assert dependency_scans == metric_count


def test_derived_cycle_memo_is_invalidated_by_graph_version():
    graph = SemanticGraph()
    first = Metric(name="first", type="derived", sql="second")
    graph.add_metric(first)

    assert _check_circular_dependencies(first, graph, set()) is None

    graph.add_metric(Metric(name="second", type="derived", sql="first"))

    assert _check_circular_dependencies(first, graph, set()) == ["first", "second", "first"]
