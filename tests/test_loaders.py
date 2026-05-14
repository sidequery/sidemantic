"""Tests for directory loaders."""

import builtins
import sys
from pathlib import Path

from sidemantic import SemanticLayer
from sidemantic.loaders import load_from_directory


def test_load_from_directory_does_not_require_antlr4_without_antlr_formats(tmp_path, monkeypatch):
    """Cube loading should work even when ANTLR runtime is unavailable."""
    fixture_file = Path(__file__).parent / "fixtures" / "cube" / "orders.yml"
    (tmp_path / "orders.yml").write_text(fixture_file.read_text())

    monkeypatch.delitem(sys.modules, "sidemantic.adapters.holistics", raising=False)
    for module_name in list(sys.modules):
        if module_name == "antlr4" or module_name.startswith("antlr4."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    real_import = builtins.__import__

    def blocked_antlr4_import(name, *args, **kwargs):
        if name == "antlr4" or name.startswith("antlr4."):
            raise ImportError("simulated missing antlr4")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_antlr4_import)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models


def test_load_from_directory_surfaces_adapter_parse_failures(tmp_path, monkeypatch):
    from sidemantic.adapters.sidemantic import SidemanticAdapter

    (tmp_path / "broken.yml").write_text("models:\n  - name: broken\n")

    def _raise_parse_failure(self, path):
        raise ValueError("simulated native yaml failure")

    monkeypatch.setattr(SidemanticAdapter, "parse", _raise_parse_failure)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    warnings = layer.describe_models()["import_warnings"]
    assert warnings == [
        {
            "code": "adapter_parse_error",
            "context": "loader",
            "source_format": "Sidemantic",
            "source_file": "broken.yml",
            "message": "simulated native yaml failure",
        }
    ]


def test_load_from_directory_surfaces_tmdl_project_parse_failures(tmp_path, monkeypatch):
    from sidemantic.adapters.tmdl import TMDLAdapter

    tmdl_file = tmp_path / "definition" / "tables" / "Sales.tmdl"
    tmdl_file.parent.mkdir(parents=True)
    tmdl_file.write_text("table Sales\n")

    def _raise_parse_failure(self, path):
        raise ValueError("simulated tmdl failure")

    monkeypatch.setattr(TMDLAdapter, "parse", _raise_parse_failure)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    warnings = layer.describe_models()["import_warnings"]
    assert {
        "code": "tmdl_parse_error",
        "context": "loader",
        "source_format": "TMDL",
        "source_file": "definition",
        "message": "simulated tmdl failure",
    } in warnings


def test_load_from_directory_does_not_partially_parse_tmdl_project_after_project_failure(tmp_path, monkeypatch):
    from sidemantic.adapters.tmdl import TMDLAdapter
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    definition_dir = tmp_path / "definition"
    tmdl_file = definition_dir / "tables" / "Sales.tmdl"
    tmdl_file.parent.mkdir(parents=True)
    tmdl_file.write_text("table Sales\n")

    calls: list[Path] = []

    def _parse_project_only(self, path):
        source = Path(path)
        calls.append(source)
        if source.is_dir():
            raise ValueError("simulated project-level failure")
        graph = SemanticGraph()
        graph.add_model(Model(name="PartialSales", table="sales", primary_key="id"))
        return graph

    monkeypatch.setattr(TMDLAdapter, "parse", _parse_project_only)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert calls == [definition_dir]
    assert layer.graph.models == {}
    assert layer.describe_models()["import_warnings"] == [
        {
            "code": "tmdl_parse_error",
            "context": "loader",
            "source_format": "TMDL",
            "source_file": "definition",
            "message": "simulated project-level failure",
        }
    ]
