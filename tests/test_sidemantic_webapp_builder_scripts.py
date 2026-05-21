"""Regression coverage for bundled Sidemantic webapp builder scripts."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_inspect_layer_module():
    path = Path(__file__).resolve().parents[1] / "skills" / "sidemantic-webapp-builder" / "scripts" / "inspect_layer.py"
    spec = importlib.util.spec_from_file_location("sidemantic_webapp_builder_inspect_layer", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _Description:
    def __init__(self, name: str) -> None:
        self.name = name


class _ResultWithoutFetchmany:
    description = [("metric_value", "INTEGER"), _Description("category")]

    def __init__(self) -> None:
        self._rows = iter([(10, "A"), (7, "B"), (3, "C")])

    def fetchone(self):
        return next(self._rows, None)


class _Adapter:
    def execute(self, sql: str):
        assert sql == "select metric_value, category from t"
        return _ResultWithoutFetchmany()

    def fetchone(self, result):
        return result.fetchone()


def test_execute_sample_uses_adapter_fetchone_without_fetchmany() -> None:
    module = _load_inspect_layer_module()
    layer = SimpleNamespace(adapter=_Adapter())

    result = module._execute_sample(layer, "select metric_value, category from t", sample_rows=2)

    assert result == {
        "columns": ["metric_value", "category"],
        "sample_rows": [
            {"metric_value": 10, "category": "A"},
            {"metric_value": 7, "category": "B"},
        ],
        "sample_row_count": 2,
    }
