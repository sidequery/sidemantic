"""Regression coverage for bundled Sidemantic webapp builder scripts."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace


def _load_script_module(script_name: str, module_name: str):
    path = Path(__file__).resolve().parents[1] / "skills" / "sidemantic-webapp-builder" / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(module_name, path)
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
    module = _load_script_module("inspect_layer.py", "sidemantic_webapp_builder_inspect_layer")
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


def _metric_totals_query(model: str):
    return {
        "metrics": [f"{model}.count"],
        "dimensions": [],
        "sql": f"select count(*) as count from {model}",
        "result": {
            "columns": ["count"],
            "sample_rows": [{"count": 1}],
            "sample_row_count": 1,
        },
    }


def _leaderboard_query(model: str):
    return {
        "metrics": [f"{model}.count"],
        "dimensions": [f"{model}.category"],
        "sql": f"select category, count(*) as count from {model} group by category",
        "result": {
            "columns": ["category", "count"],
            "sample_rows": [{"category": "A", "count": 1}],
            "sample_row_count": 1,
        },
    }


def test_static_scaffold_preserves_requested_model_candidate(tmp_path: Path) -> None:
    scaffold_module = _load_script_module("scaffold_static_app.py", "sidemantic_webapp_builder_scaffold_static_app")
    verify_module = _load_script_module("verify_static_app.py", "sidemantic_webapp_builder_verify_static_app")
    spec_path = tmp_path / "app-spec.json"
    output_dir = tmp_path / "dashboard"
    spec_path.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "name": "first_model",
                        "primary_key": "id",
                        "dimensions": [{"name": "category", "type": "categorical"}],
                    },
                    {
                        "name": "requested_model",
                        "primary_key": "id",
                        "dimensions": [{"name": "category", "type": "categorical"}],
                    },
                ],
                "app_candidates": [
                    {
                        "model": "first_model",
                        "queries": {
                            "metric_totals": {"metrics": ["first_model.count"], "dimensions": []},
                            "dimension_leaderboard": {
                                "metrics": ["first_model.count"],
                                "dimensions": ["first_model.category"],
                            },
                        },
                    },
                    {
                        "model": "requested_model",
                        "queries": {
                            "metric_totals": _metric_totals_query("requested_model"),
                            "dimension_leaderboard": _leaderboard_query("requested_model"),
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    scaffold_module.scaffold(
        SimpleNamespace(
            app_spec=spec_path,
            model="requested_model",
            output=output_dir,
            title=None,
        )
    )

    index_html = (output_dir / "index.html").read_text(encoding="utf-8")
    app_js = (output_dir / "app.js").read_text(encoding="utf-8")

    assert 'data-model="requested_model"' in index_html
    assert "candidates.find((item) => item.model === selectedModel)" in app_js

    report = verify_module.verify(SimpleNamespace(app_dir=output_dir, app_spec=None))
    assert report["selected_model"] == "requested_model"
    assert all(report["checks"].values())
