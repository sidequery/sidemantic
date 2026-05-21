#!/usr/bin/env python3
"""Verify a static Sidemantic dashboard scaffold without browser dependencies."""

from __future__ import annotations

import argparse
import json
import sys
from html.parser import HTMLParser
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


class _DashboardShellParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.selected_model: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "main":
            return
        attr_map = dict(attrs)
        if attr_map.get("data-testid") == "dashboard-shell":
            self.selected_model = attr_map.get("data-model")


def _selected_model(index_path: Path) -> str | None:
    parser = _DashboardShellParser()
    parser.feed(index_path.read_text(encoding="utf-8"))
    return parser.selected_model


def _candidate_for_model(spec: dict[str, Any], selected_model: str | None) -> dict[str, Any] | None:
    candidates = spec.get("app_candidates") or []
    if selected_model:
        return next((candidate for candidate in candidates if candidate.get("model") == selected_model), None)
    return candidates[0] if candidates else None


def _query(candidate: dict[str, Any], name: str) -> dict[str, Any]:
    return (candidate.get("queries") or {}).get(name) or {}


def _has_executed_result(query: dict[str, Any]) -> bool:
    result = query.get("result")
    if not isinstance(result, dict):
        return False
    columns = result.get("columns")
    sample_rows = result.get("sample_rows")
    return isinstance(columns, list) and bool(columns) and isinstance(sample_rows, list)


def _dimension_type(spec: dict[str, Any], model_name: str, dimension_ref: str) -> str | None:
    if "." not in dimension_ref:
        return None
    _, dimension_name = dimension_ref.split(".", 1)
    for model in spec.get("models") or []:
        if model.get("name") != model_name:
            continue
        for dimension in model.get("dimensions") or []:
            if dimension.get("name") == dimension_name:
                return dimension.get("type")
    return None


def _is_non_id_dimension(spec: dict[str, Any], model_name: str, dimension_ref: str) -> bool:
    if "." not in dimension_ref:
        return False
    _, dimension_name = dimension_ref.split(".", 1)
    model = next((item for item in spec.get("models") or [] if item.get("name") == model_name), {})
    primary_key = model.get("primary_key")
    return bool(
        dimension_name
        and dimension_name != primary_key
        and dimension_name != "id"
        and not dimension_name.endswith("_id")
        and not dimension_name.endswith("_key")
        and not dimension_name.endswith("_uuid")
    )


def verify(args: argparse.Namespace) -> dict[str, Any]:
    app_dir = args.app_dir.resolve()
    spec_path = args.app_spec.resolve() if args.app_spec else app_dir / "data" / "app-spec.json"
    index_path = app_dir / "index.html"
    app_js_path = app_dir / "app.js"
    component_js_path = app_dir / "sidemantic-components.js"
    styles_path = app_dir / "styles.css"

    report: dict[str, Any] = {"checks": {}, "app_dir": str(app_dir), "app_spec": str(spec_path)}
    checks = report["checks"]

    checks["files_exist"] = all(
        path.exists() for path in (spec_path, index_path, app_js_path, component_js_path, styles_path)
    )
    if not checks["files_exist"]:
        return report

    spec = _load_json(spec_path)
    selected_model = _selected_model(index_path)
    report["selected_model"] = selected_model
    candidate = _candidate_for_model(spec, selected_model)
    checks["has_app_candidate"] = candidate is not None
    checks["selected_model_candidate"] = selected_model is None or (
        candidate is not None and candidate.get("model") == selected_model
    )
    if candidate is None:
        return report

    model_name = candidate.get("model")
    totals = _query(candidate, "metric_totals")
    leaderboard = _query(candidate, "dimension_leaderboard")

    checks["totals_executed"] = _has_executed_result(totals)
    checks["leaderboard_executed"] = _has_executed_result(leaderboard)
    checks["totals_true_total"] = (
        totals.get("result", {}).get("sample_row_count") == 1 and "group by" not in (totals.get("sql") or "").lower()
    )

    leaderboard_dimension = (leaderboard.get("dimensions") or [""])[0]
    dimension_type = _dimension_type(spec, model_name, leaderboard_dimension)
    checks["leaderboard_non_id"] = _is_non_id_dimension(spec, model_name, leaderboard_dimension)
    checks["leaderboard_categorical_or_boolean"] = dimension_type in ("categorical", "boolean")
    report["leaderboard_dimension"] = leaderboard_dimension
    report["leaderboard_dimension_type"] = dimension_type

    source = "\n".join(
        path.read_text(encoding="utf-8") for path in (index_path, app_js_path, component_js_path, styles_path)
    )
    checks["references_app_spec"] = "data/app-spec.json" in source
    checks["uses_copyable_components"] = "sidemantic-components.js" in source and ".sdm-metric-card" in source
    checks["has_metric_totals_selector"] = 'data-testid="metric-totals"' in source
    checks["has_leaderboard_selector"] = 'data-testid="dimension-leaderboard"' in source
    checks["has_leaderboard_rows_selector"] = 'data-testid="leaderboard-rows"' in source
    checks["has_metric_data_binding"] = "dataset.metric" in source or "data-metric" in source
    checks["has_dimension_data_binding"] = "dataset.dimension" in source or "data-dimension" in source
    checks["avoids_inner_html"] = "innerHTML" not in source
    checks["sparkline_bounded_if_present"] = ".sdm-sparkline" not in source or (
        "overflow: hidden" in source and 'setAttribute("viewBox"' in source
    )
    checks["no_persistent_state_gallery"] = not all(
        text in source
        for text in (
            "Loading: metrics are refreshing",
            "Empty: no rows for the current filter set",
            "Error: query failed",
        )
    )

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("app_dir", type=Path, help="Static app directory to verify")
    parser.add_argument("--app-spec", type=Path, help="App spec JSON; defaults to app_dir/data/app-spec.json")
    args = parser.parse_args()

    report = verify(args)
    print(json.dumps(report, indent=2, sort_keys=True))
    failed = [name for name, passed in report.get("checks", {}).items() if not passed]
    if failed:
        print("Verification failed: " + ", ".join(failed), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
