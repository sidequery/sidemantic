#!/usr/bin/env python3
"""Scaffold a minimal static Sidemantic dashboard from copyable components."""

from __future__ import annotations

import argparse
import html
import json
import shutil
from pathlib import Path
from typing import Any

SKILL_ROOT = Path(__file__).resolve().parents[1]
STATIC_COMPONENT_ROOT = SKILL_ROOT / "assets" / "components" / "static"
STATIC_TEMPLATE_ROOT = SKILL_ROOT / "assets" / "templates" / "static-dashboard"


def _select_candidate(spec: dict[str, Any], model: str | None) -> dict[str, Any]:
    candidates = spec.get("app_candidates") or []
    if not candidates:
        raise ValueError("App spec has no app_candidates")
    if model is None:
        return candidates[0]
    for candidate in candidates:
        if candidate.get("model") == model:
            return candidate
    raise ValueError(f"Model {model!r} not found in app_candidates")


def _require_query(candidate: dict[str, Any], name: str) -> dict[str, Any]:
    query = (candidate.get("queries") or {}).get(name)
    if not query:
        raise ValueError(f"Candidate {candidate.get('model')} has no {name} query")
    result = query.get("result")
    if not result or "columns" not in result or "sample_rows" not in result:
        raise ValueError(f"{name} query has no executed result. Re-run inspect_layer.py with --require-execute.")
    return query


def _render_template(template_name: str, replacements: dict[str, str]) -> str:
    template_path = STATIC_TEMPLATE_ROOT / template_name
    content = template_path.read_text(encoding="utf-8")
    for token, value in replacements.items():
        content = content.replace("{{" + token + "}}", value)
    return content


def _write_index(path: Path, title: str) -> None:
    path.write_text(_render_template("index.html", {"TITLE": html.escape(title)}), encoding="utf-8")


def _write_app(path: Path) -> None:
    path.write_text(_render_template("app.js", {}), encoding="utf-8")


def scaffold(args: argparse.Namespace) -> None:
    spec_path = args.app_spec.resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    candidate = _select_candidate(spec, args.model)
    _require_query(candidate, "metric_totals")
    _require_query(candidate, "dimension_leaderboard")

    output_dir = args.output.resolve()
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    shutil.copyfile(spec_path, data_dir / "app-spec.json")
    _write_index(output_dir / "index.html", args.title or f"{candidate['model']} Dashboard")
    shutil.copyfile(STATIC_COMPONENT_ROOT / "sidemantic-components.css", output_dir / "styles.css")
    shutil.copyfile(STATIC_COMPONENT_ROOT / "sidemantic-components.js", output_dir / "sidemantic-components.js")
    _write_app(output_dir / "app.js")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("app_spec", type=Path, help="Executed app spec JSON from inspect_layer.py --execute")
    parser.add_argument("--output", "-o", type=Path, required=True, help="Output directory for the static app")
    parser.add_argument("--model", help="Model candidate to scaffold; defaults to the first app candidate")
    parser.add_argument("--title", help="Dashboard title")
    args = parser.parse_args()

    scaffold(args)
    print(f"Wrote static app to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
