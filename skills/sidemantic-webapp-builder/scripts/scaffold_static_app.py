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


def _write_index(path: Path, title: str) -> None:
    safe_title = html.escape(title)
    path.write_text(
        f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{safe_title}</title>
    <link rel="stylesheet" href="styles.css" />
  </head>
  <body>
    <main class="sdm-shell">
      <header class="sdm-shell__header">
        <div>
          <p class="sdm-eyebrow">Sidemantic</p>
          <h1>{safe_title}</h1>
        </div>
        <p class="sdm-status" data-testid="app-status">Loading app spec...</p>
      </header>
      <section class="sdm-metric-grid" data-testid="metric-totals"></section>
      <section class="sdm-leaderboard" data-testid="dimension-leaderboard">
        <div class="sdm-section-heading">
          <h2 data-testid="leaderboard-title">Leaderboard</h2>
          <p data-testid="leaderboard-subtitle"></p>
        </div>
        <div data-testid="leaderboard-rows"></div>
      </section>
      <details class="sdm-debug-panel">
        <summary>Generated SQL</summary>
        <pre data-testid="query-debug"></pre>
      </details>
    </main>
    <script type="module" src="app.js"></script>
  </body>
</html>
""",
        encoding="utf-8",
    )


def _write_app(path: Path) -> None:
    path.write_text(
        """import { renderLeaderboard, renderMetricCards, renderQueryDebug } from "./sidemantic-components.js";

const statusEl = document.querySelector('[data-testid="app-status"]');
const totalsEl = document.querySelector('[data-testid="metric-totals"]');
const leaderboardEl = document.querySelector('[data-testid="leaderboard-rows"]');
const leaderboardTitleEl = document.querySelector('[data-testid="leaderboard-title"]');
const leaderboardSubtitleEl = document.querySelector('[data-testid="leaderboard-subtitle"]');
const debugEl = document.querySelector('[data-testid="query-debug"]');

async function main() {
  const response = await fetch("data/app-spec.json");
  if (!response.ok) throw new Error(`Failed to load app spec: ${response.status}`);
  const spec = await response.json();
  const candidate = spec.app_candidates?.[0];
  if (!candidate) throw new Error("App spec has no app candidates");
  const queries = candidate.queries || {};

  renderMetricCards(totalsEl, queries.metric_totals);
  renderLeaderboard(leaderboardEl, queries.dimension_leaderboard, {
    titleEl: leaderboardTitleEl,
    subtitleEl: leaderboardSubtitleEl,
  });
  renderQueryDebug(debugEl, {
    metric_totals: queries.metric_totals,
    dimension_leaderboard: queries.dimension_leaderboard,
  });
  statusEl.textContent = `${candidate.model} ready`;
}

main().catch((error) => {
  statusEl.textContent = error.message;
  statusEl.dataset.error = "true";
});
""",
        encoding="utf-8",
    )


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
