"""Regression coverage for bundled Sidemantic webapp builder scripts."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace


def _load_script_module(script_name: str, module_name: str):
    path = (
        Path(__file__).resolve().parents[1]
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "scripts"
        / script_name
    )
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


class _Generator:
    def generate(self, *, metrics, dimensions, **_kwargs):
        return f"select {', '.join([*dimensions, *metrics])}"


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


def test_leaderboard_dimension_honors_explicit_identifier_like_dimension() -> None:
    module = _load_script_module("inspect_layer.py", "sidemantic_webapp_builder_inspect_layer_dimensions")
    model = SimpleNamespace(
        table="events",
        primary_key="id",
        metrics=[SimpleNamespace(name="count")],
        dimensions=[
            SimpleNamespace(name="category", type="categorical"),
            SimpleNamespace(name="user_id", type="categorical"),
        ],
    )

    candidate = module._candidate_for_model(
        _Generator(),
        SimpleNamespace(),
        "events",
        model,
        max_metrics=1,
        max_dimensions=12,
        execute=False,
        sample_rows=5,
        leaderboard_dimension="events.user_id",
    )

    assert candidate["default_leaderboard_dimension"] == "events.user_id"
    assert candidate["explicit_leaderboard_dimension"] is True
    assert candidate["recommended_dimensions"][0] == "events.user_id"
    assert candidate["available_leaderboard_dimensions"][0]["identifier_like"] is True
    assert candidate["queries"]["dimension_leaderboard"]["dimensions"] == ["events.user_id"]


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


def _leaderboard_query(model: str, sample_rows: list[dict[str, object]] | None = None):
    rows = [{"category": "A", "count": 1}] if sample_rows is None else sample_rows
    return {
        "metrics": [f"{model}.count"],
        "dimensions": [f"{model}.category"],
        "sql": f"select category, count(*) as count from {model} group by category",
        "result": {
            "columns": ["category", "count"],
            "sample_rows": rows,
            "sample_row_count": len(rows),
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
                "connection": "postgresql://user:secret@example.com/warehouse",
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
                        "connection": "postgresql://nested:secret@example.com/warehouse",
                        "queries": {
                            "metric_totals": _metric_totals_query("requested_model"),
                            "dimension_leaderboard": _leaderboard_query("requested_model", sample_rows=[]),
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
    public_spec = json.loads((output_dir / "data" / "app-spec.json").read_text(encoding="utf-8"))

    assert 'data-model="requested_model"' in index_html
    assert "candidates.find((item) => item.model === selectedModel)" in app_js
    assert 'data-testid="filter-pills"' in index_html
    assert 'data-action="reset"' in index_html
    assert 'data-testid="data-preview"' in index_html
    assert "interactive: true" in app_js
    assert "onSelect: setFilter" in app_js
    assert "metricTotalsForFilters" in app_js
    assert "renderMetricCards(totalsEl, filteredTotals" in app_js
    assert "toggleFilterValue" in app_js
    assert "selectedValues: selectedLeaderboardValues" in app_js
    assert "removeFilterValue" in app_js
    assert "filterZeroMetricRows" in app_js
    assert 'renderFilterPills(filterPillsEl, state.filters, removeFilter, { emptyLabel: "No filters" })' in app_js
    assert "renderHighlightedQueryDebug" in app_js
    assert "connection" not in public_spec
    assert "connection" not in public_spec["app_candidates"][1]

    report = verify_module.verify(SimpleNamespace(app_dir=output_dir, app_spec=None))
    assert report["selected_model"] == "requested_model"
    assert all(report["checks"].values())


def test_interaction_verifier_waits_for_rendered_metric_cards() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "scripts"
        / "verify_static_interactions.mjs"
    )
    source = path.read_text(encoding="utf-8")

    assert "waitForRenderedDashboard" in source
    assert "'[data-testid=\"metric-totals\"] [data-metric]'" in source
    assert "assertRequiredInteraction" in source
    assert "single metric card" in source
    assert "firstUnselectedIndex" in source
    assert "waitForSnapshotChange" in source
    assert "page.waitForTimeout" in source
    assert source.index("const leaderboard = await clickLeaderboardRow") < source.index(
        "const filter = await clickFirstFilterRemove"
    )
    leaderboard_block = source[
        source.index("async function clickLeaderboardRow") : source.index("async function clickMetricCard")
    ]
    leaderboard_assertion = leaderboard_block[
        leaderboard_block.index(
            'await waitForSnapshotChange(\n    "Clicking a leaderboard row"'
        ) : leaderboard_block.index("return { skipped: false, before, after };")
    ]
    assert '"selectedRowCount"' not in leaderboard_assertion
    assert "rows.nth(rowIndex).click" in leaderboard_block

    metric_block = source[source.index("async function clickMetricCard") : source.index("async function clickReset")]
    assert "metrics.nth(metricIndex).click" in metric_block


def test_inspector_leaderboard_query_includes_all_selected_metrics() -> None:
    module = _load_script_module("inspect_layer.py", "sidemantic_webapp_builder_inspect_layer_metric_set")
    model = SimpleNamespace(
        table="events",
        primary_key="id",
        metrics=[SimpleNamespace(name="count"), SimpleNamespace(name="revenue")],
        dimensions=[SimpleNamespace(name="category", type="categorical")],
    )

    candidate = module._candidate_for_model(
        _Generator(),
        SimpleNamespace(),
        "events",
        model,
        max_metrics=2,
        max_dimensions=12,
        execute=False,
        sample_rows=5,
        leaderboard_dimension=None,
    )

    assert candidate["queries"]["dimension_leaderboard"]["metrics"] == ["events.count", "events.revenue"]


def test_static_verifier_allows_explicit_identifier_leaderboard_dimension(tmp_path: Path) -> None:
    scaffold_module = _load_script_module(
        "scaffold_static_app.py", "sidemantic_webapp_builder_scaffold_static_app_explicit_id"
    )
    verify_module = _load_script_module(
        "verify_static_app.py", "sidemantic_webapp_builder_verify_static_app_explicit_id"
    )
    spec_path = tmp_path / "app-spec.json"
    output_dir = tmp_path / "dashboard"
    spec_path.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "name": "events",
                        "primary_key": "id",
                        "dimensions": [{"name": "user_id", "type": "categorical"}],
                    }
                ],
                "app_candidates": [
                    {
                        "model": "events",
                        "explicit_leaderboard_dimension": True,
                        "queries": {
                            "metric_totals": _metric_totals_query("events"),
                            "dimension_leaderboard": {
                                **_leaderboard_query("events"),
                                "dimensions": ["events.user_id"],
                                "output_aliases": {"events.user_id": "category", "events.count": "count"},
                            },
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    scaffold_module.scaffold(
        SimpleNamespace(
            app_spec=spec_path,
            model="events",
            output=output_dir,
            title=None,
        )
    )

    report = verify_module.verify(SimpleNamespace(app_dir=output_dir, app_spec=None))
    assert report["explicit_leaderboard_dimension"] is True
    assert report["checks"]["leaderboard_non_id"] is True


def _obsolete_test_column_chart_components_support_negative_values() -> None:
    root = (
        Path(__file__).resolve().parents[1]
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "assets"
        / "components"
    )
    static_source = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")
    static_css = (root / "static" / "sidemantic-components.css").read_text(encoding="utf-8")
    react_source = (root / "react-tailwind" / "column-chart.tsx").read_text(encoding="utf-8")

    for source in (static_source, react_source):
        assert "Math.min(0, ...values)" in source
        assert "baselineY" in source
        assert "Math.abs(valueY - baselineY)" in source
        assert 'data-tone={value < 0 ? "negative" : "positive"}' in source or (
            'rect.dataset.tone = value < 0 ? "negative" : "positive";' in source
        )

    assert '.sdm-column-chart rect[data-tone="negative"]' in static_css


def _obsolete_test_metric_sparklines_visually_distinguish_selected_state() -> None:
    root = (
        Path(__file__).resolve().parents[1]
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "assets"
        / "components"
    )
    static_css = (root / "static" / "sidemantic-components.css").read_text(encoding="utf-8")

    assert '.sdm-metric-card[data-selected="true"] .sdm-sparkline-wrap' in static_css
    assert '.sdm-metric-card[data-selected="true"] .sdm-sparkline__area' in static_css
    assert '.sdm-metric-card[data-selected="true"] .sdm-sparkline__line' in static_css
    assert "stroke: #64748b;" in static_css
    assert "stroke: var(--sdm-accent);" in static_css


def _obsolete_test_static_component_aliases_preserve_time_grain_suffixes() -> None:
    root = Path(__file__).resolve().parents[1]
    component_paths = [
        root
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "assets"
        / "components"
        / "static"
        / "sidemantic-components.js",
        root / "examples" / "sidemantic_wasm_demo" / "src" / "components" / "sidemantic" / "sidemantic-components.js",
        root / "examples" / "sidemantic_wasm_demo" / "src" / "queries.js",
    ]

    for path in component_paths:
        source = path.read_text(encoding="utf-8")
        if path.name == "sidemantic-components.js":
            source += (path.parent / "ui-core.js").read_text(encoding="utf-8")

        assert '.replace("__", "_")' not in source
        assert '.replaceAll("__", "_")' not in source
        assert 'String(ref || "").split(".").at(-1);' in source


def _obsolete_test_static_filter_helpers_normalize_nullish_values() -> None:
    root = Path(__file__).resolve().parents[1]
    component_paths = [
        root
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "assets"
        / "components"
        / "static"
        / "sidemantic-components.js",
        root / "examples" / "sidemantic_wasm_demo" / "src" / "components" / "sidemantic" / "sidemantic-components.js",
    ]

    for path in component_paths:
        source = path.read_text(encoding="utf-8")
        core = (path.parent / "ui-core.js").read_text(encoding="utf-8")

        assert "export function normalizeFilterValue(value)" in core
        assert 'return String(value ?? "");' in core
        assert "const normalized = normalizeFilterValue(value);" in core
        assert ".map(normalizeFilterValue)" in core
        assert "const dimensionValue = normalizeFilterValue(row[dimensionKey]);" in source
        assert "selectedValues.has(dimensionValue)" in source
        assert "const stringValue = String(value);" not in source

    static_app = (
        root
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "assets"
        / "templates"
        / "static-dashboard"
        / "app.js"
    ).read_text(encoding="utf-8")
    assert "normalizeFilterValue," in static_app
    assert "new Set((values || []).map(normalizeFilterValue))" in static_app
    assert "accepted.has(normalizeFilterValue(row[key]))" in static_app
    assert "new Set(filterValues.map(normalizeFilterValue))" in static_app
    assert "accepted.has(normalizeFilterValue(row[dimensionKey]))" in static_app


def _obsolete_test_leaderboard_components_support_negative_values() -> None:
    root = (
        Path(__file__).resolve().parents[1]
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "assets"
        / "components"
    )
    static_source = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")
    static_css = (root / "static" / "sidemantic-components.css").read_text(encoding="utf-8")
    react_source = (root / "react-tailwind" / "leaderboard.tsx").read_text(encoding="utf-8")

    for source in (static_source, react_source):
        assert "maxMagnitude" in source
        assert "Math.abs(" in source

    assert 'item.dataset.tone = value < 0 ? "negative" : "positive";' in static_source
    assert '.sdm-leaderboard-row[data-tone="negative"]::before' in static_css
    assert 'const tone = metricValue < 0 ? "negative" : "positive";' in react_source
    assert "data-tone={tone}" in react_source
    assert "selectedValues?: string[]" in react_source


def _components_root() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "assets"
        / "components"
    )


def _obsolete_test_line_chart_exists_in_both_implementations() -> None:
    root = _components_root()
    react = (root / "react-tailwind" / "line-chart.tsx").read_text(encoding="utf-8")
    static = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")
    css = (root / "static" / "sidemantic-components.css").read_text(encoding="utf-8")
    index = (root / "react-tailwind" / "index.ts").read_text(encoding="utf-8")

    assert "export function LineChart(" in react
    assert "export function renderLineChart(" in static
    assert ".sdm-line-chart__line" in css
    assert "./line-chart" in index


def _obsolete_test_value_formatting_parity() -> None:
    root = _components_root()
    react = (root / "react-tailwind" / "types.ts").read_text(encoding="utf-8")
    static = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")
    core = (root / "static" / "ui-core.js").read_text(encoding="utf-8")

    assert "metricValueFormat" in react
    assert "metricValueFormat" in static
    assert 'style: "currency"' in react
    assert "formatUiValue as formatValue" in static
    assert 'style: currency ? "currency" : "decimal"' in core


def _obsolete_test_query_debug_highlighting_parity() -> None:
    root = _components_root()
    react = (root / "react-tailwind" / "query-debug-panel.tsx").read_text(encoding="utf-8")
    static = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")

    assert "SQL_KEYWORDS" in react
    assert "date_trunc" in react
    assert "renderHighlightedQueryDebug" in static
    assert "SQL_KEYWORDS" in static


def _obsolete_test_preview_pagination_parity() -> None:
    root = _components_root()
    react = (root / "react-tailwind" / "data-preview-table.tsx").read_text(encoding="utf-8")
    static = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")
    css = (root / "static" / "sidemantic-components.css").read_text(encoding="utf-8")

    assert "pageSize" in react
    assert 'data-testid="data-preview-pager"' in react
    assert "pageSize" in static
    assert "sdm-data-preview__pager" in static
    assert ".sdm-data-preview__pager" in css


def _obsolete_test_leaderboard_expand_parity() -> None:
    root = _components_root()
    react = (root / "react-tailwind" / "leaderboard.tsx").read_text(encoding="utf-8")
    static = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")

    for source in (react, static):
        assert "leaderboard-expand" in source
        assert "leaderboard-back" in source
        assert "extraColumns" in source


def _obsolete_test_state_trio_parity() -> None:
    root = _components_root()
    react = (root / "react-tailwind" / "states.tsx").read_text(encoding="utf-8")
    static = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")

    assert "LoadingState" in react and "EmptyState" in react and "ErrorState" in react
    assert 'state.kind === "loading"' in static
    assert "sdm-loading-state" in static


def _obsolete_test_chart_axes_and_tooltips_parity() -> None:
    root = _components_root()
    react_line = (root / "react-tailwind" / "line-chart.tsx").read_text(encoding="utf-8")
    react_col = (root / "react-tailwind" / "column-chart.tsx").read_text(encoding="utf-8")
    static = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")
    css = (root / "static" / "sidemantic-components.css").read_text(encoding="utf-8")

    for source in (react_line, react_col):
        assert "axisTicks" in source
        assert "formatCompact" in source
        assert "ResizeObserver" in source
        assert 'role="img"' in source
        assert "useChartTooltip" in source

    assert "renderLineChart" in static and "renderColumnChart" in static
    assert "export function axisTicks(" in static
    assert "ResizeObserver" in static
    assert "bindChartTooltip" in static
    assert 'svg.setAttribute("role", "img")' in static
    assert ".sdm-chart-tooltip" in css
    assert ".sdm-chart__grid" in css


def _obsolete_test_sparkline_area_and_a11y_parity() -> None:
    root = _components_root()
    react = (root / "react-tailwind" / "sparkline.tsx").read_text(encoding="utf-8")
    static = (root / "static" / "sidemantic-components.js").read_text(encoding="utf-8")
    css = (root / "static" / "sidemantic-components.css").read_text(encoding="utf-8")

    assert "fill-slate-500/10" in react
    assert 'role="img"' in react
    assert "useChartTooltip" in react
    assert "sdm-sparkline__area" in static
    assert "sdm-sparkline__dot" in css
    assert 'svg.setAttribute("role", "img")' in static


def _obsolete_test_wasm_demo_static_components_match_canonical_skill_assets() -> None:
    root = Path(__file__).resolve().parents[1]
    canonical = root / "plugins" / "sidemantic" / "skills" / "webapp-builder" / "assets" / "components" / "static"
    wasm_copy = root / "examples" / "sidemantic_wasm_demo" / "src" / "components" / "sidemantic"

    for filename in ("sidemantic-components.js", "sidemantic-components.css", "ui-core.js"):
        assert (wasm_copy / filename).read_bytes() == (canonical / filename).read_bytes()


def _obsolete_test_react_tooltip_distributable_matches_product_canonical_source() -> None:
    root = Path(__file__).resolve().parents[1]
    canonical = root / "webapp" / "src" / "components" / "ChartTooltip.tsx"
    distributable = (
        root
        / "plugins"
        / "sidemantic"
        / "skills"
        / "webapp-builder"
        / "assets"
        / "components"
        / "react-tailwind"
        / "chart-tooltip.tsx"
    )
    assert distributable.read_bytes() == canonical.read_bytes()


def _obsolete_test_component_copy_check_detects_and_repairs_drift(tmp_path: Path) -> None:
    module = _load_script_module("copy_components.py", "sidemantic_webapp_builder_copy_components")
    args = SimpleNamespace(kind="static", components=["all"], target=tmp_path, force=False, dry_run=False)

    assert len(module.check_components(args)) == 3
    module.copy_components(args)
    assert module.check_components(args) == []

    copied_js = tmp_path / "sidemantic-components.js"
    copied_js.write_text("// drifted\n", encoding="utf-8")
    assert module.check_components(args) == [copied_js]
