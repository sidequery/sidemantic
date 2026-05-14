---
name: sidemantic-webapp-builder
description: Build interactive analytics webapps, demos, dashboards, or embedded app surfaces from Sidemantic semantic models using copyable component primitives and deterministic query inspection. Use when asked to create a web UI around Sidemantic models, generate a metric explorer, copy reusable analytics components into a project, connect a frontend to Sidemantic query APIs, build a Pyodide/DuckDB-WASM demo, expose Sidemantic through an app server, or adapt the Sidemantic widget/UI patterns into a product webapp.
---

# Sidemantic Webapp Builder

Build webapps around a validated Sidemantic semantic layer. Default to project-owned source components copied from this skill, then adapt them to the target app and wire them to inspected Sidemantic query contracts.

## Component-First Pattern

Treat `assets/components/` like a small shadcn-style source library for analytics primitives. Copy components into the target project, then edit those copied files as normal app code. Do not retype component source into the answer or keep it as a hidden runtime dependency.

Copy React + Tailwind components for product apps:

```bash
uv run skills/sidemantic-webapp-builder/scripts/copy_components.py \
  --kind react-tailwind \
  --target src/components/sidemantic
```

Copy the static kit for plain HTML demos or generated scaffolds:

```bash
uv run skills/sidemantic-webapp-builder/scripts/copy_components.py \
  --kind static \
  --target public/sidemantic-components
```

Use `--component metric-card --component leaderboard` to copy a narrower React subset. Use `--list` before copying when you need the available names. Existing target files are never overwritten unless `--force` is passed.

Available primitives:

- `DashboardShell`: dense analytics page frame with status and toolbar slots.
- `MetricCard`: metric label, value, delta, loading, selected state.
- `Leaderboard`: ranked dimension rows with bars, selection, and stable data attributes.
- `FilterPill`: active filter display and removal.
- `Sparkline`: small SVG trend line.
- `ColumnChart`: compact categorical bars for comparisons.
- `QueryDebugPanel`: generated SQL/debug surface.
- `DataPreviewTable`: stable sample row preview.
- `LoadingState`, `EmptyState`, `ErrorState`: fixed-height status surfaces.

State primitives are conditional UI branches. Do not render loading, empty, and error examples as permanent app content unless the user explicitly asks for a component gallery.

## Core Workflow

1. Load-check the semantic layer before building UI. Use `info` and the inspector in noninteractive agent work. Use `validate` only when the current CLI exits cleanly in your environment:

```bash
uv run sidemantic info path/to/models
uv run skills/sidemantic-webapp-builder/scripts/inspect_layer.py path/to/models \
  --db path/to/data.duckdb \
  --require-execute
```

2. Generate an app inventory:

```bash
uv run skills/sidemantic-webapp-builder/scripts/inspect_layer.py path/to/models \
  --db path/to/data.duckdb \
  --require-execute \
  --output docs/sidemantic-app-spec.json
```

Use `--leaderboard-dimension field_name` when domain judgment says one dimension should drive the first leaderboard. Without it, the inspector prefers common categorical dimensions over identifiers and booleans.

3. Copy component source into the project before building UI:

```bash
uv run skills/sidemantic-webapp-builder/scripts/copy_components.py \
  --kind react-tailwind \
  --target src/components/sidemantic
```

Adapt imports, class names, and styling conventions after copying. Preserve the data contract conventions: `data-metric`, `data-dimension`, `data-value`, and `data-testid` hooks for metric totals, dimension leaderboards, and query debug surfaces.

For a minimal static app scaffold from the executed spec:

```bash
uv run skills/sidemantic-webapp-builder/scripts/scaffold_static_app.py \
  docs/sidemantic-app-spec.json \
  --output dist/sidemantic-dashboard \
  --title "Metrics Dashboard"
```

The scaffold copies readable source from `assets/templates/static-dashboard/` and the static component kit. If you need a richer generated app, edit those copied source files in the target project; do not bury application JavaScript in Python strings or generated HTML fragments.

4. Choose the app shape:

- Existing app: follow its framework, routing, styling, and data-fetch patterns.
- New product webapp: use Bun, React Router v7 as framework, Tailwind v3, and Hono only when a TypeScript API/proxy is needed.
- Python-backed analytics app: use `sidemantic.api_server.create_app()` or `start_api_server()` when a FastAPI API is acceptable.
- Browser-only demo: use Pyodide + DuckDB-WASM only for static demos or docs pages that must run without a backend.
- Notebook or Python embedded view: use `sidemantic.widget.MetricsExplorer` instead of rebuilding the widget.
- MCP app surface: use `sidemantic mcp-serve --apps --http --port 4100` and existing chart resources when the target is an MCP Apps-compatible host.

5. Implement a narrow query contract. Prefer structured query payloads over ad hoc SQL strings:

```json
{
  "metrics": ["orders.revenue"],
  "dimensions": ["orders.order_date__day"],
  "filters": ["orders.status = 'completed'"],
  "order_by": ["orders.order_date__day"],
  "limit": 500
}
```

6. Build the UI around the copied components and query contract:

- Metric cards: aggregate value, compact sparkline, selected state.
- Dimension leaderboards: top values for the selected metric, horizontal bars, click-to-filter.
- Filter pills: active dimension filters plus brush/date-range filters, removable.
- Time controls: date range, grain select, brushable sparklines when a time dimension exists.
- Optional debug surfaces: generated SQL, raw rows preview, query timing. Use these in demos and internal tools, not as default product chrome.

If a control is visible, it must change the app state or data. Do not satisfy interaction requirements by only changing a status label. Removing a filter must recompute metric cards, leaderboards, charts, and preview rows. Clicking a leaderboard row must add or toggle a filter. Selecting a metric must change the leaderboard ranking metric.

7. Verify end to end:

```bash
uv run sidemantic info path/to/models
uv run skills/sidemantic-webapp-builder/scripts/inspect_layer.py path/to/models --db path/to/data.duckdb --require-execute
uv run sidemantic query "SELECT metric_name FROM model_name LIMIT 5" --models path/to/models --db path/to/data.duckdb
uv run skills/sidemantic-webapp-builder/scripts/verify_static_app.py dist/sidemantic-dashboard
bunx --bun -p playwright node skills/sidemantic-webapp-builder/scripts/verify_static_interactions.mjs --url http://127.0.0.1:5174/
bun run build
```

For frontend changes, run the app on a 4xxx-5xxx port and verify with browser screenshots at desktop and mobile widths. If browser tooling is unavailable, run `verify_static_app.py` or another deterministic DOM/data check and state that real browser visual verification was not run.

For interactive dashboards, verify behavior, not just render counts:

- Remove a filter pill and confirm at least one metric value changes.
- Click a leaderboard row and confirm metrics, chart bars, selected row state, and preview rows reflect that value.
- Click the same active leaderboard row or remove its pill and confirm the broader result set returns.
- Select a different metric card and confirm the leaderboard ranking metric changes.
- Confirm visible sparklines and column charts stay clipped inside their cards at desktop and mobile widths.

For static dashboards that use the bundled component contracts, use the smoke-test script after starting a local server:

```bash
bunx --bun -p playwright playwright install chromium  # first run only, if Playwright reports a missing browser
bunx --bun -p playwright node skills/sidemantic-webapp-builder/scripts/verify_static_interactions.mjs \
  --url http://127.0.0.1:4519/
```

The script clicks filter pills, leaderboard rows, metric cards, and reset controls, and fails if visible data does not change.

## Query Patterns

Use the generated app spec first. For deeper implementation details, read `references/webapp-patterns.md`.

Default query set:

- Time series: selected metrics grouped by `model.time_dimension__grain`, ordered by time, capped around 500 points.
- Totals: selected metrics with no dimensions.
- Dimension leaderboard: selected metric grouped by one dimension, ordered descending, capped to 5-10 rows.
- Preview table: ungrouped/raw rows only when the app needs a data inspector.

Use `inspect_layer.py --require-execute` when a database is available. This adds `result.columns`, `result.sample_rows`, and `sample_row_count` to each compiled query and exits nonzero if execution is missing or fails. Use plain `--execute` only when a warning is acceptable.

For crossfilter leaderboards, exclude the dimension's own filter while querying that same dimension. If `device_os = iOS` is active, the device OS card should still show peer OS values while other cards show values within iOS.

Use explicit aliases at API boundaries so UI column names stay stable. The inspector emits `output_aliases` and, with `--execute`, actual result columns. Do not make display components depend on database-specific column casing or quoted identifiers.

Run DuckDB validation serially against a file database. Do not run the inspector and `sidemantic query` concurrently against the same `.duckdb` path; DuckDB file locks can make valid workflows fail.

## API Modes

When the app can call Python directly, prefer the existing HTTP API:

- `GET /health`
- `GET /models`
- `GET /graph`
- `POST /compile`
- `POST /query?format=json`
- `POST /query?format=arrow`
- `POST /sql`

When building a TypeScript frontend with a separate backend, keep Sidemantic execution in Python unless the project already has a stable Python service. A Hono server can proxy to the Sidemantic API, add auth/session context, and normalize responses.

Never concatenate user-entered filter values into SQL in the frontend. Pass structured filter values to a server-side query builder or quote them with the same rules as Sidemantic/widget code.

The CLI `sidemantic query` auto-adds default time dimensions for metrics when a model has `default_time_dimension`. For exact app query shapes like true totals, prefer the inspector-generated SQL/result samples or the Python/API structured query path using `skip_default_time_dimensions=True` internally.

## Bundled Scripts

- `scripts/inspect_layer.py`: inspect models, compile app query shapes, execute samples with `--execute`, or require execution with `--require-execute`.
- `scripts/copy_components.py`: copy React + Tailwind or static component source from `assets/components/` into a project.
- `scripts/scaffold_static_app.py`: create a small static dashboard from an executed app spec by copying templates and components. It writes `index.html`, `styles.css`, `sidemantic-components.js`, `app.js`, and `data/app-spec.json`.
- `scripts/verify_static_app.py`: dependency-free fallback verifier for static dashboards. It checks files, executed result samples, true totals, non-id leaderboard dimensions, and expected DOM/data bindings.
- `scripts/verify_static_interactions.mjs`: Playwright smoke test for standard static component contracts. It verifies real data changes for filter, leaderboard, metric, reset, and chart-bounds behavior.

## Bundled Assets

- `assets/components/react-tailwind/`: copyable React source for analytics apps using Tailwind v3.
- `assets/components/static/`: copyable plain JS/CSS kit for generated demos and no-build static pages.
- `assets/templates/static-dashboard/`: readable static app templates used by `scaffold_static_app.py`.

After copying assets into a project, treat them as that project's code. Modify them to match local component APIs, naming, tests, and design system constraints.

## Browser-Only Demos

Use browser-only Pyodide + DuckDB-WASM for static demos, docs, and shareable examples. Preserve these constraints:

- Install Sidemantic into Pyodide with dependency constraints that match the repo's Pyodide rules.
- Keep large data files out of git. Download or cache Parquet at build/runtime.
- Generate SQL in Pyodide; execute data queries in DuckDB-WASM.
- Show loading progress and skeletons because Pyodide and DuckDB-WASM initialization is visible to users.
- Test in a real browser. Static HTML that imports WASM/CDN modules often cannot be trusted from file-only inspection.

## Design Rules

Analytics webapps should feel work-focused:

- Dense, scannable layouts beat marketing sections.
- Do not add hero pages unless the requested artifact is a public landing page.
- Avoid nested cards. Use full-width tool surfaces, tables, panels, and repeated item cards only where they represent actual data units.
- Use stable dimensions for cards, grids, sparklines, toolbar controls, and result panes to avoid layout shift during loading.
- Keep text small and container-appropriate inside dashboards.
- Use existing app colors/components first. For net-new Sidemantic demos, use a restrained neutral UI with a single accent and clear positive/negative colors.

## Common Failures

- Building UI before the model validates. Validate first.
- Running interactive validation in automation. If `sidemantic validate` requires `textual` or opens a TUI, use `sidemantic info` plus `inspect_layer.py` as the noninteractive check.
- Trusting compiled SQL alone. Use `inspect_layer.py --require-execute` when possible so result columns and sample rows are checked and failures are nonzero.
- Running parallel DuckDB checks against the same database file. Run them serially or use separate database copies.
- Treating Python API examples as the default user path. Sidemantic is CLI-first; use API calls as app internals.
- Missing a time dimension. Fall back to totals and dimension leaderboards, and omit brush/grain controls.
- Letting high-cardinality dimensions dominate the UI. Cap rows, rank by selected metric, and let users search only if needed.
- Filtering a dimension leaderboard by its own active value. Use self-filter exclusion.
- Fake interactivity. A control that only updates a status label is not done; it must change filters, selected metric, query payload, or rendered rows.
- Calling a component gallery an app. A kitchen-sink example can show primitives, but if it has dashboard controls, they must drive real local or server-backed state.
- Rendering loading, empty, and error states as persistent content in a dashboard. Show state components only for their actual branch, or label the surface as a component gallery.
- Letting SVG charts paint outside cards. Use bounded `viewBox`, padding, and `overflow: hidden`; verify with screenshots.
- Pulling optional dependencies into core imports. Keep web/API/widget dependencies lazy and optional.
- Using ports `3000` or `8000` in worktrees. Prefer `4100`, `4400`, `5174`, or another available 4xxx-5xxx port.
- Opening static HTML with `file://` when it fetches JSON/CSV. Serve it locally instead, because browser file-scheme fetch behavior differs from a real app.
