# Sidemantic Webapp Patterns

Use this reference when implementing the concrete UI/data layer after the skill workflow has selected an app shape.

## Copyable Components

Default to copied component source instead of regenerating dashboard primitives. Copy from the skill, then edit the copied files inside the target project:

```bash
uv run skills/sidemantic-webapp-builder/scripts/copy_components.py \
  --kind react-tailwind \
  --target src/components/sidemantic
```

For static or no-build demos:

```bash
uv run skills/sidemantic-webapp-builder/scripts/copy_components.py \
  --kind static \
  --target dist/sidemantic-dashboard
```

The React kit is intentionally style-light and contract-heavy. Keep these contracts unless the target project already has stronger equivalents:

- `data-metric` on metric cards.
- `data-dimension` and `data-value` on leaderboard rows and filter pills.
- `data-testid="metric-totals"` around aggregate cards.
- `data-testid="dimension-leaderboard"` and `data-testid="leaderboard-rows"` around ranked dimensions.
- `data-testid="query-debug"` for generated SQL/debug output.
- Use `Sparkline` for compact time trends and `ColumnChart` for categorical metric comparisons.

Prefer copying all primitives first for a new dashboard, then deleting unused files after the app shape is clear. Copy a subset only when fitting into an established component system.

## Proven App Shapes

Prior Sidemantic webapps converged on three useful forms:

1. Product webapp with backend API: frontend sends structured query payloads; Python/Sidemantic compiles and executes SQL; frontend renders JSON or Arrow results.
2. Static browser demo: Pyodide generates SQL from Sidemantic models; DuckDB-WASM executes local Parquet queries; vanilla JS or a small frontend renders the explorer.
3. Python widget/notebook surface: Python executes Sidemantic queries; JS receives Arrow IPC and syncs state with traitlets.

Default to app shape 1 for real apps. Use shape 2 only when backendless distribution is the point. Use shape 3 inside notebooks.

## Metric Explorer UI

Core sections:

- Header/filter row: date range, active filter pills, selected metric, time grain, refresh/debug controls only when needed.
- Metrics column: metric cards with label, aggregate value, optional comparison, and a 60px sparkline.
- Dimensions grid: repeated leaderboard cards with top values for the selected metric.
- Status area: loading/error/empty states that do not resize the layout.

Useful interactions:

- Click a metric card to select the ranking metric for leaderboards.
- Click a dimension row to toggle that value as a filter.
- Click a filter pill to remove that filter.
- Drag on a sparkline to set a brush date range; double-click to clear it.
- Change grain to refresh only time-series queries unless dimensions depend on grain.

## State Contract

Keep state small and serializable:

```ts
type ExplorerState = {
  selectedMetric: string
  filters: Record<string, string[]>
  dateRange?: [string, string]
  brushSelection?: [string, string]
  timeGrain?: "day" | "week" | "month" | "quarter" | "year"
}
```

Avoid storing result rows in URL state. URL state should include only selections and filters.

## Query Contract

Use structured queries:

```ts
type SidemanticQuery = {
  metrics: string[]
  dimensions: string[]
  filters?: string[]
  segments?: string[]
  order_by?: string[]
  limit?: number
  offset?: number
  ungrouped?: boolean
}
```

Generate these query types:

- `metricSeries`: metrics + time grain dimension, order by time, limit 500.
- `metricTotals`: metrics only.
- `dimensionLeaderboard`: selected metric + one dimension, order by selected metric descending, limit 6.
- `previewRows`: ungrouped, limit 50, only for inspect/debug views.

Example:

```json
{
  "metrics": ["auctions.bid_request_cnt"],
  "dimensions": ["auctions.__time__day"],
  "filters": [
    "auctions.__time >= cast('2025-01-01' as date)",
    "auctions.device_os = 'iOS'"
  ],
  "order_by": ["auctions.__time__day"],
  "limit": 500
}
```

## Crossfilter Rules

Always enumerate filter cases before coding:

- No filters: all metric cards and all leaderboards query the full active time range.
- One dimension value: all metrics and other dimensions use that value.
- Multiple values for one dimension: values are ORed inside that dimension.
- Multiple dimensions: dimensions are ANDed together.
- Leaderboard for the active dimension: exclude that dimension's own filter, keep all other filters.
- Brush date range: overrides the base date range until cleared.
- No time dimension: omit time filters, brush selection, and grain controls.

The UI must prove those rules through state changes. For any visible filter or selectable row:

- Removing a filter pill recomputes totals, leaderboards, charts, and preview rows.
- Clicking a dimension row adds/toggles the corresponding filter and updates selected row state.
- Clicking a selected dimension row again clears that dimension filter when the local UX supports toggle behavior.
- Selecting a metric card changes the metric used by leaderboards and categorical charts.
- Reset controls restore both filters and selected metric.

Do not ship "fake" interactions that only change status text. Status text is secondary evidence; rendered data changes are the primary evidence.

## Column Naming

Use explicit aliases or normalize response keys at the API boundary. `inspect_layer.py` emits `output_aliases` for each generated query, and `--execute` adds actual result columns:

- `orders.revenue` -> `revenue`
- `orders.order_date__month` -> `order_date__month` unless a custom alias layer changes it
- `customers.region` -> `region`

The UI should never depend on quoted SQL output names, adapter-specific casing, or generated expression text.

## Result Transport

JSON is simplest and enough for most app pages. Arrow is better when the result is wide, large, or feeding canvas/WebGL/chart libraries.

For Arrow:

- Python can return Arrow stream bytes from `/query?format=arrow`.
- Widget-style transports can use raw bytes or base64 Arrow IPC.
- JavaScript should decode into plain row objects at component boundaries unless downstream code can consume Arrow tables directly.

## Backend Integration

FastAPI path:

```python
from sidemantic import SemanticLayer, load_from_directory
from sidemantic.api_server import create_app

layer = SemanticLayer(connection="duckdb:///data.duckdb")
load_from_directory(layer, "models")
app = create_app(layer, cors_origins=["http://localhost:5174"])
```

CLI path:

```bash
uv run sidemantic info models
uv run skills/sidemantic-webapp-builder/scripts/inspect_layer.py models --db data.duckdb --require-execute
uv run sidemantic query "SELECT revenue, status FROM orders" --models models --db data.duckdb
```

Run these checks serially against DuckDB file databases. Concurrent readers through separate processes can hit file locks.

For exact totals or custom app query shapes, prefer the inspector's generated SQL or API/Python structured query path. CLI semantic SQL may auto-add a model `default_time_dimension`, which turns a totals-looking query into a time series.

## Verification Fallbacks

Preferred verification:

- Run the app locally on a 4xxx-5xxx port.
- Verify desktop and mobile in a real browser with screenshots.
- Check that metrics, leaderboards, charts, loading/error branches, and filter interactions render.
- Click through filters, leaderboard rows, metric cards, and reset controls. Assert concrete text/value changes after each click.
- Confirm state components are conditional. Loading, empty, and error boxes should not all appear as normal dashboard content unless the artifact is explicitly a component gallery.
- Confirm SVG charts are clipped to their cards and do not paint into neighboring tiles.
- Serve static artifacts when they use `fetch()` for JSON/CSV. Do not rely on `file://` behavior.

For static apps that follow the bundled component contracts, run:

```bash
bunx --bun -p playwright playwright install chromium  # first run only, if needed
bunx --bun -p playwright node skills/sidemantic-webapp-builder/scripts/verify_static_interactions.mjs \
  --url http://127.0.0.1:4519/
```

This is the minimum proof that the app is not a fake gallery. It must report successful data changes after filter removal, leaderboard selection, metric selection, and reset.

Fallback when browser tooling is unavailable:

- Use `inspect_layer.py --require-execute` and assert `result.columns` and `sample_rows` match the UI contract.
- Use `scripts/verify_static_app.py <app-dir>` for static scaffold checks when the app follows the bundled script shape.
- Use `bun` with a DOM library already available in the project, or a simple static parser, to confirm expected selectors and data bindings exist.
- State clearly that real browser visual verification was not run.

## Static Scaffold

For a quick working artifact from an executed spec:

```bash
uv run skills/sidemantic-webapp-builder/scripts/scaffold_static_app.py docs/sidemantic-app-spec.json \
  --output dist/sidemantic-dashboard \
  --title "Metrics Dashboard"
uv run skills/sidemantic-webapp-builder/scripts/verify_static_app.py dist/sidemantic-dashboard
```

The scaffold intentionally stays plain HTML/CSS/JS and consumes copied files from `assets/templates/static-dashboard/` plus the static component kit. Use it as a proof point, demo baseline, or fixture before adapting the same query contract into an existing product app.

Browser-only path:

- Pyodide owns Sidemantic model loading and SQL generation.
- DuckDB-WASM owns data registration and execution.
- Keep the model YAML and query builder visible in code so generated SQL can be debugged.

## Visual Defaults

For net-new Sidemantic analytics demos:

- Metric column width: 300-360px on desktop.
- Sparkline height: 56-72px.
- Dimension cards: `minmax(220px, 1fr)`, top 6 rows.
- Filter row: sticky at top of the tool surface.
- Loading: skeleton cards with stable height.
- Positive delta: green. Negative delta: red. Neutral/missing: muted.

Avoid oversized hero text, decorative gradients, nested cards, and one-hue dashboards.
