# Sidemantic Web UI

An embeddable analytics web app for the Sidemantic semantic layer. It talks to the **shared HTTP
contract that both backends expose identically** — Python (`sidemantic api-serve`) and Rust
(`sidemantic-server`): `GET /describe` · `GET /graph` · `GET /models` · `POST /compile` ·
`POST /query` (JSON or Arrow).

It's a fast metrics dashboard — big-number KPIs, a time series with period-over-period comparison,
and dimension leaderboards for slice-and-dice — with a hairline-grid design, click-to-crossfilter,
and deep-linkable URL state.

## Views

- **Explore** — a KPI scorecard strip (sparklines + period-over-period deltas), an interactive time
  series for the focused metric, and dimension leaderboards. Click a KPI to focus it; click a
  leaderboard row to toggle a crossfilter (a dimension's own leaderboard excludes its own filter, so
  it always shows every value).
- **Pivot** — a grouped, sortable table over N dimensions × M metrics, with an ungrouped raw-rows
  toggle.

### Interactions

- **Interactive chart** — hover for a crosshair + tooltip (current, previous, and delta); drag to
  brush-zoom (sets the date range), double-click to clear.
- **Period-over-period** — set a date range (or brush) and KPIs + the chart show a dashed previous
  period and percentage deltas.
- **Dark mode** — toggle in the top bar, persisted to localStorage (respects `prefers-color-scheme`).
- **Live status** — a spinner shows while queries are in flight; **Reset** clears filters + range.

All selections + filters are serialized to the URL for shareable, deep-linkable views.

## Develop

The dev server proxies the API paths to a running backend, so the SPA is same-origin (no CORS) with
hot reload.

```bash
# 1. Start a backend (Python example, against the bundled ecommerce model):
uv run --extra dev sidemantic api-serve examples/ecommerce/models \
  --db examples/ecommerce/data/ecommerce.db --port 4400

# 2. Run the UI (proxies /query, /graph, /describe, ... to :4400):
cd webapp
bun install
bun run dev            # http://localhost:4321
# Point at a different backend:  SIDEMANTIC_API=http://host:port bun run dev
```

The Rust backend (`sidemantic-server`, built with the `runtime-server` feature) exposes the same
contract — set `SIDEMANTIC_API` to its address to develop against it instead.

## Build / embed

```bash
bun run build                      # type-checks, then emits dist/ (base="./")

# Or build + sync the bundle into both backends for embedding (run from the repo root):
uv run scripts/build_webapp.py     # -> sidemantic/ui/static and sidemantic-rs/ui (committed)
```

## Architecture

- `src/data/` — backend adapter. `SidemanticBackend` interface with an `HttpBackend` implementation
  (Arrow-or-JSON). A future in-browser `wasmAdapter` (Rust-WASM compile + DuckDB-WASM execute) can
  drop in behind the same interface with no UI change.
- `src/lib/` — catalog builder (`/describe` rich, `/graph` names-only fallback), query builders,
  formatting, time/grain helpers.
- `src/state/` — small serializable explorer state + URL (de)serialization + the async query hook
  (stale-response-guarded).
- `src/components/`, `src/views/` — presentational components and the two views (Explore, Pivot).

## Embedding

The built bundle is served by either backend from a single process:

- **Python** — `sidemantic api-serve` serves the UI at `/` by default (`--no-ui` to disable). The
  bundle lives at `sidemantic/ui/static/` (force-included in the wheel); UI routes are public while
  the data endpoints stay authenticated. The UI exchanges a bearer entered in its login prompt for
  a short-lived HttpOnly session cookie; bearer tokens are never accepted from URLs or persisted in
  browser storage.
- **Rust** — `sidemantic-server` (built with the `runtime-server` feature) bakes the bundle into the
  binary via `rust-embed` and serves it from a router fallback registered after the auth layer.
  Until that backend exposes the session exchange, the UI keeps a prompted bearer only in memory
  for the lifetime of the page.

Both copies are committed (synced by `scripts/build_webapp.py`), so neither backend build needs a JS
toolchain. Both backends serve `/describe`, so the UI gets the rich (typed) catalog on either — it
only falls back to the names-only `/graph` catalog against a backend that doesn't expose `/describe`.
