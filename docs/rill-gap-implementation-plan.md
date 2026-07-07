# Rill Gap Closure — Implementation Plan

Derived from the July 2026 four-dimension comparison against rilldata/rill. Goal: close the gaps that
change adoption outcomes (security, correctness, serving concurrency, compile latency, explore-UI
perception) while explicitly NOT chasing Rill-the-product (canvas builder, alerts, scheduled reports,
Druid/Pinot drivers).

Each work item below is written as a self-contained spec: an implementer should be able to execute it
without reading the comparison reports. File paths and symbols were verified against this checkout on
2026-07-07; re-verify line numbers before editing (they drift).

---

## Ground rules for every work item (do not skip)

1. Python: always `uv run`, never bare python. Add deps with `uv add`.
2. **Pyodide constraint**: no new module-level imports of heavy/optional deps anywhere under
   `sidemantic/` core. Optional deps are imported lazily inside functions. After any dependency
   change, this must pass:
   ```bash
   uv sync --extra dev
   uv run python -c "import sys; from sidemantic import Dimension, Metric, Model; assert 'sidemantic_dax' not in sys.modules"
   ```
3. Before claiming an item done, run the full CI-equivalent gate:
   ```bash
   uv sync --extra dev --extra dax
   uv run ruff check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar
   uv run ruff format --check . <same excludes>
   uv run pytest -v
   ```
4. Webapp: use `bun` (never npm/npx). Runtime deps are deliberately just `react` + `apache-arrow` —
   do not add UI/chart/table libraries without explicit approval. Tests: `bun test` for unit,
   `bun run playwright test` for e2e (config at `webapp/playwright.config.ts`).
5. Tests go in the real suites (`tests/`, `webapp/src/**/*.test.ts`, `webapp/tests/`), never
   throwaway scripts.
6. Never remove existing code comments. Match surrounding code style; comments state constraints,
   not narration.
7. Do not create new files under `docs/` or `scripts/` as deliverables; those dirs are scratch.

---

## Workstream A — Enforced security (row filters, field visibility, user attributes)

**Why first**: the one *categorical* gap vs Rill. Sidemantic currently enforces nothing: the
`public` flag (`sidemantic/core/dimension.py:53`, `sidemantic/core/metric.py:373`) is never read by
the SQL generator, and Cube `access_policy` is imported into `meta` with a warning
(`sidemantic/adapters/cube.py:467-480`, `:1292-1295`).

### A1. Security policy model (~2 days)

- New file `sidemantic/core/security.py` with a pydantic model:
  ```python
  class SecurityPolicy(BaseModel):
      access: str | bool = True        # Jinja expr over user attrs; falsy => model not queryable
      row_filters: list[str] = []      # SQL fragments with Jinja, e.g. "region = '{{ user.region }}'"
      # v2 (not this item): field_access include/exclude rules
  ```
- Add `security: SecurityPolicy | None = None` to `Model` (`sidemantic/core/model.py`). Follow the
  existing pattern used by other optional model fields (e.g. `pre_aggregations`) for YAML
  serialization — check `sidemantic/adapters/sidemantic.py` (native format adapter) round-trips it.
- Jinja rendering must reuse the existing template infra in `sidemantic/core/template.py` — do NOT
  add a second templating path. User attributes are exposed under the `user` namespace only.
- **Deny-by-default rule (copy Rill's semantics)**: if `security` is present and a query arrives
  with `user_attributes=None`, raise `SecurityError` (new exception, put it next to the existing
  exceptions — grep for `class PreaggregationStrictError` to find the module). A model *without* a
  `security` block is unrestricted (back-compat).
- Tests: `tests/core/test_security_policy.py` — construction, YAML round-trip via the native
  adapter, deny-by-default error.

### A2. Enforcement in compile/query path (~3-4 days)

- Thread `user_attributes: dict | None = None` through:
  - `SemanticLayer.query()` (`sidemantic/core/semantic_layer.py:543`) and
    `SemanticLayer.compile()` (`:749`) — new keyword param, docstring entry matching existing style.
  - Down into the generator entry points `generate()` / `generate_view()`
    (`sidemantic/sql/generator.py:589`, `:558`).
- In the generator: for **every model that participates in the query** (base model AND all joined
  models — walk the same model set the join resolver produces, do not only filter the base model),
  render each `row_filters` entry with the user attributes and AND it into that model's WHERE at the
  model-CTE level (i.e., inside the per-model CTE, so the filter applies before joins/aggregation —
  this is what makes fan-out-safe row security). Grep the generator for where model-level `filters`
  from segments are injected and use the identical mechanism.
- Rendered row filters must go through the same parameter/escaping path as user-supplied filters. A
  user attribute containing `'` must not produce SQL injection: render via Jinja, then parse the
  resulting fragment with sqlglot exactly like existing filter strings are parsed; on parse failure,
  raise `SecurityError` (never silently drop a row filter).
- **Visibility enforcement**: add `enforce_visibility: bool = False` to `SemanticLayer.__init__`.
  When True, requesting a dimension/metric with `public=False` raises a clear error
  (`Field 'orders.margin' is not public`), and catalog/introspection listings
  (`sidemantic/core/catalog.py`, `sidemantic/core/introspection.py`) omit non-public fields.
  Default False so library users are unaffected; servers turn it on (A3).
- Edge cases to cover in tests (`tests/core/test_security_enforcement.py`):
  1. Row filter on a joined model (not just the base model) actually appears in generated SQL.
  2. `access: "{{ user.role == 'admin' }}"` false → SecurityError before any SQL is generated.
  3. `security` present + `user_attributes=None` → SecurityError (deny-by-default).
  4. Malicious attribute value (`"x' OR '1'='1"`) is neutralized (assert generated SQL parses and
     the literal is quoted).
  5. Pre-aggregation routing: a query with row filters must NOT route to a pre-agg unless the
     pre-agg is filtered identically — simplest correct v1: **disable preagg routing whenever row
     filters are active** (add a guard in `SemanticLayer.compile` near the routing call around
     `:645`, and a test asserting raw-table SQL is used).
  6. Result-cache interaction: see C2 — cache key must include user-attribute hash.

### A3. Server integration (~2 days)

- HTTP server (`sidemantic/api_server.py`): accept user attributes from a configurable trusted
  header (default `X-Sidemantic-User`, JSON object), parse once per request, pass into
  `layer.query(..., user_attributes=...)`. Add a startup flag `--require-user-attrs` that rejects
  requests missing the header when any model declares `security`. Construct the layer with
  `enforce_visibility=True`.
- PG wire server (`sidemantic/server/connection.py`): map the Postgres startup `user` plus an
  optional startup parameter (e.g. `options=-c sidemantic.user_attrs=<json>`) into the same dict.
  If that plumbing is awkward in riffq, v1 fallback: a per-server static
  `--user-attrs-file <json>` mapping usernames → attribute dicts. Document the limitation.
- MCP server (`sidemantic/mcp_server.py`): same layer flag; attributes from server config.
- Tests in `tests/server/`: header present/absent/malformed; non-public field rejected over HTTP.

### A4. Adapter import mapping (~2 days, mechanical — good codex candidate)

- `sidemantic/adapters/cube.py`: where `access_policy` is currently preserved+warned
  (`:467-480`, `:1292-1295`), translate the mechanical subset: Cube `rowLevel.filters` with
  `member`/`operator`/`values` → `SecurityPolicy.row_filters` SQL fragments; keep the warning only
  for constructs that don't map. Update the existing warning text to say what WAS imported.
- `sidemantic/adapters/rill.py`: map Rill metrics-view `security:` blocks (`access`, `row_filter`,
  and structured dimension rules) → `SecurityPolicy`. Rill reference: `runtime/parser/`
  `parse_partial_security_policy.go` semantics — `access` is a Go-template boolean over
  `{{ .user.x }}`; translate `.user.` → `user.` Jinja.
- Round-trip tests in `tests/adapters/` following the existing fixture pattern.

---

## Workstream B — `non_additive_dimension` correctness (stop silently wrong results)

Field exists at `sidemantic/core/metric.py:364` and is imported/round-tripped but the generator
never consumes it → over-aggregated (wrong) results for semi-additive measures. Same
"parsed-but-inert" pattern already documented for Cube imports.

### B1. Fail loudly (ship immediately, ~1 day)

- In the generator, when a queried metric has `non_additive_dimension` set, raise
  `UnsupportedMetricError` with message naming the metric, the dimension, and the workaround
  (pre-aggregate upstream), UNLESS `SemanticLayer(allow_non_additive_unsafe=True)`.
- Add the same check to `query()`/`compile()` docstrings. Update every adapter that imports the
  field (grep `non_additive` across `sidemantic/adapters/`) so import warnings say "will raise at
  query time" instead of implying support.
- Tests: query raises; escape hatch works; adapters still round-trip the field.

### B2. Implement `last`-value semantics (Phase 3, ~1 week)

- Semantics (MetricFlow-compatible): for a metric with `non_additive_dimension: <time_dim>`,
  aggregate only rows at the max value of `<time_dim>` per group (window subquery:
  `QUALIFY <time_dim> = MAX(<time_dim>) OVER (PARTITION BY <group dims>)`, or a self-join for
  dialects without QUALIFY). Generate inside the model CTE.
- Extend the field to a small struct if needed (`window_choice: min|max`,
  `window_groupings: list[str]`) — check what MetricFlow/Hex adapters actually populate first and
  match that shape.
- This interacts with symmetric aggregates (`sidemantic/core/symmetric_aggregate.py`) — if both
  trigger, raise rather than compose (document why).
- Tests against DuckDB with a fixture table where additive vs semi-additive answers differ, so the
  assertion is on VALUES not SQL text. Put in `tests/metrics/`.

---

## Workstream C — Serving concurrency + result cache

Measured problems: `api_server.py:147` creates `app.state.lock = threading.RLock()` and every query
handler serializes on it (effective concurrency 1); the PG server threads share one DuckDB
connection (`sidemantic/db/duckdb.py` — single `duckdb.connect`); there is no result cache and the
`.sql()` string cache (`semantic_layer.py:1528` + cache at `:104`) only hits on byte-identical SQL.

### C1. Remove the global query lock (~2-3 days)

- DuckDB adapter (`sidemantic/db/duckdb.py`): add a `cursor()` method returning
  `self.conn.cursor()` — duckdb-python cursors are independent connections sharing the database and
  are the sanctioned way to do multithreaded reads. Check every other adapter in `sidemantic/db/`
  for an equivalent (Postgres: connection pool via the existing driver; if an adapter can't do
  concurrent handles, have `cursor()` fall back to a per-adapter lock so behavior is unchanged
  there).
- `api_server.py`: scope `app.state.lock` down to **layer mutation only** (model registration /
  config reload endpoints). Query handlers execute on a fresh cursor per request:
  compile (pure CPU, no lock) → execute on cursor → serialize result. Verify each handler between
  `:184-346` — none of the read-only ones should take the lock afterwards.
- PG server `sidemantic/server/connection.py:168`: same change — the executor-submitted work takes
  a cursor, not the shared connection.
- **Proof test** (`tests/server/test_concurrency.py`): start the HTTP app with a DuckDB table +
  `SELECT ... sleep`-style slow query (duckdb has no sleep; instead use a large cross-join capped
  by LIMIT to burn ~200ms), fire 4 concurrent requests via threads, assert wall time < 2× single
  query time. Mark `@pytest.mark.slow` if the suite has that convention (grep conftest.py).

### C2. Content-keyed result cache with singleflight (~3-4 days)

- New file `sidemantic/core/result_cache.py`. API:
  ```python
  class ResultCache:
      def __init__(self, max_bytes: int, ttl_seconds: float | None): ...
      def get_or_compute(self, key: str, compute: Callable[[], pa.Table]) -> pa.Table: ...
      def invalidate_all(self) -> None: ...
  ```
  Plain dict + OrderedDict LRU, size-accounted by `Table.nbytes`, `threading.Lock` for the map and
  per-key locks for singleflight (concurrent identical keys → one execution). **No new
  dependencies.** pyarrow is already optional (`serve` extra) — import it lazily inside methods; the
  cache module must import cleanly without pyarrow installed.
- Key = sha256 of: compiled SQL (post preagg-routing) + adapter identity (dialect + connection
  fingerprint) + **graph generation counter** + **sorted user-attributes hash** (security!). Add a
  monotonically increasing `self._generation` to `SemanticLayer`, bumped in `add_model`,
  `add_metric`, and anywhere `_sql_cache` is currently cleared (grep for the existing clearing sites
  near `semantic_layer.py:104` and mirror them).
- Wire-up: servers only, opt-in. `api_server.py` flag `--result-cache-mb 256 --result-cache-ttl 60`;
  PG server likewise. Do NOT cache inside library `query()` by default (breaks freshness
  expectations for notebook users).
- Tests (`tests/core/test_result_cache.py`): hit/miss, TTL expiry (inject a clock, don't sleep),
  generation bump invalidates, differing user attrs → different keys, singleflight (two threads,
  compute called once — use an Event to hold the first compute open).

### C3. Structured compile cache (~1 day, after D1 lands measure again — may be unnecessary)

- `compile()` currently re-runs the full generator every call. Add an LRU keyed on the frozen
  tuple of all `compile()` args + generation counter, storing the SQL string. Reuse the eviction
  pattern of the existing `.sql()` cache. Only do this if post-D1 compile is still >5ms median;
  otherwise close as won't-fix with the benchmark numbers.

---

## Workstream D — Compile latency (Python engine, pre-Rust)

Measured: 2-model join compile median 25.7ms / p95 48ms; **sqlglot is 86% of it**, invoked ~108×
per compile because `sidemantic/sql/generator.py` (6005 LOC) builds SQL fragments as f-strings and
re-parses them (`parse_one(...)`) repeatedly. Bare `parse_one` is 0.066ms — the cost is the count.
First `.sql()` call pays ~550ms sqlglot dialect warmup. This workstream is the bridge until
sidemantic-rs is default (tracked separately; do not block on it).

### D1. Fragment parse cache + parse-once-per-model (~3-5 days, highest perf ROI)

- Step 1 — measure: add `tests/optimizations/test_compile_benchmark.py` with a
  `pytest.mark.benchmark`-style timing (plain `time.perf_counter`, generous threshold like
  median < 15ms so CI never flakes; print actuals). Also add cProfile capture behind an env var so
  regressions are diagnosable. This test defines "done" for the workstream.
- Step 2 — module-level memo: in `sidemantic/sql/generator.py`, add
  `@functools.lru_cache(maxsize=4096)` around a helper
  `_parse_fragment(sql: str, dialect: str) -> exp.Expression` and route existing `parse_one` call
  sites through it, **returning `.copy()` when the caller mutates the tree** (audit each call site:
  if the result is embedded into a larger tree, sqlglot mutates parents — always copy; the memo
  still wins because parse >> copy).
- Step 3 — parse model expressions once: dimension/metric `sql` expressions are re-parsed on every
  compile. Cache parsed ASTs per (model, field) in a dict on the **generator or graph**, keyed by
  the layer generation counter from C2 — do NOT stash unhashable ASTs on pydantic models
  (validation/copy semantics will bite).
- Step 4 — replace round-trips with builders where mechanical: patterns like
  `parse_one(f"{a} AND {b}")` become `exp.and_(a_ast, b_ast)`; `parse_one(f"DATE_TRUNC('{g}', {c})")`
  becomes `exp.func("DATE_TRUNC", exp.Literal.string(g), col_ast)`. Do this incrementally —
  every replacement must keep `uv run pytest tests/queries -x` green; stop at diminishing returns
  rather than converting all 6005 lines.
- Acceptance: benchmark median join compile < 8ms (target 5); zero test regressions; no behavior
  change in generated SQL for the existing snapshot/fixture tests.

### D2. Cold-start (~1-2 days, mechanical — good codex candidate)

- `python -X importtime -c "import sidemantic"` → attack the top entries (measured: sidemantic
  import 531ms; sqlglot 165ms; duckdb 67ms). Defer sqlglot/duckdb imports into the functions that
  need them where module-level today (check `sidemantic/__init__.py` chain and `cli.py` — CLI
  commands like `--help`/`validate` shouldn't import duckdb at all). Keep the Pyodide lazy-import
  rules in mind — this work aligns with them.
- Acceptance: `time uv run sidemantic --help` < 900ms warm-disk (from ~1.5-2.3s); core-import
  assertion in ground rule 2 still passes.

---

## Workstream E — Webapp explore quick wins

All webapp state lives in `webapp/src/state/explorerState.ts` (single reducer) with URL
serialization in `webapp/src/state/url.ts` (add versioned params, old URLs must keep working) and
query building in `webapp/src/lib/queries.ts`. Each item: update reducer + URL + queries + component
+ unit tests (`queries.test.ts`, `url.test.ts`) + one playwright spec in `webapp/tests/`.
Rill references are for semantics only — do not port Svelte code.

### E1. Real filter editor — include/exclude + search + contains (~1-2 weeks, biggest perception win)

- Extend `FilterState` so each dimension filter is
  `{ mode: 'include' | 'exclude' | 'contains', values: string[], pattern?: string }`. Old URL/state
  shape (bare value list) deserializes as `mode: 'include'`.
- `filterExprs()` (`webapp/src/lib/queries.ts:35`): emit `NOT IN (...)` for exclude,
  `ILIKE '%' || <escaped> || '%'` for contains. Preserve the existing NULL-token handling
  (`IS NULL` / `IS NOT NULL` — for exclude mode the NULL token becomes `IS NOT NULL`). Escape `%`
  and `_` in contains patterns. Keep `composeFilters`' `excludeDim` crossfilter semantics untouched.
- New `webapp/src/components/FilterEditor.tsx`: opens from `FilterPill` and from a "+ Filter"
  affordance in the leaderboard header. Value list = `SELECT DISTINCT <dim> ... LIMIT 50` through
  the existing backend, filtered server-side by the search box (ILIKE). Debounce 200ms; reuse
  `useQueryResult`'s stale-guard pattern rather than hand-rolling.
- Rill semantic reference: `web-common/src/features/dashboards/filters/dimension-filters/`.

### E2. Leaderboard context columns (~3-5 days)

- Toggle in `LeaderboardPanel.tsx` header: `none | % of total | Δ | Δ%` (single global setting in
  reducer, URL-serialized).
- `% of total`: one extra aggregate per panel — the focused metric with the same composed filters
  but WITHOUT the dimension grouping (the pattern in `ExplorerView.tsx:37-65` that shares
  aggregates across the scorecard strip is the template; reuse, don't duplicate requests).
- `Δ` / `Δ%`: per-dimension leaderboard query over `previousRange(range)`
  (`webapp/src/lib/time.ts:50`), joined client-side by dimension value. Missing previous value
  renders `–`, not 0. Formatting via existing `lib/format.ts`.
- Rill reference: `web-common/src/features/dashboards/leaderboard/leaderboard-context-column.ts`.

### E3. Comparison-range picker (~1 week)

- Today comparison is hardcoded to the immediately-preceding equal-length window
  (`previousRange`). Add a comparison selector to `DateRangeControl.tsx`:
  `off | previous period | previous year | custom range`.
- `lib/time.ts`: add `previousYearRange(range)` (same dates, year−1 — clamp Feb 29 → Feb 28) and
  pass the chosen comparison range explicitly to `MetricCard`/`TimeSeriesChart` instead of them
  calling `previousRange` themselves (grep for `previousRange` call sites and thread the value
  down). The chart's bucket-offset alignment logic for sparse comparison series must keep working —
  it aligns by index offset; previous-year with weekly grain is the edge case to unit-test.
- Rill reference: `super-pill/ComparisonPill.svelte` semantics (not code).

### E4. Timezone support (~4-6 days)

- Backend already accepts `timezone` on query (`SemanticLayer.query(..., timezone=...)`,
  `semantic_layer.py:543`); confirm the HTTP `POST /query` contract exposes it (check
  `api_server.py` request model and `webapp/src/data/backend.ts` request builder) — if absent,
  add it end-to-end first.
- Webapp: tz selector (default UTC) using `Intl.supportedValuesOf('timeZone')` filtered to a
  curated common list + search. Thread through: reducer → URL → query payload → time-axis label
  formatting in `TimeSeriesChart.tsx`/`lib/time.ts` (bucket labels must render in the selected tz —
  use `Intl.DateTimeFormat` with `timeZone`, no date library).
- Edge case: date-range picker boundaries are currently UTC day boundaries (`lib/time.ts`); when a
  tz is selected, range boundaries should be that tz's day boundaries. Unit-test DST transitions
  (America/New_York, 2026-03-08).

### E5 (stretch). Measure/HAVING filters (~3-5 days)

Only after E1-E4. Add `having` to the webapp query payload and a `MeasureFilterForm`; backend
compile already supports metric filters via semantic SQL HAVING — verify through the HTTP contract
before starting, and skip if the API needs a new concept (that becomes a backend item first).

---

## Workstream F — Dimension ergonomics (mechanical, Phase 3, codex-friendly)

Small, independent, adapter-visible features Rill has and we lack. Each follows the identical
recipe: field on `Dimension` (`sidemantic/core/dimension.py`) → generator support (DuckDB dialect
first, error cleanly elsewhere) → native-format round-trip → import mapping in
`sidemantic/adapters/rill.py` → tests in `tests/core/` + `tests/adapters/`.

- **F1** `uri: bool = False` on Dimension — metadata-only passthrough (catalog/introspection/HTTP
  `/describe` expose it; webapp renders leaderboard values as links). ~1 day.
- **F2** Fiscal calendar: `first_day_of_week: int (1-7)`, `first_month_of_year: int (1-12)` on
  `Model` — affects week/quarter/year truncation in the generator's date_trunc path (the
  timezone-aware truncation code near `sidemantic/sql/generator.py:189` is where grain truncation
  lives). DuckDB: implement week via offset arithmetic, not `date_trunc('week')`. ~3-4 days.
- **F3** `unnest: bool = False` on Dimension for array columns — LATERAL UNNEST in the model CTE;
  interacts with symmetric aggregates (unnest fans out!) so require the model to have a
  `primary_key` and route through the existing symmetric-aggregate machinery; otherwise raise. ~1 week — needs review, not pure codex.
- **F4** Lookup dimensions (`lookup_table`/`lookup_key`/`lookup_value`/`lookup_default`) — LEFT
  JOIN to a side table inside the model CTE. Consider instead documenting the existing
  relationship-join answer and skipping; decide at Phase 3 start. 

---

## Sequencing

| Phase | Items | Effort (1 experienced dev equiv.) | Exit criterion |
|---|---|---|---|
| 1 (now) | B1 fail-loud, C1 lock removal, D1 fragment cache, E1 filter editor | ~3 weeks | wrong-results hole closed; server concurrency >1; compile <8ms; filters demo-able |
| 2 | A1-A3 security, C2 result cache, E2 + E3, D2 cold start | ~4 weeks | multi-tenant deployable behind trusted header; dashboard interactions cache-hit |
| 3 | A4 adapter mapping, B2 last-value semantics, E4 timezone, F1-F3, C3 if still needed | ~4 weeks | Rill-import carries security; semi-additive metrics compute correctly |

Dependency edges: C2 depends on A2's user-attribute threading (cache key) — build the key interface
in C2 with a user-attrs slot from day one even if it's always None in Phase 1. E2/E3 are independent
of everything. D1 before C3 (C3 may become unnecessary).

Delegation guidance: A4, D2, F1, F2, and the test-writing halves of C1/C2 are well-specified
mechanical work (cheap model / codex). A2 (generator surgery), B2, F3, and D1 step 4 touch
correctness-critical SQL generation — implement with a strong model and review the diff against the
edge-case lists above before merging.

Explicitly out of scope (deliberate, revisit only with new evidence): canvas dashboard builder,
alerts/scheduled reports, Druid/Pinot drivers, iframe embed SDK, full pivot rows×columns rewrite,
rilltime-style time DSL (our relative-date operators + E3/E4 cover the common cases), making
sidemantic-rs default (separate initiative with its own parity gate).
