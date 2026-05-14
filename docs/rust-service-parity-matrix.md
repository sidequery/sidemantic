# Rust Service Parity Matrix

This tracks the Rust standalone parity work for HTTP, MCP, LSP, workbench, and ADBC execution. The contract is behavioral parity with the Python/user-facing Sidemantic surface unless a difference is explicitly marked as an intentional non-goal with executable evidence.

## Status Legend

- `pass`: covered by Rust implementation and tests.
- `partial`: implemented for the common path, with known parity gaps.
- `gap`: missing or not proven.
- `skip`: intentionally not implemented or not runnable in this environment, with reason.

## HTTP API

| Behavior | Python contract | Current Rust status | Next action |
| --- | --- | --- | --- |
| Process startup | HTTP API starts with a model directory and fails on a bad model path | `pass`: bad model path covered by Rust HTTP smoke. Current commands are `sidemantic server` / `sidemantic serve` and the `sidemantic-server` binary; `api-serve` is not implemented yet | Decide whether to add `api-serve` or keep Rust HTTP command naming separate |
| Readiness | `GET /readyz -> {"status":"ok"}`, unauthenticated | `pass`: route and smoke coverage added | Keep covered |
| Health | `GET /health -> status, version, dialect, model_count` | `pass`: Python-compatible shape covered | Keep covered |
| Model list | `GET /models` with table/dimensions/metrics/relationships | `pass`: summary shape covered | Keep covered |
| Single model | Python does not expose this route | `pass`: Rust extra route `/models/{model}` | Keep as Rust extension |
| Graph | `GET /graph` with models, graph_metrics, joinable_pairs | `pass`: route and joinable-pair smoke added | Keep covered |
| Structured compile | `POST /compile` with dimensions, metrics, where, filters, segments, order_by, limit, offset, ungrouped, parameters, preagg flag | `partial`: route, common fields, offset, ungrouped, and parameter interpolation are covered; `segments` and preagg routing are implemented but need direct HTTP assertions | Add targeted HTTP tests for segments and `use_preaggregations` |
| Structured execute | `POST /query` executes via configured DB adapter, JSON or Arrow | `pass` for Rust contract: `/query` alias and DuckDB ADBC JSON E2E covered; Arrow stream output is an explicit non-goal for this Rust HTTP baseline | Revisit Arrow only if binary IPC becomes a product requirement |
| Semantic SQL compile | `POST /sql/compile` rewrites SQL | `pass`: route and smoke coverage added | Keep covered |
| Semantic SQL execute | `POST /sql` rewrites and executes | `pass`: DuckDB ADBC E2E covered | Keep covered |
| Raw SQL execute | `POST /raw` executes select-only SQL without rewrite | `pass`: select-only guard and DuckDB ADBC E2E covered; Rust intentionally uses a conservative string guard until a parser-backed classifier is needed | Parser-backed classification is optional hardening |
| Auth | Bearer token protects application routes except `/readyz` when configured | `pass`: `--auth-token` and env-backed middleware covered; unauthenticated `/readyz` and CORS `OPTIONS` preflight remain open | Keep covered |
| CORS | Configurable origins | `pass`: `--cors-origin` and env-backed response headers covered | Keep covered |
| Body limit | Rejects large write payloads with 413 JSON | `pass`: `--max-request-body-bytes` and env-backed JSON 413 covered | Keep covered |
| Error shape | Python uses JSON `error`/FastAPI detail depending path | `pass` for Rust contract: Rust returns JSON `error` bodies with route-specific messages; exact FastAPI `detail` shape is not a standalone Rust contract | Keep actionable JSON errors covered |

## MCP

| Behavior | Python contract | Current Rust status | Next action |
| --- | --- | --- | --- |
| Startup | `sidemantic mcp DIRECTORY`, fails on bad path | `pass` | Keep covered |
| Initialize lifecycle | JSON-RPC initialize/initialized/shutdown | `pass` for smoke lifecycle | Keep covered |
| Tool listing | Includes query, catalog, graph, SQL, chart tools | `pass`: query, graph, validation, SQL, chart tools, and catalog resource covered | Keep covered |
| `get_models` | Enriched model/dimension/metric/segment/relationship metadata | `pass`: enriched details covered | Keep covered |
| `run_query` | dimensions, metrics, where, filters, segments, order_by, limit, offset, ungrouped, dry_run | `partial`: core fields, offset, dry-run, and DuckDB ADBC execution are covered; MCP does not yet accept `parameters`, and not every structured field has a direct protocol assertion | Add MCP `parameters` support or document narrower schema; add targeted field tests |
| `validate_query` | Returns `valid` and `errors` without executing | `pass`: tool and smoke coverage added | Keep covered |
| `get_semantic_graph` | Returns graph payload | `pass`: tool and smoke coverage added | Keep covered |
| `run_sql` | Rewrites semantic SQL, executes against configured DB | `pass`: DuckDB ADBC E2E covered | Keep covered |
| `create_chart` | Executes query and returns Vega-Lite plus PNG base64 | `pass` for Rust contract: DuckDB ADBC E2E proves SQL execution, Vega-Lite payload, row count, and PNG data URL; Rust renderer is intentionally simpler than Python Altair/vl-convert and no MCP Apps UI resource is emitted | Add richer visual parity only if chart rendering becomes a product promise |
| `semantic://catalog` resource | Returns JSON catalog metadata | `pass`: `resources/list` and `resources/read` covered; missing resource returns MCP resource-not-found | Keep covered |
| Non-ADBC behavior | Clear built-without-ADBC error | `pass`: covered for structured, SQL, and chart execution | Keep covered |
| Error shape | JSON-RPC tool errors with useful messages | `pass`: invalid references, missing ADBC, missing resources, and unknown tools are covered | Keep actionable errors covered |

## LSP

| Behavior | Python contract | Current Rust status | Next action |
| --- | --- | --- | --- |
| Lifecycle | initialize, initialized, shutdown | `pass` smoke | Keep covered |
| Diagnostics | SQL definition diagnostics on open/change/save | `partial` for intended SQL-definition scope: open/change parse diagnostics, repair clearing, and unsupported adapter diagnostics are covered; save-specific diagnostics need a direct assertion before claiming save coverage | Add `didSave` smoke assertion if save diagnostics are part of the contract |
| Completion | SQL definition completions and Python constructor completions | `pass` for intended Rust LSP SQL-definition scope; Python constructor support is an explicit non-goal for this Rust standalone baseline | Keep SQL scope covered |
| Hover | Keyword/property hover | `pass` for intended SQL-definition scope: keyword/property hover is covered | Add model/dimension/metric instance docs only if Rust LSP scope grows |
| Formatting | Formats SQL definition documents | `pass`: canonical multiline formatting covered | Keep covered |
| Code actions | Missing name quick fix | `pass`: quick fix covered | Keep covered |
| Signature help | SQL/Python constructor signatures | `pass` for intended Rust LSP SQL-definition scope; Python constructor support is an explicit non-goal for this Rust standalone baseline | Keep SQL scope covered |
| Definition/references | Jump and references for model members | `pass` for current-document SQL definitions; cross-file/Python indexing is explicitly out of scope | Keep current-document scope covered |
| Document symbols | SQL and Python constructor symbols | `pass` for intended Rust LSP SQL-definition scope; Python constructor symbols are out of scope | Keep SQL scope covered |
| Rename | Workspace edit for model/member rename | `pass` for current-document SQL definitions; cross-file/Python edits are out of scope | Keep current-document scope covered |
| Python definition docs | Python `Model`, `Dimension`, `Metric` extraction | `skip`: Rust LSP is scoped to SQL definitions in this branch | Revisit only if Rust LSP becomes a Python authoring server |

## Workbench

| Behavior | Python contract | Current Rust status | Next action |
| --- | --- | --- | --- |
| CLI launch | `sidemantic workbench DIRECTORY`, `tree` alias, demo/db/connection options | `pass`: Rust workbench supports model path/db/connection/demo modes and `tree` delegates to workbench; Python CLI remains a separate package surface | Keep Rust standalone CLI covered |
| Bad model path | Fails clearly | `pass` | Keep covered |
| Browse models | TUI model browsing | `pass`: deterministic key/state tests cover sorted model list, focus, selection, and template loading | Keep covered |
| SQL rewrite | Query editor rewrite workflow | `pass`: F5/rewrite covered by TUI state tests and smoke | Keep covered |
| DB execution | Execute rewritten SQL when ADBC enabled | `pass`: DuckDB ADBC PTY smoke covers DB-backed execution and result text | Keep DuckDB covered; broader drivers remain matrix-gated skips |
| No DB execution | Clear disabled/no-connection state | `pass` smoke | Keep covered |
| Result rendering | Table/error rendering in TUI | `pass` for deterministic table/chart render snapshots and no-DB execution status; richer visual snapshots remain optional | Keep covered |
| Chart controls | Chart/table switching | `pass`: deterministic state/render tests cover chart/table view switching and chart control state | Keep covered |
| Exit | Esc/Ctrl-C exits | `pass` smoke | Keep covered |

## ADBC Integration Matrix

| Driver/service | Existing Python coverage | Rust target | Local status | CI/status action |
| --- | --- | --- | --- | --- |
| DuckDB | Python adapter supports DuckDB URL/dialect; local Python wheel `adbc_driver_duckdb` is installed | Direct Rust ADBC, CLI `run`, HTTP structured/SQL/raw, MCP structured/SQL/chart, workbench smoke | `pass`: real DuckDB shared library found and E2E green | Keep in local and CI when DuckDB dylib/so is available |
| SQLite | Python adapter tests target SQLite ADBC package/DBC path | Rust ADBC if SQLite ADBC driver shared library is available | `skip`: only native `libsqlite3` found locally, no SQLite ADBC driver library/package | Env-gated Rust matrix test skips until `SIDEMANTIC_TEST_ADBC_SQLITE_DRIVER` is set |
| PostgreSQL | `tests/db/test_adbc_ci_smoke.py` gated by `ADBC_TEST=1`, URL/env/service required | Rust ADBC URL/option parsing plus live execution when driver/service env exists | `skip`: no local ADBC driver/service credentials | Env-gated Rust matrix test skips until `SIDEMANTIC_TEST_ADBC_POSTGRES_DRIVER` is set |
| BigQuery | Python ADBC CI smoke gated by env/credentials | Rust ADBC URL/option parsing plus live execution when credentials exist | `skip`: no local ADBC driver/credentials | Env-gated Rust matrix test skips until `SIDEMANTIC_TEST_ADBC_BIGQUERY_DRIVER` is set |
| Snowflake | Python ADBC CI smoke gated by env/credentials | Rust ADBC URL/option parsing plus live execution when credentials exist | `skip`: no local ADBC driver/credentials | Env-gated Rust matrix test skips until `SIDEMANTIC_TEST_ADBC_SNOWFLAKE_DRIVER` is set |
| ClickHouse | Python ADBC CI smoke gated by env/service | Rust ADBC URL/option parsing plus live execution when driver/service env exists | `skip`: no local ADBC driver/service | Env-gated Rust matrix test skips until `SIDEMANTIC_TEST_ADBC_CLICKHOUSE_DRIVER` is set |

## Future Work

Detailed follow-up work is tracked in `docs/rust-standalone-followup-work.md`.

Highest-priority follow-ups:

1. Decide whether to add `api-serve` or keep Rust HTTP command naming separate from the Python HTTP command.
2. Add MCP `parameters` support or keep MCP documented as a narrower structured query schema than HTTP.
3. Add direct HTTP/MCP assertions for `segments`, pre-aggregation routing, and every field claimed in this matrix.
4. Add LSP `didSave` and unknown-method assertions only if those are part of the Rust LSP contract.
5. Add richer MCP Apps chart UI parity only if clients require embedded UI resources instead of plain Vega-Lite plus PNG.
6. Keep ADBC matrix expanding as drivers/services become available through env-configured CI.
