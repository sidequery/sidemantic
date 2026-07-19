# UI architecture

Sidemantic supports several HTML hosts through one React component implementation. Runtime adapters may translate data and mount components, but must not contain component markup or styling.

## Canonical owners

| Concern | Canonical source | Other surfaces |
| --- | --- | --- |
| Product application shell, declarative dashboards, catalog, query orchestration, date controls, and interactive time series | `webapp/src/` | Built into the Python and Rust servers; `dashboard serve` supplies `GET /dashboard` |
| Reusable charts, leaderboards, previews, query debugging, and state primitives | `webapp/src/components/` via `webapp/src/ui.ts` | Built as React ESM and self-contained browser distributions |
| Portable standalone charts and experimental live crossfilter adapters | `sidemantic/viz.py` | Used by `sidemantic chart` and library callers |
| Notebook trait synchronization and mounting | `js/widget.js` and `sidemantic/widget/` | Mounts the canonical React distribution |
| MCP Apps chart embedding | `sidemantic/apps/chart_widget.html` | Returned by the MCP `create_chart` tool |

The React webapp, static/WASM apps, and AnyWidget have separate lifecycle adapters but share the same compiled component code. `webapp/src/static-api.tsx` is the sole imperative mount adapter.

The framework-free/WASM leaderboard is the visual champion for ranked cards: contiguous hairline panels, compact rows, lavender magnitude fills, full formatted values, and an understated expand affordance. The React `LeaderboardPanel` keeps ownership of querying, null handling, stale-result protection, and crossfilter behavior while rendering that presentation.

The consolidated component gallery is available at `/components`. `scripts/build_ui_distribution.py` produces `sidemantic-ui.js`, `sidemantic-ui-static.js`, and `sidemantic-ui.css` directly from `webapp/src`; there are no parallel JSX, DOM-renderer, or CSS component sources.

## Generated and synchronized copies

These files are deployment artifacts or synchronized examples, not independent component sources:

| Copy | Canonical source | Update command |
| --- | --- | --- |
| `sidemantic/ui/static/` | `webapp/` | `uv run scripts/build_webapp.py` |
| `sidemantic-rs/ui/` | `webapp/` | `uv run scripts/build_webapp.py` |
| `sidemantic/widget/static/widget.js` | `js/widget.js` | `cd js && bun run build` |
| `plugins/sidemantic/skills/webapp-builder/assets/ui-dist/` | `webapp/src/ui.ts` and `webapp/src/static-api.tsx` | `uv run scripts/build_ui_distribution.py` |
| `examples/sidemantic_wasm_demo/src/components/sidemantic/` | Built UI distribution | See below |

Synchronize the WASM demo with the canonical static component kit:

```bash
uv run plugins/sidemantic/skills/webapp-builder/scripts/copy_components.py \
  --kind static \
  --target examples/sidemantic_wasm_demo/src/components/sidemantic \
  --force
```

Check for drift without writing files:

```bash
uv run plugins/sidemantic/skills/webapp-builder/scripts/copy_components.py \
  --kind static \
  --target examples/sidemantic_wasm_demo/src/components/sidemantic \
  --check
```

The embedded React bundles have the same check mode:

```bash
uv run scripts/build_webapp.py --check
```

The AnyWidget bundle is checked the same way:

```bash
uv run scripts/build_widget.py --check
```

CI runs the React and AnyWidget check modes whenever their sources, build scripts, or committed artifacts change. The Python suite separately enforces exact parity between the framework-free kit and the WASM demo copy.

## Consolidation rules

1. Component markup and styling exist only under `webapp/src`.
2. Host-specific code is limited to transport, lifecycle, data adaptation, and mounting.
3. Standalone HTML consumes the self-contained browser bundle; it needs no server or package registry at runtime.
4. Generated distributions are never hand-edited.
