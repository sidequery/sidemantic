# Sidemantic

Universal SQL-first semantic layer in Python. Imports from Cube/LookML/dbt/Hex/Rill/Malloy, queries against DuckDB/Postgres/BigQuery/Snowflake/etc.

## Usage Preference (Important)

- Sidemantic usage is overwhelmingly CLI-first in practice (roughly 95% CLI usage).
- Default to `sidemantic` CLI workflows (`validate`, `info`, `query`, `migrate`, etc.).
- Treat Python API examples as secondary, advanced automation paths, not the default onboarding path.

## Codebase Structure

**Main Python package:** `sidemantic/`
- `core/` - SemanticLayer, Model, Dimension, Metric, SemanticGraph
- `adapters/` - Format parsers (Cube, LookML, MetricFlow, Hex, Rill, Malloy, etc.)
- `db/` - Database adapters
- `sql/` - SQL generation and query rewriting
- `widget/`, `server/`, `workbench/`, `mcp_server.py` - Optional features with lazy imports

**Rust runtime and DuckDB extension (experimental):**
- `sidemantic-rs/` - Rust native runtime, also built as the `sidemantic-rs` Python extension. Python opts in through `sidemantic/rust_bridge.py`, the optional `rust` extra, and `--engine rust|auto`; Python stays the default engine (see `docs/rust-engine-mode.md`).
- `sidemantic-duckdb/` - DuckDB extension wrapping sidemantic-rs

CI runs the Rust and DuckDB jobs when their dirs change, and the Rust jobs also when the Python code they exercise changes (bridge, core, SQL, CLI; see the paths filter in `.github/workflows/ci.yml`).

## Version Management

Update BOTH when releasing:
- `pyproject.toml`: `version = "X.Y.Z"`
- `sidemantic/__init__.py`: `__version__ = "X.Y.Z"`

## Validation Before Commits

Match validation scope to the change instead of running the entire suite automatically.

- For small, localized changes, run ruff on the changed Python files and the narrowest relevant tests. This is the default.
- Expand to the affected package or subsystem when shared code, planners, adapters, or fixtures may have broader impact.
- Run the full CI-equivalent sequence only when explicitly requested, preparing a release, changing dependencies/CI/tooling, making a broad cross-cutting refactor, or when targeted checks cannot bound the blast radius.
- Do not rerun the full suite for a small follow-up on a branch that already had a green full run when targeted tests cover the follow-up.
- Before handoff, report exactly what ran and what was left to CI.

When full CI parity is warranted, run these commands in order:

```bash
# Run these in order:
uv sync --extra dev
uv run python - <<'PY'
import sys

from sidemantic import Dimension, Metric, Model

assert Model.__name__ == "Model"
assert Dimension.__name__ == "Dimension"
assert Metric.__name__ == "Metric"
assert "sidemantic_dax" not in sys.modules
PY
uv sync --extra dev --extra dax
uv run ruff check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar
uv run ruff format --check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar
uv run python -m playwright install --with-deps chromium
uv run pytest -v
```

If any fail, fix them:
```bash
# Fix ruff issues
uv run ruff check --fix . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar
uv run ruff format . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar
```

**PRs:** Do not include test commands in PR bodies unless explicitly requested.

**Why this matters:**
- Targeted checks keep small changes fast while still covering the affected behavior
- CI runs the full matrix and remains the final integration gate
- CI installs with `uv sync --extra dev`, so ruff and other CI tools must be in `[project.optional-dependencies] dev`
- `[dependency-groups] dev` alone is not enough (uv-only; CI reads the `dev` extra)

## Dependency Management

- Use `uv` for all Python package management
- Dev tools such as ruff go in the `dev` extra (`[project.optional-dependencies] dev`); `[dependency-groups] dev` mirrors them for local uv use
- DO NOT add dev tools to main dependencies unless explicitly requested
- Optional features use `[project.optional-dependencies]`:
  - `workbench` - textual, plotext, textual-plotext (for TUI, NOT Pyodide compatible)
  - `serve` - riffq, pyarrow (for the PostgreSQL wire server, `sidemantic server postgres`; NOT Pyodide compatible)
  - `api` / `mcp` - fastapi, uvicorn, pyarrow / mcp[cli] (for `sidemantic serve`, `server api`, `server mcp`)
  - `dev` - pytest, ruff, pandas, numpy (for development)

**CRITICAL: Pyodide Compatibility Rules**
- **Core dependencies** (in main `dependencies` list) MUST work in Pyodide/WASM
- **Optional dependencies** can use packages incompatible with Pyodide (textual, riffq, pyarrow, mcp)
- **ALL imports** of optional deps MUST be lazy (inside functions, NOT at module level)
- **Test**: `from sidemantic import Model, Dimension, Metric` must work without any optional deps

## Pyodide Compatibility

**Import Structure:**
- `sidemantic/__init__.py` - Only imports core classes (Model, Dimension, etc.)
- `sidemantic/cli.py` - Imports typer (core dep) at top, workbench/server imports inside command functions
- `sidemantic/workbench/__init__.py` - Lazy imports textual inside `run_workbench()` function
- `sidemantic/server/` - Not imported by `sidemantic/__init__.py`; `server.py`/`connection.py` (riffq) load only for `sidemantic server postgres`, and the dependency-free `common.py` is shared with the API, MCP, and chart modules

**Pyodide install path** (`docs/pyodide.md`, `.github/workflows/pyodide-test.yml`):
- Pyodide's bundled `pydantic`, `pyyaml`, and `jinja2` are loaded first; `sqlglot`, `lkml`, and `inflect` are installed with `deps=False`
- Bundled Pyodide packages pin their own versions (including typing-extensions), so a core dep that needs newer versions breaks this path

**Version constraints with "PYODIDE:" comments are REQUIRED:**
- Heavy deps (textual, riffq, mcp) are optional to avoid Pyodide conflicts
- Pyodide CI builds local wheel and installs it with `deps=False`
- This ensures code changes are tested in Pyodide before publish
- If adding new core deps, check they work in Pyodide or make them optional

**If Pyodide CI fails:**
- Check if a dep version changed that requires newer typing-extensions
- Either constrain that dep OR update the install sequence in `docs/pyodide.md` and `pyodide-test.yml` together
- CI installs local wheel to test current code, not PyPI version

## Testing

Use the validation scope above. Prefer focused test paths or test names for localized changes; reserve `uv run pytest -v` for cases that warrant full CI parity.

## Publishing to PyPI

```bash
gh workflow run publish.yml
```

## Common Mistakes to Avoid

1. **Breaking ruff** - Always ensure ruff is installed in dev dependencies
2. **Pyodide compatibility** - Keep heavy deps (textual, pygments) optional
3. **Not linting** - Format and lint EVERY TIME before commit
