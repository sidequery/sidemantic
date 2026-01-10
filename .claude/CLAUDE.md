# Sidemantic

Universal SQL-first semantic layer in Python. Imports from Cube/LookML/dbt/Hex/Rill/Malloy, queries against DuckDB/Postgres/BigQuery/Snowflake/etc.

## Codebase Structure

**Main Python package:** `sidemantic/`
- `core/` - SemanticLayer, Model, Dimension, Metric, SemanticGraph
- `adapters/` - Format parsers (Cube, LookML, MetricFlow, Hex, Rill, Malloy, etc.)
- `db/` - Database adapters
- `sql/` - SQL generation and query rewriting
- `widget/`, `server/`, `workbench/`, `mcp_server.py` - Optional features with lazy imports

**Separate experimental implementations (not the main codebase):**
- `sidemantic-rs/` - Rust rewrite (WIP)
- `sidemantic-duckdb/` - DuckDB extension wrapping sidemantic-rs

These don't share code with Python. CI runs them only when their dirs change.

## Version Management

Update BOTH when releasing:
- `pyproject.toml`: `version = "X.Y.Z"`
- `sidemantic/__init__.py`: `__version__ = "X.Y.Z"`

## CRITICAL: Before Every Commit

**ALWAYS run the EXACT same commands CI runs before committing:**

```bash
# Run these in order:
uv run ruff check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar
uv run ruff format --check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar
uv run pytest -v
```

If any fail, fix them:
```bash
# Fix ruff issues
uv run ruff check --fix . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar
uv run ruff format . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar
```

This is NON-NEGOTIABLE. You MUST run these BEFORE every commit.

**Why this matters:**
- CI runs these exact commands and will fail if they don't pass
- You keep pushing broken code because you don't run these locally
- Ruff must be in `[project.optional-dependencies] dev` for CI
- NOT in `[dependency-groups]` (that's uv-specific, CI uses optional-dependencies)

## Dependency Management

- Use `uv` for all Python package management
- Ruff should be in dev dependencies (`[dependency-groups] dev`)
- DO NOT add dev tools to main dependencies unless explicitly requested
- Optional features use `[project.optional-dependencies]`:
  - `workbench` - textual, plotext, textual-plotext (for TUI, NOT Pyodide compatible)
  - `serve` - mcp[cli], riffq, pyarrow (for PostgreSQL server, NOT Pyodide compatible)
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
- `sidemantic/server/` - Never imported unless `sidemantic serve` command is run

**Pyodide typing-extensions issue:**
- Pyodide has typing-extensions==4.11.0
- Some deps (pydantic>=2.10, inflect>=7.2) require typing-extensions>=4.12+
- Dashboard handles this by installing pydantic<2.10 with deps=False
- inflect<7.2 constraint in core deps marked with `# PYODIDE:` comment

**Version constraints with "PYODIDE:" comments are REQUIRED:**
- Heavy deps (textual, riffq, mcp) are optional to avoid Pyodide conflicts
- Pyodide CI builds local wheel and installs it with `deps=False`
- This ensures code changes are tested in Pyodide before publish
- If adding new core deps, check they work in Pyodide or make them optional

**If Pyodide CI fails:**
- Check if a dep version changed that requires newer typing-extensions
- Either downgrade that dep OR add workaround in dashboard.qmd install
- CI installs local wheel to test current code, not PyPI version

## Testing

Run tests before committing significant changes:
```bash
uv run pytest -v
```

## Publishing to PyPI

```bash
gh workflow run publish.yml
```

## Common Mistakes to Avoid

1. **Breaking ruff** - Always ensure ruff is installed in dev dependencies
2. **Pyodide compatibility** - Keep heavy deps (textual, pygments) optional
3. **Not linting** - Format and lint EVERY TIME before commit
