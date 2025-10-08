# Sidemantic Project Guidelines

## CRITICAL: Before Every Commit

**ALWAYS run the EXACT same commands CI runs before committing:**

```bash
# Run these in order:
uv run ruff check . --exclude docs/_extensions
uv run ruff format --check . --exclude docs/_extensions
uv run pytest -v
```

If any fail, fix them:
```bash
# Fix ruff issues
uv run ruff check --fix . --exclude docs/_extensions
uv run ruff format . --exclude docs/_extensions
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
  - `workbench` - textual, plotext (for TUI)
  - `serve` - riffq, pyarrow (for PostgreSQL server)
  - `mcp` - mcp[cli] (for MCP server, requires pydantic>=2.11 incompatible with Pyodide)

## Pyodide Compatibility

**Pyodide typing-extensions issue:**
- Pyodide has typing-extensions==4.11.0
- Some deps (pydantic>=2.10, inflect>=7.2) require typing-extensions>=4.12+
- Dashboard handles this by installing pydantic<2.10 with deps=False
- inflect<7.2 constraint in core deps marked with `# PYODIDE:` comment

**Version constraints with "PYODIDE:" comments are REQUIRED:**
- Heavy deps (textual, riffq) are optional to avoid Pyodide conflicts
- CI tests basic imports in Pyodide environment
- If adding new core deps, check they work in Pyodide or make them optional

## Testing

Run tests before committing significant changes:
```bash
uv run pytest -v
```

## Common Mistakes to Avoid

1. **Breaking ruff** - Always ensure ruff is installed in dev dependencies
2. **Pyodide compatibility** - Keep heavy deps (textual, pygments) optional
3. **Not linting** - Format and lint EVERY TIME before commit
