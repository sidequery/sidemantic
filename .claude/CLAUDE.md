# Sidemantic Project Guidelines

## CRITICAL: Before Every Commit

**ALWAYS run linting and formatting before committing:**

```bash
uv run ruff check --fix .
uv run ruff format .
```

This is NON-NEGOTIABLE. If you modify Python code, you MUST lint and format before committing.

**Why this matters:**
- CI runs ruff check and will fail if code isn't formatted
- Ruff must be installed in `[project.optional-dependencies] dev` for CI
- NOT in `[dependency-groups]` (that's uv-specific, CI uses optional-dependencies)

## Dependency Management

- Use `uv` for all Python package management
- Ruff should be in dev dependencies (`[dependency-groups] dev`)
- DO NOT add dev tools to main dependencies unless explicitly requested
- Optional features use `[project.optional-dependencies]`:
  - `workbench` - textual, plotext (for TUI)
  - `serve` - riffq, pyarrow (for PostgreSQL server)

## Testing

Run tests before committing significant changes:
```bash
uv run pytest -v
```

## Common Mistakes to Avoid

1. **Breaking ruff** - Always ensure ruff is installed in dev dependencies
2. **Pyodide compatibility** - Keep heavy deps (textual, pygments) optional
3. **Not linting** - Format and lint EVERY TIME before commit
