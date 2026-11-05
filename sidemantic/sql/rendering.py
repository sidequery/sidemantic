"""SQLGlot emission that retains window NULL placement across target sessions."""

from __future__ import annotations

import sqlglot
from sqlglot import exp


def render_sql(expression: exp.Expression, *, dialect: str | sqlglot.Dialect, **options) -> str:
    """Render window sort items with explicit NULLS FIRST or NULLS LAST.

    SQLGlot ordinarily drops clauses matching its target's nominal default.
    DuckDB and Snowflake allow a session to change that default. Preserve the
    parsed ordering semantics without changing the keys, their evaluation, or
    RANGE frame arity. This works with both compiled and pure-Python SQLGlot.
    The opaque leaves exist only during final emission; the caller's AST stays
    available for subsequent structural transformations.
    """
    if not any(True for _ in expression.find_all(exp.Window)):
        return expression.sql(dialect=dialect, **options)
    rendered_tree = expression.copy()
    for ordered in reversed(list(rendered_tree.find_all(exp.Ordered))):
        if ordered.find_ancestor(exp.Window) is None:
            continue
        rendered = ordered.sql(dialect=dialect)
        for suffix in (" NULLS FIRST", " NULLS LAST"):
            if rendered.endswith(suffix):
                rendered = rendered[: -len(suffix)]
        rendered += " NULLS FIRST" if ordered.args.get("nulls_first") else " NULLS LAST"
        ordered.replace(exp.Var(this=rendered))
    return rendered_tree.sql(dialect=dialect, **options)
