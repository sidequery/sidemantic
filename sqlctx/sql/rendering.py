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


def render_ossie_sql(expression: exp.Expression, *, dialect: str | sqlglot.Dialect, **options) -> str:
    """Render Ossie FILTER counts consistently across supported SQLGlot versions.

    Some older generators put a wildcard or DISTINCT inside CASE when lowering
    FILTER. Normalize only COUNT, whose conditional NULL inputs are ignored;
    aggregates that retain NULL values must keep their own lowering semantics.
    """
    filters = [node for node in expression.find_all(exp.Filter) if isinstance(node.this, exp.Count)]
    if not filters:
        return render_sql(expression, dialect=dialect, **options)
    target = sqlglot.Dialect.get_or_raise(dialect).__class__.__name__.lower()
    tree = expression.copy()
    for filtered in list(tree.find_all(exp.Filter)):
        count = filtered.this
        if not isinstance(count, exp.Count):
            continue
        argument = count.this
        if type(argument) is exp.Star and not any(argument.args.values()):
            count.set("this", exp.Literal.number(1))
            argument = count.this
        if target not in {"snowflake", "bigquery"} or argument is None:
            continue
        # Qualified or modified stars are not equivalent to COUNT(1).
        if argument.find(exp.Star) is not None:
            continue
        predicate = filtered.expression
        if isinstance(predicate, exp.Where):
            predicate = predicate.this
        if predicate is None:
            continue

        def conditional(value: exp.Expression) -> exp.Expression:
            return exp.Case(
                ifs=[exp.If(this=predicate.copy(), true=value.copy())],
                default=exp.Null(),
            )

        if isinstance(argument, exp.Distinct):
            count.set("this", exp.Distinct(expressions=[conditional(value) for value in argument.expressions]))
        else:
            count.set("this", conditional(argument))
        if filtered is tree:
            tree = count.copy()
        else:
            filtered.replace(count.copy())
    return render_sql(tree, dialect=dialect, **options)
