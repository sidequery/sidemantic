"""Structural validation for executable Apache Ossie SQL expressions.

The expression-language proposal is versioned independently from the JSON
schemas. This validator deliberately enforces only its stable scalar-expression
boundary. Dialect-specific functions remain extensions and are not rejected by
an ANSI function allowlist.
"""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp

OSSIE_EXPRESSION_PROPOSAL_COMMIT = "88e0011148283302c9a04cd0287e00e0b9d87354"
OSSIE_EXPRESSION_PROPOSAL_PATH = "core-spec/expression_language.md"

_FORBIDDEN_NODE_NAMES = (
    "Query",
    "DDL",
    "DML",
    "Command",
    "Drop",
    "Transaction",
    "Commit",
    "Rollback",
    "Where",
    "Group",
    "Join",
    "With",
)
_FORBIDDEN_NODE_TYPES = tuple(
    node_type for name in _FORBIDDEN_NODE_NAMES if isinstance((node_type := getattr(exp, name, None)), type)
)


def scalar_sql_expression_error(expression: str, *, sqlglot_dialect: str | None) -> str | None:
    """Return why SQL is not one scalar Ossie expression, otherwise ``None``.

    Parsing uses the selected executable dialect. The structural gate rejects
    queries, subqueries, clauses, set operations, DDL, DML, and commands even
    when SQLGlot accepts them as valid warehouse SQL.
    """

    try:
        parsed = sqlglot.parse(expression, read=sqlglot_dialect)
    except sqlglot.errors.SqlglotError as exc:
        return str(exc)
    if len(parsed) != 1 or parsed[0] is None:
        return "expression must contain exactly one SQL expression"

    root = parsed[0]
    for node in root.walk():
        if isinstance(node, _FORBIDDEN_NODE_TYPES):
            return f"Ossie expressions cannot contain {type(node).__name__}"
    return None
