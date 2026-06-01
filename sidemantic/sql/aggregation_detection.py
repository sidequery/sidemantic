"""Shared SQL aggregate-expression detection helpers."""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

# SQLGlot treats some engine-specific aggregate functions as Anonymous.
# Keep this focused on known aggregate forms we need to support.
_ANONYMOUS_AGGREGATE_FUNCTIONS = {
    "entropy",
    "geometric_mean",
    "kurtosis",
    "mode",
    "product",
    "skewness",
    "weighted_avg",
}

_AGGREGATE_REGEX = re.compile(
    r"\b("
    r"sum|count|avg|min|max|median|stddev|stddev_pop|variance|variance_pop|var_pop|mode|"
    r"quantile|percentile|product|entropy|kurtosis|skewness|geometric_mean|weighted_avg|list"
    r")\s*\(",
    re.IGNORECASE,
)


def expression_has_aggregate(expression: exp.Expression) -> bool:
    """Return True when expression contains an aggregate function."""
    if any(isinstance(node, exp.AggFunc) for node in expression.walk()):
        return True

    for node in expression.walk():
        if isinstance(node, exp.List):
            return True
        if isinstance(node, exp.Anonymous) and (node.name or "").lower() in _ANONYMOUS_AGGREGATE_FUNCTIONS:
            return True

    return False


def sql_has_aggregate(sql_expr: str, dialect: str = "duckdb") -> bool:
    """Return True when SQL expression contains an aggregate function."""
    try:
        parsed = sqlglot.parse_one(sql_expr, read=dialect)
        return expression_has_aggregate(parsed)
    except Exception:
        return bool(_AGGREGATE_REGEX.search(sql_expr))
