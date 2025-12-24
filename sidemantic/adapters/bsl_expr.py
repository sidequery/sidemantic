"""Parser for Boring Semantic Layer (BSL) Ibis expression strings.

BSL uses Ibis-style expressions in string form:
- _.column -> column reference
- _.count() -> count aggregation
- _.column.sum() -> sum of column
- _.column.nunique() -> count distinct
- _.column.year() -> year extraction
- _.nested.field -> nested struct access
"""

import re
from dataclasses import dataclass


@dataclass
class ParsedExpr:
    """Parsed BSL expression."""

    column: str | None = None
    aggregation: str | None = None
    date_part: str | None = None  # year, month, day, etc.


# Aggregation method to sidemantic agg type
AGG_METHOD_MAP = {
    "sum": "sum",
    "mean": "avg",
    "avg": "avg",
    "min": "min",
    "max": "max",
    "count": "count",
    "nunique": "count_distinct",
}

# Sidemantic agg type to BSL method
AGG_TO_METHOD_MAP = {
    "sum": "sum",
    "avg": "mean",
    "min": "min",
    "max": "max",
    "count": "count",
    "count_distinct": "nunique",
}

# Date extraction methods
DATE_METHODS = {"year", "month", "day", "hour", "minute", "second", "week", "quarter"}

# Time grain mapping: BSL -> sidemantic granularity
TIME_GRAIN_MAP = {
    "TIME_GRAIN_DAY": "day",
    "TIME_GRAIN_WEEK": "week",
    "TIME_GRAIN_MONTH": "month",
    "TIME_GRAIN_QUARTER": "quarter",
    "TIME_GRAIN_YEAR": "year",
    "TIME_GRAIN_HOUR": "hour",
    "TIME_GRAIN_MINUTE": "minute",
    "TIME_GRAIN_SECOND": "second",
}

# Sidemantic granularity -> BSL time grain
GRANULARITY_TO_TIME_GRAIN = {v: k for k, v in TIME_GRAIN_MAP.items()}


def parse_bsl_expr(expr: str) -> ParsedExpr:
    """Parse BSL expression like '_.column.sum()' into components.

    Args:
        expr: BSL expression string

    Returns:
        ParsedExpr with column, aggregation, and/or date_part

    Examples:
        >>> parse_bsl_expr("_.column")
        ParsedExpr(column='column', aggregation=None, date_part=None)
        >>> parse_bsl_expr("_.count()")
        ParsedExpr(column=None, aggregation='count', date_part=None)
        >>> parse_bsl_expr("_.amount.sum()")
        ParsedExpr(column='amount', aggregation='sum', date_part=None)
        >>> parse_bsl_expr("_.created_at.year()")
        ParsedExpr(column='created_at', aggregation=None, date_part='year')
        >>> parse_bsl_expr("_.trafficSource.source")
        ParsedExpr(column='trafficSource.source', aggregation=None, date_part=None)
    """
    expr = expr.strip()

    if not expr.startswith("_."):
        # Not a BSL expression, might be a calc measure reference
        return ParsedExpr(column=expr)

    # Remove the leading "_."
    rest = expr[2:]

    # Check for _.count() - aggregation without column
    if rest == "count()":
        return ParsedExpr(aggregation="count")

    # Check for method call at the end: .method()
    method_match = re.match(r"^(.+)\.(\w+)\(\)$", rest)
    if method_match:
        column_part = method_match.group(1)
        method = method_match.group(2)

        if method in AGG_METHOD_MAP:
            return ParsedExpr(column=column_part, aggregation=method)
        elif method in DATE_METHODS:
            return ParsedExpr(column=column_part, date_part=method)
        else:
            # Unknown method, treat as column reference
            return ParsedExpr(column=rest)

    # No method call, just a column reference (possibly nested)
    return ParsedExpr(column=rest)


def bsl_to_sql(expr: str) -> tuple[str | None, str | None, str | None]:
    """Convert BSL expression to SQL expression, aggregation type, and date part.

    Args:
        expr: BSL expression string

    Returns:
        Tuple of (sql_expr, agg_type, date_part)

    Examples:
        >>> bsl_to_sql("_.amount.sum()")
        ('amount', 'sum', None)
        >>> bsl_to_sql("_.count()")
        (None, 'count', None)
        >>> bsl_to_sql("_.created_at.year()")
        ('created_at', None, 'year')
    """
    parsed = parse_bsl_expr(expr)

    sql_expr = parsed.column
    agg_type = AGG_METHOD_MAP.get(parsed.aggregation) if parsed.aggregation else None
    date_part = parsed.date_part

    return sql_expr, agg_type, date_part


def sql_to_bsl(sql: str | None, agg: str | None, date_part: str | None = None) -> str:
    """Convert SQL expression and aggregation to BSL expression.

    Args:
        sql: SQL column expression
        agg: Aggregation type (sum, avg, count, etc.)
        date_part: Date extraction part (year, month, etc.)

    Returns:
        BSL expression string

    Examples:
        >>> sql_to_bsl("amount", "sum", None)
        '_.amount.sum()'
        >>> sql_to_bsl(None, "count", None)
        '_.count()'
        >>> sql_to_bsl("created_at", None, "year")
        '_.created_at.year()'
        >>> sql_to_bsl("status", None, None)
        '_.status'
    """
    if agg == "count" and not sql:
        return "_.count()"

    if not sql:
        return "_."

    base = f"_.{sql}"

    if date_part and date_part in DATE_METHODS:
        return f"{base}.{date_part}()"

    if agg:
        method = AGG_TO_METHOD_MAP.get(agg)
        if method:
            return f"{base}.{method}()"

    return base


def is_calc_measure_expr(expr: str) -> bool:
    """Check if expression references other measures (calc measure).

    Calc measures reference other measures without the underscore prefix,
    using operators like + - * /

    Args:
        expr: Expression string

    Returns:
        True if this appears to be a calc measure expression

    Examples:
        >>> is_calc_measure_expr("revenue / order_count")
        True
        >>> is_calc_measure_expr("_.amount.sum()")
        False
    """
    # If it starts with _., it's a regular expression
    if expr.strip().startswith("_."):
        return False

    # Check for operators that indicate a calc measure
    operators = ["/", "+", "-", "*"]
    return any(op in expr for op in operators)


def parse_calc_measure(expr: str) -> list[str]:
    """Extract measure references from a calc measure expression.

    Args:
        expr: Calc measure expression like "revenue / order_count"

    Returns:
        List of referenced measure names

    Examples:
        >>> parse_calc_measure("revenue / order_count")
        ['revenue', 'order_count']
        >>> parse_calc_measure("(total_sales - total_costs) / total_sales")
        ['total_sales', 'total_costs', 'total_sales']
    """
    # Remove parentheses and split by operators
    # Find all word tokens that could be measure names
    tokens = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", expr)

    # Filter out SQL keywords and functions
    sql_keywords = {
        "NULLIF",
        "COALESCE",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
        "IF",
        "IIF",
    }

    return [t for t in tokens if t.upper() not in sql_keywords]
