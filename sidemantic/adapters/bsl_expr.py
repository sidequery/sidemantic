"""Parser for Boring Semantic Layer (BSL) Ibis expression strings.

BSL uses Ibis-style expressions in string form:
- _.column -> column reference
- _.count() -> count aggregation
- _.column.sum() -> sum of column
- _.column.nunique() -> count distinct
- _.column.year() -> year extraction
- _.nested.field -> nested struct access
- (_.col1 - _.col2).sum() -> compound arithmetic with aggregation
- (_.col == "val").sum() -> boolean comparison with aggregation

Uses Python's ast module for parsing since these are valid Python expressions.
"""

import ast
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


def _collect_attrs(node: ast.AST) -> list[str]:
    """Collect attribute chain from AST node, returning list of attr names.

    For `_.foo.bar.baz`, returns ['foo', 'bar', 'baz'].
    """
    attrs = []
    while isinstance(node, ast.Attribute):
        attrs.append(node.attr)
        node = node.value
    # Should end at Name('_')
    if isinstance(node, ast.Name) and node.id == "_":
        attrs.reverse()
        return attrs
    return []


# Python AST operator to SQL operator
_OP_MAP: dict[type, str] = {
    ast.Add: "+",
    ast.Sub: "-",
    ast.Mult: "*",
    ast.Div: "/",
    ast.Mod: "%",
}

# Python AST comparison operator to SQL operator
_CMP_OP_MAP: dict[type, str] = {
    ast.Eq: "=",
    ast.NotEq: "!=",
    ast.Lt: "<",
    ast.LtE: "<=",
    ast.Gt: ">",
    ast.GtE: ">=",
}


def _expr_to_sql(node: ast.AST) -> str | None:
    """Convert an AST expression node to SQL.

    Handles _.column, _.nested.column, BinOp, Compare, and numeric constants.
    """
    if isinstance(node, ast.Attribute):
        attrs = _collect_attrs(node)
        if attrs:
            return ".".join(attrs)
        return None

    if isinstance(node, ast.BinOp):
        return _binop_to_sql(node)

    if isinstance(node, ast.Compare):
        return _compare_to_sql(node)

    if isinstance(node, ast.Constant):
        if isinstance(node.value, str):
            escaped = node.value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(node.value, (int, float)):
            return str(node.value)

    if isinstance(node, ast.Name) and node.id == "_":
        return None

    return None


def _binop_to_sql(node: ast.BinOp) -> str | None:
    """Convert a BinOp AST node to a SQL expression string.

    Wraps nested BinOp children in parens to preserve grouping:
    (_.a - (_.b + _.c)) -> "a - (b + c)"
    """
    left = _expr_to_sql(node.left)
    right = _expr_to_sql(node.right)
    op = _OP_MAP.get(type(node.op))

    if left is None or right is None or op is None:
        return None

    if isinstance(node.left, ast.BinOp):
        left = f"({left})"
    if isinstance(node.right, ast.BinOp):
        right = f"({right})"

    return f"{left} {op} {right}"


def _compare_to_sql(node: ast.Compare) -> str | None:
    """Convert a Compare AST node to SQL expression.

    (_.column == "value") -> "column = 'value'"
    """
    left = _expr_to_sql(node.left)
    if left is None:
        return None

    if len(node.ops) != 1 or len(node.comparators) != 1:
        return None

    op = _CMP_OP_MAP.get(type(node.ops[0]))
    if op is None:
        return None

    right = _expr_to_sql(node.comparators[0])
    if right is None:
        return None

    return f"{left} {op} {right}"


def _filter_node_to_sql(node: ast.AST) -> str | None:
    """Convert a BSL filter AST node to a SQL WHERE clause fragment.

    Handles comparisons, logical operators (& for AND, | for OR),
    and column references.
    """
    if isinstance(node, ast.Compare):
        return _compare_to_sql(node)

    # BSL uses & and | (bitwise ops) for logical AND/OR in filter expressions
    # because Python's `and`/`or` don't work with Ibis deferred expressions.
    # Wrap each side in parens to preserve precedence in mixed AND/OR filters.
    if isinstance(node, ast.BinOp):
        if isinstance(node.op, ast.BitAnd):
            left = _filter_node_to_sql(node.left)
            right = _filter_node_to_sql(node.right)
            if left and right:
                return f"({left}) AND ({right})"
            return None
        if isinstance(node.op, ast.BitOr):
            left = _filter_node_to_sql(node.left)
            right = _filter_node_to_sql(node.right)
            if left and right:
                return f"({left}) OR ({right})"
            return None
        return _binop_to_sql(node)

    if isinstance(node, ast.BoolOp):
        op = "AND" if isinstance(node.op, ast.And) else "OR"
        parts = [f"({p})" for p in (_filter_node_to_sql(v) for v in node.values) if p]
        if len(parts) == len(node.values):
            return f" {op} ".join(parts)
        return None

    # BSL uses ~ for logical NOT (Ibis convention)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Invert):
        operand = _filter_node_to_sql(node.operand)
        if operand:
            return f"NOT ({operand})"
        return None

    if isinstance(node, ast.Attribute):
        attrs = _collect_attrs(node)
        if attrs:
            return ".".join(attrs)
        return None

    if isinstance(node, ast.Constant):
        if isinstance(node.value, str):
            escaped = node.value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(node.value, (int, float)):
            return str(node.value)

    return None


def bsl_filter_to_sql(expr: str) -> str:
    """Convert a BSL filter expression to a SQL WHERE clause.

    Uses AST parsing for proper conversion of operators and column references.

    Examples:
        >>> bsl_filter_to_sql("_.year > 2020")
        'year > 2020'
        >>> bsl_filter_to_sql("(_.year > 2020) & (_.origin == 'LAX')")
        "year > 2020 AND origin = 'LAX'"
    """
    try:
        tree = ast.parse(expr.strip(), mode="eval")
        result = _filter_node_to_sql(tree.body)
        if result:
            return result
    except SyntaxError:
        pass
    # Fallback for unparseable expressions
    return re.sub(r"_\.(\w+)", r"\1", str(expr))


def _sql_to_bsl_expr(sql: str, agg: str | None) -> str:
    """Best-effort conversion of SQL expression to BSL form.

    Used only for cross-format conversion (e.g. Cube -> BSL) where
    no original BSL expression is available. For BSL->BSL roundtrip,
    the adapter stores and reuses the original expression instead.
    """
    # Simple column + aggregation (the common case for cross-format)
    if not any(f" {op} " in sql for op in ["+", "-", "*", "/", "%", "=", "!=", "<", ">", "<="]):
        base = f"_.{sql}"
        if agg:
            method = AGG_TO_METHOD_MAP.get(agg)
            if method:
                return f"{base}.{method}()"
        return base

    # Compound or comparison SQL: prefix column-like tokens with _.
    parts = re.split(r"(\s*(?:[+\-*/%]|[<>!=]=?)\s*)", sql)
    bsl_parts = []
    for part in parts:
        stripped = part.strip()
        if re.match(r"^[a-zA-Z_]\w*(\.\w+)*$", stripped):
            bsl_parts.append(f"_.{stripped}")
        else:
            bsl_parts.append(part)
    inner = "".join(bsl_parts)

    if agg:
        method = AGG_TO_METHOD_MAP.get(agg)
        if method:
            return f"({inner}).{method}()"
    return inner


def parse_bsl_expr(expr: str) -> ParsedExpr:
    """Parse BSL expression like '_.column.sum()' into components.

    Uses Python's ast module since BSL expressions are valid Python.

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

    if not expr.startswith("_.") and not expr.startswith("("):
        # Not a BSL expression, might be a calc measure reference
        return ParsedExpr(column=expr)

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError:
        # Fall back to treating as plain column reference
        return ParsedExpr(column=expr[2:] if expr.startswith("_.") else expr)

    node = tree.body

    # Case 1: Method call like _.count() or _.column.sum()
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        method = node.func.attr
        attrs = _collect_attrs(node.func.value)

        # _.count() - no column, just aggregation
        if not attrs and isinstance(node.func.value, ast.Name) and node.func.value.id == "_":
            if method in AGG_METHOD_MAP:
                return ParsedExpr(aggregation=method)

        # _.column.sum() or _.nested.field.sum()
        if attrs:
            column = ".".join(attrs)
            if method in AGG_METHOD_MAP:
                return ParsedExpr(column=column, aggregation=method)
            elif method in DATE_METHODS:
                return ParsedExpr(column=column, date_part=method)

        # Case 1b: Method call on compound expression like (_.a - _.b).sum()
        base = node.func.value
        if isinstance(base, (ast.BinOp, ast.Compare)) and method in AGG_METHOD_MAP:
            sql_expr = _expr_to_sql(base)
            if sql_expr:
                return ParsedExpr(column=sql_expr, aggregation=method)

    # Case 2: Attribute access like _.column or _.nested.field
    if isinstance(node, ast.Attribute):
        attrs = _collect_attrs(node)
        if attrs:
            return ParsedExpr(column=".".join(attrs))

    # Fallback
    return ParsedExpr(column=expr[2:] if expr.startswith("_.") else expr)


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
    expr = expr.strip()

    # If it starts with _., it's a regular BSL expression
    if expr.startswith("_."):
        return False

    # If it starts with ( and contains _. references, it's a compound BSL expression
    if expr.startswith("(") and "_." in expr:
        return False

    # Try to parse as Python and check for binary operations
    try:
        tree = ast.parse(expr, mode="eval")
        # Walk the tree looking for BinOp nodes (arithmetic operations)
        for node in ast.walk(tree):
            if isinstance(node, ast.BinOp):
                return True
        return False
    except SyntaxError:
        # If it doesn't parse as Python, check for operators as fallback
        return any(op in expr for op in ["/", "+", "-", "*"])


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
    # SQL keywords to filter out
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

    try:
        tree = ast.parse(expr, mode="eval")
        # Collect all Name nodes (variable references)
        names = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id.upper() not in sql_keywords:
                names.append(node.id)
        return names
    except SyntaxError:
        # Fallback: won't happen for valid calc measures, but be safe
        return []
