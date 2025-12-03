"""Relative date range helper for parsing common date expressions."""

import re


def _date_trunc(granularity: str, column_expr: str, dialect: str) -> str:
    """Generate dialect-specific DATE_TRUNC expression."""
    if dialect == "bigquery":
        return f"DATE_TRUNC({column_expr}, {granularity.upper()})"
    else:
        return f"DATE_TRUNC('{granularity}', {column_expr})"


class RelativeDateRange:
    """Helper for parsing and converting relative date expressions to SQL.

    Supports expressions like:
    - "last 7 days", "last 30 days"
    - "this week", "last week", "next week"
    - "this month", "last month", "next month"
    - "this quarter", "last quarter"
    - "this year", "last year"
    - "today", "yesterday", "tomorrow"
    """

    # Patterns that don't need DATE_TRUNC
    SIMPLE_PATTERNS = {
        r"^today$": lambda: "CURRENT_DATE",
        r"^yesterday$": lambda: "CURRENT_DATE - 1",
        r"^tomorrow$": lambda: "CURRENT_DATE + 1",
        r"^last (\d+) day(?:s)?$": lambda n: f"CURRENT_DATE - {n}",
        r"^last (\d+) week(?:s)?$": lambda n: f"CURRENT_DATE - {int(n) * 7}",
    }

    # Patterns that need DATE_TRUNC - return (granularity, template) tuples
    # Template uses {trunc} as placeholder for the DATE_TRUNC expression
    TRUNC_PATTERNS = {
        r"^last (\d+) month(?:s)?$": ("month", lambda n: "{trunc} - INTERVAL '" + n + " months'"),
        r"^last (\d+) year(?:s)?$": ("year", lambda n: "{trunc} - INTERVAL '" + n + " years'"),
        r"^this week$": ("week", lambda: "{trunc}"),
        r"^last week$": ("week", lambda: "{trunc} - INTERVAL '1 week'"),
        r"^next week$": ("week", lambda: "{trunc} + INTERVAL '1 week'"),
        r"^this month$": ("month", lambda: "{trunc}"),
        r"^last month$": ("month", lambda: "{trunc} - INTERVAL '1 month'"),
        r"^next month$": ("month", lambda: "{trunc} + INTERVAL '1 month'"),
        r"^this quarter$": ("quarter", lambda: "{trunc}"),
        r"^last quarter$": ("quarter", lambda: "{trunc} - INTERVAL '3 months'"),
        r"^next quarter$": ("quarter", lambda: "{trunc} + INTERVAL '3 months'"),
        r"^this year$": ("year", lambda: "{trunc}"),
        r"^last year$": ("year", lambda: "{trunc} - INTERVAL '1 year'"),
        r"^next year$": ("year", lambda: "{trunc} + INTERVAL '1 year'"),
    }

    @classmethod
    def parse(cls, expr: str, dialect: str = "duckdb") -> str | None:
        """Parse a relative date expression to SQL.

        Args:
            expr: Relative date expression (e.g., "last 7 days")
            dialect: SQL dialect for DATE_TRUNC syntax (default: duckdb)

        Returns:
            SQL date expression or None if not recognized

        Examples:
            >>> RelativeDateRange.parse("last 7 days")
            'CURRENT_DATE - 7'
            >>> RelativeDateRange.parse("this month")
            "DATE_TRUNC('month', CURRENT_DATE)"
        """
        expr = expr.lower().strip()

        # Check simple patterns first
        for pattern, sql_func in cls.SIMPLE_PATTERNS.items():
            match = re.match(pattern, expr)
            if match:
                if match.groups():
                    return sql_func(match.group(1))
                else:
                    return sql_func()

        # Check patterns that need DATE_TRUNC
        for pattern, (granularity, template_func) in cls.TRUNC_PATTERNS.items():
            match = re.match(pattern, expr)
            if match:
                trunc = _date_trunc(granularity, "CURRENT_DATE", dialect)
                if match.groups():
                    template = template_func(match.group(1))
                else:
                    template = template_func()
                return template.replace("{trunc}", trunc)

        return None

    @classmethod
    def to_range(cls, expr: str, column: str = "date_col", dialect: str = "duckdb") -> str | None:
        """Convert relative date expression to a SQL range filter.

        Args:
            expr: Relative date expression
            column: Column name to filter on
            dialect: SQL dialect for DATE_TRUNC syntax (default: duckdb)

        Returns:
            SQL WHERE clause expression or None if not recognized

        Examples:
            >>> RelativeDateRange.to_range("last 7 days", "created_at")
            'created_at >= CURRENT_DATE - 7'
            >>> RelativeDateRange.to_range("this month", "order_date")
            "order_date >= DATE_TRUNC('month', CURRENT_DATE) AND order_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"
        """
        expr = expr.lower().strip()

        # For "last N days/weeks" - use >= comparison
        if expr.startswith("last ") and any(unit in expr for unit in ["day", "week"]):
            sql_expr = cls.parse(expr, dialect)
            if sql_expr:
                return f"{column} >= {sql_expr}"

        # For "this/last month/quarter/year" - use range
        if any(word in expr for word in ["month", "quarter", "year"]) and expr.startswith(("this ", "last ", "next ")):
            start_sql = cls.parse(expr, dialect)
            if start_sql:
                # Determine the interval to add for end date
                if "month" in expr:
                    interval = "1 month"
                elif "quarter" in expr:
                    interval = "3 months"
                elif "year" in expr:
                    interval = "1 year"
                elif "week" in expr:
                    interval = "1 week"
                else:
                    interval = "1 day"

                return f"{column} >= {start_sql} AND {column} < {start_sql} + INTERVAL '{interval}'"

        # For single day expressions
        if expr in ["today", "yesterday", "tomorrow"]:
            sql_expr = cls.parse(expr, dialect)
            if sql_expr:
                return f"{column} = {sql_expr}"

        return None

    @classmethod
    def is_relative_date(cls, expr: str) -> bool:
        """Check if expression is a recognized relative date.

        Args:
            expr: Expression to check

        Returns:
            True if recognized as relative date expression
        """
        return cls.parse(expr) is not None
