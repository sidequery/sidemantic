"""Relative date range helper for parsing common date expressions."""

import re


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

    PATTERNS = {
        # Today/yesterday/tomorrow
        r"^today$": lambda: "CURRENT_DATE",
        r"^yesterday$": lambda: "CURRENT_DATE - 1",
        r"^tomorrow$": lambda: "CURRENT_DATE + 1",
        # Last N days/weeks/months/years
        r"^last (\d+) day(?:s)?$": lambda n: f"CURRENT_DATE - {n}",
        r"^last (\d+) week(?:s)?$": lambda n: f"CURRENT_DATE - {int(n) * 7}",
        r"^last (\d+) month(?:s)?$": lambda n: f"DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{n} months'",
        r"^last (\d+) year(?:s)?$": lambda n: f"DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '{n} years'",
        # This/last/next week
        r"^this week$": lambda: "DATE_TRUNC('week', CURRENT_DATE)",
        r"^last week$": lambda: "DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'",
        r"^next week$": lambda: "DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'",
        # This/last/next month
        r"^this month$": lambda: "DATE_TRUNC('month', CURRENT_DATE)",
        r"^last month$": lambda: "DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'",
        r"^next month$": lambda: "DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'",
        # This/last/next quarter
        r"^this quarter$": lambda: "DATE_TRUNC('quarter', CURRENT_DATE)",
        r"^last quarter$": lambda: "DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months'",
        r"^next quarter$": lambda: "DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'",
        # This/last/next year
        r"^this year$": lambda: "DATE_TRUNC('year', CURRENT_DATE)",
        r"^last year$": lambda: "DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'",
        r"^next year$": lambda: "DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'",
    }

    @classmethod
    def parse(cls, expr: str) -> str | None:
        """Parse a relative date expression to SQL.

        Args:
            expr: Relative date expression (e.g., "last 7 days")

        Returns:
            SQL date expression or None if not recognized

        Examples:
            >>> RelativeDateRange.parse("last 7 days")
            'CURRENT_DATE - 7'
            >>> RelativeDateRange.parse("this month")
            "DATE_TRUNC('month', CURRENT_DATE)"
        """
        expr = expr.lower().strip()

        for pattern, sql_func in cls.PATTERNS.items():
            match = re.match(pattern, expr)
            if match:
                if match.groups():
                    # Extract numeric argument
                    return sql_func(match.group(1))
                else:
                    return sql_func()

        return None

    @classmethod
    def to_range(cls, expr: str, column: str = "date_col") -> str | None:
        """Convert relative date expression to a SQL range filter.

        Args:
            expr: Relative date expression
            column: Column name to filter on

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
            sql_expr = cls.parse(expr)
            if sql_expr:
                return f"{column} >= {sql_expr}"

        # For "this/last month/quarter/year" - use range
        if any(word in expr for word in ["month", "quarter", "year"]) and expr.startswith(("this ", "last ", "next ")):
            start_sql = cls.parse(expr)
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
            sql_expr = cls.parse(expr)
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
