"""Parameter definitions for dynamic query input.

Parameters allow users to pass values at query time that affect SQL generation,
similar to LookML parameters or dbt vars.
"""

from typing import Any, Literal

from pydantic import BaseModel, Field


class Parameter(BaseModel):
    """Parameter definition for user input.

    Parameters can be referenced in filters, SQL expressions, and metric definitions
    to create dynamic, user-configurable queries.
    """

    name: str = Field(..., description="Unique parameter name")
    type: Literal["string", "number", "date", "unquoted", "yesno"] = Field(
        ..., description="Parameter data type"
    )
    description: str | None = Field(None, description="Human-readable description")
    label: str | None = Field(None, description="Display label for UI")

    # Default value
    default_value: Any = Field(None, description="Default value if not provided")

    # For string/number types with limited options
    allowed_values: list[Any] | None = Field(
        None, description="List of allowed values (for dropdown/select)"
    )

    # For date parameters
    default_to_today: bool = Field(
        False, description="Default to current date (for date parameters)"
    )

    def __hash__(self) -> int:
        return hash(self.name)

    def format_value(self, value: Any) -> str:
        """Format parameter value for SQL interpolation.

        Args:
            value: Raw parameter value

        Returns:
            Formatted value safe for SQL
        """
        if value is None:
            value = self.default_value

        if self.type == "string":
            # Quote string values and escape internal quotes
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
        elif self.type == "date":
            # Format as quoted date string (SQLGlot will handle casting)
            return f"'{value}'"
        elif self.type == "number":
            # Numbers don't need quoting
            return str(value)
        elif self.type == "unquoted":
            # Unquoted (for table names, column names, etc.)
            return str(value)
        elif self.type == "yesno":
            # Boolean
            return "TRUE" if value else "FALSE"
        else:
            return str(value)


class ParameterSet:
    """Collection of parameter values for a query execution."""

    def __init__(self, parameters: dict[str, Parameter], values: dict[str, Any] | None = None):
        """Initialize parameter set.

        Args:
            parameters: Available parameters
            values: User-provided values
        """
        self.parameters = parameters
        self.values = values or {}

    def get(self, name: str) -> Any:
        """Get parameter value.

        Args:
            name: Parameter name

        Returns:
            Parameter value (user-provided or default)

        Raises:
            KeyError: If parameter doesn't exist
        """
        if name not in self.parameters:
            raise KeyError(f"Parameter {name} not found")

        param = self.parameters[name]

        # Check if user provided value
        if name in self.values:
            return self.values[name]

        # Use default
        if param.default_to_today and param.type == "date":
            from datetime import date

            return date.today().isoformat()

        return param.default_value

    def format(self, name: str) -> str:
        """Get formatted parameter value for SQL.

        Args:
            name: Parameter name

        Returns:
            Formatted SQL value
        """
        param = self.parameters[name]
        value = self.get(name)
        return param.format_value(value)

    def interpolate(self, sql: str) -> str:
        """Interpolate parameters into SQL string.

        Supports both simple {{ parameter_name }} and full Jinja templates.

        Args:
            sql: SQL with parameter placeholders or templates

        Returns:
            SQL with parameters interpolated
        """
        import re

        from sidemantic.core.template import is_sql_template, render_sql_template

        # Check if this is a full Jinja template (has conditionals, loops, etc.)
        if is_sql_template(sql) and any(marker in sql for marker in ["{%", "{#"]):
            # Use full template rendering
            # Build context with raw parameter values
            context = {}
            for name in self.parameters:
                context[name] = self.get(name)
            return render_sql_template(sql, context)

        # Otherwise use simple parameter substitution with SQL formatting
        # Find all {{ parameter_name }} patterns
        pattern = r"\{\{\s*(\w+)\s*\}\}"

        def replace(match):
            param_name = match.group(1)
            if param_name in self.parameters:
                return self.format(param_name)
            else:
                # Leave unchanged if not a parameter
                return match.group(0)

        return re.sub(pattern, replace, sql)
