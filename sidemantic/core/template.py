"""Template rendering for SQL fields."""

from jinja2 import Environment, TemplateSyntaxError


class SQLTemplateRenderer:
    """Renderer for Jinja2 templates in SQL fields.

    Integrates with the parameter system to provide templating in SQL expressions.
    """

    def __init__(self):
        """Initialize template environment with SQL-friendly settings."""
        self.env = Environment(
            # Use different delimiters to avoid conflicts with SQL
            variable_start_string="{{",
            variable_end_string="}}",
            block_start_string="{%",
            block_end_string="%}",
            comment_start_string="{#",
            comment_end_string="#}",
            # Don't auto-escape since we're generating SQL
            autoescape=False,
        )

    def render(self, template_str: str, context: dict) -> str:
        """Render a Jinja template with given context.

        Args:
            template_str: Template string with Jinja syntax
            context: Dictionary of variables to make available in template

        Returns:
            Rendered SQL string

        Raises:
            TemplateSyntaxError: If template has syntax errors

        Examples:
            >>> renderer = SQLTemplateRenderer()
            >>> renderer.render("SELECT * FROM {{ table }}", {"table": "orders"})
            'SELECT * FROM orders'
            >>> renderer.render("{% if active %}status = 'active'{% endif %}", {"active": True})
            "status = 'active'"
        """
        try:
            template = self.env.from_string(template_str)
            return template.render(**context)
        except TemplateSyntaxError as e:
            raise ValueError(f"Template syntax error: {e}") from e

    def is_template(self, sql: str) -> bool:
        """Check if a SQL string contains Jinja template syntax.

        Args:
            sql: SQL string to check

        Returns:
            True if string contains Jinja syntax
        """
        return any(marker in sql for marker in ["{{", "{%", "{#"])

    def render_if_template(self, sql: str, context: dict) -> str:
        """Render SQL only if it contains template syntax.

        Args:
            sql: SQL string that may contain templates
            context: Template context

        Returns:
            Rendered SQL if template found, otherwise original SQL
        """
        if self.is_template(sql):
            return self.render(sql, context)
        return sql


# Global renderer instance
_renderer = SQLTemplateRenderer()


def render_sql_template(sql: str, context: dict) -> str:
    """Render a SQL template with context.

    Args:
        sql: SQL string potentially containing Jinja templates
        context: Dictionary of variables for template

    Returns:
        Rendered SQL string
    """
    return _renderer.render_if_template(sql, context)


def is_sql_template(sql: str) -> bool:
    """Check if SQL contains Jinja template syntax.

    Args:
        sql: SQL string to check

    Returns:
        True if contains Jinja syntax
    """
    return _renderer.is_template(sql)
