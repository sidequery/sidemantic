"""Template rendering for SQL fields."""

from jinja2 import Environment, TemplateSyntaxError


def render_parameter_template(sql: str, context: dict, parameters: dict, dialect: str | None = None) -> str:
    """Keep Jinja control values raw while formatting every emitted SQL value.

    Opaque output markers let us determine the actual SQL quoting context after
    conditionals and loops run, without letting parameter content affect it.
    The generic trusted SQL renderer below intentionally retains its raw API.
    """
    import math
    import uuid

    from jinja2 import nodes
    from jinja2.visitor import NodeTransformer
    from sqlglot import Dialect, exp, parse_one
    from sqlglot.errors import TokenError
    from sqlglot.tokens import TokenType

    marker_prefix = "sidemantic_value_" + uuid.uuid4().hex + "_"
    outputs = {}

    def capture(value, name):
        marker = marker_prefix + str(len(outputs)) + "_end"
        outputs[marker] = (value, parameters.get(name))
        return marker

    class FormatOutputs(NodeTransformer):
        def visit_Output(self, node, *args, **kwargs):  # noqa: N802 - Jinja visitor API
            node.nodes = [
                child
                if isinstance(child, nodes.TemplateData)
                else nodes.Call(
                    nodes.Name("__sidemantic_emit", "load"),
                    [child, nodes.Const(child.name if isinstance(child, nodes.Name) else None)],
                    [],
                    None,
                    None,
                )
                for child in node.nodes
            ]
            return node

    env = Environment(autoescape=False)
    try:
        tree = FormatOutputs().visit(env.parse(sql))
        template = env.template_class.from_code(env, env.compile(tree), env.globals, None)
    except TemplateSyntaxError as exc:
        raise ValueError(f"Template syntax error: {exc}") from exc
    rendered = template.render(**context, __sidemantic_emit=capture)
    try:
        tokens = Dialect.get_or_raise(dialect).tokenize(rendered)
    except TokenError as exc:
        raise ValueError("Invalid SQL quoting in parameter template") from exc
    replacements = {}
    literal_values = {}
    for marker, (value, parameter) in outputs.items():
        start = rendered.find(marker)
        token = next((token for token in tokens if token.start <= start <= token.end), None)
        if token is None or marker not in token.text:
            raise ValueError("Parameter output requires a SQL value context")
        if parameter is not None:
            formatted = parameter.format_value(value, dialect=dialect)
        elif isinstance(value, bool):
            formatted = "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            if not math.isfinite(value):
                raise ValueError("Invalid numeric template value")
            formatted = str(value)
        elif value is None:
            formatted = "NULL"
        elif isinstance(value, str):
            formatted = exp.Literal.string(value).sql(dialect=dialect)
        else:
            raise ValueError("Template output must be a scalar SQL value")
        if token.token_type in {TokenType.STRING, TokenType.BYTE_STRING}:
            # Tokenization is dialect-aware and decodes the entire original
            # literal, including static escaped quotes before an interpolation.
            # Rebuild it as an ordinary literal so subsequent SQLGlot rewrites
            # never pass caller content through lossy ByteString serialization.
            prefix = rendered[token.start : start]
            if token.token_type == TokenType.BYTE_STRING or dialect in {
                "mysql",
                "bigquery",
                "spark",
                "databricks",
                "hive",
            }:
                if (len(prefix) - len(prefix.rstrip("\\"))) % 2:
                    raise ValueError("Parameter output cannot follow an unpaired SQL escape character")
            parsed_value = parse_one(formatted, read=dialect)
            text_value = (
                parsed_value.this if isinstance(parsed_value, exp.Literal) and parsed_value.is_string else formatted
            )
            key = (token.start, token.end + 1)
            literal_values[key] = literal_values.get(key, token.text).replace(marker, text_value)
        elif token.token_type == TokenType.VAR and token.text == marker:
            replacements[(token.start, token.end + 1)] = formatted
        else:
            raise ValueError("Parameter output requires a SQL value context")
    for key, value in literal_values.items():
        replacements[key] = exp.Literal.string(value).sql(dialect=dialect)
    for (start, end), replacement in sorted(replacements.items(), reverse=True):
        rendered = rendered[:start] + replacement + rendered[end:]
    return rendered


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
