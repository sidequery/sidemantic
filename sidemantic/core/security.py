"""Security policies for models: access control and row-level filtering.

A ``SecurityPolicy`` attaches to a ``Model`` and declares two independent controls:

- ``access``: a boolean gate. Either a literal ``bool`` or a Jinja boolean
  expression over the ``user`` namespace (e.g. ``"{{ user.role == 'admin' }}"``).
  A falsy result denies the whole query touching that model.
- ``row_filters``: a list of SQL filter templates rendered per-request over the
  same ``user`` namespace (e.g. ``"tenant_id = {{ user.tenant_id }}"``). Each is
  AND-ed into that model's own CTE so rows are scoped before joins/aggregation.

The ONLY template namespace is ``user``. Rendering uses ``StrictUndefined`` so a
row filter referencing an attribute the caller did not supply raises rather than
silently rendering an empty (and therefore unscoped) predicate.
"""

from __future__ import annotations

from jinja2 import Environment, StrictUndefined, TemplateError, UndefinedError
from pydantic import BaseModel, Field

# A dedicated environment for security templates. It uses StrictUndefined so any
# reference to an undefined `user` attribute raises instead of rendering empty
# (an empty row filter would silently widen access). Uses the same delimiters as
# the SQL template renderer to stay consistent with the rest of the codebase.
_security_env = Environment(
    variable_start_string="{{",
    variable_end_string="}}",
    block_start_string="{%",
    block_end_string="%}",
    comment_start_string="{#",
    comment_end_string="#}",
    autoescape=False,
    undefined=StrictUndefined,
)


class SecurityPolicy(BaseModel):
    """Model-level security policy: access gate plus row-level filters.

    Attributes:
        access: Whether the model may be queried. A literal ``bool`` or a Jinja
            boolean expression over ``user`` (rendered as ``{{ (EXPR) }}`` and
            interpreted truthy/falsy). Defaults to ``True`` (no access restriction).
        row_filters: SQL filter templates rendered per-request over ``user`` and
            AND-ed into the model's CTE (row-level security). Defaults to empty.
    """

    access: str | bool = Field(default=True, description="Access gate: bool or Jinja boolean expression over `user`")
    row_filters: list[str] = Field(
        default_factory=list,
        description="Row-level filter templates rendered over `user` and AND-ed into the model CTE",
    )


def _sql_escape_attributes(value):
    """Recursively SQL-escape string values so they cannot break out of a quoted literal.

    Doubles single quotes in strings (the standard SQL escape). Dicts and lists are walked
    so nested ``user`` attribute access stays escaped. Non-string scalars pass through
    unchanged so numeric/boolean predicates render as-is.
    """
    if isinstance(value, str):
        return value.replace("'", "''")
    if isinstance(value, dict):
        return {key: _sql_escape_attributes(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_sql_escape_attributes(item) for item in value)
    return value


def render_row_filter(filter_template: str, user_attributes: dict) -> str:
    """Render a row-filter template against user attributes.

    The only namespace exposed is ``user``. Rendering uses ``StrictUndefined`` so
    that a filter referencing an attribute the caller did not supply raises a
    ``SecurityError`` rather than rendering an empty (unscoped) predicate.

    Args:
        filter_template: SQL filter template, e.g. ``"tenant_id = {{ user.tenant_id }}"``.
        user_attributes: Mapping bound to the ``user`` namespace.

    Returns:
        The rendered SQL fragment.

    Raises:
        SecurityError: If the template references an undefined ``user`` attribute
            or is otherwise malformed.
    """
    # Imported lazily to avoid a circular import (semantic_layer imports this module).
    from sidemantic.core.semantic_layer import SecurityError

    try:
        template = _security_env.from_string(filter_template)
        # SQL-escape string attribute values before interpolation. Row filter templates
        # commonly wrap the value in single quotes (e.g. "email = '{{ user.email }}'"); a raw
        # value like "x' OR '1'='1" would otherwise break out of the literal and inject a
        # boolean condition. Doubling embedded single quotes keeps the value a single quoted
        # literal after sqlglot re-parses/re-serializes the fragment. Non-string values
        # (ints, bools) are left untouched so numeric predicates render correctly.
        safe_user = _sql_escape_attributes(user_attributes if user_attributes is not None else {})
        return template.render(user=safe_user)
    except UndefinedError as exc:
        raise SecurityError(
            f"Row filter {filter_template!r} references an undefined user attribute: {exc}. "
            "Provide the attribute in user_attributes or remove it from the filter."
        ) from exc
    except TemplateError as exc:
        raise SecurityError(f"Row filter {filter_template!r} failed to render: {exc}") from exc


def evaluate_access(access: str | bool, user_attributes: dict | None) -> bool:
    """Evaluate a model access gate to a boolean.

    Args:
        access: Literal ``bool`` (used directly) or a Jinja boolean expression
            over ``user``, compiled and evaluated to a truthy/falsy value.
        user_attributes: Mapping bound to the ``user`` namespace. ``None`` is
            treated as an empty mapping for evaluation purposes (deny-by-default
            for missing attributes is enforced by the caller, not here).

    Returns:
        The boolean result of the gate.

    Raises:
        SecurityError: If the expression references an undefined ``user`` attribute
            or is otherwise malformed.
    """
    if isinstance(access, bool):
        return access

    from sidemantic.core.semantic_layer import SecurityError

    # Accept both a bare Jinja expression ("user.role == 'admin'") and a fully wrapped
    # variable ("{{ user.role == 'admin' }}"). compile_expression wants the bare form, so
    # strip a single enclosing {{ ... }} when present.
    expr_source = access.strip()
    if expr_source.startswith("{{") and expr_source.endswith("}}"):
        expr_source = expr_source[2:-2].strip()

    try:
        # compile_expression evaluates a single Jinja expression and returns its
        # native Python value, so `user.role == 'admin'` yields a real bool.
        expr = _security_env.compile_expression(expr_source)
        return bool(expr(user=user_attributes if user_attributes is not None else {}))
    except UndefinedError as exc:
        raise SecurityError(
            f"Access expression {access!r} references an undefined user attribute: {exc}. "
            "Provide the attribute in user_attributes."
        ) from exc
    except TemplateError as exc:
        raise SecurityError(f"Access expression {access!r} failed to evaluate: {exc}") from exc
