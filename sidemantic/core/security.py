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

import re

from jinja2 import Environment, StrictUndefined, TemplateError, Undefined, UndefinedError
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

    def __hash__(self) -> int:
        return hash((self.access, tuple(self.row_filters)))


def _sql_literal(value) -> str:
    """Convert a value produced by a ``{{ }}`` output to a safe SQL literal string.

    Strings are single-quoted with embedded quotes doubled; bools become TRUE/FALSE;
    ints/floats render bare; None becomes NULL. Unsupported types raise so a caller
    cannot smuggle an object whose ``repr``/``str`` is attacker-controlled SQL.
    """
    if isinstance(value, Undefined):
        # A missing attribute under StrictUndefined: force it to raise its UndefinedError
        # (deny) rather than falling through to the unsupported-type branch below.
        str(value)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"unsupported user-attribute type for a row filter: {type(value).__name__}")


# Dedicated environment for rendering row filters. ``finalize`` converts the value of each
# ``{{ ... }}`` OUTPUT expression to a SQL literal, so interpolated attributes are always safely
# quoted -- but control-flow expressions (``{% if user.is_admin %}``, ``user.role == 'admin'``)
# still see the RAW Python values, preserving truthiness and comparisons. This is why we pass
# raw user_attributes to render() rather than pre-wrapping them.
_row_filter_env = Environment(
    variable_start_string="{{",
    variable_end_string="}}",
    block_start_string="{%",
    block_end_string="%}",
    comment_start_string="{#",
    comment_end_string="#}",
    autoescape=False,
    undefined=StrictUndefined,
    finalize=_sql_literal,
)


# Matches a single- or double-quote pair immediately hugging a ``{{ ... }}`` placeholder,
# e.g. ``'{{ user.region }}'``. Such author quotes are stripped before rendering because the
# value already renders as a complete, correctly-quoted SQL literal; leaving the author quotes
# would double-quote strings (``''US''``) and, for the unquoted form, is what allowed injection.
_HUGGING_QUOTES = re.compile(r"(['\"])\s*(\{\{.*?\}\})\s*\1")


def render_row_filter(filter_template: str, user_attributes: dict) -> str:
    """Render a row-filter template against user attributes as a safe SQL fragment.

    The only namespace exposed is ``user``. Every ``{{ user.x }}`` renders as a
    complete, type-correct SQL literal (strings always single-quoted and escaped),
    so an attribute value can never break out of its predicate -- this holds for
    BOTH the quoted (``col = '{{ user.x }}'``) and unquoted (``col = {{ user.x }}``)
    template forms; author quotes hugging a placeholder are stripped first.
    Rendering uses ``StrictUndefined`` so a filter referencing an attribute the
    caller did not supply raises rather than silently rendering an unscoped predicate.

    Args:
        filter_template: SQL filter template, e.g. ``"tenant_id = {{ user.tenant_id }}"``.
        user_attributes: Mapping bound to the ``user`` namespace.

    Returns:
        The rendered SQL fragment.

    Raises:
        SecurityError: If the template references an undefined ``user`` attribute,
            is otherwise malformed, or an attribute has an unsupported type.
    """
    # Imported lazily to avoid a circular import (semantic_layer imports this module).
    from sidemantic.core.semantic_layer import SecurityError

    try:
        normalized = _HUGGING_QUOTES.sub(r"\2", filter_template)
        template = _row_filter_env.from_string(normalized)
        # Pass RAW attributes: the environment's finalize quotes each {{ }} output into a SQL
        # literal, while {% if %}/comparisons see real values (so booleans and equality work).
        return template.render(user=user_attributes if user_attributes is not None else {})
    except UndefinedError as exc:
        raise SecurityError(
            f"Row filter {filter_template!r} references an undefined user attribute: {exc}. "
            "Provide the attribute in user_attributes or remove it from the filter."
        ) from exc
    except TypeError as exc:
        raise SecurityError(f"Row filter {filter_template!r} has an unsupported attribute value: {exc}") from exc
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
