"""Security policy definitions - model access control and row-level filtering.

This module defines ONLY the data model and a pure rendering helper for security
policies. It performs NO enforcement: nothing here is wired into the query path.
A separate follow-up work item builds enforcement on top of these definitions, so
the field names and semantics documented below are load-bearing.
"""

from pydantic import BaseModel, Field

# NOTE: SecurityError lives in sidemantic.core.semantic_layer (next to the other custom
# errors). It is imported lazily inside render_row_filter rather than at module level,
# because semantic_layer pulls in the SQL generator / rust bridge; importing it eagerly
# here would drag those into every `from sidemantic import Model` (Model imports this
# module), which must stay lightweight for Pyodide.


class SecurityPolicy(BaseModel):
    """Security policy attached to a model via ``Model.security``.

    A security policy declares who may query a model (``access``) and how the
    model's rows must be filtered per user (``row_filters``). Both use Jinja
    templates whose only namespace is ``user`` - a mapping of the querying user's
    attributes (e.g. ``{{ user.region }}``, ``{{ user.role }}``).

    Intended enforcement semantics (implemented by the future enforcement layer,
    NOT here):

    - ``access`` is evaluated as a Jinja expression over the ``user`` namespace.
      A falsy result means the model is not queryable by that user. A plain bool
      is allowed as a constant (``True`` = always allowed, ``False`` = never).
    - Each entry in ``row_filters`` is a SQL fragment template. At query time the
      rendered fragments are ANDed together and injected into the model's WHERE
      clause *inside that model's CTE*, so they scope the model's own rows before
      any join or aggregation.
    - ``user`` is the ONLY template namespace exposed to these templates.
    - Deny-by-default: if a model carries a ``security`` block and a query supplies
      no user attributes, the enforcement layer raises (a query cannot silently
      bypass a declared policy). Undefined attribute references likewise raise.

    Example:
        SecurityPolicy(
            access="user.role in ['analyst', 'admin']",
            row_filters=["region = '{{ user.region }}'"],
        )
    """

    access: str | bool = Field(
        default=True,
        description=(
            "Jinja expression over the `user` namespace deciding model queryability; "
            "a falsy result means the model is not queryable. A plain bool is a constant "
            "(True = always allowed, False = never)."
        ),
    )
    row_filters: list[str] = Field(
        default_factory=list,
        description=(
            "SQL fragment templates with Jinja `user` refs (e.g. \"region = '{{ user.region }}'\"). "
            "Rendered fragments are ANDed into the model's WHERE clause inside its CTE at query time."
        ),
    )

    def __hash__(self) -> int:
        return hash((self.access, tuple(self.row_filters)))


def render_row_filter(filter_template: str, user_attributes: dict) -> str:
    """Render a single row-filter template against a user's attributes.

    Pure helper: it renders one ``row_filters`` template and returns the resulting
    SQL fragment. It performs no escaping, quoting, or validation of the injected
    values - that is the responsibility of the enforcement layer. This helper is
    NOT wired into the query path; it exists so the enforcement work item can reuse
    a single, tested rendering path.

    The user's attributes are exposed under the ``user`` namespace only, matching
    the documented ``SecurityPolicy`` semantics. Rendering uses StrictUndefined, so
    any reference to an attribute not present in ``user_attributes`` raises
    ``SecurityError`` rather than silently rendering an empty string.

    Args:
        filter_template: A single ``row_filters`` entry, e.g. "region = '{{ user.region }}'".
        user_attributes: Mapping of the querying user's attributes, exposed as ``user``.

    Returns:
        The rendered SQL fragment.

    Raises:
        SecurityError: If the template references an undefined user attribute, or has
            a Jinja syntax error.
    """
    # Reuse the SQL-friendly Jinja environment from the template module rather than
    # duplicating its delimiter/autoescape configuration.
    from jinja2 import StrictUndefined, TemplateError

    from sidemantic.core.semantic_layer import SecurityError
    from sidemantic.core.template import SQLTemplateRenderer

    env = SQLTemplateRenderer().env.overlay(undefined=StrictUndefined)
    try:
        template = env.from_string(filter_template)
        return template.render(user=user_attributes)
    except TemplateError as e:
        raise SecurityError(f"Failed to render row filter {filter_template!r}: {e}") from e
