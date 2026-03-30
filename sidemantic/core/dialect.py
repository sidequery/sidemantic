"""SQLGlot dialect extensions for Sidemantic SQL syntax.

Compatible with sqlglot's mypyc C extension (sqlglotc) by avoiding
subclasses of compiled classes (Parser, Expression). Uses factory
functions that return exp.Anonymous/exp.EQ nodes instead.
"""

import threading

from sqlglot import exp, parser, tokens
from sqlglot.dialects.dialect import Dialect

# Property name aliases (SQL syntax -> Python field name)
# Shared between parser and sql_definitions module
PROPERTY_ALIASES = {
    "expression": "sql",
    "aggregation": "agg",
    "filter": "filters",
}

# ---------------------------------------------------------------------------
# Definition type constants
# ---------------------------------------------------------------------------
MODELDEF = "ModelDef"
DIMENSIONDEF = "DimensionDef"
RELATIONSHIPDEF = "RelationshipDef"
METRICDEF = "MetricDef"
SEGMENTDEF = "SegmentDef"
PARAMETERDEF = "ParameterDef"
PREAGGREGATIONDEF = "PreAggregationDef"

_DEF_TYPES = {MODELDEF, DIMENSIONDEF, RELATIONSHIPDEF, METRICDEF, SEGMENTDEF, PARAMETERDEF, PREAGGREGATIONDEF}

_KEYWORD_TO_DEF = {
    "MODEL": MODELDEF,
    "DIMENSION": DIMENSIONDEF,
    "RELATIONSHIP": RELATIONSHIPDEF,
    "METRIC": METRICDEF,
    "SEGMENT": SEGMENTDEF,
    "PARAMETER": PARAMETERDEF,
    "PRE_AGGREGATION": PREAGGREGATIONDEF,
}

# ---------------------------------------------------------------------------
# Factory functions (same call-site syntax as the old classes)
# ---------------------------------------------------------------------------


def ModelDef(expressions):  # noqa: N802
    return exp.Anonymous(this=MODELDEF, expressions=expressions)


def DimensionDef(expressions):  # noqa: N802
    return exp.Anonymous(this=DIMENSIONDEF, expressions=expressions)


def RelationshipDef(expressions):  # noqa: N802
    return exp.Anonymous(this=RELATIONSHIPDEF, expressions=expressions)


def MetricDef(expressions):  # noqa: N802
    return exp.Anonymous(this=METRICDEF, expressions=expressions)


def SegmentDef(expressions):  # noqa: N802
    return exp.Anonymous(this=SEGMENTDEF, expressions=expressions)


def ParameterDef(expressions):  # noqa: N802
    return exp.Anonymous(this=PARAMETERDEF, expressions=expressions)


def PreAggregationDef(expressions):  # noqa: N802
    return exp.Anonymous(this=PREAGGREGATIONDEF, expressions=expressions)


def PropertyEQ(this, expression):  # noqa: N802
    eq = exp.EQ(this=this, expression=expression)
    eq.set("_property_eq", True)
    return eq


# ---------------------------------------------------------------------------
# Type-checking helpers (replace isinstance checks)
# ---------------------------------------------------------------------------


def is_definition(node) -> bool:
    """Check if a node is any Sidemantic definition."""
    return isinstance(node, exp.Anonymous) and node.name in _DEF_TYPES


def is_model_def(node) -> bool:
    return isinstance(node, exp.Anonymous) and node.name == MODELDEF


def is_dimension_def(node) -> bool:
    return isinstance(node, exp.Anonymous) and node.name == DIMENSIONDEF


def is_relationship_def(node) -> bool:
    return isinstance(node, exp.Anonymous) and node.name == RELATIONSHIPDEF


def is_metric_def(node) -> bool:
    return isinstance(node, exp.Anonymous) and node.name == METRICDEF


def is_segment_def(node) -> bool:
    return isinstance(node, exp.Anonymous) and node.name == SEGMENTDEF


def is_parameter_def(node) -> bool:
    return isinstance(node, exp.Anonymous) and node.name == PARAMETERDEF


def is_pre_aggregation_def(node) -> bool:
    return isinstance(node, exp.Anonymous) and node.name == PREAGGREGATIONDEF


def is_property_eq(node) -> bool:
    return isinstance(node, exp.EQ) and node.args.get("_property_eq", False)


def def_type_name(node) -> str | None:
    """Get the definition keyword (e.g. 'MODEL', 'METRIC') from a node."""
    if isinstance(node, exp.Anonymous) and node.name in _DEF_TYPES:
        return node.name.replace("Def", "").upper()
    return None


# ---------------------------------------------------------------------------
# Monkey-patching infrastructure (thread-safe)
# ---------------------------------------------------------------------------

_sidemantic_parsing = threading.local()
_original_parse_statement = None
_patch_installed = False


def _get_property_names() -> set[str]:
    """Derive property names from all Sidemantic models."""
    from sidemantic.core.dimension import Dimension
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.parameter import Parameter
    from sidemantic.core.pre_aggregation import PreAggregation
    from sidemantic.core.relationship import Relationship
    from sidemantic.core.segment import Segment

    names = set()
    for cls in (Model, Dimension, Relationship, Metric, Segment, Parameter, PreAggregation):
        names.update(field.upper() for field in cls.model_fields.keys())
    names.update(alias.upper() for alias in PROPERTY_ALIASES.keys())
    return names


def _parse_property(self) -> exp.Expression | None:
    """Parse property assignment: name value or name 'value'.

    Operates on the parser instance (self) passed from the monkey-patched method.
    """
    if not self._match_texts(_get_property_names()):
        return None

    key = self._prev.text.lower()

    depth = 0
    value_parts = []

    while self._curr:
        if self._curr.token_type in (
            tokens.TokenType.L_PAREN,
            tokens.TokenType.L_BRACKET,
            tokens.TokenType.L_BRACE,
        ):
            depth += 1
            value_parts.append(self._curr.text)
            self._advance()
        elif self._curr.token_type in (
            tokens.TokenType.R_PAREN,
            tokens.TokenType.R_BRACKET,
            tokens.TokenType.R_BRACE,
        ):
            if self._curr.token_type == tokens.TokenType.R_PAREN and depth == 0:
                break
            if depth > 0:
                depth -= 1
            value_parts.append(self._curr.text)
            self._advance()
        elif self._curr.token_type == tokens.TokenType.COMMA and depth == 0:
            break
        elif self._curr.token_type == tokens.TokenType.STRING:
            if value_parts and value_parts[-1] not in ("(", ",", "=", " "):
                value_parts.append(" ")
            value_parts.append(f"'{self._curr.text}'")
            self._advance()
        else:
            curr_text = self._curr.text
            needs_space_before = value_parts and value_parts[-1] not in ("(", ",", " ")
            needs_space_after_prev = value_parts and value_parts[-1] in (" ",)

            if needs_space_before and not needs_space_after_prev:
                if curr_text not in (")", ","):
                    value_parts.append(" ")

            value_parts.append(curr_text)

            if curr_text == "=":
                value_parts.append(" ")

            self._advance()

    value = "".join(value_parts).strip()
    if not value:
        return None

    return PropertyEQ(this=exp.Identifier(this=key), expression=exp.Literal.string(value))


def _patched_parse_statement(self):
    """Replacement for parser.Parser._parse_statement when Sidemantic parsing is active."""
    if not getattr(_sidemantic_parsing, "active", False):
        return _original_parse_statement(self)

    if self._match_texts(("MODEL", "DIMENSION", "RELATIONSHIP", "METRIC", "SEGMENT", "PARAMETER", "PRE_AGGREGATION")):
        func_name = self._prev.text.upper()
        self._match(tokens.TokenType.L_PAREN)

        properties = []
        while not self._match(tokens.TokenType.R_PAREN):
            prop = _parse_property(self)
            if prop:
                properties.append(prop)
            if not self._match(tokens.TokenType.COMMA):
                self._match(tokens.TokenType.R_PAREN)
                break

        def_name = _KEYWORD_TO_DEF.get(func_name)
        if def_name:
            return exp.Anonymous(this=def_name, expressions=properties)

    return _original_parse_statement(self)


def _install_parser_patch():
    """Install the Sidemantic parser patch on parser.Parser (once)."""
    global _original_parse_statement, _patch_installed
    if _patch_installed:
        return
    _original_parse_statement = parser.Parser._parse_statement
    parser.Parser._parse_statement = _patched_parse_statement
    _patch_installed = True


_install_parser_patch()


# ---------------------------------------------------------------------------
# SidemanticDialect (Dialect is not compiled, safe to subclass)
# ---------------------------------------------------------------------------


class SidemanticDialect(Dialect):
    """Sidemantic SQL dialect with METRIC and SEGMENT support."""

    pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_one(sql: str) -> exp.Expression:
    """Parse SQL with Sidemantic extensions."""
    _sidemantic_parsing.active = True
    try:
        dialect = SidemanticDialect()
        return dialect.parse_one(sql)
    finally:
        _sidemantic_parsing.active = False


def parse(sql: str) -> list[exp.Expression]:
    """Parse multiple SQL statements with Sidemantic extensions."""
    _sidemantic_parsing.active = True
    try:
        dialect = SidemanticDialect()
        return list(dialect.parse(sql))
    finally:
        _sidemantic_parsing.active = False
