"""SQLGlot dialect extensions for Sidemantic SQL syntax.

SQLGlot expression classes can be compiled in some installations, and custom
Expression subclasses have been brittle across sqlglot releases. Sidemantic
therefore stores its custom syntax as normal SQLGlot nodes:

- definition calls are ``exp.Anonymous`` nodes tagged with Sidemantic names
- property assignments are ``exp.EQ`` nodes tagged as Sidemantic properties

The small factory classes below preserve the old public call-site shape
(``MetricDef([...])`` and ``isinstance(node, MetricDef)``) without introducing
new SQLGlot Expression subclasses.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp, parser, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Token, TokenType

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
TABLEBLOCKMODELDEF = "TableBlockModelDef"
TABLEBLOCKPRIMARYKEYDEF = "TableBlockPrimaryKeyDef"
TABLEBLOCKDEFAULTTIMEDEF = "TableBlockDefaultTimeDef"
TABLEBLOCKSEGMENTDEF = "TableBlockSegmentDef"
TABLEBLOCKJOINDEF = "TableBlockJoinDef"
TABLEBLOCKFIELDDEF = "TableBlockFieldDef"

_DEF_TYPES = {MODELDEF, DIMENSIONDEF, RELATIONSHIPDEF, METRICDEF, SEGMENTDEF, PARAMETERDEF, PREAGGREGATIONDEF}
_TABLE_BLOCK_DEF_TYPES = {
    TABLEBLOCKMODELDEF,
    TABLEBLOCKPRIMARYKEYDEF,
    TABLEBLOCKDEFAULTTIMEDEF,
    TABLEBLOCKSEGMENTDEF,
    TABLEBLOCKJOINDEF,
    TABLEBLOCKFIELDDEF,
}

_KEYWORD_TO_DEF = {
    "MODEL": MODELDEF,
    "DIMENSION": DIMENSIONDEF,
    "RELATIONSHIP": RELATIONSHIPDEF,
    "METRIC": METRICDEF,
    "SEGMENT": SEGMENTDEF,
    "PARAMETER": PARAMETERDEF,
    "PRE_AGGREGATION": PREAGGREGATIONDEF,
}


def _anonymous_def(def_name: str, *, expressions=None, **kwargs):
    node = exp.Anonymous(this=def_name, expressions=expressions or [])
    for key, value in kwargs.items():
        node.set(key, value)
    return node


# ---------------------------------------------------------------------------
# Factory functions (same call-site syntax as the old classes)
# ---------------------------------------------------------------------------


def ModelDef(expressions):  # noqa: N802
    return _anonymous_def(MODELDEF, expressions=expressions)


def DimensionDef(expressions):  # noqa: N802
    return _anonymous_def(DIMENSIONDEF, expressions=expressions)


def RelationshipDef(expressions):  # noqa: N802
    return _anonymous_def(RELATIONSHIPDEF, expressions=expressions)


def MetricDef(expressions):  # noqa: N802
    return _anonymous_def(METRICDEF, expressions=expressions)


def SegmentDef(expressions):  # noqa: N802
    return _anonymous_def(SEGMENTDEF, expressions=expressions)


def ParameterDef(expressions):  # noqa: N802
    return _anonymous_def(PARAMETERDEF, expressions=expressions)


def PreAggregationDef(expressions):  # noqa: N802
    return _anonymous_def(PREAGGREGATIONDEF, expressions=expressions)


def TableBlockModelDef(this, table=None, source_sql=None, expressions=None):  # noqa: N802
    node = exp.Anonymous(this=this, expressions=expressions or [])
    node.set("sidemantic_def_type", TABLEBLOCKMODELDEF)
    node.set("model_name", this)
    node.set("table", table)
    node.set("source_sql", source_sql)
    return node


def TableBlockPrimaryKeyDef(columns):  # noqa: N802
    return _anonymous_def(TABLEBLOCKPRIMARYKEYDEF, columns=columns)


def TableBlockDefaultTimeDef(this, grain=None):  # noqa: N802
    node = exp.Anonymous(this=this, expressions=[])
    node.set("sidemantic_def_type", TABLEBLOCKDEFAULTTIMEDEF)
    node.set("field", this)
    node.set("grain", grain)
    return node


def TableBlockSegmentDef(this, sql):  # noqa: N802
    node = exp.Anonymous(this=this, expressions=[])
    node.set("sidemantic_def_type", TABLEBLOCKSEGMENTDEF)
    node.set("name", this)
    node.set("sql", sql)
    return node


def TableBlockJoinDef(this, relationship_type, local_keys, target_keys):  # noqa: N802
    node = exp.Anonymous(this=this, expressions=[])
    node.set("sidemantic_def_type", TABLEBLOCKJOINDEF)
    node.set("target", this)
    node.set("relationship_type", relationship_type)
    node.set("local_keys", local_keys)
    node.set("target_keys", target_keys)
    return node


def TableBlockFieldDef(this, sql, dimension_type=None, granularity=None):  # noqa: N802
    node = exp.Anonymous(this=this, expressions=[])
    node.set("sidemantic_def_type", TABLEBLOCKFIELDDEF)
    node.set("name", this)
    node.set("sql", sql)
    node.set("dimension_type", dimension_type)
    node.set("granularity", granularity)
    return node


def PropertyEQ(this, expression):  # noqa: N802
    eq = exp.EQ(this=this, expression=expression)
    eq.set("_property_eq", True)
    return eq


_MODELDEF_FACTORY = ModelDef
_DIMENSIONDEF_FACTORY = DimensionDef
_RELATIONSHIPDEF_FACTORY = RelationshipDef
_METRICDEF_FACTORY = MetricDef
_SEGMENTDEF_FACTORY = SegmentDef
_PARAMETERDEF_FACTORY = ParameterDef
_PREAGGREGATIONDEF_FACTORY = PreAggregationDef
_TABLEBLOCKMODELDEF_FACTORY = TableBlockModelDef
_TABLEBLOCKPRIMARYKEYDEF_FACTORY = TableBlockPrimaryKeyDef
_TABLEBLOCKDEFAULTTIMEDEF_FACTORY = TableBlockDefaultTimeDef
_TABLEBLOCKSEGMENTDEF_FACTORY = TableBlockSegmentDef
_TABLEBLOCKJOINDEF_FACTORY = TableBlockJoinDef
_TABLEBLOCKFIELDDEF_FACTORY = TableBlockFieldDef
_PROPERTYEQ_FACTORY = PropertyEQ


class TableBlockParseError(ValueError):
    """Raised when compact table-block model syntax is invalid."""


class _ExpressionFactoryMeta(type):
    """Callable factory class that also supports isinstance checks."""

    def __call__(cls, *args, **kwargs):
        return cls._factory(*args, **kwargs)

    def __instancecheck__(cls, instance):
        def_name = cls._def_name
        if def_name == "_property_eq":
            return isinstance(instance, exp.EQ) and instance.args.get("_property_eq", False)
        return isinstance(instance, exp.Anonymous) and (
            instance.args.get("sidemantic_def_type") == def_name or instance.name == def_name
        )


class ModelDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = MODELDEF
    _factory = staticmethod(_MODELDEF_FACTORY)


class DimensionDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = DIMENSIONDEF
    _factory = staticmethod(_DIMENSIONDEF_FACTORY)


class RelationshipDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = RELATIONSHIPDEF
    _factory = staticmethod(_RELATIONSHIPDEF_FACTORY)


class MetricDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = METRICDEF
    _factory = staticmethod(_METRICDEF_FACTORY)


class SegmentDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = SEGMENTDEF
    _factory = staticmethod(_SEGMENTDEF_FACTORY)


class ParameterDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = PARAMETERDEF
    _factory = staticmethod(_PARAMETERDEF_FACTORY)


class PreAggregationDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = PREAGGREGATIONDEF
    _factory = staticmethod(_PREAGGREGATIONDEF_FACTORY)


class TableBlockModelDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = TABLEBLOCKMODELDEF
    _factory = staticmethod(_TABLEBLOCKMODELDEF_FACTORY)


class TableBlockPrimaryKeyDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = TABLEBLOCKPRIMARYKEYDEF
    _factory = staticmethod(_TABLEBLOCKPRIMARYKEYDEF_FACTORY)


class TableBlockDefaultTimeDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = TABLEBLOCKDEFAULTTIMEDEF
    _factory = staticmethod(_TABLEBLOCKDEFAULTTIMEDEF_FACTORY)


class TableBlockSegmentDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = TABLEBLOCKSEGMENTDEF
    _factory = staticmethod(_TABLEBLOCKSEGMENTDEF_FACTORY)


class TableBlockJoinDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = TABLEBLOCKJOINDEF
    _factory = staticmethod(_TABLEBLOCKJOINDEF_FACTORY)


class TableBlockFieldDef(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = TABLEBLOCKFIELDDEF
    _factory = staticmethod(_TABLEBLOCKFIELDDEF_FACTORY)


class PropertyEQ(metaclass=_ExpressionFactoryMeta):  # noqa: N801
    _def_name = "_property_eq"
    _factory = staticmethod(_PROPERTYEQ_FACTORY)


# ---------------------------------------------------------------------------
# Type-checking helpers (replace isinstance checks)
# ---------------------------------------------------------------------------


def _is_anonymous_named(node, name: str) -> bool:
    return isinstance(node, exp.Anonymous) and (node.args.get("sidemantic_def_type") == name or node.name == name)


def is_definition(node) -> bool:
    """Check if a node is any Sidemantic definition."""
    return isinstance(node, exp.Anonymous) and node.name in _DEF_TYPES


def is_model_def(node) -> bool:
    return _is_anonymous_named(node, MODELDEF)


def is_dimension_def(node) -> bool:
    return _is_anonymous_named(node, DIMENSIONDEF)


def is_relationship_def(node) -> bool:
    return _is_anonymous_named(node, RELATIONSHIPDEF)


def is_metric_def(node) -> bool:
    return _is_anonymous_named(node, METRICDEF)


def is_segment_def(node) -> bool:
    return _is_anonymous_named(node, SEGMENTDEF)


def is_parameter_def(node) -> bool:
    return _is_anonymous_named(node, PARAMETERDEF)


def is_pre_aggregation_def(node) -> bool:
    return _is_anonymous_named(node, PREAGGREGATIONDEF)


def is_table_block_model_def(node) -> bool:
    return _is_anonymous_named(node, TABLEBLOCKMODELDEF)


def is_table_block_primary_key_def(node) -> bool:
    return _is_anonymous_named(node, TABLEBLOCKPRIMARYKEYDEF)


def is_table_block_default_time_def(node) -> bool:
    return _is_anonymous_named(node, TABLEBLOCKDEFAULTTIMEDEF)


def is_table_block_segment_def(node) -> bool:
    return _is_anonymous_named(node, TABLEBLOCKSEGMENTDEF)


def is_table_block_join_def(node) -> bool:
    return _is_anonymous_named(node, TABLEBLOCKJOINDEF)


def is_table_block_field_def(node) -> bool:
    return _is_anonymous_named(node, TABLEBLOCKFIELDDEF)


def is_property_eq(node) -> bool:
    return isinstance(node, exp.EQ) and node.args.get("_property_eq", False)


def def_type_name(node) -> str | None:
    """Get the definition keyword (e.g. 'MODEL', 'METRIC') from a node."""
    if isinstance(node, exp.Anonymous) and node.name in _DEF_TYPES:
        return node.name.replace("Def", "").upper()
    return None


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


"""Extended parser with MODEL, DIMENSION, RELATIONSHIP, METRIC, and SEGMENT support."""
_IDENTIFIER_TOKEN_TYPES = {
    TokenType.IDENTIFIER,
    TokenType.SCHEMA,
    TokenType.TABLE,
    TokenType.VAR,
}
_TIME_GRAINS = {"second", "minute", "hour", "day", "week", "month", "quarter", "year"}
_DIMENSION_TYPES = {"categorical", "time", "boolean", "numeric"}
_DIMENSION_TYPE_ALIASES = {
    "bool": "boolean",
    "date": "time",
    "number": "numeric",
    "string": "categorical",
}
_CARDINALITY_ALIASES = {
    "one": "many_to_one",
    "many": "one_to_many",
    "many_to_one": "many_to_one",
    "one_to_many": "one_to_many",
    "one_to_one": "one_to_one",
}
_STATEMENT_CONTINUATION_TOKEN_TYPES = {
    TokenType.ALIAS,
    TokenType.AND,
    TokenType.ARROW,
    TokenType.BETWEEN,
    TokenType.COLON,
    TokenType.COMMA,
    TokenType.DARROW,
    TokenType.DCOLON,
    TokenType.DASH,
    TokenType.DOT,
    TokenType.ELSE,
    TokenType.EQ,
    TokenType.ESCAPE,
    TokenType.GT,
    TokenType.GTE,
    TokenType.ILIKE,
    TokenType.IN,
    TokenType.IS,
    TokenType.L_BRACE,
    TokenType.L_BRACKET,
    TokenType.L_PAREN,
    TokenType.LIKE,
    TokenType.LT,
    TokenType.LTE,
    TokenType.MOD,
    TokenType.NEQ,
    TokenType.NOT,
    TokenType.ON,
    TokenType.OR,
    TokenType.PERCENT,
    TokenType.PLUS,
    TokenType.RLIKE,
    TokenType.SLASH,
    TokenType.STAR,
    TokenType.THEN,
    TokenType.WHEN,
}


def _parse_table_block_model(self) -> TableBlockModelDef:
    model_name = _parse_table_block_identifier(self)
    if not model_name:
        raise TableBlockParseError("Compact model block requires a model name")

    if not self._match(TokenType.FROM):
        raise TableBlockParseError(f"Table-block model '{model_name}' must use `model {model_name} from <table> (...)`")

    table = None
    source_sql = None
    if self._curr and self._curr.token_type == TokenType.L_PAREN:
        source_sql = _parse_table_block_derived_source(self, model_name)
    else:
        table = _parse_table_block_source_table(self, model_name)

    if not self._curr or self._curr.token_type != TokenType.L_PAREN:
        source = "derived SQL source" if source_sql else "table source"
        raise TableBlockParseError(f"Model '{model_name}' must include a body block after the {source}")

    body_tokens = _consume_balanced_tokens(self, model_name, "model block")
    body_expressions = [
        _parse_table_block_body_statement(self, model_name, statement_tokens)
        for statement_tokens in _split_table_block_body_statements(self, body_tokens)
    ]

    return TableBlockModelDef(
        this=exp.to_identifier(model_name),
        table=table,
        source_sql=source_sql,
        expressions=body_expressions,
    )


def _parse_table_block_derived_source(self, model_name: str) -> str:
    open_token = self._curr
    source_tokens = _consume_balanced_tokens(self, model_name, "derived SQL source")
    if not source_tokens:
        raise TableBlockParseError(f"Derived SQL source for model '{model_name}' cannot be empty")

    close_token = self._tokens[self._index - 1]
    source_sql = self.sql[open_token.end + 1 : close_token.start].strip()
    if not source_sql:
        raise TableBlockParseError(f"Derived SQL source for model '{model_name}' cannot be empty")
    return source_sql


def _parse_table_block_source_table(self, model_name: str) -> str:
    parts = []
    first = _parse_table_block_identifier(self)
    if not first:
        raise TableBlockParseError(f"Model '{model_name}' must declare a table or derived SQL source after `from`")
    parts.append(first)

    while self._match(TokenType.DOT):
        part = _parse_table_block_identifier(self)
        if not part:
            raise TableBlockParseError(f"Model '{model_name}' has an invalid table source")
        parts.append(part)

    return ".".join(parts)


def _consume_balanced_tokens(self, model_name: str, label: str) -> list[Token]:
    if not self._match(TokenType.L_PAREN):
        raise TableBlockParseError(f"Model '{model_name}' must include a {label}")

    body_tokens = []
    depth = 1
    while self._curr:
        if self._curr.token_type == TokenType.L_PAREN:
            depth += 1
        elif self._curr.token_type == TokenType.R_PAREN:
            depth -= 1
            if depth == 0:
                self._advance()
                return body_tokens

        body_tokens.append(self._curr)
        self._advance()

    raise TableBlockParseError(f"Unclosed {label} for model '{model_name}'")


def _split_table_block_body_statements(self, body_tokens: list[Token]) -> list[list[Token]]:
    statements = []
    statement = []
    depth = 0
    previous_token = None

    for token in body_tokens:
        if (
            previous_token
            and statement
            and depth == 0
            and _has_statement_separator_between(self, statement, previous_token, token)
        ):
            statements.append(statement)
            statement = []

        if token.token_type == TokenType.SEMICOLON and depth == 0:
            if statement:
                statements.append(statement)
                statement = []
            previous_token = token
            continue

        statement.append(token)

        if token.token_type in (TokenType.L_PAREN, TokenType.L_BRACKET, TokenType.L_BRACE):
            depth += 1
        elif token.token_type in (TokenType.R_PAREN, TokenType.R_BRACKET, TokenType.R_BRACE):
            depth = max(depth - 1, 0)

        previous_token = token

    if statement:
        statements.append(statement)

    return statements


def _has_statement_separator_between(self, statement_tokens: list[Token], left: Token, right: Token) -> bool:
    gap = self.sql[left.end + 1 : right.start]
    if ";" in gap:
        return True
    if "\n" not in gap:
        return False
    return _table_block_statement_can_end(self, statement_tokens) and not _token_continues_statement(self, right)


def _table_block_statement_can_end(self, statement_tokens: list[Token]) -> bool:
    if not statement_tokens or _token_continues_statement(self, statement_tokens[-1]):
        return False

    first_text = statement_tokens[0].text.lower()
    first_type = statement_tokens[0].token_type

    if first_type == TokenType.PRIMARY_KEY or first_text == "primary_key":
        return len(statement_tokens) > 1

    if first_type == TokenType.DEFAULT:
        return len(statement_tokens) == 3 or len(statement_tokens) == 5

    if first_text == "segment":
        alias_idx = _find_top_level_token(self, statement_tokens, TokenType.ALIAS, start=2)
        return alias_idx is not None and alias_idx < len(statement_tokens) - 1

    if first_type == TokenType.JOIN:
        on_idx = _find_top_level_token(self, statement_tokens, TokenType.ON, start=1)
        return on_idx is not None and on_idx < len(statement_tokens) - 1

    colon_idx = _find_top_level_token(self, statement_tokens, TokenType.COLON)
    base_tokens = statement_tokens[:colon_idx] if colon_idx is not None else statement_tokens
    if not base_tokens:
        return False

    alias_idx = _find_top_level_token(self, base_tokens, TokenType.ALIAS)
    if alias_idx is not None:
        name_tokens = base_tokens[alias_idx + 1 :]
        return alias_idx > 0 and len(name_tokens) == 1 and _identifier_from_token(self, name_tokens[0]) is not None

    return len(base_tokens) == 1 and _identifier_from_token(self, base_tokens[0]) is not None


def _token_continues_statement(self, token: Token) -> bool:
    return token.token_type in _STATEMENT_CONTINUATION_TOKEN_TYPES


def _parse_table_block_body_statement(self, model_name: str, statement_tokens: list[Token]) -> exp.Expression:
    first_text = statement_tokens[0].text.lower()
    first_type = statement_tokens[0].token_type

    if first_type == TokenType.TABLE:
        raise TableBlockParseError(
            f"Model '{model_name}' uses table source inside the block; use `model {model_name} from <table> (...)`"
        )
    if first_type == TokenType.PRIMARY_KEY or first_text == "primary_key":
        return _parse_table_block_primary_key(self, model_name, statement_tokens)
    if first_type == TokenType.DEFAULT:
        return _parse_table_block_default_time(self, model_name, statement_tokens)
    if first_text == "segment":
        return _parse_table_block_segment(self, model_name, statement_tokens)
    if first_type == TokenType.JOIN:
        return _parse_table_block_join(self, model_name, statement_tokens)

    field = _parse_table_block_field(self, model_name, statement_tokens)
    if field:
        return field

    raise TableBlockParseError(
        f"Unrecognized statement in model '{model_name}': {_statement_sql(self, statement_tokens)}"
    )


def _parse_table_block_primary_key(
    self,
    model_name: str,
    statement_tokens: list[Token],
) -> TableBlockPrimaryKeyDef:
    value_tokens = statement_tokens[1:]
    if not value_tokens:
        raise TableBlockParseError("Primary key requires at least one column")

    if value_tokens[0].token_type == TokenType.L_PAREN and value_tokens[-1].token_type == TokenType.R_PAREN:
        value_tokens = value_tokens[1:-1]
    if not value_tokens:
        raise TableBlockParseError("Primary key requires at least one column")

    columns = _parse_identifier_list(self, value_tokens)
    if not columns:
        raise TableBlockParseError("Primary key requires at least one column")

    return TableBlockPrimaryKeyDef(columns=columns)


def _parse_table_block_default_time(
    self,
    model_name: str,
    statement_tokens: list[Token],
) -> TableBlockDefaultTimeDef:
    if len(statement_tokens) < 3 or statement_tokens[1].text.lower() != "time":
        raise TableBlockParseError(
            f"Invalid default time in model '{model_name}': {_statement_sql(self, statement_tokens)}"
        )

    dimension_name = _identifier_from_token(self, statement_tokens[2])
    if not dimension_name:
        raise TableBlockParseError(
            f"Invalid default time in model '{model_name}': {_statement_sql(self, statement_tokens)}"
        )

    grain = None
    if len(statement_tokens) > 3:
        if (
            len(statement_tokens) != 5
            or statement_tokens[3].text.lower() not in ("grain", "granularity")
            or not _identifier_from_token(self, statement_tokens[4])
        ):
            raise TableBlockParseError(
                f"Invalid default time in model '{model_name}': {_statement_sql(self, statement_tokens)}"
            )
        grain = statement_tokens[4].text.lower()
        _validate_time_grain(self, model_name, "default time", grain)

    return TableBlockDefaultTimeDef(this=exp.to_identifier(dimension_name), grain=grain)


def _parse_table_block_segment(
    self,
    model_name: str,
    statement_tokens: list[Token],
) -> TableBlockSegmentDef:
    if len(statement_tokens) < 4:
        raise TableBlockParseError(f"Invalid segment in model '{model_name}': {_statement_sql(self, statement_tokens)}")

    segment_name = _identifier_from_token(self, statement_tokens[1])
    alias_idx = _find_top_level_token(self, statement_tokens, TokenType.ALIAS, start=2)
    if not segment_name or alias_idx is None or alias_idx == len(statement_tokens) - 1:
        raise TableBlockParseError(f"Invalid segment in model '{model_name}': {_statement_sql(self, statement_tokens)}")

    expression_sql = _statement_sql(self, statement_tokens[alias_idx + 1 :])
    if not expression_sql:
        raise TableBlockParseError(f"Segment '{segment_name}' in model '{model_name}' requires a SQL expression")

    return TableBlockSegmentDef(this=exp.to_identifier(segment_name), sql=expression_sql)


def _parse_table_block_join(
    self,
    model_name: str,
    statement_tokens: list[Token],
) -> TableBlockJoinDef:
    on_idx = _find_top_level_token(self, statement_tokens, TokenType.ON, start=1)
    if on_idx is None or on_idx <= 1 or on_idx == len(statement_tokens) - 1:
        raise TableBlockParseError(f"Invalid join in model '{model_name}': {_statement_sql(self, statement_tokens)}")

    header_tokens = statement_tokens[1:on_idx]
    cardinality = None
    target_idx = 0
    if header_tokens[0].text.lower() in _CARDINALITY_ALIASES:
        cardinality = header_tokens[0].text.lower()
        target_idx = 1

    if target_idx >= len(header_tokens):
        raise TableBlockParseError(f"Invalid join in model '{model_name}': {_statement_sql(self, statement_tokens)}")

    target_model = _identifier_from_token(self, header_tokens[target_idx])
    if not target_model:
        raise TableBlockParseError(f"Invalid join in model '{model_name}': {_statement_sql(self, statement_tokens)}")

    alias = None
    alias_tokens = header_tokens[target_idx + 1 :]
    if alias_tokens:
        if len(alias_tokens) == 2 and alias_tokens[0].token_type == TokenType.ALIAS:
            alias = _identifier_from_token(self, alias_tokens[1])
        elif len(alias_tokens) == 1:
            alias = _identifier_from_token(self, alias_tokens[0])
        if not alias:
            raise TableBlockParseError(
                f"Invalid join in model '{model_name}': {_statement_sql(self, statement_tokens)}"
            )

    relationship_type = _CARDINALITY_ALIASES.get(cardinality or "one", "many_to_one")
    on_expression = _statement_sql(self, statement_tokens[on_idx + 1 :])
    join_keys = _extract_table_block_join_keys(
        self,
        on_expression=on_expression,
        current_model_name=model_name,
        target_model_name=target_model,
        target_alias=alias,
    )
    if not join_keys:
        raise TableBlockParseError(
            f"Join in model '{model_name}' must compare model columns: {_statement_sql(self, statement_tokens)}"
        )

    local_keys, target_keys = join_keys
    return TableBlockJoinDef(
        this=exp.to_identifier(target_model),
        relationship_type=relationship_type,
        local_keys=local_keys,
        target_keys=target_keys,
    )


def _parse_table_block_field(
    self,
    model_name: str,
    statement_tokens: list[Token],
) -> TableBlockFieldDef | None:
    colon_idx = _find_top_level_token(self, statement_tokens, TokenType.COLON)
    base_tokens = statement_tokens[:colon_idx] if colon_idx is not None else statement_tokens
    annotation_tokens = statement_tokens[colon_idx + 1 :] if colon_idx is not None else []
    if not base_tokens:
        return None

    dimension_type, granularity = _parse_table_block_field_annotation(self, model_name, base_tokens, annotation_tokens)
    alias_idx = _find_top_level_token(self, base_tokens, TokenType.ALIAS)
    if alias_idx is not None:
        if alias_idx == 0 or alias_idx == len(base_tokens) - 1:
            return None
        name_tokens = base_tokens[alias_idx + 1 :]
        if len(name_tokens) != 1:
            return None
        name = _identifier_from_token(self, name_tokens[0])
        if not name:
            return None
        return TableBlockFieldDef(
            this=exp.to_identifier(name),
            sql=_statement_sql(self, base_tokens[:alias_idx]),
            dimension_type=dimension_type,
            granularity=granularity,
        )

    if len(base_tokens) == 1:
        name = _identifier_from_token(self, base_tokens[0])
        if name:
            return TableBlockFieldDef(
                this=exp.to_identifier(name),
                sql=name,
                dimension_type=dimension_type,
                granularity=granularity,
            )

    return None


def _parse_table_block_field_annotation(
    self,
    model_name: str,
    base_tokens: list[Token],
    annotation_tokens: list[Token],
) -> tuple[str | None, str | None]:
    if not annotation_tokens:
        return None, None

    field_name = _statement_sql(self, base_tokens)
    dimension_type = None
    granularity = None
    idx = 0

    while idx < len(annotation_tokens):
        token = annotation_tokens[idx].text.lower()
        normalized_type = _DIMENSION_TYPE_ALIASES.get(token, token)

        if normalized_type in _DIMENSION_TYPES:
            if dimension_type and dimension_type != normalized_type:
                raise TableBlockParseError(
                    f"Field '{field_name}' in model '{model_name}' has conflicting type annotations"
                )
            dimension_type = normalized_type
            idx += 1
            continue

        if token in ("grain", "granularity"):
            if idx + 1 >= len(annotation_tokens):
                raise TableBlockParseError(f"Field '{field_name}' in model '{model_name}' is missing a {token} value")
            grain = annotation_tokens[idx + 1].text.lower()
            _validate_time_grain(self, model_name, field_name, grain)
            granularity = grain
            if not dimension_type:
                dimension_type = "time"
            elif dimension_type != "time":
                raise TableBlockParseError(
                    f"Field '{field_name}' in model '{model_name}' cannot use grain with type '{dimension_type}'"
                )
            idx += 2
            continue

        raise TableBlockParseError(
            f"Field '{field_name}' in model '{model_name}' has unknown annotation '{annotation_tokens[idx].text}'"
        )

    return dimension_type, granularity


def _extract_table_block_join_keys(
    self,
    on_expression: str,
    current_model_name: str,
    target_model_name: str,
    target_alias: str | None,
) -> tuple[list[str], list[str]] | None:
    try:
        parsed = sqlglot.parse_one(on_expression, read="duckdb")
    except Exception:
        return None

    equalities = _table_block_join_equalities(self, parsed)
    if equalities is None:
        return None

    local_keys = []
    target_keys = []
    for equality in equalities:
        left = equality.left
        right = equality.right
        if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
            return None

        left_side = _classify_join_column(self, left, current_model_name, target_model_name, target_alias)
        right_side = _classify_join_column(self, right, current_model_name, target_model_name, target_alias)
        if left_side == "local" and right_side == "target":
            local_keys.append(left.name)
            target_keys.append(right.name)
            continue
        if left_side == "target" and right_side == "local":
            local_keys.append(right.name)
            target_keys.append(left.name)
            continue
        return None

    if not local_keys:
        return None
    return local_keys, target_keys


def _table_block_join_equalities(self, expression: exp.Expression) -> list[exp.EQ] | None:
    expression = _unwrap_table_block_join_expression(self, expression)
    if isinstance(expression, exp.EQ):
        return [expression]
    if isinstance(expression, exp.And):
        left = _table_block_join_equalities(self, expression.left)
        right = _table_block_join_equalities(self, expression.right)
        if left is None or right is None:
            return None
        return left + right
    return None


def _unwrap_table_block_join_expression(self, expression: exp.Expression) -> exp.Expression:
    while isinstance(expression, exp.Paren) and isinstance(expression.this, exp.Expression):
        expression = expression.this
    return expression


def _classify_join_column(
    self,
    column: exp.Column,
    current_model_name: str,
    target_model_name: str,
    target_alias: str | None,
) -> str | None:
    table = column.table
    if not table or table == current_model_name:
        return "local"
    if table == target_model_name or table == target_alias:
        return "target"
    return None


def _find_top_level_token(
    self,
    statement_tokens: list[Token],
    token_type: TokenType,
    start: int = 0,
) -> int | None:
    depth = 0
    for idx, token in enumerate(statement_tokens[start:], start=start):
        if token.token_type in (TokenType.L_PAREN, TokenType.L_BRACKET, TokenType.L_BRACE):
            depth += 1
            continue
        if token.token_type in (TokenType.R_PAREN, TokenType.R_BRACKET, TokenType.R_BRACE):
            depth = max(depth - 1, 0)
            continue
        if depth == 0 and token.token_type == token_type:
            return idx
    return None


def _parse_identifier_list(self, value_tokens: list[Token]) -> list[str]:
    columns = []
    current = []
    depth = 0

    for token in value_tokens:
        if token.token_type == TokenType.COMMA and depth == 0:
            column = _identifier_from_tokens(self, current)
            if not column:
                return []
            columns.append(column)
            current = []
            continue

        if token.token_type in (TokenType.L_PAREN, TokenType.L_BRACKET, TokenType.L_BRACE):
            depth += 1
        elif token.token_type in (TokenType.R_PAREN, TokenType.R_BRACKET, TokenType.R_BRACE):
            depth = max(depth - 1, 0)
        current.append(token)

    column = _identifier_from_tokens(self, current)
    if not column:
        return []
    columns.append(column)
    return columns


def _identifier_from_tokens(self, value_tokens: list[Token]) -> str | None:
    if len(value_tokens) != 1:
        return None
    return _identifier_from_token(self, value_tokens[0])


def _parse_table_block_identifier(self) -> str | None:
    if not self._curr:
        return None
    identifier = _identifier_from_token(self, self._curr)
    if identifier:
        self._advance()
    return identifier


def _identifier_from_token(self, token: Token) -> str | None:
    if token.token_type not in _IDENTIFIER_TOKEN_TYPES:
        return None
    return token.text


def _statement_sql(self, statement_tokens: list[Token]) -> str:
    if not statement_tokens:
        return ""
    return self.sql[statement_tokens[0].start : statement_tokens[-1].end + 1].strip()


def _validate_time_grain(self, model_name: str, field_name: str, grain: str) -> None:
    if grain not in _TIME_GRAINS:
        raise TableBlockParseError(f"Field '{field_name}' in model '{model_name}' uses invalid grain '{grain}'")


def _has_table_block_model(raw_tokens: list[Token]) -> bool:
    for idx, token in enumerate(raw_tokens):
        if token.text.upper() != "MODEL":
            continue
        if idx + 1 >= len(raw_tokens):
            continue
        if raw_tokens[idx + 1].token_type != TokenType.L_PAREN:
            return True
    return False


def _parse_property(self) -> exp.Expression | None:
    """Parse property assignment: name value or name 'value'."""
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


def _parse_table_block_statements(self, raw_tokens: list[Token], sql: str):
    self.reset()
    self.sql = sql or ""
    self._index = -1
    self._tokens = raw_tokens
    self._tokens_size = len(raw_tokens)
    self._advance()

    expressions = []
    while self._curr:
        if self._match(TokenType.SEMICOLON):
            continue
        statement = _parse_sidemantic_statement(self)
        if statement:
            expressions.append(statement)
            continue
        self.raise_error("Expected Sidemantic SQL statement")

    self.check_errors()
    return expressions


def _parse_sidemantic_statement(self):
    """Parse Sidemantic definitions before falling back to SQLGlot SQL."""
    if self._match_texts(("MODEL", "DIMENSION", "RELATIONSHIP", "METRIC", "SEGMENT", "PARAMETER", "PRE_AGGREGATION")):
        func_name = self._prev.text.upper()
        if func_name == "MODEL" and (not self._curr or self._curr.token_type != tokens.TokenType.L_PAREN):
            return _parse_table_block_model(self)

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

    return parser.Parser._parse_statement(self)


# ---------------------------------------------------------------------------
# Sidemantic dialect
# ---------------------------------------------------------------------------


class SidemanticDialect(Dialect):
    """Sidemantic SQL dialect with METRIC, SEGMENT, and compact model-block support."""

    def parse(self, sql: str, **opts) -> list[exp.Expression | None]:
        sidemantic_parser = self.parser(**opts)
        raw_tokens = self.tokenize(sql)

        # Keep custom parsing scoped to this dialect instance. With sqlglot[c],
        # Parser is compiled and cannot be subclassed or patched per instance.
        if _has_table_block_model(raw_tokens):
            return _parse_table_block_statements(sidemantic_parser, raw_tokens, sql)

        return sidemantic_parser._parse(
            parse_method=_parse_sidemantic_statement,
            raw_tokens=raw_tokens,
            sql=sql,
        )


# Singleton instance for convenience functions.
_dialect = SidemanticDialect()


def parse_one(sql: str) -> exp.Expression:
    """Parse SQL with Sidemantic extensions."""
    statements = parse(sql)
    if not statements:
        raise sqlglot.errors.ParseError("No expression was parsed")
    return statements[0]


def parse(sql: str) -> list[exp.Expression]:
    """Parse multiple SQL statements with Sidemantic extensions."""
    return list(_dialect.parse(sql))
