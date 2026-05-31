"""Graphene GSQL adapter for importing semantic model files.

This is a clean-room compatibility importer for Graphene model files. It
implements Sidemantic's own parser for the documented `.gsql` model syntax and
does not vendor, port, or generate from Graphene's parser or grammar.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import sqlglot
from sqlglot import exp, tokens
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Token, TokenType

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

_AGGREGATE_FUNCTIONS = {
    "avg",
    "count",
    "max",
    "median",
    "min",
    "mode",
    "stddev",
    "stddev_pop",
    "sum",
    "variance",
    "variance_pop",
    "var_pop",
    "var_samp",
}

_VALID_GRANULARITIES = {"second", "minute", "hour", "day", "week", "month", "quarter", "year"}
_ANNOTATION_FLAGS = {"pii", "pct", "ratio"}
_ANNOTATION_VALUE_KEYS = {"currency", "description", "timeGrain", "timeOrdinal", "unit"}
_METADATA_TOKEN_RE = re.compile(
    r"(?P<prefix>^|\s)(?P<hash>#)?(?P<key>[A-Za-z][\w-]*)"
    r"(?:\s*=\s*(?P<value>\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'|[^\s#]+))?"
)


class _GrapheneDialect(Dialect):
    """SQLGlot tokenizer configuration for GSQL model files."""

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", ("/*", "*/"), "#"]


_GRAPHENE_DIALECT = _GrapheneDialect()


@dataclass
class _Comments:
    descriptions: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def extend(self, other: _Comments) -> None:
        self.descriptions.extend(other.descriptions)
        self.metadata.update(other.metadata)

    @property
    def description(self) -> str | None:
        description = " ".join(part.strip() for part in self.descriptions if part.strip()).strip()
        return description or None


@dataclass(frozen=True)
class _GsqlColumn:
    name: str
    data_type: str
    primary_key: bool
    comments: _Comments


@dataclass(frozen=True)
class _GsqlJoin:
    cardinality: Literal["one", "many"]
    target_ref: str
    alias: str | None
    on_sql: str
    comments: _Comments


@dataclass(frozen=True)
class _GsqlComputed:
    name: str
    expression: str
    comments: _Comments


_GsqlBlockItem = _GsqlColumn | _GsqlJoin | _GsqlComputed


@dataclass(frozen=True)
class _GsqlStatement:
    kind: Literal["table", "extend"]
    ref: str
    comments: _Comments
    items: list[_GsqlBlockItem] = field(default_factory=list)
    source_query: str | None = None


@dataclass(frozen=True)
class _ParsedJoin:
    relationship: Relationship
    target_model: str
    alias_model: str | None
    local_key: str | list[str] | None
    target_key: str | list[str] | None


class GrapheneParseError(ValueError):
    """Raised when a `.gsql` model file cannot be parsed."""


class GrapheneAdapter(BaseAdapter):
    """Adapter for Graphene `.gsql` semantic model files.

    The importer intentionally targets semantic model declarations:
    - `table name (...)` physical table definitions
    - `table name as (...)` derived table definitions, preserving query text
    - `extend name (...)` blocks
    - `join one` / `join many` relationships
    - base columns, computed dimensions, and aggregate measures
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")

        graph = SemanticGraph()
        extends: list[tuple[str, list[_GsqlBlockItem]]] = []
        primary_key_candidates: dict[str, list[str]] = {}
        explicit_primary_keys: set[str] = set()

        files = sorted(source_path.rglob("*.gsql")) if source_path.is_dir() else [source_path]
        for file_path in files:
            self._parse_gsql_file(file_path, graph, extends, primary_key_candidates, explicit_primary_keys)

        self._apply_extends(graph, extends, primary_key_candidates)
        self._resolve_primary_keys(graph, primary_key_candidates, explicit_primary_keys)
        self._add_alias_models(graph)
        return graph

    def _parse_gsql_file(
        self,
        path: Path,
        graph: SemanticGraph,
        extends: list[tuple[str, list[_GsqlBlockItem]]],
        primary_key_candidates: dict[str, list[str]],
        explicit_primary_keys: set[str],
    ) -> None:
        content = path.read_text()
        document = _GsqlParser(content, path).parse()
        for statement in document:
            model_name = _model_name_from_ref(statement.ref)

            if statement.kind == "extend":
                extends.append((model_name, statement.items))
                continue

            if statement.source_query is not None:
                model = Model(
                    name=model_name,
                    sql=statement.source_query.strip(),
                    description=statement.comments.description,
                    primary_key="id",
                    dimensions=_dimensions_from_view_query(statement.source_query),
                    metadata={"graphene": {"table_ref": statement.ref, "type": "view"}},
                )
            else:
                model = self._model_from_table_statement(statement, primary_key_candidates, explicit_primary_keys)

            if model.name in graph.models:
                continue
            graph.add_model(model)

    def _model_from_table_statement(
        self,
        statement: _GsqlStatement,
        primary_key_candidates: dict[str, list[str]],
        explicit_primary_keys: set[str],
    ) -> Model:
        model_name = _model_name_from_ref(statement.ref)
        dimensions: list[Dimension] = []
        metrics: list[Metric] = []
        relationships: list[Relationship] = []
        explicit_primary_key: str | None = None
        metric_names = _computed_metric_names(statement.items)

        for item in statement.items:
            if isinstance(item, _GsqlJoin):
                join = self._relationship_from_join(item, model_name, primary_key_candidates)
                relationships.append(join.relationship)
            elif isinstance(item, _GsqlComputed):
                computed = self._field_from_computed(item, metric_names)
                if isinstance(computed, Metric):
                    metrics.append(computed)
                else:
                    dimensions.append(computed)
            else:
                dimension = _dimension_from_column(item)
                dimensions.append(dimension)
                if item.primary_key:
                    explicit_primary_key = item.name

        if explicit_primary_key is not None:
            explicit_primary_keys.add(model_name)
        primary_key = explicit_primary_key or _choose_primary_key(dimensions, primary_key_candidates.get(model_name))
        return Model(
            name=model_name,
            table=statement.ref,
            primary_key=primary_key,
            description=statement.comments.description,
            dimensions=dimensions,
            metrics=metrics,
            relationships=relationships,
            metadata={"graphene": {"table_ref": statement.ref, "type": "table"}},
        )

    def _apply_extends(
        self,
        graph: SemanticGraph,
        extends: list[tuple[str, list[_GsqlBlockItem]]],
        primary_key_candidates: dict[str, list[str]],
    ) -> None:
        for model_name, items in extends:
            model = graph.models.get(model_name)
            if not model:
                continue

            metric_names = _computed_metric_names(items, {metric.name for metric in model.metrics})
            for item in items:
                if isinstance(item, _GsqlJoin):
                    join = self._relationship_from_join(item, model_name, primary_key_candidates)
                    model.relationships.append(join.relationship)
                elif isinstance(item, _GsqlComputed):
                    computed = self._field_from_computed(item, metric_names)
                    if isinstance(computed, Metric):
                        model.metrics.append(computed)
                    else:
                        model.dimensions.append(computed)

    def _relationship_from_join(
        self,
        item: _GsqlJoin,
        model_name: str,
        primary_key_candidates: dict[str, list[str]],
    ) -> _ParsedJoin:
        target_model = _model_name_from_ref(item.target_ref)
        alias_model = item.alias
        relationship_name = alias_model or target_model
        target_scope = alias_model or target_model
        local_key, target_key = _extract_join_keys(item.on_sql, model_name, target_scope, target_model)

        metadata = {
            "cardinality": item.cardinality,
            "on": item.on_sql,
            "target_table": target_model,
            "target_ref": item.target_ref,
        }
        if alias_model:
            metadata["alias"] = alias_model

        if item.cardinality == "one":
            _append_primary_key_candidates(primary_key_candidates, target_model, target_key)
            if alias_model:
                _append_primary_key_candidates(primary_key_candidates, alias_model, target_key)
            relationship = Relationship(
                name=relationship_name,
                type="many_to_one",
                foreign_key=local_key,
                primary_key=target_key,
                metadata={"graphene": metadata},
            )
        else:
            _append_primary_key_candidates(primary_key_candidates, model_name, local_key)
            relationship = Relationship(
                name=relationship_name,
                type="one_to_many",
                foreign_key=target_key,
                primary_key=local_key,
                metadata={"graphene": metadata},
            )

        return _ParsedJoin(
            relationship=relationship,
            target_model=target_model,
            alias_model=alias_model,
            local_key=local_key,
            target_key=target_key,
        )

    def _field_from_computed(self, item: _GsqlComputed, local_metric_names: set[str]) -> Dimension | Metric:
        expression = _normalize_sql_fragment(item.expression)
        expression, has_graphene_percentile = _rewrite_graphene_percentile_shorthand(expression)
        formatting = _formatting_from_metadata(item.comments.metadata)

        if _is_metric_expression(expression, local_metric_names):
            metric_kwargs: dict[str, Any] = {
                "name": item.name,
                "sql": expression,
                "description": item.comments.description,
                "metadata": _graphene_metadata(item.comments.metadata),
                "format": formatting.get("format"),
                "value_format_name": formatting.get("value_format_name"),
            }
            if has_graphene_percentile or not _has_inline_aggregate(expression):
                metric_kwargs["type"] = "derived"
            return Metric(**metric_kwargs)

        return Dimension(
            name=item.name,
            type=_dimension_type_from_expression(expression, item.name, item.comments.metadata),
            sql=expression,
            granularity=_granularity_from_metadata(item.comments.metadata, expression),
            description=item.comments.description,
            metadata=_graphene_metadata(item.comments.metadata),
            format=formatting.get("format"),
            value_format_name=formatting.get("value_format_name"),
        )

    def _resolve_primary_keys(
        self,
        graph: SemanticGraph,
        candidates: dict[str, list[str]],
        explicit_primary_keys: set[str],
    ) -> None:
        for model_name, model in graph.models.items():
            if model_name in explicit_primary_keys:
                continue
            if model.primary_key != "id":
                continue
            model.primary_key = _choose_primary_key(model.dimensions, candidates.get(model_name))

    def _add_alias_models(self, graph: SemanticGraph) -> None:
        aliases: dict[str, str] = {}
        for model in graph.models.values():
            for relationship in model.relationships:
                metadata = (relationship.metadata or {}).get("graphene") or {}
                alias = metadata.get("alias")
                target = metadata.get("target_table")
                if alias and target and alias not in graph.models and target in graph.models:
                    aliases[alias] = target

        for alias, target in aliases.items():
            target_model = graph.models[target]
            alias_model = copy.deepcopy(target_model)
            alias_model.name = alias
            alias_model.metadata = dict(alias_model.metadata or {})
            alias_model.metadata["graphene"] = {
                **((target_model.metadata or {}).get("graphene") or {}),
                "alias_for": target,
            }
            graph.add_model(alias_model)


class _GsqlParser:
    def __init__(self, source: str, path: Path) -> None:
        self.source = source
        self.path = path
        self.tokens = _tokenize_gsql(source)
        self.index = 0

    def parse(self) -> list[_GsqlStatement]:
        statements: list[_GsqlStatement] = []
        while self._current is not None:
            if self._matches_text("table"):
                statements.append(self._parse_table())
            elif self._matches_text("extend"):
                statements.append(self._parse_extend())
            else:
                self.index += 1
        return statements

    @property
    def _current(self) -> Token | None:
        return self.tokens[self.index] if self.index < len(self.tokens) else None

    def _parse_table(self) -> _GsqlStatement:
        table_token = self._advance()
        ref = self._parse_ref("table")
        source_query: str | None = None

        if self._matches_text("as"):
            self._advance()
            body_tokens, open_token, close_token = self._consume_balanced("derived table query")
            source_query = self.source[open_token.end + 1 : close_token.start].strip()
            items: list[_GsqlBlockItem] = []
        else:
            body_tokens, _, _ = self._consume_balanced("table body")
            items = _GsqlBlockParser(self.source, body_tokens, self.path).parse()

        return _GsqlStatement(
            kind="table",
            ref=ref,
            comments=_comments_from_tokens([table_token]),
            items=items,
            source_query=source_query,
        )

    def _parse_extend(self) -> _GsqlStatement:
        extend_token = self._advance()
        ref = self._parse_ref("extend")
        body_tokens, _, _ = self._consume_balanced("extend body")
        return _GsqlStatement(
            kind="extend",
            ref=ref,
            comments=_comments_from_tokens([extend_token]),
            items=_GsqlBlockParser(self.source, body_tokens, self.path).parse(),
        )

    def _parse_ref(self, statement_kind: str) -> str:
        parts = []
        first = self._consume_name()
        if first is None:
            raise self._error(f"Expected table reference after `{statement_kind}`")
        parts.append(first)

        while self._matches(TokenType.DOT):
            self._advance()
            part = self._consume_name()
            if part is None:
                raise self._error(f"Expected identifier after `.` in `{statement_kind}` reference")
            parts.append(part)

        return ".".join(parts)

    def _consume_balanced(self, label: str) -> tuple[list[Token], Token, Token]:
        open_token = self._current
        if open_token is None or open_token.token_type != TokenType.L_PAREN:
            raise self._error(f"Expected `(` to start {label}")
        self._advance()

        body_tokens: list[Token] = []
        depth = 1
        while self._current is not None:
            token = self._current
            if token.token_type == TokenType.L_PAREN:
                depth += 1
            elif token.token_type == TokenType.R_PAREN:
                depth -= 1
                if depth == 0:
                    self._advance()
                    return body_tokens, open_token, token

            body_tokens.append(token)
            self._advance()

        raise self._error(f"Unclosed {label}")

    def _consume_name(self) -> str | None:
        token = self._current
        if token is None or not _is_name_token(token):
            return None
        self._advance()
        return _name_text(token)

    def _advance(self) -> Token:
        token = self._current
        if token is None:
            raise self._error("Unexpected end of file")
        self.index += 1
        return token

    def _matches(self, token_type: TokenType) -> bool:
        return self._current is not None and self._current.token_type == token_type

    def _matches_text(self, text: str) -> bool:
        return self._current is not None and self._current.text.lower() == text

    def _error(self, message: str) -> GrapheneParseError:
        token = self._current
        if token is None:
            return GrapheneParseError(f"{self.path}: {message}")
        return GrapheneParseError(f"{self.path}:{token.line}:{token.col}: {message}")


class _GsqlBlockParser:
    def __init__(self, source: str, body_tokens: list[Token], path: Path) -> None:
        self.source = source
        self.body_tokens = body_tokens
        self.path = path

    def parse(self) -> list[_GsqlBlockItem]:
        return [self._parse_item(tokens) for tokens in self._split_items() if tokens]

    def _split_items(self) -> list[list[Token]]:
        items: list[list[Token]] = []
        current: list[Token] = []
        depth = 0
        previous: Token | None = None

        for idx, token in enumerate(self.body_tokens):
            if current and depth == 0 and token.token_type in {TokenType.COMMA, TokenType.SEMICOLON}:
                items.append(current)
                current = []
                previous = token
                continue

            if (
                current
                and previous is not None
                and depth == 0
                and _has_newline_between(self.source, previous, token)
                and _item_can_end(current)
                and self._starts_item_at(idx)
            ):
                items.append(current)
                current = []

            current.append(token)
            depth = _updated_depth(depth, token)
            previous = token

        if current:
            items.append(current)
        return items

    def _starts_item_at(self, idx: int) -> bool:
        token = self.body_tokens[idx]
        if token.text.lower() in {"end", "else", "then", "when"}:
            return False
        return token.text.lower() == "join" or _is_name_token(token)

    def _parse_item(self, item_tokens: list[Token]) -> _GsqlBlockItem:
        comments = _comments_from_tokens(item_tokens)
        first = item_tokens[0]
        if first.text.lower() == "join":
            return self._parse_join(item_tokens, comments)

        colon_idx = _find_top_level_token(item_tokens, TokenType.COLON)
        if colon_idx is not None:
            if colon_idx != 1 or not _is_name_token(item_tokens[0]) or colon_idx == len(item_tokens) - 1:
                raise self._error(item_tokens[0], f"Invalid computed field: {_source_sql(self.source, item_tokens)}")
            return _GsqlComputed(
                name=_name_text(item_tokens[0]),
                expression=_source_sql(self.source, item_tokens[colon_idx + 1 :]),
                comments=comments,
            )

        as_idx = _find_top_level_text(item_tokens, "as")
        if as_idx is not None:
            if as_idx == 0 or as_idx != len(item_tokens) - 2 or not _is_name_token(item_tokens[-1]):
                raise self._error(item_tokens[0], f"Invalid computed field: {_source_sql(self.source, item_tokens)}")
            return _GsqlComputed(
                name=_name_text(item_tokens[-1]),
                expression=_source_sql(self.source, item_tokens[:as_idx]),
                comments=comments,
            )

        return self._parse_column(item_tokens, comments)

    def _parse_column(self, item_tokens: list[Token], comments: _Comments) -> _GsqlColumn:
        if len(item_tokens) < 2 or not _is_name_token(item_tokens[0]):
            raise self._error(item_tokens[0], f"Invalid column definition: {_source_sql(self.source, item_tokens)}")

        primary_key = item_tokens[-1].text.lower() == "primary_key"
        type_tokens = item_tokens[1:-1] if primary_key else item_tokens[1:]
        if not type_tokens:
            raise self._error(item_tokens[0], f"Column `{_name_text(item_tokens[0])}` requires a data type")

        return _GsqlColumn(
            name=_name_text(item_tokens[0]),
            data_type=_source_sql(self.source, type_tokens),
            primary_key=primary_key,
            comments=comments,
        )

    def _parse_join(self, item_tokens: list[Token], comments: _Comments) -> _GsqlJoin:
        if len(item_tokens) < 5:
            raise self._error(item_tokens[0], f"Invalid join definition: {_source_sql(self.source, item_tokens)}")

        cardinality_token = item_tokens[1]
        cardinality = cardinality_token.text.lower()
        if cardinality not in {"one", "many"}:
            raise self._error(cardinality_token, "Graphene joins must use `join one` or `join many`")

        on_idx = _find_top_level_text(item_tokens, "on", start=2)
        if on_idx is None or on_idx <= 2 or on_idx == len(item_tokens) - 1:
            raise self._error(
                item_tokens[0], f"Join requires an `on` expression: {_source_sql(self.source, item_tokens)}"
            )

        target_tokens = item_tokens[2:on_idx]
        alias: str | None = None
        as_idx = _find_top_level_text(target_tokens, "as")
        if as_idx is not None:
            alias_tokens = target_tokens[as_idx + 1 :]
            target_tokens = target_tokens[:as_idx]
            if len(alias_tokens) != 1 or not _is_name_token(alias_tokens[0]):
                raise self._error(item_tokens[0], f"Invalid join alias: {_source_sql(self.source, item_tokens)}")
            alias = _name_text(alias_tokens[0])

        target_ref = _ref_from_tokens(target_tokens)
        if not target_ref:
            raise self._error(item_tokens[0], f"Invalid join target: {_source_sql(self.source, item_tokens)}")

        return _GsqlJoin(
            cardinality=cardinality,  # type: ignore[arg-type]
            target_ref=target_ref,
            alias=alias,
            on_sql=_source_sql(self.source, item_tokens[on_idx + 1 :]),
            comments=comments,
        )

    def _error(self, token: Token, message: str) -> GrapheneParseError:
        return GrapheneParseError(f"{self.path}:{token.line}:{token.col}: {message}")


def _tokenize_gsql(source: str) -> list[Token]:
    return _GRAPHENE_DIALECT.tokenize(source)


def _is_name_token(token: Token) -> bool:
    text = token.text
    if not text:
        return False
    if token.token_type == TokenType.IDENTIFIER:
        return True
    return text[0].isalpha() or text[0] == "_"


def _name_text(token: Token) -> str:
    return token.text


def _ref_from_tokens(ref_tokens: list[Token]) -> str | None:
    if not ref_tokens or not _is_name_token(ref_tokens[0]):
        return None

    parts = [_name_text(ref_tokens[0])]
    idx = 1
    while idx < len(ref_tokens):
        if ref_tokens[idx].token_type != TokenType.DOT or idx + 1 >= len(ref_tokens):
            return None
        next_token = ref_tokens[idx + 1]
        if not _is_name_token(next_token):
            return None
        parts.append(_name_text(next_token))
        idx += 2
    return ".".join(parts)


def _updated_depth(depth: int, token: Token) -> int:
    if token.token_type in {TokenType.L_PAREN, TokenType.L_BRACKET, TokenType.L_BRACE}:
        return depth + 1
    if token.token_type in {TokenType.R_PAREN, TokenType.R_BRACKET, TokenType.R_BRACE}:
        return max(depth - 1, 0)
    return depth


def _has_newline_between(source: str, left: Token, right: Token) -> bool:
    return "\n" in source[left.end + 1 : right.start]


def _item_can_end(item_tokens: list[Token]) -> bool:
    if not item_tokens:
        return False

    if _looks_like_column_definition(item_tokens):
        return True

    last = item_tokens[-1]
    if last.token_type in {
        TokenType.ALIAS,
        TokenType.AND,
        TokenType.BETWEEN,
        TokenType.COLON,
        TokenType.COMMA,
        TokenType.DASH,
        TokenType.DCOLON,
        TokenType.DOT,
        TokenType.ELSE,
        TokenType.EQ,
        TokenType.GT,
        TokenType.GTE,
        TokenType.L_PAREN,
        TokenType.LT,
        TokenType.LTE,
        TokenType.MOD,
        TokenType.NEQ,
        TokenType.ON,
        TokenType.OR,
        TokenType.PLUS,
        TokenType.SLASH,
        TokenType.STAR,
        TokenType.THEN,
    }:
        return False
    if not _case_expressions_balanced(item_tokens):
        return False

    first_text = item_tokens[0].text.lower()
    if first_text == "join":
        on_idx = _find_top_level_text(item_tokens, "on", start=2)
        return on_idx is not None and on_idx < len(item_tokens) - 1
    if _find_top_level_token(item_tokens, TokenType.COLON) is not None:
        colon_idx = _find_top_level_token(item_tokens, TokenType.COLON)
        return colon_idx is not None and colon_idx < len(item_tokens) - 1
    as_idx = _find_top_level_text(item_tokens, "as")
    if as_idx is not None:
        return as_idx == len(item_tokens) - 2 and _is_name_token(item_tokens[-1])
    return len(item_tokens) >= 2


def _looks_like_column_definition(item_tokens: list[Token]) -> bool:
    return (
        len(item_tokens) >= 2
        and item_tokens[0].text.lower() != "join"
        and _find_top_level_token(item_tokens, TokenType.COLON) is None
        and _find_top_level_text(item_tokens, "as") is None
        and _is_name_token(item_tokens[0])
        and any(_is_name_token(token) for token in item_tokens[1:])
    )


def _case_expressions_balanced(tokens_: list[Token]) -> bool:
    case_count = sum(1 for token in tokens_ if token.text.lower() == "case")
    end_count = sum(1 for token in tokens_ if token.text.lower() == "end")
    return end_count >= case_count


def _find_top_level_text(tokens_: list[Token], text: str, start: int = 0) -> int | None:
    return _find_top_level(tokens_, lambda token: token.text.lower() == text, start=start)


def _find_top_level_token(tokens_: list[Token], token_type: TokenType, start: int = 0) -> int | None:
    return _find_top_level(tokens_, lambda token: token.token_type == token_type, start=start)


def _find_top_level(tokens_: list[Token], predicate: Any, start: int = 0) -> int | None:
    depth = 0
    for idx, token in enumerate(tokens_):
        if idx >= start and depth == 0 and predicate(token):
            return idx
        depth = _updated_depth(depth, token)
    return None


def _source_sql(source: str, tokens_: list[Token]) -> str:
    if not tokens_:
        return ""
    return _normalize_sql_fragment(_remove_gsql_comments(source[tokens_[0].start : tokens_[-1].end + 1]))


def _remove_gsql_comments(sql: str) -> str:
    chars: list[str] = []
    quote: str | None = None
    idx = 0
    while idx < len(sql):
        char = sql[idx]
        next_pair = sql[idx : idx + 2]

        if quote:
            chars.append(char)
            if char == quote:
                if quote == "'" and idx + 1 < len(sql) and sql[idx + 1] == "'":
                    idx += 1
                    chars.append(sql[idx])
                else:
                    quote = None
            elif char == "\\" and idx + 1 < len(sql):
                idx += 1
                chars.append(sql[idx])
            idx += 1
            continue

        if char in {"'", '"', "`"}:
            quote = char
            chars.append(char)
            idx += 1
            continue
        if next_pair == "--":
            idx = _skip_to_line_end(sql, idx + 2)
            chars.append(" ")
            continue
        if next_pair == "/*":
            end = sql.find("*/", idx + 2)
            idx = len(sql) if end == -1 else end + 2
            chars.append(" ")
            continue
        if char == "#":
            idx = _skip_to_line_end(sql, idx + 1)
            chars.append(" ")
            continue

        chars.append(char)
        idx += 1
    return "".join(chars)


def _skip_to_line_end(text: str, idx: int) -> int:
    while idx < len(text) and text[idx] not in {"\n", "\r"}:
        idx += 1
    return idx


def _comments_from_tokens(tokens_: list[Token]) -> _Comments:
    comments = _Comments()
    for token in tokens_:
        for raw_comment in token.comments or []:
            _collect_comment_text(raw_comment, comments)
    return comments


def _collect_comment_text(text: str, comments: _Comments) -> None:
    cleaned = text.strip()
    if not cleaned:
        return

    metadata, description = _parse_metadata(cleaned)
    comments.metadata.update(metadata)
    if description:
        comments.descriptions.append(description)


def _parse_metadata(text: str) -> tuple[dict[str, Any], str | None]:
    metadata: dict[str, Any] = {}
    description_parts: list[str] = []
    cursor = 0

    for match in _METADATA_TOKEN_RE.finditer(text):
        key = match.group("key")
        raw_value = match.group("value")
        has_hash = bool(match.group("hash"))
        is_known = key in _ANNOTATION_FLAGS or key in _ANNOTATION_VALUE_KEYS
        is_assignment = raw_value is not None
        is_metadata = has_hash or (is_known and (is_assignment or key in _ANNOTATION_FLAGS))

        if not is_metadata:
            continue

        description_parts.append(text[cursor : match.start()])
        cursor = match.end()
        metadata[key] = _metadata_value(raw_value)

    description_parts.append(text[cursor:])
    description = " ".join(part.strip() for part in description_parts if part.strip()).strip()

    if isinstance(metadata.get("description"), str):
        description = f"{description} {metadata['description']}".strip()

    return metadata, description or None


def _metadata_value(raw_value: str | None) -> Any:
    if raw_value is None:
        return True
    value = raw_value.strip()
    if len(value) >= 2 and value[0] in {"'", '"'} and value[-1] == value[0]:
        value = value[1:-1]
    return value.replace(r"\"", '"').replace(r"\'", "'")


def _dimension_from_column(column: _GsqlColumn) -> Dimension:
    formatting = _formatting_from_metadata(column.comments.metadata)
    return Dimension(
        name=column.name,
        type=_dimension_type_from_data_type(column.data_type, column.name),
        sql=column.name,
        granularity=_granularity_from_metadata(column.comments.metadata, column.data_type),
        description=column.comments.description,
        metadata=_graphene_metadata(column.comments.metadata, {"data_type": column.data_type}),
        format=formatting.get("format"),
        value_format_name=formatting.get("value_format_name"),
    )


def _dimensions_from_view_query(query: str) -> list[Dimension]:
    tokens_ = _tokenize_gsql(query)
    select_idx = _find_top_level_token(tokens_, TokenType.SELECT)
    if select_idx is None:
        return []

    end_idx = len(tokens_)
    clause_tokens = {
        TokenType.FROM,
        TokenType.WHERE,
        TokenType.GROUP_BY,
        TokenType.HAVING,
        TokenType.ORDER_BY,
        TokenType.LIMIT,
        TokenType.UNION,
        TokenType.EXCEPT,
        TokenType.INTERSECT,
    }
    depth = 0
    for idx in range(select_idx + 1, len(tokens_)):
        token = tokens_[idx]
        if depth == 0 and token.token_type in clause_tokens:
            end_idx = idx
            break
        depth = _updated_depth(depth, token)

    dimensions: list[Dimension] = []
    for projection_tokens in _split_top_level_commas(tokens_[select_idx + 1 : end_idx]):
        if not projection_tokens or projection_tokens[0].token_type == TokenType.STAR:
            continue

        name, expression_tokens = _projection_name_and_expression(projection_tokens, query)
        if not name or not expression_tokens:
            continue

        expression = _source_sql(query, expression_tokens)
        dimensions.append(
            Dimension(
                name=name,
                type=_dimension_type_from_expression(expression, name, {}),
                sql=expression,
                granularity=_granularity_from_metadata({}, expression),
            )
        )
    return dimensions


def _split_top_level_commas(tokens_: list[Token]) -> list[list[Token]]:
    groups: list[list[Token]] = []
    current: list[Token] = []
    depth = 0
    for token in tokens_:
        if depth == 0 and token.token_type == TokenType.COMMA:
            groups.append(current)
            current = []
            continue
        current.append(token)
        depth = _updated_depth(depth, token)
    groups.append(current)
    return groups


def _projection_name_and_expression(projection_tokens: list[Token], source: str) -> tuple[str | None, list[Token]]:
    as_idx = _find_top_level_text(projection_tokens, "as")
    if as_idx is not None and as_idx == len(projection_tokens) - 2 and _is_name_token(projection_tokens[-1]):
        return _name_text(projection_tokens[-1]), projection_tokens[:as_idx]

    if len(projection_tokens) >= 2 and _is_name_token(projection_tokens[-1]):
        candidate_expr = projection_tokens[:-1]
        if (
            candidate_expr
            and projection_tokens[-2].token_type != TokenType.DOT
            and _parse_expression(_source_sql(source, candidate_expr))
        ):
            return _name_text(projection_tokens[-1]), candidate_expr

    ref = _ref_from_tokens(projection_tokens)
    if ref:
        return _model_name_from_ref(ref), projection_tokens

    expression = _source_sql(source, projection_tokens)
    return _name_from_expression(expression), projection_tokens


def _name_from_expression(expression: str) -> str | None:
    parsed = _parse_expression(expression)
    if isinstance(parsed, exp.Column):
        return parsed.name
    if isinstance(parsed, exp.Alias):
        return parsed.alias
    return None


def _extract_join_keys(
    on_expr: str,
    model_name: str,
    target_scope: str,
    target_model: str,
) -> tuple[str | list[str] | None, str | list[str] | None]:
    parsed = _parse_expression(on_expr)
    if not parsed:
        return None, None

    local_keys: list[str] = []
    target_keys: list[str] = []
    for equality in _join_equalities(parsed):
        left = _column_parts(equality.left)
        right = _column_parts(equality.right)
        if not left or not right:
            continue

        left_side = _join_ref_side(left, model_name, target_scope, target_model)
        right_side = _join_ref_side(right, model_name, target_scope, target_model)
        if left_side == "local" and right_side == "target":
            local_keys.append(left[-1])
            target_keys.append(right[-1])
        elif left_side == "target" and right_side == "local":
            local_keys.append(right[-1])
            target_keys.append(left[-1])

    return _one_or_many(local_keys), _one_or_many(target_keys)


def _join_equalities(expression: exp.Expression) -> list[exp.EQ]:
    if isinstance(expression, exp.EQ):
        return [expression]
    if isinstance(expression, exp.And):
        return _join_equalities(expression.left) + _join_equalities(expression.right)
    return []


def _column_parts(expression: exp.Expression) -> list[str] | None:
    if not isinstance(expression, exp.Column):
        return None
    return [part.name for part in expression.parts]


def _join_ref_side(parts: list[str], model_name: str, target_scope: str, target_model: str) -> str | None:
    if len(parts) == 1:
        return "local"

    qualifier = parts[-2]
    if qualifier in {target_scope, target_model}:
        return "target"
    if qualifier == model_name:
        return "local"
    return None


def _one_or_many(values: list[str]) -> str | list[str] | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return values


def _append_primary_key_candidates(
    candidates: dict[str, list[str]],
    model_name: str,
    key: str | list[str] | None,
) -> None:
    if not key:
        return
    keys = [key] if isinstance(key, str) else key
    candidates.setdefault(model_name, []).extend(keys)


def _computed_metric_names(items: list[_GsqlBlockItem], seed_names: set[str] | None = None) -> set[str]:
    expressions = {
        item.name: _normalize_sql_fragment(item.expression) for item in items if isinstance(item, _GsqlComputed)
    }
    metric_names = set(seed_names or set())
    metric_names.update(name for name, expression in expressions.items() if _has_inline_aggregate(expression))

    changed = True
    while changed:
        changed = False
        for name, expression in expressions.items():
            if name in metric_names:
                continue
            if metric_names & _referenced_field_names(expression):
                metric_names.add(name)
                changed = True

    return metric_names


def _is_metric_expression(expression: str, local_metric_names: set[str]) -> bool:
    if _has_inline_aggregate(expression):
        return True
    return bool(local_metric_names & _referenced_field_names(expression))


def _rewrite_graphene_percentile_shorthand(expression: str) -> tuple[str, bool]:
    tokens_ = _tokenize_gsql(expression)
    replacements: list[tuple[int, int, str]] = []
    idx = 0
    while idx < len(tokens_) - 1:
        token = tokens_[idx]
        fraction = _graphene_percentile_fraction(token.text)
        if fraction is None or tokens_[idx + 1].token_type != TokenType.L_PAREN:
            idx += 1
            continue

        close_idx = _matching_right_paren_index(tokens_, idx + 1)
        if close_idx is None:
            idx += 1
            continue

        argument_tokens = tokens_[idx + 2 : close_idx]
        if len(_split_top_level_commas(argument_tokens)) == 1:
            argument_sql = expression[tokens_[idx + 1].end + 1 : tokens_[close_idx].start].strip()
            if argument_sql:
                replacements.append(
                    (
                        token.start,
                        tokens_[close_idx].end + 1,
                        f"PERCENTILE_CONT({fraction}) WITHIN GROUP (ORDER BY {argument_sql})",
                    )
                )
        idx = close_idx + 1

    if not replacements:
        return expression, False

    rewritten = expression
    for start, end, replacement in reversed(replacements):
        rewritten = f"{rewritten[:start]}{replacement}{rewritten[end:]}"
    return rewritten, True


def _matching_right_paren_index(tokens_: list[Token], open_idx: int) -> int | None:
    depth = 0
    for idx in range(open_idx, len(tokens_)):
        token = tokens_[idx]
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
            if depth == 0:
                return idx
    return None


def _has_inline_aggregate(expression: str) -> bool:
    parsed = _parse_expression(expression)
    if parsed:
        for node in parsed.walk():
            if isinstance(node, exp.AggFunc):
                return True
            if isinstance(node, exp.Anonymous):
                function_name = node.name.lower()
                if function_name in _AGGREGATE_FUNCTIONS or _graphene_percentile_fraction(function_name) is not None:
                    return True
        return False

    tokens_ = _tokenize_gsql(expression)
    for idx, token in enumerate(tokens_[:-1]):
        function_name = token.text.lower()
        if tokens_[idx + 1].token_type == TokenType.L_PAREN and (
            function_name in _AGGREGATE_FUNCTIONS or _graphene_percentile_fraction(function_name) is not None
        ):
            return True
    return False


def _graphene_percentile_fraction(function_name: str) -> str | None:
    match = re.fullmatch(r"p(\d{1,3})", function_name.lower())
    if not match:
        return None

    percentile = int(match.group(1))
    if not 0 <= percentile <= 100:
        return None
    return f"{percentile / 100:g}"


def _referenced_field_names(expression: str) -> set[str]:
    parsed = _parse_expression(expression)
    if parsed:
        return {column.name for column in parsed.find_all(exp.Column)}

    names: set[str] = set()
    tokens_ = _tokenize_gsql(expression)
    for idx, token in enumerate(tokens_):
        if not _is_name_token(token):
            continue
        if idx + 1 < len(tokens_) and tokens_[idx + 1].token_type == TokenType.L_PAREN:
            continue
        names.add(_name_text(token))
    return names


def _parse_expression(expression: str) -> exp.Expression | None:
    try:
        return sqlglot.parse_one(expression, read="duckdb")
    except Exception:
        return None


def _dimension_type_from_data_type(data_type: str, name: str) -> str:
    lowered = data_type.lower()
    if any(token in lowered for token in ("date", "time", "timestamp", "datetime")):
        return "time"
    if any(token in lowered for token in ("bool", "boolean")):
        return "boolean"
    if any(
        token in lowered
        for token in (
            "int",
            "float",
            "double",
            "decimal",
            "numeric",
            "number",
            "real",
        )
    ):
        if name.lower().endswith("_id") or name.lower() == "id":
            return "categorical"
        return "numeric"
    return "categorical"


def _dimension_type_from_expression(expression: str, name: str, metadata: dict[str, Any]) -> str:
    parsed = _parse_expression(expression)
    if _granularity_from_metadata(metadata, expression):
        return "time"
    if name.lower().startswith(("is_", "has_")):
        return "boolean"
    if parsed:
        if isinstance(
            parsed,
            (exp.And, exp.Between, exp.EQ, exp.GT, exp.GTE, exp.In, exp.Is, exp.LT, exp.LTE, exp.NEQ, exp.Not, exp.Or),
        ):
            return "boolean"
        if any(isinstance(node, (exp.Cast, exp.Date, exp.Timestamp, exp.TimestampTrunc)) for node in parsed.walk()):
            rendered = parsed.sql(dialect="duckdb").lower()
            if any(token in rendered for token in ("date", "time", "timestamp")):
                return "time"
        if any(isinstance(node, (exp.Add, exp.Div, exp.Mod, exp.Mul, exp.Sub)) for node in parsed.walk()):
            return "numeric"

    lowered = expression.lower()
    if any(token in lowered for token in ("date_trunc", "date_bin", "::date", "::timestamp", "timestamp(", "date(")):
        return "time"
    if any(op in expression for op in ("=", "<>", "!=", "<=", ">=", "<", ">")) and not lowered.strip().startswith(
        "case"
    ):
        return "boolean"
    if any(op in expression for op in ("+", "-", "*", "/")):
        return "numeric"
    return "categorical"


def _granularity_from_metadata(metadata: dict[str, Any], expression_or_type: str) -> str | None:
    grain = metadata.get("timeGrain") or metadata.get("timegrain")
    if isinstance(grain, str) and grain.lower() in _VALID_GRANULARITIES:
        return grain.lower()

    parsed = _parse_expression(expression_or_type)
    if parsed:
        for node in parsed.walk():
            if isinstance(node, exp.TimestampTrunc):
                unit = node.args.get("unit")
                unit_name = getattr(unit, "name", str(unit)).lower() if unit is not None else ""
                if unit_name in _VALID_GRANULARITIES:
                    return unit_name

    lowered = expression_or_type.lower()
    if any(token in lowered for token in ("date", "timestamp", "datetime")):
        return "day"
    return None


def _formatting_from_metadata(metadata: dict[str, Any]) -> dict[str, str]:
    if "currency" in metadata and metadata["currency"] is not True:
        return {"value_format_name": str(metadata["currency"]).lower()}
    if "ratio" in metadata or "pct" in metadata:
        return {"value_format_name": "percent"}
    return {}


def _graphene_metadata(metadata: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any] | None:
    payload: dict[str, Any] = {}
    if metadata:
        payload["annotations"] = dict(metadata)
    if extra:
        payload.update(extra)
    return {"graphene": payload} if payload else None


def _choose_primary_key(dimensions: list[Dimension], candidates: list[str] | None) -> str:
    dimension_names = {dimension.name for dimension in dimensions}
    if candidates:
        for candidate in candidates:
            if candidate in dimension_names:
                return candidate
        return candidates[0]
    if "id" in dimension_names:
        return "id"
    for dimension in dimensions:
        if dimension.name.endswith("_id"):
            return dimension.name
    return dimensions[0].name if dimensions else "id"


def _model_name_from_ref(ref: str) -> str:
    return ref.split(".")[-1]


def _normalize_sql_fragment(expression: str) -> str:
    return re.sub(r"\s+", " ", expression).strip()
