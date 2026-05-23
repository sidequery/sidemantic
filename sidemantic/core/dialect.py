"""SQLGlot dialect extensions for Sidemantic SQL syntax."""

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


class ModelDef(exp.Expression):
    """MODEL() definition statement.

    Syntax:
        MODEL (
            name orders,
            table orders,
            primary_key order_id
        );
    """

    arg_types = {"expressions": True}


class DimensionDef(exp.Expression):
    """DIMENSION() definition statement.

    Syntax:
        DIMENSION (
            name status,
            type categorical,
            sql status
        );
    """

    arg_types = {"expressions": True}


class RelationshipDef(exp.Expression):
    """RELATIONSHIP() definition statement.

    Syntax:
        RELATIONSHIP (
            name customer,
            type many_to_one,
            foreign_key customer_id
        );
    """

    arg_types = {"expressions": True}


class MetricDef(exp.Expression):
    """METRIC() definition statement.

    Syntax:
        METRIC (
            name revenue,
            expression SUM(amount),
            description 'Total revenue'
        );
    """

    arg_types = {"expressions": True}


class SegmentDef(exp.Expression):
    """SEGMENT() definition statement.

    Syntax:
        SEGMENT (
            name active_users,
            expression status = 'active'
        );
    """

    arg_types = {"expressions": True}


class ParameterDef(exp.Expression):
    """PARAMETER() definition statement.

    Syntax:
        PARAMETER (
            name region,
            type string,
            default_value 'us'
        );
    """

    arg_types = {"expressions": True}


class PreAggregationDef(exp.Expression):
    """PRE_AGGREGATION() definition statement.

    Syntax:
        PRE_AGGREGATION (
            name daily_rollup,
            measures [order_count, revenue],
            dimensions [status],
            time_dimension order_date,
            granularity day
        );
    """

    arg_types = {"expressions": True}


class TableBlockModelDef(exp.Expression):
    """Compact model block definition.

    Syntax:
        model orders from orders (
            primary key (order_id)
            status
            sum(amount) as revenue
        )
    """

    arg_types = {
        "this": True,
        "table": False,
        "source_sql": False,
        "expressions": False,
    }


class TableBlockPrimaryKeyDef(exp.Expression):
    """Primary key declaration inside a compact model block."""

    arg_types = {"columns": True}


class TableBlockDefaultTimeDef(exp.Expression):
    """Default time declaration inside a compact model block."""

    arg_types = {"this": True, "grain": False}


class TableBlockSegmentDef(exp.Expression):
    """Segment declaration inside a compact model block."""

    arg_types = {"this": True, "sql": True}


class TableBlockJoinDef(exp.Expression):
    """Relationship declaration inside a compact model block."""

    arg_types = {
        "this": True,
        "relationship_type": True,
        "local_keys": True,
        "target_keys": True,
    }


class TableBlockFieldDef(exp.Expression):
    """Dimension or metric field declaration inside a compact model block."""

    arg_types = {
        "this": True,
        "sql": True,
        "dimension_type": False,
        "granularity": False,
    }


class PropertyEQ(exp.Expression):
    """Property assignment in METRIC/SEGMENT definitions.

    Represents: name value or name 'string value'
    """

    arg_types = {"this": True, "expression": True}


class TableBlockParseError(ValueError):
    """Raised when compact table-block model syntax is invalid."""


class SidemanticParser(parser.Parser):
    """Extended parser with MODEL, DIMENSION, RELATIONSHIP, METRIC, and SEGMENT support."""

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "MODEL": lambda args: ModelDef(expressions=args),
        "DIMENSION": lambda args: DimensionDef(expressions=args),
        "RELATIONSHIP": lambda args: RelationshipDef(expressions=args),
        "METRIC": lambda args: MetricDef(expressions=args),
        "SEGMENT": lambda args: SegmentDef(expressions=args),
        "PARAMETER": lambda args: ParameterDef(expressions=args),
        "PRE_AGGREGATION": lambda args: PreAggregationDef(expressions=args),
    }

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

    def parse(self, raw_tokens: list[Token], sql: str | None = None) -> list[exp.Expression | None]:
        """Parse Sidemantic SQL, including newline-delimited compact model blocks."""
        if not self._has_table_block_model(raw_tokens):
            return super().parse(raw_tokens, sql)

        self.reset()
        self.sql = sql or ""
        self._index = -1
        self._tokens = raw_tokens
        self._advance()

        expressions = []
        while self._curr:
            if self._match(TokenType.SEMICOLON):
                continue
            statement = self._parse_statement()
            if statement:
                expressions.append(statement)
                continue
            self.raise_error("Expected Sidemantic SQL statement")

        self.check_errors()
        return expressions

    @classmethod
    def _has_table_block_model(cls, raw_tokens: list[Token]) -> bool:
        for idx, token in enumerate(raw_tokens):
            if token.text.upper() != "MODEL":
                continue
            if idx + 1 >= len(raw_tokens):
                continue
            if raw_tokens[idx + 1].token_type != TokenType.L_PAREN:
                return True
        return False

    def _parse_statement(self) -> exp.Expression | None:
        """Override to handle MODEL, DIMENSION, RELATIONSHIP, METRIC, and SEGMENT as statements."""
        if self._match_texts(
            ("MODEL", "DIMENSION", "RELATIONSHIP", "METRIC", "SEGMENT", "PARAMETER", "PRE_AGGREGATION")
        ):
            func_name = self._prev.text.upper()
            if func_name == "MODEL" and (not self._curr or self._curr.token_type != tokens.TokenType.L_PAREN):
                return self._parse_table_block_model()

            self._match(tokens.TokenType.L_PAREN)

            # Parse properties
            properties = []
            while not self._match(tokens.TokenType.R_PAREN):
                prop = self._parse_property()
                if prop:
                    properties.append(prop)

                # Handle comma between properties
                if not self._match(tokens.TokenType.COMMA):
                    self._match(tokens.TokenType.R_PAREN)
                    break

            # Return appropriate definition type
            if func_name == "MODEL":
                return ModelDef(expressions=properties)
            elif func_name == "DIMENSION":
                return DimensionDef(expressions=properties)
            elif func_name == "RELATIONSHIP":
                return RelationshipDef(expressions=properties)
            elif func_name == "METRIC":
                return MetricDef(expressions=properties)
            elif func_name == "SEGMENT":
                return SegmentDef(expressions=properties)
            elif func_name == "PARAMETER":
                return ParameterDef(expressions=properties)
            else:  # PRE_AGGREGATION
                return PreAggregationDef(expressions=properties)

        return super()._parse_statement()

    def _parse_table_block_model(self) -> TableBlockModelDef:
        model_name = self._parse_table_block_identifier()
        if not model_name:
            raise TableBlockParseError("Compact model block requires a model name")

        if not self._match(TokenType.FROM):
            raise TableBlockParseError(
                f"Table-block model '{model_name}' must use `model {model_name} from <table> (...)`"
            )

        table = None
        source_sql = None
        if self._curr and self._curr.token_type == TokenType.L_PAREN:
            source_sql = self._parse_table_block_derived_source(model_name)
        else:
            table = self._parse_table_block_source_table(model_name)

        if not self._curr or self._curr.token_type != TokenType.L_PAREN:
            source = "derived SQL source" if source_sql else "table source"
            raise TableBlockParseError(f"Model '{model_name}' must include a body block after the {source}")

        body_tokens = self._consume_balanced_tokens(model_name, "model block")
        body_expressions = [
            self._parse_table_block_body_statement(model_name, statement_tokens)
            for statement_tokens in self._split_table_block_body_statements(body_tokens)
        ]

        return TableBlockModelDef(
            this=exp.to_identifier(model_name),
            table=table,
            source_sql=source_sql,
            expressions=body_expressions,
        )

    def _parse_table_block_derived_source(self, model_name: str) -> str:
        open_token = self._curr
        source_tokens = self._consume_balanced_tokens(model_name, "derived SQL source")
        if not source_tokens:
            raise TableBlockParseError(f"Derived SQL source for model '{model_name}' cannot be empty")

        close_token = self._tokens[self._index - 1]
        source_sql = self.sql[open_token.end + 1 : close_token.start].strip()
        if not source_sql:
            raise TableBlockParseError(f"Derived SQL source for model '{model_name}' cannot be empty")
        return source_sql

    def _parse_table_block_source_table(self, model_name: str) -> str:
        parts = []
        first = self._parse_table_block_identifier()
        if not first:
            raise TableBlockParseError(f"Model '{model_name}' must declare a table or derived SQL source after `from`")
        parts.append(first)

        while self._match(TokenType.DOT):
            part = self._parse_table_block_identifier()
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
                and self._has_statement_separator_between(statement, previous_token, token)
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
        return self._table_block_statement_can_end(statement_tokens) and not self._token_continues_statement(right)

    def _table_block_statement_can_end(self, statement_tokens: list[Token]) -> bool:
        if not statement_tokens or self._token_continues_statement(statement_tokens[-1]):
            return False

        first_text = statement_tokens[0].text.lower()
        first_type = statement_tokens[0].token_type

        if first_type == TokenType.PRIMARY_KEY or first_text == "primary_key":
            return len(statement_tokens) > 1

        if first_type == TokenType.DEFAULT:
            return len(statement_tokens) == 3 or len(statement_tokens) == 5

        if first_text == "segment":
            alias_idx = self._find_top_level_token(statement_tokens, TokenType.ALIAS, start=2)
            return alias_idx is not None and alias_idx < len(statement_tokens) - 1

        if first_type == TokenType.JOIN:
            on_idx = self._find_top_level_token(statement_tokens, TokenType.ON, start=1)
            return on_idx is not None and on_idx < len(statement_tokens) - 1

        colon_idx = self._find_top_level_token(statement_tokens, TokenType.COLON)
        base_tokens = statement_tokens[:colon_idx] if colon_idx is not None else statement_tokens
        if not base_tokens:
            return False

        alias_idx = self._find_top_level_token(base_tokens, TokenType.ALIAS)
        if alias_idx is not None:
            name_tokens = base_tokens[alias_idx + 1 :]
            return alias_idx > 0 and len(name_tokens) == 1 and self._identifier_from_token(name_tokens[0]) is not None

        return len(base_tokens) == 1 and self._identifier_from_token(base_tokens[0]) is not None

    def _token_continues_statement(self, token: Token) -> bool:
        return token.token_type in self._STATEMENT_CONTINUATION_TOKEN_TYPES

    def _parse_table_block_body_statement(self, model_name: str, statement_tokens: list[Token]) -> exp.Expression:
        first_text = statement_tokens[0].text.lower()
        first_type = statement_tokens[0].token_type

        if first_type == TokenType.TABLE:
            raise TableBlockParseError(
                f"Model '{model_name}' uses table source inside the block; use `model {model_name} from <table> (...)`"
            )
        if first_type == TokenType.PRIMARY_KEY or first_text == "primary_key":
            return self._parse_table_block_primary_key(model_name, statement_tokens)
        if first_type == TokenType.DEFAULT:
            return self._parse_table_block_default_time(model_name, statement_tokens)
        if first_text == "segment":
            return self._parse_table_block_segment(model_name, statement_tokens)
        if first_type == TokenType.JOIN:
            return self._parse_table_block_join(model_name, statement_tokens)

        field = self._parse_table_block_field(model_name, statement_tokens)
        if field:
            return field

        raise TableBlockParseError(
            f"Unrecognized statement in model '{model_name}': {self._statement_sql(statement_tokens)}"
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

        columns = self._parse_identifier_list(value_tokens)
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
                f"Invalid default time in model '{model_name}': {self._statement_sql(statement_tokens)}"
            )

        dimension_name = self._identifier_from_token(statement_tokens[2])
        if not dimension_name:
            raise TableBlockParseError(
                f"Invalid default time in model '{model_name}': {self._statement_sql(statement_tokens)}"
            )

        grain = None
        if len(statement_tokens) > 3:
            if (
                len(statement_tokens) != 5
                or statement_tokens[3].text.lower() not in ("grain", "granularity")
                or not self._identifier_from_token(statement_tokens[4])
            ):
                raise TableBlockParseError(
                    f"Invalid default time in model '{model_name}': {self._statement_sql(statement_tokens)}"
                )
            grain = statement_tokens[4].text.lower()
            self._validate_time_grain(model_name, "default time", grain)

        return TableBlockDefaultTimeDef(this=exp.to_identifier(dimension_name), grain=grain)

    def _parse_table_block_segment(
        self,
        model_name: str,
        statement_tokens: list[Token],
    ) -> TableBlockSegmentDef:
        if len(statement_tokens) < 4:
            raise TableBlockParseError(
                f"Invalid segment in model '{model_name}': {self._statement_sql(statement_tokens)}"
            )

        segment_name = self._identifier_from_token(statement_tokens[1])
        alias_idx = self._find_top_level_token(statement_tokens, TokenType.ALIAS, start=2)
        if not segment_name or alias_idx is None or alias_idx == len(statement_tokens) - 1:
            raise TableBlockParseError(
                f"Invalid segment in model '{model_name}': {self._statement_sql(statement_tokens)}"
            )

        expression_sql = self._statement_sql(statement_tokens[alias_idx + 1 :])
        if not expression_sql:
            raise TableBlockParseError(f"Segment '{segment_name}' in model '{model_name}' requires a SQL expression")

        return TableBlockSegmentDef(this=exp.to_identifier(segment_name), sql=expression_sql)

    def _parse_table_block_join(
        self,
        model_name: str,
        statement_tokens: list[Token],
    ) -> TableBlockJoinDef:
        on_idx = self._find_top_level_token(statement_tokens, TokenType.ON, start=1)
        if on_idx is None or on_idx <= 1 or on_idx == len(statement_tokens) - 1:
            raise TableBlockParseError(f"Invalid join in model '{model_name}': {self._statement_sql(statement_tokens)}")

        header_tokens = statement_tokens[1:on_idx]
        cardinality = None
        target_idx = 0
        if header_tokens[0].text.lower() in self._CARDINALITY_ALIASES:
            cardinality = header_tokens[0].text.lower()
            target_idx = 1

        if target_idx >= len(header_tokens):
            raise TableBlockParseError(f"Invalid join in model '{model_name}': {self._statement_sql(statement_tokens)}")

        target_model = self._identifier_from_token(header_tokens[target_idx])
        if not target_model:
            raise TableBlockParseError(f"Invalid join in model '{model_name}': {self._statement_sql(statement_tokens)}")

        alias = None
        alias_tokens = header_tokens[target_idx + 1 :]
        if alias_tokens:
            if len(alias_tokens) == 2 and alias_tokens[0].token_type == TokenType.ALIAS:
                alias = self._identifier_from_token(alias_tokens[1])
            elif len(alias_tokens) == 1:
                alias = self._identifier_from_token(alias_tokens[0])
            if not alias:
                raise TableBlockParseError(
                    f"Invalid join in model '{model_name}': {self._statement_sql(statement_tokens)}"
                )

        relationship_type = self._CARDINALITY_ALIASES.get(cardinality or "one", "many_to_one")
        on_expression = self._statement_sql(statement_tokens[on_idx + 1 :])
        join_keys = self._extract_table_block_join_keys(
            on_expression=on_expression,
            current_model_name=model_name,
            target_model_name=target_model,
            target_alias=alias,
        )
        if not join_keys:
            raise TableBlockParseError(
                f"Join in model '{model_name}' must compare model columns: {self._statement_sql(statement_tokens)}"
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
        colon_idx = self._find_top_level_token(statement_tokens, TokenType.COLON)
        base_tokens = statement_tokens[:colon_idx] if colon_idx is not None else statement_tokens
        annotation_tokens = statement_tokens[colon_idx + 1 :] if colon_idx is not None else []
        if not base_tokens:
            return None

        dimension_type, granularity = self._parse_table_block_field_annotation(
            model_name, base_tokens, annotation_tokens
        )
        alias_idx = self._find_top_level_token(base_tokens, TokenType.ALIAS)
        if alias_idx is not None:
            if alias_idx == 0 or alias_idx == len(base_tokens) - 1:
                return None
            name_tokens = base_tokens[alias_idx + 1 :]
            if len(name_tokens) != 1:
                return None
            name = self._identifier_from_token(name_tokens[0])
            if not name:
                return None
            return TableBlockFieldDef(
                this=exp.to_identifier(name),
                sql=self._statement_sql(base_tokens[:alias_idx]),
                dimension_type=dimension_type,
                granularity=granularity,
            )

        if len(base_tokens) == 1:
            name = self._identifier_from_token(base_tokens[0])
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

        field_name = self._statement_sql(base_tokens)
        dimension_type = None
        granularity = None
        idx = 0

        while idx < len(annotation_tokens):
            token = annotation_tokens[idx].text.lower()
            normalized_type = self._DIMENSION_TYPE_ALIASES.get(token, token)

            if normalized_type in self._DIMENSION_TYPES:
                if dimension_type and dimension_type != normalized_type:
                    raise TableBlockParseError(
                        f"Field '{field_name}' in model '{model_name}' has conflicting type annotations"
                    )
                dimension_type = normalized_type
                idx += 1
                continue

            if token in ("grain", "granularity"):
                if idx + 1 >= len(annotation_tokens):
                    raise TableBlockParseError(
                        f"Field '{field_name}' in model '{model_name}' is missing a {token} value"
                    )
                grain = annotation_tokens[idx + 1].text.lower()
                self._validate_time_grain(model_name, field_name, grain)
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

        equalities = self._table_block_join_equalities(parsed)
        if equalities is None:
            return None

        local_keys = []
        target_keys = []
        for equality in equalities:
            left = equality.left
            right = equality.right
            if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                return None

            left_side = self._classify_join_column(left, current_model_name, target_model_name, target_alias)
            right_side = self._classify_join_column(right, current_model_name, target_model_name, target_alias)
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
        expression = self._unwrap_table_block_join_expression(expression)
        if isinstance(expression, exp.EQ):
            return [expression]
        if isinstance(expression, exp.And):
            left = self._table_block_join_equalities(expression.left)
            right = self._table_block_join_equalities(expression.right)
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
                column = self._identifier_from_tokens(current)
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

        column = self._identifier_from_tokens(current)
        if not column:
            return []
        columns.append(column)
        return columns

    def _identifier_from_tokens(self, value_tokens: list[Token]) -> str | None:
        if len(value_tokens) != 1:
            return None
        return self._identifier_from_token(value_tokens[0])

    def _parse_table_block_identifier(self) -> str | None:
        if not self._curr:
            return None
        identifier = self._identifier_from_token(self._curr)
        if identifier:
            self._advance()
        return identifier

    def _identifier_from_token(self, token: Token) -> str | None:
        if token.token_type not in self._IDENTIFIER_TOKEN_TYPES:
            return None
        return token.text

    def _statement_sql(self, statement_tokens: list[Token]) -> str:
        if not statement_tokens:
            return ""
        return self.sql[statement_tokens[0].start : statement_tokens[-1].end + 1].strip()

    def _validate_time_grain(self, model_name: str, field_name: str, grain: str) -> None:
        if grain not in self._TIME_GRAINS:
            raise TableBlockParseError(f"Field '{field_name}' in model '{model_name}' uses invalid grain '{grain}'")

    def _parse_property(self) -> exp.Expression | None:
        """Parse property assignment: name value or name 'value'."""
        if not self._match_texts(self._get_property_names()):
            return None

        key = self._prev.text.lower()

        # Collect tokens until comma or closing paren, respecting parentheses depth
        depth = 0
        value_parts = []

        while self._curr:
            if self._curr.token_type in (
                tokens.TokenType.L_PAREN,
                tokens.TokenType.L_BRACKET,
                tokens.TokenType.L_BRACE,
            ):
                depth += 1
                # Don't add space before opening paren if last token was identifier/function name
                if value_parts and value_parts[-1] not in ("(", ",", "="):
                    value_parts.append(self._curr.text)
                else:
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
                # Preserve string quotes
                if value_parts and value_parts[-1] not in ("(", ",", "=", " "):
                    value_parts.append(" ")
                value_parts.append(f"'{self._curr.text}'")
                self._advance()
            else:
                # Add space before token if needed
                curr_text = self._curr.text
                needs_space_before = value_parts and value_parts[-1] not in ("(", ",", " ")
                needs_space_after_prev = value_parts and value_parts[-1] in (" ",)  # Space already added

                if needs_space_before and not needs_space_after_prev:
                    if curr_text not in (")", ","):
                        value_parts.append(" ")

                value_parts.append(curr_text)

                # Add space after =
                if curr_text == "=":
                    value_parts.append(" ")

                self._advance()

        value = "".join(value_parts).strip()

        if not value:
            return None

        return PropertyEQ(this=exp.Identifier(this=key), expression=exp.Literal.string(value))

    @staticmethod
    def _get_property_names() -> set[str]:
        """Derive property names from all Sidemantic models."""
        from sidemantic.core.dimension import Dimension
        from sidemantic.core.metric import Metric
        from sidemantic.core.model import Model
        from sidemantic.core.parameter import Parameter
        from sidemantic.core.pre_aggregation import PreAggregation
        from sidemantic.core.relationship import Relationship
        from sidemantic.core.segment import Segment

        # Get all field names from all models
        names = set()
        names.update(field.upper() for field in Model.model_fields.keys())
        names.update(field.upper() for field in Dimension.model_fields.keys())
        names.update(field.upper() for field in Relationship.model_fields.keys())
        names.update(field.upper() for field in Metric.model_fields.keys())
        names.update(field.upper() for field in Segment.model_fields.keys())
        names.update(field.upper() for field in Parameter.model_fields.keys())
        names.update(field.upper() for field in PreAggregation.model_fields.keys())

        # Add alias keys (SQL syntax variants)
        names.update(alias.upper() for alias in PROPERTY_ALIASES.keys())

        return names


class SidemanticDialect(Dialect):
    """Sidemantic SQL dialect with METRIC and SEGMENT support."""

    class Parser(SidemanticParser):
        pass


def parse_one(sql: str) -> exp.Expression:
    """Parse SQL with Sidemantic extensions.

    Args:
        sql: SQL string with METRIC/SEGMENT definitions

    Returns:
        Parsed expression tree
    """
    dialect = SidemanticDialect()
    return dialect.parse_one(sql)


def parse(sql: str) -> list[exp.Expression]:
    """Parse multiple SQL statements with Sidemantic extensions.

    Args:
        sql: SQL string with METRIC/SEGMENT definitions

    Returns:
        List of parsed expression trees
    """
    dialect = SidemanticDialect()
    return list(dialect.parse(sql))
