"""SQL query rewriter for semantic layer.

Parses user SQL and rewrites it to use the semantic layer.
"""

from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.aggregation_detection import sql_has_aggregate
from sidemantic.sql.generator import SQLGenerator


@dataclass
class _YardstickAggregateCall:
    placeholder: str
    argument_sql: str
    modifiers: list[str]
    include_visible_default: bool


class QueryRewriter:
    """Rewrites user SQL queries to use the semantic layer."""

    def __init__(self, graph: SemanticGraph, dialect: str = "duckdb"):
        """Initialize query rewriter.

        Args:
            graph: Semantic graph with models and metrics
            dialect: SQL dialect for parsing/generation
        """
        self.graph = graph
        self.dialect = dialect
        self.generator = SQLGenerator(graph, dialect=dialect)

    def rewrite(self, sql: str, strict: bool = True) -> str:
        """Rewrite user SQL to use semantic layer.

        Supports:
        - Direct semantic layer queries: SELECT revenue FROM orders
        - CTEs with semantic queries: WITH agg AS (SELECT revenue FROM orders) SELECT * FROM agg
        - Subqueries: SELECT * FROM (SELECT revenue FROM orders) WHERE revenue > 100

        Args:
            sql: User SQL query
            strict: If True, raise errors for invalid SQL or non-SELECT queries.
                   If False, pass through queries that can't be rewritten.

        Returns:
            Rewritten SQL using semantic layer

        Raises:
            ValueError: If SQL cannot be rewritten (unsupported features, invalid references, etc.)
                       Only raised when strict=True
        """
        sql = sql.strip()

        if self._looks_like_yardstick_query(sql):
            try:
                return self._rewrite_yardstick_query(sql, strict=strict)
            except Exception:
                if strict:
                    raise
                # Keep non-strict passthrough behavior when Yardstick rewrite cannot be applied safely.
                return sql

        # Handle multiple statements (some PostgreSQL clients send these)
        if ";" in sql:
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            if len(statements) > 1:
                if strict:
                    raise ValueError("Multiple statements are not supported")
                # In non-strict mode, pass through
                return sql

        # Parse SQL
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        except Exception as e:
            if strict:
                raise ValueError(f"Failed to parse SQL: {e}")
            # In non-strict mode, pass through unparseable SQL (e.g., SHOW, SET commands)
            return sql

        if not isinstance(parsed, exp.Select):
            if strict:
                raise ValueError("Only SELECT queries are supported")
            # In non-strict mode, pass through non-SELECT queries
            return sql

        if self._contains_implicit_yardstick_measure_query(parsed):
            try:
                return self._rewrite_yardstick_query(sql, strict=strict, allow_plain_measures=True)
            except Exception:
                if strict:
                    raise
                return sql

        # Projection-only SQL (no root FROM/CTE) should pass through unless Yardstick paths above matched.
        if parsed.args.get("from") is None and parsed.args.get("with") is None:
            if any(isinstance(expr, exp.Star) for expr in parsed.expressions):
                if strict:
                    raise ValueError("SELECT * requires a FROM clause with a single table")
                return sql
            return sql

        # Check if this is a CTE-based query or has subqueries
        has_ctes = parsed.args.get("with") is not None
        has_subquery_in_from = self._has_subquery_in_from(parsed)

        if has_ctes or has_subquery_in_from:
            # Handle CTEs and subqueries
            return self._rewrite_with_ctes_or_subqueries(parsed)

        if not self._references_semantic_model(parsed):
            return sql

        # Otherwise, treat as simple semantic layer query
        return self._rewrite_simple_query(parsed)

    def _looks_like_yardstick_query(self, sql: str) -> bool:
        """Return True if query appears to use Yardstick query syntax."""
        try:
            tokens = sqlglot.tokenize(sql, read=self.dialect)
        except Exception:
            return False

        if not tokens:
            return False

        if tokens[0].text.upper() == "SEMANTIC":
            return True

        if self._contains_yardstick_curly_measure_reference(sql, tokens):
            return True

        for i in range(len(tokens) - 1):
            if tokens[i].text.upper() == "AGGREGATE" and tokens[i + 1].token_type == TokenType.L_PAREN:
                return True
            # Support Yardstick `measure AT (...)` syntax without AGGREGATE wrapper.
            if tokens[i].token_type == TokenType.VAR and tokens[i + 1].text.upper() == "AT":
                if i + 2 < len(tokens) and tokens[i + 2].token_type == TokenType.L_PAREN:
                    return True
            if (
                i + 3 < len(tokens)
                and tokens[i].token_type == TokenType.VAR
                and tokens[i + 1].token_type == TokenType.DOT
                and tokens[i + 2].token_type == TokenType.VAR
                and tokens[i + 3].text.upper() == "AT"
            ):
                if i + 4 < len(tokens) and tokens[i + 4].token_type == TokenType.L_PAREN:
                    return True

        return False

    def _is_yardstick_identifier_token(self, token) -> bool:
        return token.token_type in {TokenType.VAR, TokenType.IDENTIFIER, TokenType.SCHEMA}

    def _is_yardstick_curly_measure_inner(self, inner_tokens: list) -> bool:
        if not inner_tokens:
            return False

        expect_identifier = True
        for token in inner_tokens:
            if expect_identifier:
                if not self._is_yardstick_identifier_token(token):
                    return False
            else:
                if token.token_type != TokenType.DOT:
                    return False
            expect_identifier = not expect_identifier
        return not expect_identifier

    def _contains_yardstick_curly_measure_reference(self, sql: str, tokens: list) -> bool:
        i = 0
        while i < len(tokens):
            if tokens[i].token_type != TokenType.L_BRACE:
                i += 1
                continue

            depth = 1
            j = i + 1
            while j < len(tokens):
                if tokens[j].token_type == TokenType.L_BRACE:
                    depth += 1
                elif tokens[j].token_type == TokenType.R_BRACE:
                    depth -= 1
                    if depth == 0:
                        break
                j += 1

            if depth != 0:
                raise ValueError("Invalid Yardstick measure reference: unclosed '{...}'")

            inner_tokens = tokens[i + 1 : j]
            if self._is_yardstick_curly_measure_inner(inner_tokens):
                return True
            i = j + 1

        return False

    def _expand_yardstick_curly_measure_references(self, sql: str) -> str:
        """Expand Yardstick `{measure}` shorthand into plain measure references."""
        tokens = sqlglot.tokenize(sql, read=self.dialect)
        rewritten_parts: list[str] = []
        cursor = 0
        i = 0

        while i < len(tokens):
            if tokens[i].token_type != TokenType.L_BRACE:
                i += 1
                continue

            depth = 1
            j = i + 1
            while j < len(tokens):
                if tokens[j].token_type == TokenType.L_BRACE:
                    depth += 1
                elif tokens[j].token_type == TokenType.R_BRACE:
                    depth -= 1
                    if depth == 0:
                        break
                j += 1

            if depth != 0:
                raise ValueError("Invalid Yardstick measure reference: unclosed '{...}'")

            inner_tokens = tokens[i + 1 : j]
            if self._is_yardstick_curly_measure_inner(inner_tokens):
                rewritten_parts.append(sql[cursor : tokens[i].start])
                inner_start = inner_tokens[0].start
                inner_end = inner_tokens[-1].end + 1
                rewritten_parts.append(sql[inner_start:inner_end].strip())
                cursor = tokens[j].end + 1

            i = j + 1

        rewritten_parts.append(sql[cursor:])
        return "".join(rewritten_parts)

    def _contains_implicit_yardstick_measure_query(self, parsed: exp.Select) -> bool:
        """Return True when query references plain measure columns in Yardstick contexts."""
        for select_scope in parsed.find_all(exp.Select):
            source_models = self._extract_source_models_from_select(select_scope)
            if not source_models:
                continue
            if not any(self._is_yardstick_model(model_name) for model_name in source_models.values()):
                continue

            single_model_scope = len(source_models) == 1 and self._has_single_source_relation(select_scope)
            default_alias = next(iter(source_models)) if single_model_scope else None
            placeholder_names: set[str] = set()

            candidate_expressions: list[exp.Expression] = list(select_scope.expressions)
            having_clause = select_scope.args.get("having")
            if having_clause:
                candidate_expressions.append(having_clause.this)
            order_clause = select_scope.args.get("order")
            if order_clause:
                candidate_expressions.extend(order_expr.this for order_expr in order_clause.expressions)

            for candidate_expr in candidate_expressions:
                for column in candidate_expr.find_all(exp.Column):
                    if self._resolve_implicit_yardstick_measure_reference(
                        column,
                        source_models=source_models,
                        default_alias=default_alias,
                        single_model_scope=single_model_scope,
                        placeholder_names=placeholder_names,
                    ):
                        return True

        return False

    def _is_yardstick_model(self, model_name: str) -> bool:
        model = self.graph.get_model(model_name)
        metadata = model.metadata or {}
        return isinstance(metadata, dict) and "yardstick" in metadata

    def _rewrite_yardstick_query(self, sql: str, strict: bool = True, allow_plain_measures: bool = False) -> str:
        """Rewrite Yardstick-style SQL (`SEMANTIC`, `AGGREGATE`, `AT`) to plain SQL."""
        sql = self._expand_yardstick_curly_measure_references(sql)
        transformed_sql, calls = self._replace_yardstick_aggregate_calls(sql)

        # SEMANTIC prefix without AGGREGATE: fall back to normal SQL rewrite path.
        if not calls and not allow_plain_measures:
            return self.rewrite(transformed_sql, strict=strict)

        try:
            parsed = sqlglot.parse_one(transformed_sql, dialect=self.dialect)
        except Exception as e:
            raise ValueError(f"Failed to parse Yardstick SQL: {e}") from e

        if not isinstance(parsed, exp.Select):
            raise ValueError("Yardstick rewrite currently supports SELECT queries only")

        call_map = {call.placeholder: call for call in calls}
        placeholder_names = set(call_map)
        rewritten_root: exp.Expression = parsed
        # Rewrite innermost SELECT scopes first so nested Yardstick placeholders are
        # resolved in their own FROM/JOIN context before outer scopes are processed.
        for select_scope in reversed(list(parsed.find_all(exp.Select))):
            rewritten_scope = self._rewrite_yardstick_select_scope(
                select_scope,
                call_map=call_map,
                placeholder_names=placeholder_names,
                allow_plain_measures=allow_plain_measures,
            )
            if rewritten_scope is select_scope:
                continue
            if select_scope.parent:
                select_scope.replace(rewritten_scope)
            else:
                rewritten_root = rewritten_scope

        return rewritten_root.sql(dialect=self.dialect)

    def _rewrite_yardstick_select_scope(
        self,
        select_scope: exp.Select,
        call_map: dict[str, _YardstickAggregateCall],
        placeholder_names: set[str],
        allow_plain_measures: bool = False,
    ) -> exp.Select:
        initial_placeholders = {
            column.name
            for column in self._columns_without_nested_scopes(select_scope)
            if not column.table and column.name in placeholder_names
        }
        if not initial_placeholders and not allow_plain_measures:
            return select_scope

        source_models = self._extract_source_models_from_select(select_scope)
        if not source_models:
            if initial_placeholders:
                raise ValueError("Yardstick query must reference at least one known semantic model in FROM/JOIN")
            return select_scope

        # Only default-qualify unaliased columns when this scope truly has a single source relation.
        single_model_scope = len(source_models) == 1 and self._has_single_source_relation(select_scope)
        default_alias = next(iter(source_models)) if single_model_scope else None

        select_scope = self._inject_implicit_yardstick_measure_calls(
            select_scope=select_scope,
            source_models=source_models,
            default_alias=default_alias,
            single_model_scope=single_model_scope,
            call_map=call_map,
            placeholder_names=placeholder_names,
        )

        scope_placeholders = {
            column.name
            for column in self._columns_without_nested_scopes(select_scope)
            if not column.table and column.name in placeholder_names
        }
        if not scope_placeholders:
            return select_scope

        select_scope = self._expand_yardstick_dimension_references(
            select_scope,
            source_models=source_models,
            default_alias=default_alias,
            placeholder_names=placeholder_names,
        )

        if default_alias and single_model_scope:
            qualified_projections: list[exp.Expression] = []
            for projection in select_scope.expressions:
                expr_obj = projection.this if isinstance(projection, exp.Alias) else projection
                if self._expression_contains_yardstick_placeholder(expr_obj, scope_placeholders):
                    qualified_projections.append(projection)
                    continue

                qualified_expr = self._qualify_unaliased_columns(expr_obj.copy(), default_alias)
                if isinstance(projection, exp.Alias):
                    projection.set("this", qualified_expr)
                    qualified_projections.append(projection)
                else:
                    qualified_projections.append(qualified_expr)

            select_scope.set("expressions", qualified_projections)

        select_scope = self._alias_unaliased_yardstick_dimension_expressions(
            select_scope,
            placeholder_names=scope_placeholders,
        )

        has_metric_projection = False
        projection_is_metric: list[bool] = []
        projection_is_literal: list[bool] = []
        for projection in select_scope.expressions:
            expr_obj = projection.this if isinstance(projection, exp.Alias) else projection
            is_metric = self._expression_contains_yardstick_placeholder(expr_obj, scope_placeholders)
            is_literal = isinstance(expr_obj, (exp.Literal, exp.Null, exp.Boolean))
            projection_is_metric.append(is_metric)
            projection_is_literal.append(is_literal)
            has_metric_projection = has_metric_projection or is_metric

        # Yardstick semantics: when AGGREGATE appears and GROUP BY is omitted, group by non-aggregate expressions.
        if has_metric_projection and select_scope.args.get("group") is None:
            group_exprs = []
            for projection, is_metric, is_literal in zip(
                select_scope.expressions, projection_is_metric, projection_is_literal, strict=False
            ):
                if is_metric:
                    continue
                expr_obj = projection.this if isinstance(projection, exp.Alias) else projection
                if is_literal:
                    continue
                group_exprs.append(expr_obj.copy())
            if group_exprs:
                select_scope.set("group", exp.Group(expressions=group_exprs))
            elif all(
                is_metric or is_literal for is_metric, is_literal in zip(projection_is_metric, projection_is_literal)
            ):
                # Scalar aggregate projections should collapse to one row.
                select_scope.set("distinct", exp.Distinct())

        group_clause = select_scope.args.get("group")
        if group_clause:
            self._resolve_yardstick_group_ordinals(group_clause, select_scope.expressions)
        if group_clause and default_alias and single_model_scope:
            group_clause.set(
                "expressions",
                [
                    self._qualify_unaliased_columns(group_expr.copy(), default_alias)
                    for group_expr in group_clause.expressions
                ],
            )
            for group_key in ("rollup", "cube", "grouping_sets"):
                grouped_exprs = group_clause.args.get(group_key)
                if grouped_exprs:
                    group_clause.set(
                        group_key,
                        [
                            self._qualify_unaliased_columns(group_expr.copy(), default_alias)
                            for group_expr in grouped_exprs
                        ],
                    )
        group_expressions = self._collect_yardstick_group_expressions(group_clause) if group_clause else []
        outer_where = select_scope.args.get("where")
        projection_aliases: dict[str, str] = {}
        for projection in select_scope.expressions:
            if not isinstance(projection, exp.Alias):
                continue
            if self._expression_contains_yardstick_placeholder(projection.this, scope_placeholders):
                continue
            signature = self._expr_signature_without_tables(projection.this)
            projection_aliases[signature] = projection.alias

        replacement_sql: dict[str, str] = {}
        for placeholder in scope_placeholders:
            replacement_sql[placeholder] = self._build_yardstick_call_sql(
                call=call_map[placeholder],
                source_models=source_models,
                group_expressions=group_expressions,
                projection_aliases=projection_aliases,
                outer_where=outer_where,
                default_alias=default_alias,
                single_model_scope=single_model_scope,
            )

        replacement_expr_cache = {
            key: sqlglot.parse_one(value, dialect=self.dialect) for key, value in replacement_sql.items()
        }

        def replace_placeholder(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column) and not node.table and node.name in replacement_expr_cache:
                return replacement_expr_cache[node.name].copy()
            return node

        rewritten = select_scope.transform(replace_placeholder)
        return self._rewrite_source_model_relations(rewritten)

    def _collect_yardstick_group_expressions(self, group_clause: exp.Group) -> list[exp.Expression]:
        expressions: list[exp.Expression] = []
        expressions.extend(group_clause.expressions)
        for group_key in ("rollup", "cube", "grouping_sets"):
            grouped_exprs = group_clause.args.get(group_key)
            if grouped_exprs:
                expressions.extend(grouped_exprs)
        return expressions

    def _resolve_yardstick_group_ordinals(self, group_clause: exp.Group, projections: list[exp.Expression]) -> None:
        resolved: list[exp.Expression] = []
        for group_expr in group_clause.expressions:
            if isinstance(group_expr, exp.Literal) and not group_expr.is_string:
                try:
                    ordinal = int(group_expr.this)
                except ValueError:
                    resolved.append(group_expr)
                    continue

                if 1 <= ordinal <= len(projections):
                    projection = projections[ordinal - 1]
                    projection_expr = projection.this if isinstance(projection, exp.Alias) else projection
                    resolved.append(projection_expr.copy())
                    continue
            resolved.append(group_expr)
        group_clause.set("expressions", resolved)

    def _alias_unaliased_yardstick_dimension_expressions(
        self,
        select_scope: exp.Select,
        placeholder_names: set[str],
    ) -> exp.Select:
        """Alias complex unaliased dimension expressions for stable correlation references."""
        existing_aliases = {
            projection.alias
            for projection in select_scope.expressions
            if isinstance(projection, exp.Alias) and projection.alias
        }
        alias_index = 0
        rewritten_projections: list[exp.Expression] = []

        for projection in select_scope.expressions:
            if isinstance(projection, exp.Alias):
                rewritten_projections.append(projection)
                continue

            if isinstance(projection, (exp.Column, exp.Literal, exp.Null, exp.Boolean, exp.Star)):
                rewritten_projections.append(projection)
                continue

            if self._expression_contains_yardstick_placeholder(projection, placeholder_names):
                rewritten_projections.append(projection)
                continue

            if not any(isinstance(column, exp.Column) for column in projection.find_all(exp.Column)):
                rewritten_projections.append(projection)
                continue

            while True:
                alias_name = f"__ysdim_{alias_index}"
                alias_index += 1
                if alias_name not in existing_aliases:
                    existing_aliases.add(alias_name)
                    break

            rewritten_projections.append(exp.alias_(projection.copy(), alias_name, quoted=False))

        select_scope.set("expressions", rewritten_projections)
        return select_scope

    def _replace_yardstick_aggregate_calls(self, sql: str) -> tuple[str, list[_YardstickAggregateCall]]:
        """Strip optional SEMANTIC prefix and replace AGGREGATE...AT chains with placeholders."""
        tokens = sqlglot.tokenize(sql, read=self.dialect)
        calls: list[_YardstickAggregateCall] = []
        segments: list[str] = []
        cursor = 0
        i = 0
        has_semantic_prefix = False
        has_any_at_syntax = False
        plain_aggregate_calls_without_at = 0

        if tokens and tokens[0].text.upper() == "SEMANTIC":
            cursor = tokens[0].end + 1
            while cursor < len(sql) and sql[cursor].isspace():
                cursor += 1
            i = 1
            has_semantic_prefix = True

        def parse_at_chain(start_idx: int, default_end_idx: int) -> tuple[list[str], int]:
            modifiers: list[str] = []
            end_idx = default_end_idx
            k = start_idx
            while (
                k + 1 < len(tokens) and tokens[k].text.upper() == "AT" and tokens[k + 1].token_type == TokenType.L_PAREN
            ):
                m_open = k + 1
                depth = 0
                m_close = m_open
                while m_close < len(tokens):
                    if tokens[m_close].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[m_close].token_type == TokenType.R_PAREN:
                        depth -= 1
                        if depth == 0:
                            break
                    m_close += 1
                if m_close >= len(tokens):
                    raise ValueError("Invalid AT (...) modifier: unclosed parenthesis")

                mod_start = tokens[m_open].end + 1
                mod_end = tokens[m_close].start
                modifiers.append(sql[mod_start:mod_end].strip())
                end_idx = m_close
                k = m_close + 1
            return modifiers, end_idx

        while i < len(tokens):
            token = tokens[i]
            if (
                token.text.upper() == "AGGREGATE"
                and i + 1 < len(tokens)
                and tokens[i + 1].token_type == TokenType.L_PAREN
            ):
                func_start = token.start
                if (
                    i >= 2
                    and tokens[i - 1].token_type == TokenType.DOT
                    and tokens[i - 2].token_type
                    in {
                        TokenType.VAR,
                        TokenType.SCHEMA,
                    }
                ):
                    # Support qualified function syntax like `schema.AGGREGATE(...)`.
                    # The qualifier is syntactic noise for Yardstick resolution.
                    func_start = tokens[i - 2].start
                j = i + 1
                depth = 0
                while j < len(tokens):
                    if tokens[j].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[j].token_type == TokenType.R_PAREN:
                        depth -= 1
                        if depth == 0:
                            break
                    j += 1
                if j >= len(tokens):
                    raise ValueError("Invalid AGGREGATE() call: unclosed parenthesis")

                arg_start = tokens[i + 1].end + 1
                arg_end = tokens[j].start
                argument_sql = sql[arg_start:arg_end].strip()

                modifiers, end_idx = parse_at_chain(j + 1, j)

                if modifiers:
                    has_any_at_syntax = True
                else:
                    plain_aggregate_calls_without_at += 1

                placeholder = f"__ysagg_{len(calls)}"
                calls.append(
                    _YardstickAggregateCall(
                        placeholder=placeholder,
                        argument_sql=argument_sql,
                        modifiers=modifiers,
                        include_visible_default=True,
                    )
                )

                segments.append(sql[cursor:func_start])
                segments.append(placeholder)
                cursor = tokens[end_idx].end + 1
                i = end_idx + 1
                continue

            # Support Yardstick's `measure AT (...)` syntax without AGGREGATE wrapper.
            # Examples: `avg_revenue AT (VISIBLE)`, `o.avg_revenue AT (WHERE ...)`.
            if token.token_type == TokenType.VAR:
                measure_start = None
                measure_end = None
                at_index = None
                if (
                    i + 2 < len(tokens)
                    and tokens[i + 1].text.upper() == "AT"
                    and tokens[i + 2].token_type == TokenType.L_PAREN
                ):
                    measure_start = i
                    measure_end = i
                    at_index = i + 1
                elif (
                    i + 4 < len(tokens)
                    and tokens[i + 1].token_type == TokenType.DOT
                    and tokens[i + 2].token_type == TokenType.VAR
                    and tokens[i + 3].text.upper() == "AT"
                    and tokens[i + 4].token_type == TokenType.L_PAREN
                ):
                    measure_start = i
                    measure_end = i + 2
                    at_index = i + 3

                if measure_start is not None and measure_end is not None and at_index is not None:
                    argument_sql = sql[tokens[measure_start].start : tokens[measure_end].end + 1].strip()
                    modifiers, end_idx = parse_at_chain(at_index, measure_end)
                    if modifiers:
                        has_any_at_syntax = True
                        placeholder = f"__ysagg_{len(calls)}"
                        calls.append(
                            _YardstickAggregateCall(
                                placeholder=placeholder,
                                argument_sql=argument_sql,
                                modifiers=modifiers,
                                include_visible_default=False,
                            )
                        )

                        segments.append(sql[cursor : tokens[measure_start].start])
                        segments.append(placeholder)
                        cursor = tokens[end_idx].end + 1
                        i = end_idx + 1
                        continue

            i += 1

        if plain_aggregate_calls_without_at and not has_semantic_prefix and not has_any_at_syntax:
            raise ValueError("AGGREGATE(...) without AT (...) requires the SEMANTIC prefix")

        segments.append(sql[cursor:])
        return "".join(segments), calls

    def _extract_source_models_from_select(self, select: exp.Select) -> dict[str, str]:
        """Map SQL source aliases to semantic model names."""
        alias_to_model: dict[str, str] = {}

        def add_table(table_expr: exp.Expression | None) -> None:
            if not isinstance(table_expr, exp.Table):
                return
            model_name = table_expr.name
            if model_name not in self.graph.models:
                return
            alias = table_expr.alias_or_name
            alias_to_model[alias] = model_name

        from_clause = select.args.get("from")
        if from_clause:
            add_table(from_clause.this)

        for join in select.args.get("joins") or []:
            add_table(join.this)

        return alias_to_model

    def _has_single_source_relation(self, select: exp.Select) -> bool:
        """Return True only when SELECT scope has exactly one FROM relation and no JOINs."""
        from_clause = select.args.get("from")
        if not from_clause or from_clause.this is None:
            return False
        return len(select.args.get("joins") or []) == 0

    def _parse_relation_factor(self, relation_sql: str) -> exp.Expression:
        probe = sqlglot.parse_one(f"SELECT 1 FROM {relation_sql}", dialect=self.dialect)
        from_clause = probe.args.get("from")
        if not from_clause:
            raise ValueError(f"Failed to parse relation: {relation_sql}")
        return from_clause.this

    def _rewrite_source_model_relations(self, select: exp.Select) -> exp.Select:
        """Replace semantic model names in FROM/JOIN with their physical source relation."""

        def replace_table(table_expr: exp.Expression | None) -> exp.Expression | None:
            if not isinstance(table_expr, exp.Table):
                return table_expr

            model_name = table_expr.name
            if model_name not in self.graph.models:
                return table_expr

            model = self.graph.get_model(model_name)
            alias = table_expr.alias_or_name
            if model.sql:
                return self._parse_relation_factor(f"({model.sql}) AS {alias}")
            if model.table:
                return self._parse_relation_factor(f"{model.table} AS {alias}")
            return self._parse_relation_factor(f"{model_name} AS {alias}")

        from_clause = select.args.get("from")
        if from_clause:
            from_clause.set("this", replace_table(from_clause.this))

        for join in select.args.get("joins") or []:
            join.set("this", replace_table(join.this))

        return select

    def _expression_contains_yardstick_placeholder(
        self, expression: exp.Expression, placeholder_names: set[str]
    ) -> bool:
        for column in self._columns_without_nested_scopes(expression):
            if not column.table and column.name in placeholder_names:
                return True
        return False

    def _columns_without_nested_scopes(self, expression: exp.Expression) -> list[exp.Column]:
        columns: list[exp.Column] = []

        def visit(node: exp.Expression, is_root: bool = False) -> None:
            if isinstance(node, exp.Column):
                columns.append(node)
                return

            if not is_root and isinstance(node, (exp.Select, exp.Subquery)):
                return

            for arg in node.args.values():
                if isinstance(arg, exp.Expression):
                    visit(arg)
                elif isinstance(arg, list):
                    for item in arg:
                        if isinstance(item, exp.Expression):
                            visit(item)

        visit(expression, is_root=True)
        return columns

    def _qualify_unaliased_columns(self, expression: exp.Expression, table_alias: str) -> exp.Expression:
        qualified = expression.copy()
        for column in qualified.find_all(exp.Column):
            if not column.table:
                column.set("table", exp.to_identifier(table_alias))
        return qualified

    def _rewrite_tables(
        self,
        expression: exp.Expression,
        table_mapping: dict[str, str],
        default_table: str | None = None,
    ) -> exp.Expression:
        rewritten = expression.copy()
        for column in rewritten.find_all(exp.Column):
            if column.table:
                if column.table in table_mapping:
                    column.set("table", exp.to_identifier(table_mapping[column.table]))
            elif default_table:
                column.set("table", exp.to_identifier(default_table))
        return rewritten

    def _expr_signature_without_tables(self, expression: exp.Expression) -> str:
        normalized = expression.copy()
        for column in normalized.find_all(exp.Column):
            column.set("table", None)
        return normalized.sql(dialect=self.dialect).lower()

    def _resolve_implicit_yardstick_measure_reference(
        self,
        column: exp.Column,
        source_models: dict[str, str],
        default_alias: str | None,
        single_model_scope: bool,
        placeholder_names: set[str],
    ) -> str | None:
        if not column.name or column.name in placeholder_names:
            return None

        if column.table:
            if column.table not in source_models:
                return None
            model_name = source_models[column.table]
            if not self._is_yardstick_model(model_name):
                return None
            model = self.graph.get_model(model_name)
            if not model.get_metric(column.name):
                return None
            return f"{column.table}.{column.name}"

        if default_alias and single_model_scope:
            model_name = source_models[default_alias]
            if not self._is_yardstick_model(model_name):
                return None
            model = self.graph.get_model(model_name)
            if model.get_metric(column.name):
                return column.name
            return None

        candidate_aliases = [
            alias
            for alias, model_name in source_models.items()
            if self._is_yardstick_model(model_name)
            and self.graph.get_model(model_name).get_metric(column.name) is not None
        ]
        if len(candidate_aliases) != 1:
            return None
        return f"{candidate_aliases[0]}.{column.name}"

    def _inject_implicit_yardstick_measure_calls(
        self,
        select_scope: exp.Select,
        source_models: dict[str, str],
        default_alias: str | None,
        single_model_scope: bool,
        call_map: dict[str, _YardstickAggregateCall],
        placeholder_names: set[str],
    ) -> exp.Select:
        rewritten = select_scope.copy()

        def next_placeholder() -> str:
            idx = len(call_map)
            while True:
                placeholder = f"__ysagg_{idx}"
                if placeholder not in placeholder_names:
                    return placeholder
                idx += 1

        def register_implicit_measure_call(argument_sql: str) -> str:
            placeholder = next_placeholder()
            call_map[placeholder] = _YardstickAggregateCall(
                placeholder=placeholder,
                argument_sql=argument_sql,
                modifiers=[],
                include_visible_default=False,
            )
            placeholder_names.add(placeholder)
            return placeholder

        def maybe_replace_column(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column):
                return node

            argument_sql = self._resolve_implicit_yardstick_measure_reference(
                node,
                source_models=source_models,
                default_alias=default_alias,
                single_model_scope=single_model_scope,
                placeholder_names=placeholder_names,
            )
            if not argument_sql:
                return node

            placeholder = register_implicit_measure_call(argument_sql)
            return exp.column(placeholder)

        rewritten_projections: list[exp.Expression] = []
        for projection in rewritten.expressions:
            if isinstance(projection, exp.Alias):
                projection.set("this", projection.this.transform(maybe_replace_column))
                rewritten_projections.append(projection)
                continue

            if isinstance(projection, exp.Column):
                argument_sql = self._resolve_implicit_yardstick_measure_reference(
                    projection,
                    source_models=source_models,
                    default_alias=default_alias,
                    single_model_scope=single_model_scope,
                    placeholder_names=placeholder_names,
                )
                if argument_sql:
                    placeholder = register_implicit_measure_call(argument_sql)
                    rewritten_projections.append(exp.alias_(exp.column(placeholder), projection.name, quoted=False))
                    continue

            rewritten_projections.append(projection.transform(maybe_replace_column))
        rewritten.set("expressions", rewritten_projections)

        having_clause = rewritten.args.get("having")
        if having_clause:
            having_clause.set("this", having_clause.this.transform(maybe_replace_column))

        order_clause = rewritten.args.get("order")
        if order_clause:
            for order_expr in order_clause.expressions:
                order_expr.set("this", order_expr.this.transform(maybe_replace_column))

        return rewritten

    def _resolve_yardstick_dimension_expression(
        self,
        column: exp.Column,
        source_models: dict[str, str],
        default_alias: str | None,
        placeholder_names: set[str],
    ) -> tuple[exp.Expression, str] | None:
        if not column.name or column.name in placeholder_names:
            return None

        alias_candidates: list[str] = []
        if column.table:
            if column.table not in source_models:
                return None
            alias_candidates = [column.table]
        elif default_alias:
            alias_candidates = [default_alias]
        else:
            alias_candidates = [
                alias
                for alias, model_name in source_models.items()
                if self.graph.get_model(model_name).get_dimension(column.name) is not None
            ]
            if len(alias_candidates) != 1:
                return None

        table_alias = alias_candidates[0]
        model_name = source_models[table_alias]
        model = self.graph.get_model(model_name)
        dimension = model.get_dimension(column.name)
        if not dimension:
            return None

        dimension_sql = dimension.sql_expr.strip()
        if dimension_sql.lower() == column.name.lower():
            return None

        expr = sqlglot.parse_one(dimension_sql, dialect=self.dialect)
        expr = self._rewrite_tables(
            expr,
            table_mapping={model_name: table_alias},
            default_table=table_alias,
        )
        return expr, column.name

    def _expand_yardstick_dimension_references(
        self,
        select_scope: exp.Select,
        source_models: dict[str, str],
        default_alias: str | None,
        placeholder_names: set[str],
    ) -> exp.Select:
        def replace_columns(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column):
                return node

            resolved = self._resolve_yardstick_dimension_expression(
                node,
                source_models=source_models,
                default_alias=default_alias,
                placeholder_names=placeholder_names,
            )
            if not resolved:
                return node
            replacement, _ = resolved
            return replacement

        rewritten = select_scope.copy()
        rewritten_projections: list[exp.Expression] = []
        for projection in rewritten.expressions:
            if isinstance(projection, exp.Alias):
                projection.set("this", projection.this.transform(replace_columns))
                rewritten_projections.append(projection)
                continue

            if isinstance(projection, exp.Column):
                resolved = self._resolve_yardstick_dimension_expression(
                    projection,
                    source_models=source_models,
                    default_alias=default_alias,
                    placeholder_names=placeholder_names,
                )
                if resolved:
                    replacement, output_name = resolved
                    rewritten_projections.append(exp.alias_(replacement, output_name))
                    continue

            rewritten_projections.append(projection.transform(replace_columns))

        rewritten.set("expressions", rewritten_projections)

        group_clause = rewritten.args.get("group")
        if group_clause:
            group_clause.set("expressions", [expr.transform(replace_columns) for expr in group_clause.expressions])

        where_clause = rewritten.args.get("where")
        if where_clause:
            where_clause.set("this", where_clause.this.transform(replace_columns))

        having_clause = rewritten.args.get("having")
        if having_clause:
            having_clause.set("this", having_clause.this.transform(replace_columns))

        order_clause = rewritten.args.get("order")
        if order_clause:
            for order_expr in order_clause.expressions:
                order_expr.set("this", order_expr.this.transform(replace_columns))

        return rewritten

    def _resolve_yardstick_measure_call(self, argument_sql: str, source_models: dict[str, str]) -> tuple[str, str, str]:
        """Resolve AGGREGATE(argument) to (model_alias, model_name, measure_name)."""
        try:
            arg_expr = sqlglot.parse_one(argument_sql, dialect=self.dialect)
        except Exception as e:
            raise ValueError(f"Invalid AGGREGATE argument '{argument_sql}': {e}") from e

        if not isinstance(arg_expr, exp.Column):
            raise ValueError(f"AGGREGATE argument must be a metric/measure reference, got: {argument_sql}")

        measure_name = arg_expr.name
        source_alias = arg_expr.table

        if source_alias:
            if source_alias in source_models:
                model_name = source_models[source_alias]
                model_alias = source_alias
            elif source_alias in self.graph.models:
                model_name = source_alias
                aliases = [alias for alias, model in source_models.items() if model == model_name]
                if not aliases:
                    raise ValueError(f"Model '{model_name}' is not present in query FROM/JOIN")
                model_alias = aliases[0]
            else:
                raise ValueError(f"Unknown table/model alias '{source_alias}' in AGGREGATE({argument_sql})")

            model = self.graph.get_model(model_name)
            if not model.get_metric(measure_name):
                raise ValueError(f"Measure '{measure_name}' not found in model '{model_name}'")
            return model_alias, model_name, measure_name

        candidates: list[tuple[str, str]] = []
        for alias, model_name in source_models.items():
            model = self.graph.get_model(model_name)
            if model.get_metric(measure_name):
                candidates.append((alias, model_name))

        if not candidates:
            raise ValueError(f"Could not resolve AGGREGATE({measure_name}) to any model in query scope")
        if len(candidates) > 1:
            candidates_str = ", ".join(f"{alias}.{measure_name}" for alias, _ in candidates)
            raise ValueError(f"Ambiguous AGGREGATE({measure_name}); use a qualifier: {candidates_str}")

        model_alias, model_name = candidates[0]
        return model_alias, model_name, measure_name

    def _build_yardstick_call_sql(
        self,
        call: _YardstickAggregateCall,
        source_models: dict[str, str],
        group_expressions: list[exp.Expression],
        projection_aliases: dict[str, str],
        outer_where: exp.Where | None,
        default_alias: str | None,
        single_model_scope: bool,
    ) -> str:
        model_alias, model_name, measure_name = self._resolve_yardstick_measure_call(call.argument_sql, source_models)
        return self._build_yardstick_measure_sql(
            model_alias=model_alias,
            model_name=model_name,
            measure_name=measure_name,
            modifiers=call.modifiers,
            include_visible_default=call.include_visible_default,
            source_models=source_models,
            group_expressions=group_expressions,
            projection_aliases=projection_aliases,
            outer_where=outer_where,
            default_alias=default_alias,
            single_model_scope=single_model_scope,
            visiting=set(),
        )

    def _build_yardstick_measure_sql(
        self,
        model_alias: str,
        model_name: str,
        measure_name: str,
        modifiers: list[str],
        include_visible_default: bool,
        source_models: dict[str, str],
        group_expressions: list[exp.Expression],
        projection_aliases: dict[str, str],
        outer_where: exp.Where | None,
        default_alias: str | None,
        single_model_scope: bool,
        visiting: set[tuple[str, str]],
    ) -> str:
        model = self.graph.get_model(model_name)
        measure = model.get_metric(measure_name)
        if not measure:
            raise ValueError(f"Measure '{measure_name}' not found in model '{model_name}'")

        visit_key = (model_name, measure_name)
        if visit_key in visiting:
            raise ValueError(f"Circular derived measure reference detected for '{model_name}.{measure_name}'")

        is_derived_formula = measure.type == "derived" or (
            not measure.type and not measure.agg and measure.sql and not sql_has_aggregate(measure.sql, self.dialect)
        )
        if is_derived_formula and measure.sql:
            visiting.add(visit_key)
            formula_expr = sqlglot.parse_one(measure.sql, dialect=self.dialect)

            def replace_measure_refs(node: exp.Expression) -> exp.Expression:
                if not isinstance(node, exp.Column):
                    return node
                if node.table and node.table not in {model_alias, model_name}:
                    return node
                ref_name = node.name
                if ref_name == measure_name:
                    return node
                if not model.get_metric(ref_name):
                    return node
                dep_sql = self._build_yardstick_measure_sql(
                    model_alias=model_alias,
                    model_name=model_name,
                    measure_name=ref_name,
                    modifiers=modifiers,
                    include_visible_default=include_visible_default,
                    source_models=source_models,
                    group_expressions=group_expressions,
                    projection_aliases=projection_aliases,
                    outer_where=outer_where,
                    default_alias=default_alias,
                    single_model_scope=single_model_scope,
                    visiting=visiting.copy(),
                )
                return sqlglot.parse_one(dep_sql, dialect=self.dialect)

            rewritten_formula = formula_expr.transform(replace_measure_refs)
            return f"({rewritten_formula.sql(dialect=self.dialect)})"

        agg_expr = self._build_yardstick_aggregation_expr(
            measure=measure,
            model_alias=model_alias,
            model_name=model_name,
        )

        context_dimensions = self._build_yardstick_context_dimensions(
            group_expressions=group_expressions,
            model_alias=model_alias,
            model_name=model_name,
            default_alias=default_alias,
            source_models=source_models,
            projection_aliases=projection_aliases,
            single_model_scope=single_model_scope,
        )
        (
            active_dimensions,
            where_modifier_predicates,
            set_modifier_predicates,
            include_visible,
        ) = self._apply_yardstick_modifiers(
            modifiers=modifiers,
            context_dimensions=context_dimensions,
            model_alias=model_alias,
            model_name=model_name,
            default_alias=default_alias,
            single_model=single_model_scope,
            include_visible_default=include_visible_default,
            fixed_context_signatures=self._extract_fixed_context_signatures_from_where(
                outer_where=outer_where,
                default_alias=default_alias,
                single_model=single_model_scope,
                model_alias=model_alias,
                model_name=model_name,
            ),
        )

        correlation_predicates: list[str] = []
        for dim in active_dimensions:
            correlation_predicates.append(f"({dim['inner_sql']}) IS NOT DISTINCT FROM ({dim['outer_sql']})")

        base_predicates = list(where_modifier_predicates)
        if include_visible and outer_where is not None:
            visible_expr = outer_where.this.copy()
            if default_alias and single_model_scope:
                visible_expr = self._qualify_unaliased_columns(visible_expr, default_alias)
            visible_expr = self._rewrite_tables(
                visible_expr,
                table_mapping={model_alias: "_inner", model_name: "_inner"},
                default_table="_inner" if single_model_scope else None,
            )
            base_predicates.append(visible_expr.sql(dialect=self.dialect))

        for measure_filter in measure.filters or []:
            filter_expr = sqlglot.parse_one(measure_filter, dialect=self.dialect)
            if default_alias and single_model_scope:
                filter_expr = self._qualify_unaliased_columns(filter_expr, default_alias)
            filter_expr = self._rewrite_tables(
                filter_expr,
                table_mapping={model_alias: "_inner", model_name: "_inner"},
                default_table="_inner" if single_model_scope else None,
            )
            base_predicates.append(filter_expr.sql(dialect=self.dialect))

        predicates = list(base_predicates) + list(set_modifier_predicates) + list(correlation_predicates)
        where_clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        source_sql = f"({model.sql})" if model.sql else model.table

        if measure.sql and not measure.agg and self._is_window_measure_expression(measure.sql):
            pre_where_clause = f" WHERE {' AND '.join(base_predicates)}" if base_predicates else ""
            post_predicates = list(set_modifier_predicates) + list(correlation_predicates)
            post_where_clause = f" WHERE {' AND '.join(post_predicates)}" if post_predicates else ""
            window_value_subquery = (
                f"SELECT _inner.*, {agg_expr} AS __ys_window_value FROM {source_sql} AS _inner{pre_where_clause}"
            )
            return (
                "(SELECT CASE "
                "WHEN COUNT(*) = 0 THEN NULL "
                "WHEN COUNT(DISTINCT __ys_window_value) = 1 THEN MIN(__ys_window_value) "
                f"ELSE error('Window measure {measure_name} returned multiple values for the evaluation context') END "
                f"FROM ({window_value_subquery}) AS _inner{post_where_clause})"
            )

        return f"(SELECT {agg_expr} FROM {source_sql} AS _inner{where_clause})"

    def _build_yardstick_aggregation_expr(self, measure, model_alias: str, model_name: str) -> str:
        if measure.agg:
            agg = measure.agg.lower()
            if agg == "count":
                if not measure.sql or measure.sql == "*":
                    return "COUNT(*)"
                count_expr = self._rewrite_yardstick_measure_expression(
                    measure.sql, model_alias=model_alias, model_name=model_name, target_alias="_inner"
                )
                return f"COUNT({count_expr})"

            expr_sql = self._rewrite_yardstick_measure_expression(
                measure.sql_expr, model_alias=model_alias, model_name=model_name, target_alias="_inner"
            )

            if agg == "count_distinct":
                return f"COUNT(DISTINCT {expr_sql})"

            agg_map = {
                "sum": "SUM",
                "avg": "AVG",
                "min": "MIN",
                "max": "MAX",
                "median": "MEDIAN",
                "stddev": "STDDEV",
                "stddev_pop": "STDDEV_POP",
                "variance": "VARIANCE",
                "variance_pop": "VARIANCE_POP",
            }
            if agg not in agg_map:
                raise ValueError(f"Unsupported Yardstick aggregation '{measure.agg}'")
            return f"{agg_map[agg]}({expr_sql})"

        if measure.sql:
            return self._rewrite_yardstick_measure_expression(
                measure.sql, model_alias=model_alias, model_name=model_name, target_alias="_inner"
            )

        raise ValueError(f"Measure '{measure.name}' cannot be aggregated")

    def _rewrite_yardstick_measure_expression(
        self, sql_expr: str, model_alias: str, model_name: str, target_alias: str
    ) -> str:
        parsed = sqlglot.parse_one(sql_expr, dialect=self.dialect)
        parsed = self._rewrite_tables(
            parsed,
            table_mapping={
                model_alias: target_alias,
                model_name: target_alias,
                f"{model_name}_cte": target_alias,
            },
            default_table=target_alias,
        )
        return parsed.sql(dialect=self.dialect)

    def _is_window_measure_expression(self, sql_expr: str) -> bool:
        parsed = sqlglot.parse_one(sql_expr, dialect=self.dialect)
        return any(isinstance(node, exp.Window) for node in parsed.walk())

    def _build_yardstick_context_dimensions(
        self,
        group_expressions: list[exp.Expression],
        model_alias: str,
        model_name: str,
        default_alias: str | None,
        source_models: dict[str, str],
        projection_aliases: dict[str, str],
        single_model_scope: bool,
    ) -> list[dict[str, str]]:
        context_dimensions: list[dict[str, str]] = []
        model = self.graph.get_model(model_name)
        seen_signatures: set[str] = set()

        for raw_group_expr in group_expressions:
            expanded_group_expressions = self._expand_group_expression_for_yardstick_context(raw_group_expr)
            for group_expr in expanded_group_expressions:
                outer_base_expr = group_expr.copy()
                expr_obj = group_expr.copy()
                if default_alias and single_model_scope:
                    expr_obj = self._qualify_unaliased_columns(expr_obj, default_alias)

                columns = list(expr_obj.find_all(exp.Column))
                if not columns:
                    continue

                inner_base_expr = expr_obj.copy()
                tables = {column.table for column in columns if column.table}
                if tables and not tables.issubset({model_alias, model_name}):
                    # Multi-fact join case: if grouped by another alias's same-named dimensions,
                    # map those columns onto this model alias so each measure correlates correctly.
                    if not all(model.get_dimension(column.name) for column in columns):
                        continue
                    for column in inner_base_expr.find_all(exp.Column):
                        column.set("table", exp.to_identifier(model_alias))
                if not tables and not single_model_scope:
                    continue

                outer_expr = self._rewrite_tables(outer_base_expr, table_mapping={model_name: model_alias})
                inner_expr = self._rewrite_tables(
                    inner_base_expr,
                    table_mapping={model_alias: "_inner", model_name: "_inner"},
                    default_table="_inner" if single_model_scope else None,
                )
                signature = self._expr_signature_without_tables(inner_base_expr)
                if signature in seen_signatures:
                    continue
                seen_signatures.add(signature)
                outer_sql = outer_expr.sql(dialect=self.dialect)
                projection_alias = projection_aliases.get(signature)
                unsafe_aliases: set[str] = set()
                for dimension in model.dimensions:
                    dim_expr = dimension.sql_expr
                    try:
                        parsed_dim = sqlglot.parse_one(dim_expr, dialect=self.dialect)
                        if isinstance(parsed_dim, exp.Column) and parsed_dim.name.lower() == dimension.name.lower():
                            unsafe_aliases.add(dimension.name.lower())
                    except Exception:
                        if dim_expr.strip().lower() == dimension.name.lower():
                            unsafe_aliases.add(dimension.name.lower())

                if projection_alias and projection_alias.lower() not in unsafe_aliases:
                    outer_sql = exp.to_identifier(projection_alias).sql(dialect=self.dialect)

                context_dimensions.append(
                    {
                        "signature": signature,
                        "outer_sql": outer_sql,
                        "inner_sql": inner_expr.sql(dialect=self.dialect),
                    }
                )

        return context_dimensions

    def _expand_group_expression_for_yardstick_context(self, group_expr: exp.Expression) -> list[exp.Expression]:
        if isinstance(group_expr, (exp.Rollup, exp.Cube)):
            expanded: list[exp.Expression] = []
            for sub_expr in group_expr.expressions:
                expanded.extend(self._expand_group_expression_for_yardstick_context(sub_expr))
            return expanded

        if isinstance(group_expr, exp.GroupingSets):
            expanded: list[exp.Expression] = []
            for sub_expr in group_expr.expressions:
                expanded.extend(self._expand_group_expression_for_yardstick_context(sub_expr))
            return expanded

        if isinstance(group_expr, exp.Tuple):
            expanded: list[exp.Expression] = []
            for sub_expr in group_expr.expressions:
                expanded.extend(self._expand_group_expression_for_yardstick_context(sub_expr))
            return expanded

        return [group_expr]

    def _split_set_modifier(self, modifier_sql: str) -> tuple[str, str]:
        tokens = sqlglot.tokenize(modifier_sql, read=self.dialect)
        if not tokens or tokens[0].text.upper() != "SET":
            raise ValueError(f"Invalid SET modifier: {modifier_sql}")

        depth = 0
        eq_token = None
        for token in tokens[1:]:
            if token.token_type == TokenType.L_PAREN:
                depth += 1
            elif token.token_type == TokenType.R_PAREN:
                depth -= 1
            elif token.token_type == TokenType.EQ and depth == 0:
                eq_token = token
                break

        if not eq_token:
            raise ValueError(f"SET modifier must contain '=': {modifier_sql}")

        left_sql = modifier_sql[tokens[0].end + 1 : eq_token.start].strip()
        right_sql = modifier_sql[eq_token.end + 1 :].strip()
        return left_sql, right_sql

    def _split_compound_yardstick_modifier(self, modifier_sql: str) -> list[str]:
        tokens = sqlglot.tokenize(modifier_sql, read=self.dialect)
        if not tokens:
            return []

        modifier_heads = {"ALL", "SET", "WHERE", "VISIBLE"}
        split_indexes = [0]
        depth = 0

        for idx, token in enumerate(tokens):
            if token.token_type == TokenType.L_PAREN:
                depth += 1
            elif token.token_type == TokenType.R_PAREN:
                depth -= 1

            if idx == 0 or depth != 0:
                continue

            if token.text.upper() in modifier_heads:
                split_indexes.append(idx)

        parts: list[str] = []
        for i, start_idx in enumerate(split_indexes):
            end_idx = split_indexes[i + 1] - 1 if i + 1 < len(split_indexes) else len(tokens) - 1
            start_pos = tokens[start_idx].start
            end_pos = tokens[end_idx].end + 1
            part = modifier_sql[start_pos:end_pos].strip()
            if part:
                parts.append(part)

        return parts

    def _split_all_modifier_targets(self, modifier_sql: str) -> list[str]:
        """Split `ALL` modifier targets, supporting single-clause multi-dimension syntax."""
        tokens = sqlglot.tokenize(modifier_sql, read=self.dialect)
        if not tokens or tokens[0].text.upper() != "ALL":
            return []
        if len(tokens) == 1:
            return []

        raw_targets = modifier_sql[tokens[1].start :].strip()

        # For complex arithmetic/boolean expressions, keep legacy single-expression behavior.
        unsafe_top_level_ops = {"+", "-", "*", "/", "%", "AND", "OR", "=", "<>", "!=", "<", ">", "<=", ">="}
        depth = 0
        for token in tokens[1:]:
            if token.token_type == TokenType.L_PAREN:
                depth += 1
                continue
            if token.token_type == TokenType.R_PAREN:
                depth -= 1
                continue
            if depth == 0 and token.text.upper() in unsafe_top_level_ops:
                return [raw_targets]

        targets: list[str] = []
        idx = 1
        while idx < len(tokens):
            if tokens[idx].token_type == TokenType.COMMA:
                idx += 1
                continue

            start_idx = idx
            if idx + 1 < len(tokens) and tokens[idx + 1].token_type == TokenType.L_PAREN:
                # Function-style dimension (e.g., MONTH(order_date)).
                depth = 0
                idx += 1
                while idx < len(tokens):
                    if tokens[idx].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[idx].token_type == TokenType.R_PAREN:
                        depth -= 1
                        if depth == 0:
                            idx += 1
                            break
                    idx += 1
                if depth != 0:
                    return [raw_targets]
            else:
                # Identifier-style dimension, optionally qualified (table.column).
                idx += 1
                while idx + 1 < len(tokens) and tokens[idx].token_type == TokenType.DOT:
                    idx += 2

            end_idx = idx - 1
            target_sql = modifier_sql[tokens[start_idx].start : tokens[end_idx].end + 1].strip()
            if target_sql:
                targets.append(target_sql)

            if idx < len(tokens) and tokens[idx].token_type != TokenType.COMMA:
                next_text = tokens[idx].text
                # Allow space-separated target lists when next token looks like an expression start.
                if not (next_text[:1].isalpha() or next_text[:1] in {"_", '"', "`"}):
                    return [raw_targets]

        return targets or [raw_targets]

    def _extract_fixed_context_signatures_from_where(
        self,
        outer_where: exp.Where | None,
        default_alias: str | None,
        single_model: bool,
        model_alias: str,
        model_name: str,
    ) -> set[str]:
        """Extract dimensions fixed to a literal by conjunctive outer WHERE predicates."""
        if outer_where is None:
            return set()

        where_expr = outer_where.this.copy()
        if default_alias and single_model:
            where_expr = self._qualify_unaliased_columns(where_expr, default_alias)
        where_expr = self._rewrite_tables(where_expr, table_mapping={model_name: model_alias})

        signatures: set[str] = set()
        stack: list[exp.Expression] = [where_expr]
        while stack:
            condition = stack.pop()
            if isinstance(condition, exp.And):
                stack.append(condition.this)
                stack.append(condition.expression)
                continue
            if not isinstance(condition, exp.EQ):
                continue
            left = condition.this
            right = condition.expression
            if isinstance(right, exp.Literal):
                signatures.add(self._expr_signature_without_tables(left))
            elif isinstance(left, exp.Literal):
                signatures.add(self._expr_signature_without_tables(right))

        return signatures

    def _rewrite_current_keyword(
        self,
        sql_expr: str,
        context_dimensions: list[dict[str, str]],
        fixed_context_signatures: set[str] | None = None,
    ) -> str:
        """Rewrite CURRENT references to context-aware expressions.

        CURRENT <dim> resolves to <dim> only when that dimension is present in the
        current context. Otherwise it resolves to NULL (ambiguous context).
        """
        tokens = sqlglot.tokenize(sql_expr, read=self.dialect)
        if not tokens:
            return sql_expr

        context_signatures = {dim["signature"] for dim in context_dimensions}
        if fixed_context_signatures:
            context_signatures |= fixed_context_signatures
        parts: list[str] = []
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token.text.upper() != "CURRENT":
                parts.append(token.text)
                i += 1
                continue

            if i + 1 >= len(tokens):
                i += 1
                continue

            start_idx = i + 1
            end_idx = start_idx

            if start_idx + 1 < len(tokens) and tokens[start_idx + 1].token_type == TokenType.L_PAREN:
                depth = 0
                j = start_idx + 1
                while j < len(tokens):
                    if tokens[j].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[j].token_type == TokenType.R_PAREN:
                        depth -= 1
                        if depth == 0:
                            end_idx = j
                            break
                    j += 1
            else:
                end_idx = start_idx
                while end_idx + 2 < len(tokens) and tokens[end_idx + 1].token_type == TokenType.DOT:
                    end_idx += 2

            target_sql = sql_expr[tokens[start_idx].start : tokens[end_idx].end + 1].strip()

            replacement = "NULL"
            try:
                target_expr = sqlglot.parse_one(target_sql, dialect=self.dialect)
                signature = self._expr_signature_without_tables(target_expr)
                if signature in context_signatures:
                    replacement = target_sql
            except Exception:
                replacement = "NULL"

            parts.append(replacement)
            i = end_idx + 1

        return " ".join(parts)

    def _apply_yardstick_modifiers(
        self,
        modifiers: list[str],
        context_dimensions: list[dict[str, str]],
        model_alias: str,
        model_name: str,
        default_alias: str | None,
        single_model: bool,
        include_visible_default: bool,
        fixed_context_signatures: set[str] | None = None,
    ) -> tuple[list[dict[str, str]], list[str], list[str], bool]:
        expanded_modifiers: list[str] = []
        for modifier in modifiers:
            expanded_modifiers.extend(self._split_compound_yardstick_modifier(modifier))

        active_dimensions = list(context_dimensions)
        where_predicates: list[str] = []
        set_predicates: dict[str, str] = {}
        include_visible = include_visible_default
        has_set = False
        has_all_global = False
        removed_signatures: set[str] = set()
        single_where_modifier = False

        for modifier in expanded_modifiers:
            tokens = sqlglot.tokenize(modifier, read=self.dialect)
            if not tokens:
                continue
            if tokens[0].text.upper() == "SET":
                has_set = True
        if has_set:
            include_visible = False
        if len(expanded_modifiers) == 1:
            tokens = sqlglot.tokenize(expanded_modifiers[0], read=self.dialect)
            single_where_modifier = bool(tokens and tokens[0].text.upper() == "WHERE")

        for modifier in reversed(expanded_modifiers):
            tokens = sqlglot.tokenize(modifier, read=self.dialect)
            if not tokens:
                continue

            modifier_type = tokens[0].text.upper()

            if modifier_type == "ALL":
                if len(tokens) == 1:
                    active_dimensions = []
                    set_predicates.clear()
                    where_predicates.clear()
                    include_visible = False
                    has_all_global = True
                    continue

                if has_all_global:
                    continue

                for target_sql in self._split_all_modifier_targets(modifier):
                    target_expr = sqlglot.parse_one(target_sql, dialect=self.dialect)
                    if default_alias and single_model:
                        target_expr = self._qualify_unaliased_columns(target_expr, default_alias)
                    target_signature = self._expr_signature_without_tables(target_expr)
                    active_dimensions = [d for d in active_dimensions if d["signature"] != target_signature]
                    removed_signatures.add(target_signature)
                    set_predicates.pop(target_signature, None)
                continue

            if modifier_type == "WHERE":
                if has_all_global:
                    continue
                where_sql = modifier[tokens[0].end + 1 :].strip()
                where_expr = sqlglot.parse_one(where_sql, dialect=self.dialect)
                # In AT(WHERE ...), unqualified columns belong to the inner evaluation context.
                # Keep explicitly-qualified outer aliases untouched so predicates can correlate
                # (e.g. `prod_name = o.prod_name` from paper listing-style queries).
                where_expr = self._rewrite_tables(
                    where_expr,
                    table_mapping={model_name: "_inner"},
                    default_table="_inner" if single_model else None,
                )
                where_predicates.append(where_expr.sql(dialect=self.dialect))
                # Single WHERE modifier evaluates in a non-correlated context.
                if single_where_modifier:
                    active_dimensions = []
                continue

            if modifier_type == "SET":
                if has_all_global:
                    continue
                try:
                    left_sql, right_sql = self._split_set_modifier(modifier)
                except ValueError:
                    # Support Yardstick predicate-style SET forms like:
                    # AT (SET region IN ('North', 'South'))
                    set_predicate_sql = modifier[tokens[0].end + 1 :].strip()
                    set_predicate_expr = sqlglot.parse_one(set_predicate_sql, dialect=self.dialect)

                    if default_alias and single_model:
                        set_predicate_expr = self._qualify_unaliased_columns(set_predicate_expr, default_alias)

                    target_signatures: list[str] = []
                    if isinstance(set_predicate_expr, exp.In):
                        target_signatures.append(self._expr_signature_without_tables(set_predicate_expr.this))
                    else:
                        raise ValueError(f"Unsupported SET modifier: {modifier}")

                    if any(signature in removed_signatures for signature in target_signatures):
                        continue

                    for signature in target_signatures:
                        active_dimensions = [d for d in active_dimensions if d["signature"] != signature]
                        set_predicates.pop(signature, None)

                    set_inner_predicate = self._rewrite_tables(
                        set_predicate_expr,
                        table_mapping={model_name: "_inner"},
                        default_table="_inner" if single_model else None,
                    )
                    where_predicates.append(set_inner_predicate.sql(dialect=self.dialect))
                    continue

                left_expr = sqlglot.parse_one(left_sql, dialect=self.dialect)
                right_expr = sqlglot.parse_one(
                    self._rewrite_current_keyword(
                        right_sql,
                        context_dimensions,
                        fixed_context_signatures=fixed_context_signatures,
                    ),
                    dialect=self.dialect,
                )

                if default_alias and single_model:
                    left_expr = self._qualify_unaliased_columns(left_expr, default_alias)
                    right_expr = self._qualify_unaliased_columns(right_expr, default_alias)

                left_signature = self._expr_signature_without_tables(left_expr)
                active_dimensions = [d for d in active_dimensions if d["signature"] != left_signature]
                if left_signature in removed_signatures:
                    continue

                left_inner = self._rewrite_tables(
                    left_expr,
                    table_mapping={model_alias: "_inner", model_name: "_inner"},
                    default_table="_inner" if single_model else None,
                )
                right_outer = self._rewrite_tables(
                    right_expr,
                    table_mapping={model_name: model_alias},
                )
                set_predicates[left_signature] = (
                    f"({left_inner.sql(dialect=self.dialect)}) IS NOT DISTINCT FROM "
                    f"({right_outer.sql(dialect=self.dialect)})"
                )
                continue

            if modifier_type == "VISIBLE":
                if has_all_global or has_set:
                    continue
                include_visible = True
                continue

            raise ValueError(f"Unsupported AT modifier: {modifier}")

        return active_dimensions, where_predicates, list(set_predicates.values()), include_visible

    def _has_subquery_in_from(self, select: exp.Select) -> bool:
        """Check if FROM clause contains a subquery."""
        from_clause = select.args.get("from")
        if not from_clause:
            return False

        return isinstance(from_clause.this, exp.Subquery)

    def _rewrite_with_ctes_or_subqueries(self, parsed: exp.Select) -> str:
        """Rewrite query that contains CTEs or subqueries.

        Strategy:
        1. Rewrite each CTE that references semantic models
        2. Rewrite subqueries in FROM clause
        3. Return the modified SQL
        """
        # Handle CTEs
        if parsed.args.get("with"):
            with_clause = parsed.args["with"]
            for cte in with_clause.expressions:
                # Each CTE has a name (alias) and a query (this)
                cte_query = cte.this
                if isinstance(cte_query, exp.Select):
                    # Check if this CTE references a semantic model
                    if self._references_semantic_model(cte_query):
                        # Rewrite the CTE query
                        rewritten_cte_sql = self._rewrite_simple_query(cte_query)
                        # Parse the rewritten SQL and replace the CTE query
                        rewritten_cte = sqlglot.parse_one(rewritten_cte_sql, dialect=self.dialect)
                        cte.set("this", rewritten_cte)

        # Handle subquery in FROM
        from_clause = parsed.args.get("from")
        if from_clause and isinstance(from_clause.this, exp.Subquery):
            subquery = from_clause.this
            subquery_select = subquery.this
            if isinstance(subquery_select, exp.Select) and self._references_semantic_model(subquery_select):
                # Rewrite the subquery
                rewritten_subquery_sql = self._rewrite_simple_query(subquery_select)
                rewritten_subquery = sqlglot.parse_one(rewritten_subquery_sql, dialect=self.dialect)
                subquery.set("this", rewritten_subquery)

        # Return the modified SQL
        # Note: Individual CTEs/subqueries are already instrumented by _rewrite_simple_query -> generator
        # The outer query wrapper doesn't need separate instrumentation
        return parsed.sql(dialect=self.dialect)

    def _references_semantic_model(self, select: exp.Select) -> bool:
        """Check if a SELECT statement references any semantic models."""
        from_clause = select.args.get("from")
        if not from_clause:
            return False

        table_expr = from_clause.this
        if isinstance(table_expr, exp.Table):
            table_name = table_expr.name
            # "metrics" is a special virtual table for semantic layer
            if table_name == "metrics":
                return True
            # Check if this is a known model
            return table_name in self.graph.models

        return False

    def _rewrite_simple_query(self, parsed: exp.Select) -> str:
        """Rewrite a simple semantic layer query (no CTEs/subqueries).

        Args:
            parsed: Parsed SELECT statement

        Returns:
            Rewritten SQL using semantic layer
        """
        # Check for explicit JOINs - these are not supported
        if parsed.args.get("joins"):
            raise ValueError(
                "Explicit JOIN syntax is not supported. "
                "Joins are automatic based on model relationships.\n\n"
                "Instead of:\n"
                "  SELECT orders.revenue, customers.name FROM orders JOIN customers ON ...\n\n"
                "Use:\n"
                "  SELECT orders.revenue, customers.name FROM orders"
            )

        # Extract FROM table for inference
        self.inferred_table = self._extract_from_table(parsed)

        # Extract components
        metrics, dimensions, aliases = self._extract_metrics_and_dimensions(parsed)
        filters = self._extract_filters(parsed)
        order_by = self._extract_order_by(parsed)
        limit = self._extract_limit(parsed)
        offset = self._extract_offset(parsed)

        # Validate we have something to select
        if not metrics and not dimensions:
            raise ValueError("Query must select at least one metric or dimension")

        # Generate semantic layer SQL
        return self.generator.generate(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            order_by=order_by,
            limit=limit,
            offset=offset,
            aliases=aliases,
        )

    def _extract_metrics_and_dimensions(self, select: exp.Select) -> tuple[list[str], list[str], dict[str, str]]:
        """Extract metrics and dimensions from SELECT clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Tuple of (metrics, dimensions, aliases)
            where aliases is a dict mapping field reference to custom alias
        """
        metrics = []
        dimensions = []
        aliases = {}

        for projection in select.expressions:
            # Handle SELECT *
            if isinstance(projection, exp.Star):
                # Expand to all fields from the inferred table
                if not self.inferred_table:
                    raise ValueError("SELECT * requires a FROM clause with a single table")

                # FROM metrics: expand to all metrics/dimensions from all models
                if self.inferred_table == "metrics":
                    raise ValueError(
                        "SELECT * is not supported with FROM metrics.\n"
                        "You must explicitly select fields, e.g.:\n"
                        "  SELECT orders.revenue, customers.region FROM metrics"
                    )

                model = self.graph.get_model(self.inferred_table)

                # Add all dimensions
                for dim in model.dimensions:
                    dimensions.append(f"{self.inferred_table}.{dim.name}")

                # Add all measures as metrics
                for measure in model.metrics:
                    metrics.append(f"{self.inferred_table}.{measure.name}")

                continue

            # Get column name and alias
            custom_alias = None
            if isinstance(projection, exp.Alias):
                column = projection.this
                custom_alias = projection.alias
            else:
                column = projection

            # Skip literal values
            if isinstance(column, exp.Literal):
                raise ValueError(
                    "Literal values in SELECT are not supported in semantic layer queries.\n"
                    "Only metrics and dimensions can be selected."
                )

            # Extract table.column reference
            ref = self._resolve_column(column)
            if not ref:
                raise ValueError(f"Cannot resolve column: {column.sql(dialect=self.dialect)}")

            # Store custom alias if provided
            if custom_alias:
                aliases[ref] = custom_alias

            # Handle graph-level metrics (no model prefix)
            if "." not in ref:
                # This is a graph-level metric
                if ref in self.graph.metrics:
                    metrics.append(ref)
                    continue
                else:
                    raise ValueError(f"Field '{ref}' not found as a graph-level metric")

            model_name, field_name = ref.split(".", 1)

            # Check if field_name includes time granularity suffix (e.g., order_date__day)
            base_field_name = field_name
            if "__" in field_name:
                parts = field_name.rsplit("__", 1)
                potential_gran = parts[1]
                # Validate granularity
                valid_grans = ["year", "quarter", "month", "week", "day", "hour", "minute", "second"]
                if potential_gran in valid_grans:
                    base_field_name = parts[0]

            # Check if it's a metric (using base name without granularity)
            metric_ref = f"{model_name}.{base_field_name}"
            if metric_ref in self.graph.metrics:
                metrics.append(f"{model_name}.{field_name}")  # Keep original field_name with granularity
                continue

            # Check if it's a measure (should be accessed as metric)
            model = self.graph.get_model(model_name)
            if any(m.name == base_field_name for m in model.metrics):
                # Measure referenced directly - treat as implicit metric
                metrics.append(f"{model_name}.{field_name}")  # Keep original field_name
                continue

            # Check if it's a dimension
            if any(d.name == base_field_name for d in model.dimensions):
                # Keep the full ref including __granularity if present
                dimensions.append(ref)
                continue

            raise ValueError(
                f"Field '{model_name}.{base_field_name}' not found. Must be a metric, measure, or dimension in model '{model_name}'"
            )

        return metrics, dimensions, aliases

    def _extract_filters(self, select: exp.Select) -> list[str]:
        """Extract filters from WHERE clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            List of filter expressions
        """
        if not select.args.get("where"):
            return []

        where = select.args["where"].this

        # Handle compound conditions (AND/OR)
        if isinstance(where, (exp.And, exp.Or)):
            return self._extract_compound_filters(where)

        # Single condition
        return [where.sql(dialect=self.dialect)]

    def _extract_compound_filters(self, condition: exp.Expression) -> list[str]:
        """Extract filters from compound AND/OR conditions.

        Args:
            condition: Compound condition (AND/OR)

        Returns:
            List of filter expressions
        """
        filters = []

        if isinstance(condition, exp.And):
            # Split AND into separate filters
            for expr in [condition.left, condition.right]:
                if isinstance(expr, (exp.And, exp.Or)):
                    filters.extend(self._extract_compound_filters(expr))
                else:
                    filters.append(expr.sql(dialect=self.dialect))
        elif isinstance(condition, exp.Or):
            # OR must stay together as single filter
            filters.append(condition.sql(dialect=self.dialect))
        else:
            filters.append(condition.sql(dialect=self.dialect))

        return filters

    def _extract_order_by(self, select: exp.Select) -> list[str] | None:
        """Extract ORDER BY clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            List of order by expressions or None
        """
        if not select.args.get("order"):
            return None

        order_expressions = []
        for order_expr in select.args["order"].expressions:
            # Get column (might have ASC/DESC)
            if isinstance(order_expr, exp.Ordered):
                column = order_expr.this
                desc = order_expr.args.get("desc", False)
                col_name = self._get_column_name(column)
                order_expressions.append(f"{col_name} {'DESC' if desc else 'ASC'}")
            else:
                col_name = self._get_column_name(order_expr)
                order_expressions.append(col_name)

        return order_expressions if order_expressions else None

    def _extract_limit(self, select: exp.Select) -> int | None:
        """Extract LIMIT clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Limit value or None
        """
        if not select.args.get("limit"):
            return None

        limit = select.args["limit"]
        if hasattr(limit, "expression"):
            limit_expr = limit.expression
            if isinstance(limit_expr, exp.Literal):
                return int(limit_expr.this)

        return None

    def _extract_offset(self, select: exp.Select) -> int | None:
        """Extract OFFSET clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Offset value or None
        """
        if not select.args.get("offset"):
            return None

        offset = select.args["offset"]
        if hasattr(offset, "expression"):
            offset_expr = offset.expression
            if isinstance(offset_expr, exp.Literal):
                return int(offset_expr.this)

        return None

    def _extract_from_table(self, select: exp.Select) -> str | None:
        """Extract table name from FROM clause if there's only one table.

        Args:
            select: Parsed SELECT statement

        Returns:
            Table name or None if multiple tables or no FROM.
            Returns "metrics" if FROM metrics (special generic semantic layer table)
        """
        from_clause = select.args.get("from")
        if not from_clause:
            return None

        # Get the table expression
        table_expr = from_clause.this
        if isinstance(table_expr, exp.Table):
            table_name = table_expr.name
            # "metrics" is a special virtual table for generic semantic queries
            if table_name == "metrics":
                return "metrics"
            return table_name

        return None

    def _resolve_column(self, column: exp.Expression) -> str | None:
        """Resolve column reference to model.field format.

        Args:
            column: Column expression

        Returns:
            Reference like "orders.revenue" or "orders.order_date__day" or None
        """
        if isinstance(column, exp.Column):
            table = column.table
            name = column.name

            if table:
                # Explicit table.column (may include __granularity suffix)
                return f"{table}.{name}"
            else:
                # Try to infer from single FROM table
                if self.inferred_table:
                    # FROM metrics allows unqualified top-level metrics
                    if self.inferred_table == "metrics":
                        # Check if this is a top-level/graph metric
                        if name in self.graph.metrics:
                            # Top-level metric, return as-is (no model prefix)
                            return name
                        else:
                            raise ValueError(
                                f"Column '{name}' must be fully qualified when using FROM metrics.\n"
                                f"Use model.{name} for model-level metrics, or define '{name}' as a graph-level metric.\n\n"
                                f"Example: SELECT orders.revenue, total_orders FROM metrics"
                            )
                    # Column name may include __granularity suffix (e.g., order_date__day)
                    return f"{self.inferred_table}.{name}"
                else:
                    raise ValueError(f"Column '{name}' must have table prefix (e.g., orders.{name})")

        # Handle aggregate functions - must be pre-defined as measures
        if isinstance(column, exp.Func):
            func_sql = column.sql(dialect=self.dialect)
            func_name = column.key.upper()

            # Extract the expression being aggregated
            if column.args.get("this"):
                arg = column.args["this"]
                # Handle both expression objects and strings
                if isinstance(arg, str):
                    arg_sql = arg
                elif isinstance(arg, exp.Star):
                    arg_sql = "*"
                else:
                    arg_sql = arg.sql(dialect=self.dialect)
            else:
                arg_sql = "*"

            # Provide helpful error with YAML example (use wording expected by docs/tests)
            raise ValueError(
                f"Aggregate functions must be defined as a metric.\n\n"
                f"To use {func_sql}, add to your model:\n\n"
                f"measures:\n"
                f"  - name: my_metric\n"
                f"    agg: {func_name.lower()}\n"
                f"    expr: {arg_sql}\n\n"
                f"Then query with: SELECT my_metric FROM {self.inferred_table or 'your_model'}"
            )

        return None

    def _get_column_name(self, column: exp.Expression) -> str:
        """Get simple column name from expression.

        Args:
            column: Column expression

        Returns:
            Column name
        """
        if isinstance(column, exp.Column):
            return column.name
        return column.sql(dialect=self.dialect)
