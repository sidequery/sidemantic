"""Yardstick adapter for importing SQL models with AS MEASURE semantics."""

from functools import lru_cache
from pathlib import Path
from typing import Literal, get_args, get_origin

import sqlglot
from sqlglot import exp
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.tokens import TokenType

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


def _extract_literal_strings(annotation) -> set[str]:
    if get_origin(annotation) is Literal:
        return {value for value in get_args(annotation) if isinstance(value, str)}

    values = set()
    for arg in get_args(annotation):
        values.update(_extract_literal_strings(arg))
    return values


@lru_cache(maxsize=1)
def _supported_metric_aggs() -> set[str]:
    annotation = Metric.model_fields["agg"].annotation
    return _extract_literal_strings(annotation)


class YardstickDialect(DuckDB):
    """DuckDB dialect extension that supports `AS MEASURE <alias>`."""

    class Parser(DuckDB.Parser):
        """Parser extension for Yardstick's measure alias syntax."""

        def _parse_alias(self, this: exp.Expression | None, explicit: bool = False) -> exp.Expression | None:
            if self._can_parse_limit_or_offset():
                return this

            any_token = self._match(TokenType.ALIAS)
            comments = self._prev_comments or []

            if explicit and not any_token:
                return this

            if self._match(TokenType.L_PAREN):
                aliases = self.expression(
                    exp.Aliases,
                    comments=comments,
                    this=this,
                    expressions=self._parse_csv(lambda: self._parse_id_var(any_token)),
                )
                self._match_r_paren(aliases)
                return aliases

            is_measure_alias = bool(any_token and self._match_texts({"MEASURE"}))
            alias = self._parse_id_var(any_token, tokens=self.ALIAS_TOKENS) or (
                self.STRING_ALIASES and self._parse_string_as_identifier()
            )

            if alias:
                comments.extend(alias.pop_comments())
                this = self.expression(exp.Alias, comments=comments, this=this, alias=alias)
                if is_measure_alias:
                    this.set("yardstick_measure", True)

                column = this.this
                if not this.comments and column and column.comments:
                    this.comments = column.pop_comments()

            return this


class YardstickAdapter(BaseAdapter):
    """Adapter for Yardstick SQL definitions.

    Yardstick defines measures inside CREATE VIEW statements with:
    `AGG(expr) AS MEASURE measure_name`.
    """

    _SIMPLE_AGGREGATIONS: dict[type[exp.Expression], str] = {
        exp.Sum: "sum",
        exp.Avg: "avg",
        exp.Min: "min",
        exp.Max: "max",
        exp.Median: "median",
        exp.Stddev: "stddev",
        exp.StddevPop: "stddev_pop",
        exp.Variance: "variance",
        exp.VariancePop: "variance_pop",
    }
    _ANONYMOUS_AGGREGATIONS: set[str] = {"mode"}

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Yardstick SQL files into a semantic graph."""
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")

        graph = SemanticGraph()
        if source_path.is_dir():
            for sql_file in sorted(source_path.rglob("*.sql")):
                self._parse_sql_file(sql_file, graph)
        else:
            self._parse_sql_file(source_path, graph)

        return graph

    def _parse_sql_file(self, path: Path, graph: SemanticGraph) -> None:
        content = path.read_text()
        if not content.strip():
            return

        statements = self._parse_statements(content)
        for statement in statements:
            if not statement:
                continue

            if not isinstance(statement, exp.Create):
                continue

            if (statement.args.get("kind") or "").upper() != "VIEW":
                continue

            select = statement.expression
            if not isinstance(select, exp.Select):
                continue

            model = self._model_from_create_view(statement, select)
            if model:
                graph.add_model(model)

    def _parse_statements(self, sql: str) -> list[exp.Expression | None]:
        return sqlglot.parse(sql, read=YardstickDialect)

    def _model_from_create_view(self, create_stmt: exp.Create, select: exp.Select) -> Model | None:
        measure_aliases = {
            projection.output_name
            for projection in select.expressions
            if isinstance(projection, exp.Alias) and projection.args.get("yardstick_measure")
        }
        if not measure_aliases:
            return None

        view_name = create_stmt.this.name if isinstance(create_stmt.this, exp.Table) else None
        if not view_name:
            return None

        source_table, source_sql = self._extract_model_source(select)
        dimensions: list[Dimension] = []
        metrics: list[Metric] = []
        all_measure_names = set(measure_aliases)

        for projection in select.expressions:
            output_name = projection.output_name
            if not output_name:
                continue

            if output_name in measure_aliases:
                metric_expr = projection.this if isinstance(projection, exp.Alias) else projection
                metric = self._metric_from_expression(output_name, metric_expr, all_measure_names)
                metrics.append(metric)
            else:
                dim_expr = projection.this if isinstance(projection, exp.Alias) else projection
                if isinstance(dim_expr, exp.Star):
                    continue
                dim_type, dim_granularity = self._infer_dimension_type(dim_expr)
                dimensions.append(
                    Dimension(
                        name=output_name,
                        type=dim_type,
                        sql=dim_expr.sql(dialect="duckdb"),
                        granularity=dim_granularity,
                    )
                )

        if not metrics:
            return None

        yardstick_metadata: dict[str, str] = {"view_sql": select.sql(dialect="duckdb")}
        if source_table:
            yardstick_metadata["base_table"] = source_table
        if source_sql:
            yardstick_metadata["base_relation_sql"] = source_sql

        primary_key = dimensions[0].name if dimensions else "id"
        model_kwargs: dict[str, object] = {
            "name": view_name,
            "primary_key": primary_key,
            "dimensions": dimensions,
            "metrics": metrics,
            "metadata": {"yardstick": yardstick_metadata},
        }
        if source_sql:
            model_kwargs["sql"] = source_sql
        elif source_table:
            model_kwargs["table"] = source_table
        else:
            model_kwargs["table"] = view_name

        return Model(**model_kwargs)

    def _metric_from_expression(self, name: str, expression: exp.Expression, all_measure_names: set[str]) -> Metric:
        expression_sql = expression.sql(dialect="duckdb")
        if self._references_other_measures(name, expression, all_measure_names):
            return Metric(name=name, type="derived", sql=expression_sql)

        filtered_aggregation = self._extract_filtered_aggregation(expression)
        if filtered_aggregation:
            agg, inner_sql, filters = filtered_aggregation
            return Metric(name=name, agg=agg, sql=inner_sql, filters=filters)

        simple_aggregation = self._extract_supported_aggregation(expression)
        if simple_aggregation:
            agg, inner_sql = simple_aggregation
            return Metric(name=name, agg=agg, sql=inner_sql)

        if self._has_aggregate_semantics(expression):
            return Metric(name=name, sql=expression_sql)

        metric = Metric(name=name, sql=expression_sql)
        if metric.agg is None and metric.type is None:
            return Metric(name=name, type="derived", sql=expression_sql)
        return metric

    def _extract_model_source(self, select: exp.Select) -> tuple[str | None, str | None]:
        from_clause = select.args.get("from")
        joins = select.args.get("joins") or []
        where_clause = select.args.get("where")
        with_clause = select.args.get("with")

        if (
            isinstance(from_clause, exp.From)
            and isinstance(from_clause.this, exp.Table)
            and not joins
            and where_clause is None
            and with_clause is None
        ):
            table_expr = from_clause.this
            is_simple_table = isinstance(table_expr.this, exp.Identifier) and table_expr.args.get("alias") is None
            if is_simple_table:
                return table_expr.sql(dialect="duckdb"), None

        if from_clause is None:
            return None, None

        base_relation = exp.select("*")
        if with_clause is not None:
            base_relation.set("with", with_clause.copy())
        base_relation.set("from", from_clause.copy())
        if joins:
            base_relation.set("joins", [join.copy() for join in joins])
        if where_clause is not None:
            base_relation.set("where", where_clause.copy())

        return None, base_relation.sql(dialect="duckdb")

    def _references_other_measures(self, name: str, expression: exp.Expression, all_measure_names: set[str]) -> bool:
        measure_lookup = {
            measure_name.lower() for measure_name in all_measure_names if measure_name.lower() != name.lower()
        }
        referenced_columns = {column.name.lower() for column in expression.find_all(exp.Column)}
        return bool(referenced_columns & measure_lookup)

    def _extract_filtered_aggregation(self, expression: exp.Expression) -> tuple[str, str, list[str] | None] | None:
        if not isinstance(expression, exp.Filter):
            return None

        aggregation = self._extract_supported_aggregation(expression.this)
        if aggregation is None:
            return None

        agg, inner_sql = aggregation
        where_expression = expression.args.get("expression")
        if isinstance(where_expression, exp.Where):
            filter_sql = where_expression.this.sql(dialect="duckdb")
        elif isinstance(where_expression, exp.Expression):
            filter_sql = where_expression.sql(dialect="duckdb")
        else:
            filter_sql = ""

        filters = [filter_sql] if filter_sql else None
        return agg, inner_sql, filters

    def _extract_supported_aggregation(self, expression: exp.Expression) -> tuple[str, str] | None:
        if isinstance(expression, exp.Count):
            count_expr = expression.this
            if isinstance(count_expr, exp.Distinct):
                if count_expr.expressions:
                    inner_sql = ", ".join(expr.sql(dialect="duckdb") for expr in count_expr.expressions)
                else:
                    inner_sql = count_expr.sql(dialect="duckdb")
                return "count_distinct", inner_sql

            if count_expr is None or isinstance(count_expr, exp.Star):
                return "count", "*"
            return "count", count_expr.sql(dialect="duckdb")

        for expression_type, aggregation_name in self._SIMPLE_AGGREGATIONS.items():
            if isinstance(expression, expression_type):
                inner_expression = expression.this
                if inner_expression is None:
                    return aggregation_name, "*"
                return aggregation_name, inner_expression.sql(dialect="duckdb")

        if isinstance(expression, exp.Func):
            function_name = (expression.name or "").lower()
            if function_name == "count":
                count_expr = expression.this or (expression.expressions[0] if expression.expressions else None)
                if isinstance(count_expr, exp.Distinct):
                    if count_expr.expressions:
                        inner_sql = ", ".join(expr.sql(dialect="duckdb") for expr in count_expr.expressions)
                    else:
                        inner_sql = count_expr.sql(dialect="duckdb")
                    return "count_distinct", inner_sql
                if count_expr is None or isinstance(count_expr, exp.Star):
                    return "count", "*"
                return "count", count_expr.sql(dialect="duckdb")

            supported_function_aggs = _supported_metric_aggs() - {"count", "count_distinct"}
            if function_name in supported_function_aggs:
                inner_expression = expression.this or (expression.expressions[0] if expression.expressions else None)
                if inner_expression is None:
                    return function_name, "*"
                return function_name, inner_expression.sql(dialect="duckdb")

        return None

    def _has_aggregate_semantics(self, expression: exp.Expression) -> bool:
        if any(isinstance(node, exp.AggFunc) for node in expression.walk()):
            return True

        for node in expression.walk():
            if isinstance(node, exp.Anonymous) and (node.name or "").lower() in self._ANONYMOUS_AGGREGATIONS:
                return True
        return False

    def _infer_dimension_type(self, expression: exp.Expression) -> tuple[str, str | None]:
        if isinstance(expression, exp.Boolean):
            return "boolean", None
        if isinstance(expression, exp.Literal):
            if expression.is_number:
                return "numeric", None
            return "categorical", None
        if isinstance(expression, exp.Column):
            column_name = expression.name.lower()
            if "timestamp" in column_name:
                return "time", "second"
            if "date" in column_name:
                return "time", "day"
            if "time" in column_name:
                return "time", "second"
            return "categorical", None
        if isinstance(expression, exp.Func):
            function_name = (expression.name or "").lower()
            granularity_by_func = {
                "date": "day",
                "date_trunc": "day",
                "year": "year",
                "quarter": "quarter",
                "month": "month",
                "week": "week",
                "day": "day",
                "hour": "hour",
                "minute": "minute",
            }
            if function_name in granularity_by_func:
                return "time", granularity_by_func[function_name]
        return "categorical", None
