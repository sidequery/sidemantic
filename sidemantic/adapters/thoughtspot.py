"""ThoughtSpot TML adapter for importing/exporting semantic models."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

_BUCKET_MAP = {
    "HOURLY": "hour",
    "DAILY": "day",
    "WEEKLY": "week",
    "MONTHLY": "month",
    "QUARTERLY": "quarter",
    "YEARLY": "year",
}

_NUMERIC_TYPES = {"DOUBLE", "FLOAT", "INT32", "INT64", "DECIMAL", "NUMBER"}
_TIME_TYPES = {"DATE", "TIME", "DATETIME", "TIMESTAMP"}
_BOOL_TYPES = {"BOOL", "BOOLEAN"}

_AGGREGATION_MAP = {
    "SUM": "sum",
    "COUNT": "count",
    "COUNT_DISTINCT": "count_distinct",
    "AVERAGE": "avg",
    "AVG": "avg",
    "MIN": "min",
    "MAX": "max",
    "MEDIAN": "median",
}

_UNSUPPORTED_AGG_FUNCS = {
    "STD_DEVIATION": "STDDEV",
    "VARIANCE": "VARIANCE",
}

_SIMPLE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
_TML_REF = re.compile(r"\[([^\]]+)\]")
_TML_DOT_REF = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")


def _normalize(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip().upper()


def _map_bucket(bucket: str | None) -> str | None:
    return _BUCKET_MAP.get(_normalize(bucket) or "")


def _map_dimension_type(data_type: str | None, bucket: str | None) -> tuple[str, str | None]:
    if bucket:
        return "time", bucket

    dtype = _normalize(data_type)
    if dtype in _TIME_TYPES:
        granularity = "day" if dtype == "DATE" else "hour"
        return "time", granularity
    if dtype in _BOOL_TYPES:
        return "boolean", None
    if dtype in _NUMERIC_TYPES:
        return "numeric", None
    return "categorical", None


def _map_aggregation(aggregation: str | None) -> tuple[str | None, str | None]:
    if not aggregation:
        return None, None
    agg = _normalize(aggregation)
    if agg in ("NONE", "NO_AGGREGATION"):
        return None, None
    if agg in _UNSUPPORTED_AGG_FUNCS:
        return None, _UNSUPPORTED_AGG_FUNCS[agg]
    return _AGGREGATION_MAP.get(agg), None


def _convert_tml_expr(expr: str | None, table_path_lookup: dict[str, str] | None = None) -> str | None:
    if not expr:
        return expr

    def _replace(match: re.Match[str]) -> str:
        token = match.group(1)
        if "::" in token:
            table, column = token.split("::", 1)
            if table_path_lookup and table in table_path_lookup:
                table = table_path_lookup[table]
            return f"{table}.{column}"
        return token.replace("::", ".")

    return _TML_REF.sub(_replace, expr)


def _parse_ref_token(token: str) -> tuple[str | None, str]:
    if "::" in token:
        table, column = token.split("::", 1)
        return table, column
    if "." in token:
        table, column = token.split(".", 1)
        return table, column
    return None, token


def _extract_join_refs(
    expr: str | None, table_path_lookup: dict[str, str] | None = None
) -> tuple[tuple[str | None, str] | None, tuple[str | None, str] | None]:
    if not expr:
        return None, None

    tokens = _TML_REF.findall(expr)
    if len(tokens) < 2:
        tokens = [f"{t[0]}.{t[1]}" for t in _TML_DOT_REF.findall(expr)]

    if len(tokens) < 2:
        return None, None

    left = _parse_ref_token(tokens[0])
    right = _parse_ref_token(tokens[1])

    if table_path_lookup:
        if left[0] in table_path_lookup:
            left = (table_path_lookup[left[0]], left[1])
        if right[0] in table_path_lookup:
            right = (table_path_lookup[right[0]], right[1])
    return left, right


def _split_sql_identifier(sql: str | None) -> tuple[str | None, str | None]:
    if not sql or not _SIMPLE_IDENTIFIER.match(sql):
        return None, None
    if "." in sql:
        table, column = sql.split(".", 1)
        return table, column
    return None, sql


def _sql_to_tml_expr(expr: str | None, base_table: str, tables: set[str]) -> str | None:
    if not expr:
        return expr

    expr = expr.replace("{model}.", f"{base_table}.")

    def _replace(match: re.Match[str]) -> str:
        table = match.group(1)
        column = match.group(2)
        if table in tables:
            return f"[{table}::{column}]"
        return match.group(0)

    return _TML_DOT_REF.sub(_replace, expr)


def _split_table_name(table: str | None) -> tuple[str | None, str | None, str | None]:
    if not table:
        return None, None, None

    parts = table.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, parts[0]


def _simple_column(sql: str | None, fallback: str | None) -> str | None:
    if not sql:
        return fallback
    if _SIMPLE_IDENTIFIER.match(sql):
        return sql
    return fallback


class ThoughtSpotAdapter(BaseAdapter):
    """Adapter for ThoughtSpot TML (YAML) tables and worksheets."""

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse ThoughtSpot TML files into semantic graph."""
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")

        graph = SemanticGraph()
        tml_files: list[Path] = []
        if source_path.is_dir():
            tml_files = (
                list(source_path.rglob("*.tml")) + list(source_path.rglob("*.yml")) + list(source_path.rglob("*.yaml"))
            )
        else:
            tml_files = [source_path]

        for tml_file in tml_files:
            model = self._parse_file(tml_file)
            if model:
                graph.add_model(model)

        return graph

    def _parse_file(self, file_path: Path) -> Model | None:
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            return None

        if "table" in data:
            return self._parse_table(data.get("table"), data)
        if "worksheet" in data:
            return self._parse_worksheet(data.get("worksheet"), data)
        if "model" in data:
            return self._parse_worksheet(data.get("model"), data)

        return None

    def _parse_table(self, table_def: dict[str, Any] | None, full_def: dict[str, Any]) -> Model | None:
        if not table_def:
            return None

        name = table_def.get("name") or table_def.get("id")
        if not name:
            return None

        db = table_def.get("db")
        schema = table_def.get("schema")
        db_table = table_def.get("db_table") or name

        table_name = ".".join([part for part in [db, schema, db_table] if part]) if db_table else None

        dimensions: list[Dimension] = []
        metrics: list[Metric] = []

        for col_def in table_def.get("columns") or []:
            col_name = col_def.get("name")
            if not col_name:
                continue

            properties = col_def.get("properties") or {}
            column_type = _normalize(properties.get("column_type")) or "ATTRIBUTE"
            bucket = _map_bucket(properties.get("default_date_bucket"))
            data_type = col_def.get("data_type") or (col_def.get("db_column_properties") or {}).get("data_type")
            label = col_def.get("custom_name") or col_def.get("display_name")
            description = col_def.get("description")
            format_pattern = properties.get("format_pattern")
            sql = col_def.get("db_column_name") or col_name

            if column_type == "MEASURE":
                agg, unsupported_func = _map_aggregation(properties.get("aggregation"))
                metric_sql = _convert_tml_expr(sql)
                if agg:
                    metric = Metric(
                        name=col_name,
                        agg=agg,
                        sql=metric_sql,
                        label=label,
                        description=description,
                        format=format_pattern,
                    )
                else:
                    if unsupported_func:
                        metric_sql = f"{unsupported_func}({metric_sql})" if metric_sql else unsupported_func
                    metric = Metric(
                        name=col_name,
                        type="derived",
                        sql=metric_sql,
                        label=label,
                        description=description,
                        format=format_pattern,
                    )
                metrics.append(metric)
            else:
                dim_type, granularity = _map_dimension_type(data_type, bucket)
                dim = Dimension(
                    name=col_name,
                    type=dim_type,
                    sql=_convert_tml_expr(sql),
                    granularity=granularity,
                    label=label,
                    description=description,
                    format=format_pattern,
                )
                dimensions.append(dim)

        default_time_dimension = None
        default_grain = None
        for dim in dimensions:
            if dim.type == "time":
                default_time_dimension = dim.name
                default_grain = dim.granularity
                break

        primary_key = "id"
        if any(d.name.lower() == "id" for d in dimensions):
            primary_key = next(d.name for d in dimensions if d.name.lower() == "id")

        relationships = self._parse_table_relationships(table_def.get("joins_with") or [])

        model = Model(
            name=name,
            table=table_name,
            description=table_def.get("description"),
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            relationships=relationships,
            default_time_dimension=default_time_dimension,
            default_grain=default_grain,
        )
        setattr(model, "_source_tml_type", "table")
        return model

    def _parse_worksheet(self, worksheet_def: dict[str, Any] | None, full_def: dict[str, Any]) -> Model | None:
        if not worksheet_def:
            return None

        name = worksheet_def.get("name")
        if not name:
            return None

        description = worksheet_def.get("description")
        tables = worksheet_def.get("tables") or []
        joins = worksheet_def.get("joins") or []
        table_paths = worksheet_def.get("table_paths") or []

        table_name_lookup = self._table_name_lookup(tables)
        table_path_lookup = {
            tp.get("id"): table_name_lookup.get(tp.get("table"), tp.get("table")) for tp in table_paths if tp.get("id")
        }

        sql, base_table = self._build_join_sql(tables, joins, table_path_lookup, table_name_lookup)
        relationships = self._parse_join_relationships(joins, table_path_lookup, table_name_lookup)

        formulas = worksheet_def.get("formulas") or []
        formula_by_id = {f.get("id"): f for f in formulas if f.get("id")}
        formula_by_name = {f.get("name"): f for f in formulas if f.get("name")}

        dimensions: list[Dimension] = []
        metrics: list[Metric] = []

        for col_def in worksheet_def.get("worksheet_columns") or []:
            col_name = col_def.get("name")
            column_id = col_def.get("column_id")
            formula_id = col_def.get("formula_id")
            if not col_name:
                if formula_id and formula_id in formula_by_id:
                    col_name = formula_by_id[formula_id].get("name")
                elif column_id:
                    col_name = column_id.split("::")[-1]

            if not col_name:
                continue

            properties = col_def.get("properties") or {}
            column_type = _normalize(properties.get("column_type")) or "ATTRIBUTE"
            bucket = _map_bucket(properties.get("default_date_bucket"))
            label = col_def.get("custom_name") or col_def.get("display_name")
            description = col_def.get("description")
            format_pattern = properties.get("format_pattern")

            sql_expr = None
            if formula_id and formula_id in formula_by_id:
                sql_expr = formula_by_id[formula_id].get("expr")
            elif formula_id and formula_id in formula_by_name:
                sql_expr = formula_by_name[formula_id].get("expr")
            elif col_name in formula_by_name:
                sql_expr = formula_by_name[col_name].get("expr")

            if not sql_expr and column_id:
                if "::" in column_id:
                    path_id, col_ref = column_id.split("::", 1)
                    table_name = table_path_lookup.get(path_id) or table_name_lookup.get(path_id)
                    if table_name:
                        sql_expr = f"{table_name}.{col_ref}"
                    else:
                        sql_expr = col_ref
                else:
                    sql_expr = column_id

            sql_expr = _convert_tml_expr(sql_expr, table_path_lookup)

            if column_type == "MEASURE":
                agg, unsupported_func = _map_aggregation(properties.get("aggregation"))
                metric_sql = sql_expr
                if agg:
                    metric = Metric(
                        name=col_name,
                        agg=agg,
                        sql=metric_sql,
                        label=label,
                        description=description,
                        format=format_pattern,
                    )
                else:
                    if unsupported_func:
                        metric_sql = f"{unsupported_func}({metric_sql})" if metric_sql else unsupported_func
                    metric = Metric(
                        name=col_name,
                        type="derived",
                        sql=metric_sql,
                        label=label,
                        description=description,
                        format=format_pattern,
                    )
                metrics.append(metric)
            else:
                data_type = col_def.get("data_type") or (col_def.get("db_column_properties") or {}).get("data_type")
                dim_type, granularity = _map_dimension_type(data_type, bucket)
                dim = Dimension(
                    name=col_name,
                    type=dim_type,
                    sql=sql_expr,
                    granularity=granularity,
                    label=label,
                    description=description,
                    format=format_pattern,
                )
                dimensions.append(dim)

        default_time_dimension = None
        default_grain = None
        for dim in dimensions:
            if dim.type == "time":
                default_time_dimension = dim.name
                default_grain = dim.granularity
                break

        primary_key = "id"
        if any(d.name.lower() == "id" for d in dimensions):
            primary_key = next(d.name for d in dimensions if d.name.lower() == "id")

        model = Model(
            name=name,
            table=base_table if not sql else None,
            sql=sql,
            description=description,
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            relationships=relationships,
            default_time_dimension=default_time_dimension,
            default_grain=default_grain,
        )

        if base_table:
            setattr(model, "_worksheet_base_table", base_table)
        setattr(model, "_source_tml_type", "worksheet")

        return model

    def _build_join_sql(
        self,
        tables: list[dict[str, Any]],
        joins: list[dict[str, Any]],
        table_path_lookup: dict[str, str] | None = None,
        table_name_lookup: dict[str, str] | None = None,
    ) -> tuple[str | None, str | None]:
        base_table = None
        if tables:
            base_table = tables[0].get("name") or tables[0].get("id")

        joined: set[str] = set()
        if base_table:
            joined.add(base_table)

        clauses: list[str] = []
        if base_table:
            clauses.append(base_table)

        for join_def in joins:
            source = join_def.get("source")
            destination = join_def.get("destination")
            join_type = _normalize(join_def.get("type")) or "INNER"
            on_value = join_def.get("on")
            if on_value is None and True in join_def:
                on_value = join_def.get(True)
            on_expr = _convert_tml_expr(on_value, table_path_lookup or table_name_lookup)

            if not source or not destination or not on_expr:
                continue

            if table_name_lookup:
                source = table_name_lookup.get(source, source)
                destination = table_name_lookup.get(destination, destination)

            if not base_table:
                base_table = source
                clauses.append(base_table)
                joined.add(base_table)

            if source in joined and destination not in joined:
                right = destination
            elif destination in joined and source not in joined:
                right = source
            else:
                right = destination

            join_keyword = {
                "LEFT_OUTER": "LEFT",
                "RIGHT_OUTER": "RIGHT",
                "OUTER": "FULL OUTER",
                "FULL_OUTER": "FULL OUTER",
                "INNER": "INNER",
            }.get(join_type, "INNER")

            clauses.append(f"{join_keyword} JOIN {right} ON {on_expr}")
            joined.add(right)

        if not clauses:
            return None, None

        if len(clauses) == 1:
            return None, clauses[0]

        sql = "SELECT * FROM " + clauses[0]
        for clause in clauses[1:]:
            sql += f"\n{clause}"

        return sql, None

    def _parse_join_relationships(
        self,
        joins: list[dict[str, Any]],
        table_path_lookup: dict[str, str] | None = None,
        table_name_lookup: dict[str, str] | None = None,
    ) -> list[Relationship]:
        relationships: list[Relationship] = []

        for join_def in joins:
            source = join_def.get("source")
            destination = join_def.get("destination")
            if not source or not destination:
                continue

            join_type = _normalize(join_def.get("type")) or "INNER"
            on_value = join_def.get("on")
            if on_value is None and True in join_def:
                on_value = join_def.get(True)
            lookup = table_path_lookup or table_name_lookup
            left, right = _extract_join_refs(on_value, lookup)

            if table_name_lookup:
                source = table_name_lookup.get(source, source)
                destination = table_name_lookup.get(destination, destination)

            foreign_key = None
            primary_key = None

            if left and right:
                left_table, left_col = left
                right_table, right_col = right

                if left_table == source and right_table == destination:
                    foreign_key = left_col
                    primary_key = right_col
                elif left_table == destination and right_table == source:
                    foreign_key = right_col
                    primary_key = left_col
                else:
                    if left_table == source:
                        foreign_key = left_col
                    if right_table == destination:
                        primary_key = right_col

            rel_type = "one_to_one" if join_def.get("is_one_to_one") else "many_to_one"
            if join_type in {"RIGHT_OUTER", "FULL_OUTER", "OUTER"}:
                rel_type = "many_to_many"

            relationships.append(
                Relationship(
                    name=destination,
                    type=rel_type,
                    foreign_key=foreign_key,
                    primary_key=primary_key,
                )
            )

        return relationships

    def _parse_table_relationships(self, joins_with: list[dict[str, Any]]) -> list[Relationship]:
        relationships: list[Relationship] = []
        for join_def in joins_with:
            destination_def = join_def.get("destination") or {}
            destination = destination_def.get("name") if isinstance(destination_def, dict) else destination_def
            if not destination:
                continue

            join_type = _normalize(join_def.get("type")) or "INNER"
            on_value = join_def.get("on")
            if on_value is None and True in join_def:
                on_value = join_def.get(True)
            left, right = _extract_join_refs(on_value)

            foreign_key = None
            primary_key = None
            if left and right:
                foreign_key = left[1]
                primary_key = right[1]

            rel_type = "many_to_one"
            if join_def.get("is_one_to_one"):
                rel_type = "one_to_one"
            if join_type in {"RIGHT_OUTER", "FULL_OUTER", "OUTER"}:
                rel_type = "many_to_many"

            relationships.append(
                Relationship(
                    name=destination,
                    type=rel_type,
                    foreign_key=foreign_key,
                    primary_key=primary_key,
                )
            )

        return relationships

    def _table_name_lookup(self, tables: list[dict[str, Any]]) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for table in tables:
            table_id = table.get("id") or table.get("name")
            table_name = table.get("name") or table.get("id")
            if table_id and table_name:
                lookup[table_id] = table_name
        return lookup

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to ThoughtSpot table TML files."""
        output_path = Path(output_path)

        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        if output_path.is_dir() or not output_path.suffix:
            output_path.mkdir(parents=True, exist_ok=True)
            for model in resolved_models.values():
                tml = self._export_model(model)
                file_path = output_path / f"{model.name}.{tml['__type']}.tml"
                with open(file_path, "w") as f:
                    yaml.safe_dump(tml["data"], f, sort_keys=False)
        else:
            if resolved_models:
                model = next(iter(resolved_models.values()))
                tml = self._export_model(model)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, "w") as f:
                    yaml.safe_dump(tml["data"], f, sort_keys=False)

    def _export_model(self, model: Model) -> dict[str, Any]:
        if self._should_export_worksheet(model):
            return {"__type": "worksheet", "data": self._export_worksheet(model)}
        return {"__type": "table", "data": self._export_table(model)}

    def _should_export_worksheet(self, model: Model) -> bool:
        if getattr(model, "_source_tml_type", None) == "worksheet":
            return True
        if model.sql is not None:
            return True
        if model.relationships:
            return True
        return False

    def _export_table(self, model: Model) -> dict[str, Any]:
        db, schema, table = _split_table_name(model.table)

        table_def: dict[str, Any] = {
            "name": model.name,
            "description": model.description,
        }

        if db:
            table_def["db"] = db
        if schema:
            table_def["schema"] = schema
        if table:
            table_def["db_table"] = table

        columns: list[dict[str, Any]] = []

        for dim in model.dimensions:
            data_type = "VARCHAR"
            if dim.type == "numeric":
                data_type = "DOUBLE"
            elif dim.type == "boolean":
                data_type = "BOOL"
            elif dim.type == "time":
                data_type = "DATETIME" if dim.granularity in {"hour", "minute", "second"} else "DATE"

            col_def: dict[str, Any] = {
                "name": dim.name,
                "db_column_name": _simple_column(dim.sql, dim.name),
                "data_type": data_type,
                "properties": {
                    "column_type": "ATTRIBUTE",
                },
            }

            if dim.description:
                col_def["description"] = dim.description
            if dim.label:
                col_def["custom_name"] = dim.label
            if dim.format:
                col_def["properties"]["format_pattern"] = dim.format
            if dim.type == "time" and dim.granularity:
                bucket = {v: k for k, v in _BUCKET_MAP.items()}.get(dim.granularity)
                if bucket:
                    col_def["properties"]["default_date_bucket"] = bucket

            columns.append(col_def)

        for metric in model.metrics:
            data_type = "DOUBLE"
            col_def: dict[str, Any] = {
                "name": metric.name,
                "db_column_name": _simple_column(metric.sql, metric.name),
                "data_type": data_type,
                "properties": {
                    "column_type": "MEASURE",
                },
            }

            if metric.description:
                col_def["description"] = metric.description
            if metric.label:
                col_def["custom_name"] = metric.label
            if metric.format:
                col_def["properties"]["format_pattern"] = metric.format

            if metric.agg:
                agg_map = {
                    "sum": "SUM",
                    "count": "COUNT",
                    "count_distinct": "COUNT_DISTINCT",
                    "avg": "AVERAGE",
                    "min": "MIN",
                    "max": "MAX",
                    "median": "MEDIAN",
                }
                col_def["properties"]["aggregation"] = agg_map.get(metric.agg, "NONE")
            else:
                col_def["properties"]["aggregation"] = "NONE"

            columns.append(col_def)

        if columns:
            table_def["columns"] = columns

        return {
            "table": table_def,
        }

    def _export_worksheet(self, model: Model) -> dict[str, Any]:
        base_table = getattr(model, "_worksheet_base_table", None)
        if not base_table:
            base_table = model.table or model.name

        tables = [{"name": base_table}]
        joins: list[dict[str, Any]] = []
        table_paths: list[dict[str, Any]] = [{"id": base_table, "table": base_table}]

        for rel in model.relationships:
            tables.append({"name": rel.name})
            join_name = f"{base_table}_{rel.name}"
            join_type = "LEFT_OUTER" if rel.type in {"many_to_one", "one_to_one"} else "OUTER"
            on_expr = f"[{base_table}::{rel.sql_expr}] = [{rel.name}::{rel.related_key}]"
            joins.append(
                {
                    "name": join_name,
                    "source": base_table,
                    "destination": rel.name,
                    "type": join_type,
                    "on": on_expr,
                    "is_one_to_one": rel.type == "one_to_one",
                }
            )
            table_paths.append(
                {
                    "id": rel.name,
                    "table": rel.name,
                    "join_path": [{"join": [join_name]}],
                }
            )

        tables_set = {t["name"] for t in tables}

        formulas: list[dict[str, Any]] = []
        worksheet_columns: list[dict[str, Any]] = []
        formula_counter = 0

        def add_formula(name: str, expr: str | None) -> str:
            nonlocal formula_counter
            formula_counter += 1
            formula_id = f"formula_{formula_counter}"
            formulas.append(
                {
                    "name": name,
                    "expr": expr,
                    "id": formula_id,
                }
            )
            return formula_id

        for dim in model.dimensions:
            dim_sql = dim.sql or dim.name
            table_ref, col_ref = _split_sql_identifier(dim_sql)
            if not table_ref and col_ref:
                table_ref = base_table
            if table_ref in tables_set and col_ref:
                column_id = f"{table_ref}::{col_ref}"
                formula_id = None
            else:
                formula_id = add_formula(dim.name, _sql_to_tml_expr(dim_sql, base_table, tables_set))
                column_id = None

            props: dict[str, Any] = {"column_type": "ATTRIBUTE"}
            if dim.type == "time" and dim.granularity:
                bucket = {v: k for k, v in _BUCKET_MAP.items()}.get(dim.granularity)
                if bucket:
                    props["default_date_bucket"] = bucket
            if dim.format:
                props["format_pattern"] = dim.format

            col_def: dict[str, Any] = {
                "name": dim.name,
                "properties": props,
            }

            if dim.label:
                col_def["custom_name"] = dim.label
            if dim.description:
                col_def["description"] = dim.description
            if column_id:
                col_def["column_id"] = column_id
            if formula_id:
                col_def["formula_id"] = formula_id

            worksheet_columns.append(col_def)

        for metric in model.metrics:
            metric_sql = metric.sql or metric.name
            table_ref, col_ref = _split_sql_identifier(metric_sql)
            if not table_ref and col_ref:
                table_ref = base_table
            if table_ref in tables_set and col_ref:
                column_id = f"{table_ref}::{col_ref}"
                formula_id = None
            else:
                formula_id = add_formula(metric.name, _sql_to_tml_expr(metric_sql, base_table, tables_set))
                column_id = None

            props: dict[str, Any] = {"column_type": "MEASURE"}
            if metric.format:
                props["format_pattern"] = metric.format

            if metric.agg:
                agg_map = {
                    "sum": "SUM",
                    "count": "COUNT",
                    "count_distinct": "COUNT_DISTINCT",
                    "avg": "AVERAGE",
                    "min": "MIN",
                    "max": "MAX",
                    "median": "MEDIAN",
                }
                props["aggregation"] = agg_map.get(metric.agg, "NONE")
            else:
                props["aggregation"] = "NONE"

            col_def: dict[str, Any] = {
                "name": metric.name,
                "properties": props,
            }

            if metric.label:
                col_def["custom_name"] = metric.label
            if metric.description:
                col_def["description"] = metric.description
            if column_id:
                col_def["column_id"] = column_id
            if formula_id:
                col_def["formula_id"] = formula_id

            worksheet_columns.append(col_def)

        worksheet_def: dict[str, Any] = {
            "name": model.name,
            "description": model.description,
            "tables": tables,
        }

        if joins:
            worksheet_def["joins"] = joins
        if table_paths:
            worksheet_def["table_paths"] = table_paths
        if formulas:
            worksheet_def["formulas"] = formulas
        if worksheet_columns:
            worksheet_def["worksheet_columns"] = worksheet_columns

        return {
            "worksheet": worksheet_def,
        }
