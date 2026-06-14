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

_CARDINALITY_MAP = {
    "MANY_TO_ONE": "many_to_one",
    "ONE_TO_ONE": "one_to_one",
    "ONE_TO_MANY": "one_to_many",
    "MANY_TO_MANY": "many_to_many",
}

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
# A bare identifier that is NOT part of a `table.column` qualifier and is NOT a
# function call: not preceded by `.`/word char, not followed by `.` or `(`.
_BARE_IDENTIFIER = re.compile(r"(?<![\w.])([A-Za-z_][A-Za-z0-9_]*)(?![\w.])(?!\s*\()")
# A quoted string literal (single or double quoted, with doubled-quote escapes).
_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")
# Split a join predicate into conjuncts on a word-boundary `AND` (case-insensitive).
_AND_SPLIT = re.compile(r"\bAND\b", re.IGNORECASE)
# A plain `=` equality operator (not part of `<=`, `>=`, `!=`, `<>`, or `==`).
_EQUALITY_OP = re.compile(r"(?<![<>=!])=(?![=])")


def _sub_outside_strings(pattern: re.Pattern[str], repl: Any, text: str) -> str:
    """Apply ``pattern.sub(repl, ...)`` only to regions outside quoted literals.

    Column-reference rewriting must not touch string literals, otherwise a
    formula like ``[status] = 'status'`` would rewrite the literal too and change
    the predicate's meaning.
    """
    result: list[str] = []
    last = 0
    for m in _STRING_LITERAL.finditer(text):
        result.append(pattern.sub(repl, text[last : m.start()]))
        result.append(m.group(0))
        last = m.end()
    result.append(pattern.sub(repl, text[last:]))
    return "".join(result)


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


def _inline_formula_refs(
    expr: str | None,
    formula_expr_by_name: dict[str, str],
    _seen: frozenset[str] = frozenset(),
) -> str | None:
    """Recursively inline ``[formula_name]`` references in a TML formula.

    A formula that references another formula by name (e.g. ``margin`` defined as
    ``[net_revenue] / [gross_revenue]`` where ``net_revenue`` is itself a formula)
    must have the nested formula expanded inline; otherwise the bare reference is
    left unresolved and points at a column the derived subquery never projects.
    Self/cyclic references are left untouched to avoid infinite recursion.
    """
    if not expr or not formula_expr_by_name:
        return expr

    def _replace(match: re.Match[str]) -> str:
        token = match.group(1)
        # Only inline unqualified references to a known formula name.
        if "::" not in token and token in formula_expr_by_name and token not in _seen:
            inner = _inline_formula_refs(formula_expr_by_name[token], formula_expr_by_name, _seen | {token})
            return f"({inner})"
        return match.group(0)

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


def _extract_all_join_refs(
    expr: str | None, table_path_lookup: dict[str, str] | None = None
) -> list[tuple[tuple[str | None, str], tuple[str | None, str]]]:
    """Extract every ``left = right`` key pair from a (possibly composite) join.

    A composite ON clause like ``[a::x] = [b::y] AND [a::p] = [b::q]`` yields all
    equality pairs, so composite-key relationships keep both columns instead of
    silently dropping all but the first pair. Only pure equality conjuncts are
    treated as key pairs; range/non-equi predicates (e.g.
    ``[a::date] BETWEEN [b::start] AND [b::end]``) are skipped so they are not
    mistaken for additional equality keys.
    """
    if not expr:
        return []

    pairs: list[tuple[tuple[str | None, str], tuple[str | None, str]]] = []
    for conjunct in _AND_SPLIT.split(expr):
        tokens = _TML_REF.findall(conjunct)
        if len(tokens) < 2:
            tokens = [f"{t[0]}.{t[1]}" for t in _TML_DOT_REF.findall(conjunct)]

        # Only an equality between exactly two refs is a join key pair. A `=` that
        # is part of `<=`/`>=`/`!=` is not a plain equality.
        equalities = _EQUALITY_OP.findall(conjunct)
        if len(tokens) != 2 or len(equalities) != 1:
            continue

        left = _parse_ref_token(tokens[0])
        right = _parse_ref_token(tokens[1])
        if table_path_lookup:
            if left[0] in table_path_lookup:
                left = (table_path_lookup[left[0]], left[1])
            if right[0] in table_path_lookup:
                right = (table_path_lookup[right[0]], right[1])
        pairs.append((left, right))
    return pairs


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


def _simple_table_column(sql: str | None, tables: set[str]) -> tuple[str, str] | None:
    """Return the ``(table, column)`` pair if ``sql`` is exactly ``table.column``.

    Only matches when ``table`` is one of the joined ``tables`` so the reference
    corresponds to a column projected by the derived subquery.
    """
    if not sql:
        return None
    match = _TML_DOT_REF.fullmatch(sql.strip())
    if not match:
        return None
    table, column = match.group(1), match.group(2)
    if table in tables:
        return (table, column)
    return None


def _expose_joined_columns(
    sql: str | None,
    tables: set[str],
    dimensions: list[Dimension],
    metrics: list[Metric],
    base_table: str | None = None,
    primary_key: str | None = None,
    foreign_keys: dict[str, tuple[str, str]] | None = None,
) -> str | None:
    """Rewrite a joined model's derived SQL so its columns are queryable.

    A joined Model TML becomes a derived table (``FROM (<sql>) AS t``) whose
    column expressions still carry inner table qualifiers like ``sales.amount``.
    Those qualifiers are out of scope once the join is wrapped in a subquery, so
    a normal query (e.g. ``SELECT sales.amount FROM (...) AS t``) fails with
    "table sales not found". This replaces the ``SELECT *`` projection with an
    explicit list that aliases each referenced ``table.column`` to a stable,
    unqualified output name, then rewrites the dimension/metric SQL to use those
    aliases so the outer query stays in scope.

    The model's ``primary_key`` is also passed through by the SQL generator as a
    bare column, so the base table's primary key is exposed under that name when
    no model column already projects it.

    ``foreign_keys`` maps each relationship join key (the bare column name the
    SQL generator selects when this model participates in a cross-model join) to
    the ``(table, column)`` that backs it. Each one is projected under its bare
    name when no model column already exposes it, so the derived subquery stays
    joinable.
    """
    if not sql or "SELECT * FROM " not in sql:
        return sql

    # Collect every distinct `table.column` referenced by the model's columns
    # where `table` is one of the joined tables. Preserve first-seen order so
    # the generated projection is deterministic.
    projection: dict[tuple[str, str], str] = {}

    def _collect(expr: str | None) -> None:
        if not expr:
            return
        for table, column in _TML_DOT_REF.findall(expr):
            if table in tables:
                projection.setdefault((table, column), f"{table}__{column}")

    for dim in dimensions:
        _collect(dim.sql)
    for metric in metrics:
        _collect(metric.sql)

    if not projection:
        return sql

    # ThoughtSpot formulas also use unqualified references (e.g.
    # `[gross_revenue] - [sales::discount]`), which convert to bare identifiers
    # like `gross_revenue`. Map each such column name to its projected alias when
    # exactly one joined table projects that column, so the rewritten expression
    # uses the in-scope output alias instead of an out-of-scope bare column.
    def _build_map(pairs: list[tuple[str, str]]) -> dict[str, str]:
        """Map name -> alias, dropping names that map to conflicting aliases."""
        mapping: dict[str, str] = {}
        ambiguous: set[str] = set()
        for name, alias in pairs:
            if name in mapping and mapping[name] != alias:
                ambiguous.add(name)
            else:
                mapping[name] = alias
        for name in ambiguous:
            mapping.pop(name, None)
        return mapping

    # Physical DB column names map to their projected aliases.
    physical_map = _build_map([(column, alias) for (_table, column), alias in projection.items()])

    # Formulas can also reference another TML column by its model name even when
    # that name differs from the backing DB column (e.g. a column `gross_revenue`
    # mapped to `column_id: sales::gross_amt`, with formula `[gross_revenue] -
    # [discount]`). Build a separate field-name map and let it take precedence:
    # a formula's `[amount]` refers to the TML field `amount`, which may resolve
    # to a different physical column than one literally named `amount`.
    field_map = _build_map(
        [
            (field.name, projection[ref])
            for field in (*dimensions, *metrics)
            if (ref := _simple_table_column(field.sql, tables))
        ]
    )

    bare_to_alias = {**physical_map, **field_map}

    def _rewrite(expr: str | None) -> str | None:
        if not expr:
            return expr

        def _replace(match: re.Match[str]) -> str:
            table = match.group(1)
            column = match.group(2)
            alias = projection.get((table, column))
            return alias if alias else match.group(0)

        def _replace_bare(match: re.Match[str]) -> str:
            return bare_to_alias.get(match.group(1), match.group(0))

        # Rewrite column references only outside quoted string literals so a
        # literal that happens to match a column name is left untouched.
        rewritten = _sub_outside_strings(_TML_DOT_REF, _replace, expr)
        return _sub_outside_strings(_BARE_IDENTIFIER, _replace_bare, rewritten)

    for dim in dimensions:
        dim.sql = _rewrite(dim.sql)
    for metric in metrics:
        metric.sql = _rewrite(metric.sql)

    select_parts = [f"{table}.{column} AS {alias}" for (table, column), alias in projection.items()]

    # Track which bare output names already exist so pass-through keys are not
    # projected twice.
    exposed: set[str] = set(projection.values())

    # The SQL generator passes through `model.primary_key` as a bare column when
    # querying derived models. Expose `<base_table>.<primary_key>` under that
    # name so the key resolves instead of referencing an out-of-scope column.
    if base_table and primary_key and primary_key not in exposed:
        select_parts.append(f"{base_table}.{primary_key} AS {primary_key}")
        exposed.add(primary_key)

    # Relationship foreign keys are also passed through as bare columns when this
    # model is joined to a separately loaded related model. A foreign key that is
    # not already projected by a dimension/metric (or the primary key) would be
    # missing from the subquery, so expose it from its backing table.
    for fk, (fk_table, fk_column) in sorted((foreign_keys or {}).items()):
        if fk and fk not in exposed and fk_table in tables:
            select_parts.append(f"{fk_table}.{fk_column} AS {fk}")
            exposed.add(fk)

    select_list = ", ".join(select_parts)
    return sql.replace("SELECT * FROM ", f"SELECT {select_list} FROM ", 1)


def _resolve_bare_refs_to_db_columns(dimensions: list[Dimension], metrics: list[Metric]) -> None:
    """Rewrite formula bare model-name refs to their backing DB columns in place.

    For a join-less model the SQL generator queries the base table directly, so a
    formula that references another TML column by its model name only works when
    that name equals the backing DB column. Build a map of model name -> DB column
    from the non-formula fields (whose SQL is a plain ``column`` or
    ``table.column``) and rewrite each formula's bare references so they target
    the real column. Only names that differ from their DB column are rewritten;
    string literals are left untouched.
    """
    name_to_db_col: dict[str, str] = {}
    for field in (*dimensions, *metrics):
        sql = field.sql
        if not sql:
            continue
        _table, column = _split_sql_identifier(sql)
        if column and column != field.name:
            name_to_db_col[field.name] = column

    if not name_to_db_col:
        return

    def _replace_bare(match: re.Match[str]) -> str:
        return name_to_db_col.get(match.group(1), match.group(0))

    for field in (*dimensions, *metrics):
        if field.sql:
            field.sql = _sub_outside_strings(_BARE_IDENTIFIER, _replace_bare, field.sql)


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
            model_def = data.get("model")
            # TML Model objects (export_schema_version v2) use `model_tables:` and
            # model-level `columns:`. Legacy Worksheet content nested under `model:`
            # still uses `tables:`/`worksheet_columns:`, so fall back to the
            # worksheet parser for back-compat.
            if isinstance(model_def, dict) and ("model_tables" in model_def or "columns" in model_def):
                return self._parse_model(model_def, data)
            return self._parse_worksheet(model_def, data)

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

    def _parse_model(self, model_def: dict[str, Any] | None, full_def: dict[str, Any]) -> Model | None:
        """Parse a TML Model object (export_schema_version v2).

        Model TML differs from the legacy Worksheet TML:
        - tables live under `model_tables:` (vs worksheet `tables:`)
        - joins are nested inside each `model_tables` entry under `joins:`,
          using `with:`/`on:`/`type:`/`cardinality:` (vs top-level worksheet
          `joins:` with `source`/`destination`/`is_one_to_one`)
        - fields live under model-level `columns:` (vs `worksheet_columns:`)
        """
        if not model_def:
            return None

        name = model_def.get("name")
        if not name:
            return None

        description = model_def.get("description")
        model_tables = model_def.get("model_tables") or []

        table_name_lookup = self._table_name_lookup(model_tables)

        # Each model_tables entry may carry an `alias` used in column_id paths
        # and join expressions. The alias is the role identifier (e.g.
        # `ship_country`/`bill_country` both backed by `countries`), so keep the
        # alias as the join/relationship/qualifier name and only track the
        # underlying table for emitting `JOIN <table> AS <alias>`. Resolving the
        # alias away here would collapse distinct role-playing joins into a
        # single ambiguous `countries` relation.
        alias_to_table: dict[str, str] = {}
        for table in model_tables:
            table_name = table.get("name") or table.get("id")
            alias = table.get("alias")
            if alias and table_name:
                alias_to_table[alias] = table_name
        # Build the path lookup used to resolve column_id/expression table refs.
        # Aliases resolve to themselves so qualifiers stay role-scoped.
        path_lookup: dict[str, str] = dict(table_name_lookup)
        for alias in alias_to_table:
            path_lookup[alias] = alias
        # An aliased entry may still be referenced by its `id`/`name` in a
        # `column_id` or `on` expression (e.g. `id: countries_tbl, alias:
        # ship_country` with `column_id: countries_tbl::name`). The SQL relation in
        # scope is the alias (`JOIN countries AS ship_country`), so map the entry's
        # id/name tokens to the alias too; otherwise the qualifier resolves to the
        # backing table name, which is not in scope.
        for table in model_tables:
            alias = table.get("alias")
            if not alias:
                continue
            for token in (table.get("id"), table.get("name")):
                if token and token != alias:
                    path_lookup[token] = alias

        # Flatten nested joins (one per model_tables entry) into the same shape
        # the worksheet join helpers consume. When a table carries an `alias`,
        # its `column_id`/`on` qualifiers use the alias (e.g. `o::id`), so the
        # join `source` must be the alias too (not the backing table name) for
        # the join-direction logic and SQL relation name to stay consistent.
        flat_joins: list[dict[str, Any]] = []
        for table in model_tables:
            source = table.get("alias") or table.get("name") or table.get("id")
            for join_def in table.get("joins") or []:
                destination = join_def.get("with")
                if not source or not destination:
                    continue
                # Keep an aliased destination as-is (the role name); only resolve
                # non-aliased ids to their table name.
                if destination in alias_to_table:
                    resolved_dest = destination
                else:
                    resolved_dest = table_name_lookup.get(destination, destination)
                # PyYAML (YAML 1.1) parses the bare `on:` key as the boolean True.
                on_value = join_def.get("on")
                if on_value is None and True in join_def:
                    on_value = join_def.get(True)
                flat_joins.append(
                    {
                        "source": source,
                        "destination": resolved_dest,
                        "type": join_def.get("type"),
                        "on": on_value,
                        "cardinality": join_def.get("cardinality"),
                    }
                )

        sql, base_table = self._build_join_sql(model_tables, flat_joins, path_lookup, table_name_lookup, alias_to_table)
        relationships = self._parse_model_relationships(flat_joins, path_lookup, table_name_lookup)

        formulas = model_def.get("formulas") or []
        formula_by_id = {f.get("id"): f for f in formulas if f.get("id")}
        formula_by_name = {f.get("name"): f for f in formulas if f.get("name")}
        # Map formula name -> expression so nested formula references can be
        # inlined before the expression is converted/aliased.
        formula_expr_by_name = {f.get("name"): f.get("expr") for f in formulas if f.get("name") and f.get("expr")}

        dimensions: list[Dimension] = []
        metrics: list[Metric] = []

        for col_def in model_def.get("columns") or []:
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
            col_description = col_def.get("description")
            format_pattern = properties.get("format_pattern")

            sql_expr = None
            is_formula = False
            if formula_id and formula_id in formula_by_id:
                sql_expr = formula_by_id[formula_id].get("expr")
                is_formula = True
            elif formula_id and formula_id in formula_by_name:
                sql_expr = formula_by_name[formula_id].get("expr")
                is_formula = True
            elif col_name in formula_by_name:
                sql_expr = formula_by_name[col_name].get("expr")
                is_formula = True

            # Inline references to other formulas so nested formula expressions
            # resolve to physical columns instead of unprojected formula names.
            if is_formula:
                sql_expr = _inline_formula_refs(sql_expr, formula_expr_by_name)

            if not sql_expr and column_id:
                if "::" in column_id:
                    path_id, col_ref = column_id.split("::", 1)
                    table_name = path_lookup.get(path_id)
                    if table_name:
                        sql_expr = f"{table_name}.{col_ref}"
                    else:
                        sql_expr = col_ref
                else:
                    sql_expr = column_id

            sql_expr = _convert_tml_expr(sql_expr, path_lookup)

            if column_type == "MEASURE":
                agg, unsupported_func = _map_aggregation(properties.get("aggregation"))
                metric_sql = sql_expr
                if agg:
                    metric = Metric(
                        name=col_name,
                        agg=agg,
                        sql=metric_sql,
                        label=label,
                        description=col_description,
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
                        description=col_description,
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
                    description=col_description,
                    format=format_pattern,
                )
                dimensions.append(dim)

        # The SQL generator always selects `model.primary_key` from derived models
        # as a bare column, so it must name a column that exists on the base
        # table. Prefer a dimension named `id`; otherwise infer the key from a
        # base-table column so a model whose key is not literally `id` (e.g.
        # `order_key`) does not project a non-existent `id` column.
        primary_key = self._infer_model_primary_key(dimensions, base_table)

        # A joined model is exported as derived SQL (FROM (<sql>) AS t); rewrite
        # its `SELECT *` into explicit aliased columns and update the dimension/
        # metric SQL so the inner table qualifiers stay in scope when queried.
        if sql:
            known_tables = set(table_name_lookup.values())
            # Aliases (role-playing or a base-table alias) are the in-scope
            # relation names that qualify columns like `o.id`/`ship_country.name`,
            # so they must be recognized when projecting the derived columns.
            known_tables.update(alias_to_table.keys())
            for join_def in flat_joins:
                known_tables.add(join_def.get("source"))
                known_tables.add(join_def.get("destination"))
            known_tables.discard(None)

            # Resolve each relationship's join keys to the `(table, column)` they
            # come from in the join `on` clauses, so the derived projection can
            # expose them for cross-model queries. The SQL generator passes through
            # the foreign key (many_to_one) or the local primary key (one_to_one/
            # one_to_many) as bare columns from this derived subquery, so cover
            # both sides.
            fk_refs: dict[str, tuple[str, str]] = {}
            key_names: set[str] = set()
            for rel in relationships:
                if rel.foreign_key:
                    key_names.update(rel.foreign_key_columns)
                if rel.primary_key:
                    key_names.update(rel.primary_key_columns)
            for join_def in flat_joins:
                for left, right in _extract_all_join_refs(join_def.get("on"), path_lookup):
                    for ref in (left, right):
                        if ref and ref[1] in key_names and ref[0] in known_tables:
                            fk_refs.setdefault(ref[1], ref)

            sql = _expose_joined_columns(sql, known_tables, dimensions, metrics, base_table, primary_key, fk_refs)
        else:
            # Single-table (join-less) model: no derived subquery wraps it, so the
            # `_expose_joined_columns` rewrite never runs. A formula that refers to
            # another TML column by its model name (e.g. column `gross_revenue`
            # mapped from `sales::gross_amt`, formula `[gross_revenue] -
            # [discount]`) keeps the bare model name, which is not a real column on
            # the base table. Rewrite those bare refs to the backing DB column so
            # the query stays valid.
            _resolve_bare_refs_to_db_columns(dimensions, metrics)

        default_time_dimension = None
        default_grain = None
        for dim in dimensions:
            if dim.type == "time":
                default_time_dimension = dim.name
                default_grain = dim.granularity
                break

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
        setattr(model, "_source_tml_type", "model")

        return model

    def _infer_model_primary_key(self, dimensions: list[Dimension], base_table: str | None) -> str:
        """Infer a queryable primary key column for a TML Model.

        Prefer a dimension named ``id`` but resolve it to its backing physical
        column (a column named ``id`` may map to a differently named DB column,
        e.g. ``column_id: orders::order_key``). Only accept it when its SQL is
        unqualified or belongs to the base table; a joined-table ``id`` (e.g.
        ``customers::id``) is not the base model's key. Otherwise, if the base
        table is known, keep ``id`` when a base-table column actually resolves to
        ``id``; failing that, use the first base-table column so the key
        references a real column. Fall back to ``id`` only when no better
        candidate exists.
        """
        for dim in dimensions:
            if dim.name.lower() == "id":
                table, column = _split_sql_identifier(dim.sql)
                if table is None or base_table is None or table == base_table:
                    return column or dim.name
                break

        if base_table:
            base_columns: list[str] = []
            for dim in dimensions:
                table, column = _split_sql_identifier(dim.sql)
                if column and (table is None or table == base_table):
                    base_columns.append(column)
            if "id" in base_columns:
                return "id"
            if base_columns:
                return base_columns[0]

        return "id"

    def _parse_model_relationships(
        self,
        joins: list[dict[str, Any]],
        table_path_lookup: dict[str, str] | None = None,
        table_name_lookup: dict[str, str] | None = None,
    ) -> list[Relationship]:
        """Build relationships from flattened model_tables joins.

        Model joins carry `cardinality:` directly (MANY_TO_ONE/ONE_TO_ONE/
        ONE_TO_MANY/MANY_TO_MANY), unlike worksheet joins which only flag
        `is_one_to_one`.
        """
        relationships: list[Relationship] = []

        for join_def in joins:
            source = join_def.get("source")
            destination = join_def.get("destination")
            if not source or not destination:
                continue

            join_type = _normalize(join_def.get("type")) or "INNER"
            on_value = join_def.get("on")
            lookup = table_path_lookup or table_name_lookup
            # Composite predicates carry more than one key pair; keep all of them
            # so cross-model joins do not silently drop part of the key.
            ref_pairs = _extract_all_join_refs(on_value, lookup)

            if table_name_lookup:
                source = table_name_lookup.get(source, source)
                destination = table_name_lookup.get(destination, destination)

            foreign_keys: list[str] = []
            primary_keys: list[str] = []
            for left, right in ref_pairs:
                left_table, left_col = left
                right_table, right_col = right

                if left_table == source and right_table == destination:
                    foreign_keys.append(left_col)
                    primary_keys.append(right_col)
                elif left_table == destination and right_table == source:
                    foreign_keys.append(right_col)
                    primary_keys.append(left_col)
                else:
                    if left_table == source:
                        foreign_keys.append(left_col)
                    if right_table == destination:
                        primary_keys.append(right_col)

            foreign_key = foreign_keys[0] if len(foreign_keys) == 1 else (foreign_keys or None)
            primary_key = primary_keys[0] if len(primary_keys) == 1 else (primary_keys or None)

            cardinality = _normalize(join_def.get("cardinality"))
            rel_type = _CARDINALITY_MAP.get(cardinality or "", "many_to_one")
            if join_type in {"RIGHT_OUTER", "FULL_OUTER", "OUTER"} and cardinality not in _CARDINALITY_MAP:
                rel_type = "many_to_many"

            # The keys above follow the `many_to_one` convention: `foreign_key` on
            # the source (local) side, `primary_key` on the destination (related)
            # side. Sidemantic treats `one_to_many` and `one_to_one` as edges
            # where the related model owns the `foreign_key` and the local model
            # owns the `primary_key`, so swap them to match that key direction.
            if rel_type in ("one_to_many", "one_to_one"):
                foreign_key, primary_key = primary_key, foreign_key

            relationships.append(
                Relationship(
                    name=destination,
                    type=rel_type,
                    foreign_key=foreign_key,
                    primary_key=primary_key,
                )
            )

        return relationships

    def _build_join_sql(
        self,
        tables: list[dict[str, Any]],
        joins: list[dict[str, Any]],
        table_path_lookup: dict[str, str] | None = None,
        table_name_lookup: dict[str, str] | None = None,
        alias_to_table: dict[str, str] | None = None,
    ) -> tuple[str | None, str | None]:
        base_table = None
        base_alias = None
        if tables:
            base_table = tables[0].get("name") or tables[0].get("id")
            base_alias = tables[0].get("alias")

        # When the base table is aliased, its `column_id`/`on` qualifiers use the
        # alias (e.g. `o::id`), so the relation in scope is the alias. Emit
        # `FROM <table> AS <alias>` and key the join tracking on the alias so the
        # alias resolves instead of failing with "table o not found".
        base_relation = base_alias if (base_alias and base_alias != base_table) else base_table

        joined: set[str] = set()
        if base_relation:
            joined.add(base_relation)

        clauses: list[str] = []
        if base_relation:
            clauses.append(f"{base_table} AS {base_alias}" if base_relation != base_table else base_table)

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

            if not base_relation:
                base_table = source
                base_relation = source
                clauses.append(base_relation)
                joined.add(base_relation)

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

            # Role-playing joins keep the alias as the relation name; emit
            # `<table> AS <alias>` so two roles backed by the same table do not
            # produce an ambiguous, duplicated join.
            backing = alias_to_table.get(right) if alias_to_table else None
            right_clause = f"{backing} AS {right}" if backing and backing != right else right
            clauses.append(f"{join_keyword} JOIN {right_clause} ON {on_expr}")
            joined.add(right)

        if not clauses:
            return None, None

        if len(clauses) == 1:
            if base_relation != base_table:
                # Single but aliased base table (e.g. `name: orders, alias: o`
                # with `column_id: o::amount`): the fields reference `o.*`, so the
                # alias must be in scope. Emit `SELECT * FROM orders AS o`, which
                # `_expose_joined_columns` rewrites into queryable output aliases.
                return f"SELECT * FROM {clauses[0]}", base_relation
            # Single, unaliased base table: `model.table` is the real table name
            # (no subquery wraps it, so there is nothing to alias).
            return None, base_table

        sql = "SELECT * FROM " + clauses[0]
        for clause in clauses[1:]:
            sql += f"\n{clause}"

        return sql, base_relation

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
        # Model `column_id`s reference a table by its `name` even when an `id`
        # is present (e.g. `column_id: orders::amount` for a table with
        # `id: orders_tbl`). Map `name -> name` too so those qualifiers resolve
        # instead of being dropped, which would emit ambiguous unqualified
        # columns in joined models. `id -> name` mappings take precedence.
        for table in tables:
            table_name = table.get("name")
            if table_name and table_name not in lookup:
                lookup[table_name] = table_name
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
            if rel.type in {"one_to_many", "one_to_one"}:
                left_table = rel.name
                left_key = rel.sql_expr
                right_table = base_table
                right_key = model.primary_key
            else:
                left_table = base_table
                left_key = rel.sql_expr
                right_table = rel.name
                right_key = rel.related_key
            on_expr = f"[{left_table}::{left_key}] = [{right_table}::{right_key}]"
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
