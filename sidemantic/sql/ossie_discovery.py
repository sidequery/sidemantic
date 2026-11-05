"""SQL metadata for the virtual relations exposed by Ossie Layer 2."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from sidemantic.interchange.ossie.identifier import normalize_identifier

_COLUMNS = (
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "ordinal_position",
    "column_default",
    "is_nullable",
    "data_type",
)
_TABLES = ("table_catalog", "table_schema", "table_name", "table_type")


def _name(value: str) -> str:
    return value[1:-1].replace('""', '"') if value.startswith('"') else value


def is_ossie_discovery(statement: exp.Expression) -> bool:
    return isinstance(statement, exp.Describe) or any(
        table.db.lower() == "information_schema" and table.name.lower() in {"columns", "tables"}
        for table in statement.find_all(exp.Table)
    )


def _relations(graph, dialect="duckdb"):
    for model in graph.models.values():
        meta = model.metadata or {}
        if not meta.get("ossie_scope_name"):
            continue
        columns = []
        for dimension in model.dimensions:
            source = (dimension.metadata or {}).get("ossie_source_name")
            if source is not None:
                columns.append((_name(source), (dimension.logical_data_type or "UNKNOWN").upper()))
        for key, metric in sorted(
            graph.metrics.items(),
            key=lambda item: ((item[1].metadata or {}).get("ossie_dataset_metric_index", float("inf")), item[0]),
        ):
            metadata = metric.metadata or {}
            if metadata.get("ossie_dataset_metric") and graph.metric_owners.get(key) == model.name:
                columns.append(
                    (
                        _name(metadata.get("ossie_source_name", metric.name)),
                        (metric.logical_data_type or "UNKNOWN").upper() + " MEASURE",
                    )
                )
        yield None, meta["ossie_scope_name"], meta.get("ossie_source_name", model.name), columns, None
    for name, descriptors in (graph.metadata or {}).get("ossie_view_columns", {}).items():
        table = sqlglot.parse_one(name, read=dialect, into=exp.Table)
        scope = table.args["db"].sql() if table.db else "main"
        columns = []
        for descriptor in descriptors:
            dtype = descriptor.get("data_type", "UNKNOWN").upper()
            if descriptor.get("is_measure") and "MEASURE" not in dtype:
                dtype += " MEASURE"
            columns.append((descriptor["name"], dtype))
        yield table.catalog or None, scope, table.this.sql(), columns, name


def _rows(names, rows):
    statements = []
    for row in rows:
        statements.append(
            exp.select(
                *[exp.alias_(exp.convert(value), name, quoted=True) for name, value in zip(names, row, strict=True)]
            )
        )
    if not statements:
        return exp.select(*[exp.alias_(exp.Null(), name, quoted=True) for name in names]).where(exp.false())
    result = statements[0]
    for statement in statements[1:]:
        result = exp.Union(this=result, expression=statement, distinct=False)
    return result


def compile_ossie_discovery(sql: str, graph, dialect: str = "duckdb") -> str | None:
    """Expose measure markers using DESCRIBE and standard information-schema SQL.

    DuckDB preserves its complete physical catalog with UNION ALL BY NAME. Other
    dialects retain the standard metadata columns shared by semantic relations.
    """
    relations = list(_relations(graph, dialect))
    if not relations:
        return None
    statements = sqlglot.parse(sql, read=dialect)
    if len(statements) != 1 or statements[0] is None:
        return None
    statement = statements[0]
    if isinstance(statement, exp.Describe):
        table = statement.this
        if isinstance(table, exp.Query):
            from sidemantic.sql.ossie_relational import RelationalCompiler, compile_ossie_view_definition

            if not RelationalCompiler(graph, dialect).relevant(table):
                return statement.sql(dialect=dialect)
            _, descriptors = compile_ossie_view_definition(table.sql(dialect=dialect), graph, dialect)
            return _rows(
                ("column_name", "column_type", "null", "key", "default", "extra"),
                [
                    (
                        column["name"],
                        column.get("data_type", "UNKNOWN")
                        + (
                            " MEASURE"
                            if column.get("is_measure") and "MEASURE" not in column.get("data_type", "UNKNOWN")
                            else ""
                        ),
                        "YES",
                        None,
                        None,
                        None,
                    )
                    for column in descriptors
                ],
            ).sql(dialect=dialect)
        if not isinstance(table, exp.Table):
            return None
        name = normalize_identifier(table.this.sql())
        scope = normalize_identifier(table.args["db"].sql()) if table.db else None
        from sidemantic.sql.ossie_views import resolve_view_name

        view_name = resolve_view_name(graph, table, dialect)
        matches = [
            columns
            for catalog, schema, relation, columns, registered_name in relations
            if (registered_name is not None and registered_name == view_name)
            or (
                registered_name is None
                and view_name is None
                and not table.catalog
                and normalize_identifier(relation) == name
                and (scope is None or normalize_identifier(schema) == scope)
            )
        ]
        if len(matches) != 1:
            return statement.sql(dialect=dialect)
        return _rows(
            ("column_name", "column_type", "null", "key", "default", "extra"),
            [(name, dtype, "YES", None, None, None) for name, dtype in matches[0]],
        ).sql(dialect=dialect)
    changed = False
    for table in list(statement.find_all(exp.Table)):
        kind = table.name.lower()
        if table.db.lower() != "information_schema" or kind not in {"columns", "tables"}:
            continue
        names = _COLUMNS if kind == "columns" else _TABLES
        rows = []
        for catalog, scope, relation, columns, _ in relations:
            catalog = exp.convert(catalog) if catalog is not None else exp.CurrentCatalog()
            schema, relation = _name(scope), _name(relation)
            if kind == "tables":
                rows.append((catalog, schema, relation, "VIEW"))
            else:
                rows.extend(
                    (catalog, schema, relation, name, index, None, "YES", dtype)
                    for index, (name, dtype) in enumerate(columns, 1)
                )
        physical = exp.select(*(["*"] if dialect == "duckdb" else names)).from_(f"information_schema.{kind}")
        exclusions = [
            exp.and_(
                exp.column("table_catalog").eq(exp.convert(catalog) if catalog is not None else exp.CurrentCatalog()),
                exp.column("table_schema").eq(_name(scope)),
                exp.column("table_name").eq(_name(name)),
            )
            for catalog, scope, name, _, _ in relations
        ]
        physical = physical.where(exp.not_(exp.or_(*exclusions)))
        union = exp.Union(
            this=physical, expression=exp.Subquery(this=_rows(names, rows)), distinct=False, by_name=dialect == "duckdb"
        )
        alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(table.name))
        table.replace(exp.Subquery(this=union, alias=alias.copy()))
        changed = True
    return statement.sql(dialect=dialect) if changed else None
