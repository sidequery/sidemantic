"""Generate semantic models from live database schema (``sidemantic init --from``).

Introspects tables through ``information_schema``, profiles column shapes when
the source is DuckDB-backed (files, local databases), and writes native model
YAML with inferred dimensions, metrics, and relationships. The output is a
starting point the user edits, not a final model.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

# Columns with more distinct values than this are treated as free-form text,
# not categorical dimensions, when profiling is available.
CATEGORICAL_CARDINALITY_LIMIT = 200

_EXCLUDED_SCHEMAS = {"information_schema", "pg_catalog", "system", "temp"}


@dataclass
class ColumnInfo:
    name: str
    data_type: str

    @property
    def category(self) -> str:
        upper = self.data_type.upper()
        if any(token in upper for token in ("TIMESTAMP", "DATETIME", "DATE")):
            return "time"
        if "BOOL" in upper:
            return "boolean"
        if any(token in upper for token in ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL", "NUMBER")):
            return "numeric"
        return "string"


@dataclass
class TableInfo:
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: int | None = None
    distinct_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class BootstrapResult:
    tables: list[TableInfo]
    model_dicts: dict[str, dict]
    notes: list[str] = field(default_factory=list)


def introspect_connection(
    connection: str, init_sql: list[str] | None = None, *, profile: bool = True
) -> list[TableInfo]:
    """Read table/column shapes from a connection string.

    DuckDB sources are profiled (row counts, distinct counts); other databases
    are introspected from ``information_schema`` alone so we never run
    potentially expensive scans against a warehouse.
    """

    if connection.startswith("duckdb://"):
        import duckdb

        db_path = connection.removeprefix("duckdb:///") or ":memory:"
        con = duckdb.connect(db_path, read_only=db_path != ":memory:" and not init_sql)
        try:
            for statement in init_sql or []:
                con.execute(statement)
            tables = _introspect_information_schema(con)
            if profile:
                for table in tables:
                    _profile_table(con, table)
            return tables
        finally:
            con.close()

    from sidemantic import SemanticLayer

    layer = SemanticLayer(connection=connection, auto_register=False)
    try:
        return _introspect_information_schema(layer.adapter)
    finally:
        close = getattr(layer.adapter, "close", None)
        if close:
            close()


def _introspect_information_schema(executor) -> list[TableInfo]:
    excluded = ", ".join(f"'{schema}'" for schema in sorted(_EXCLUDED_SCHEMAS))
    rows = executor.execute(
        "select table_name, column_name, data_type from information_schema.columns "
        f"where lower(table_schema) not in ({excluded}) order by table_name, ordinal_position"
    ).fetchall()
    tables: dict[str, TableInfo] = {}
    for table_name, column_name, data_type in rows:
        info = tables.setdefault(table_name, TableInfo(name=table_name))
        info.columns.append(ColumnInfo(name=column_name, data_type=str(data_type)))
    return list(tables.values())


def _profile_table(con, table: TableInfo) -> None:
    quoted = table.name.replace('"', '""')
    try:
        table.row_count = con.execute(f'select count(*) from "{quoted}"').fetchone()[0]
    except Exception:
        return
    candidates = [
        column.name
        for column in table.columns
        if column.category == "string" or column.name.lower() == "id" or column.name.lower().endswith("_id")
    ]
    if not candidates:
        return
    selects = ", ".join(f'approx_count_distinct("{name}")' for name in candidates)
    try:
        counts = con.execute(f'select {selects} from "{quoted}"').fetchone()
    except Exception:
        return
    table.distinct_counts = dict(zip(candidates, counts))


def _model_name(table_name: str) -> str:
    import re

    name = re.sub(r"[^0-9A-Za-z_]", "_", table_name).strip("_").lower() or "model"
    return f"t_{name}" if name[0].isdigit() else name


def _singulars(name: str) -> set[str]:
    forms = {name}
    if name.endswith("ies"):
        forms.add(name[:-3] + "y")
    if name.endswith("s"):
        forms.add(name[:-1])
    return forms


def _pick_primary_key(table: TableInfo) -> str | None:
    names = {column.name.lower(): column.name for column in table.columns}
    if "id" in names:
        return names["id"]
    for singular in _singulars(_model_name(table.name)):
        candidate = f"{singular}_id"
        if candidate in names:
            return names[candidate]
    if table.row_count:
        for column in table.columns:
            if table.distinct_counts.get(column.name) == table.row_count and column.name.lower().endswith("_id"):
                return column.name
    return None


def generate_model_dict(table: TableInfo, all_model_names: set[str]) -> dict:
    """Infer one native model definition from an introspected table."""

    model_name = _model_name(table.name)
    primary_key = _pick_primary_key(table)
    key_columns = {primary_key.lower()} if primary_key else set()

    relationships = []
    for column in table.columns:
        lowered = column.name.lower()
        if not lowered.endswith("_id") or lowered in key_columns:
            continue
        base = lowered.removesuffix("_id")
        for related in (base, f"{base}s", f"{base[:-1]}ies" if base.endswith("y") else None):
            if related and related in all_model_names and related != model_name:
                relationships.append({"name": related, "type": "many_to_one", "foreign_key": column.name})
                key_columns.add(lowered)
                break

    dimensions = []
    metrics = [{"name": "record_count", "agg": "count"}]
    for column in table.columns:
        lowered = column.name.lower()
        if lowered in key_columns or lowered == "id" or (primary_key and column.name == primary_key):
            continue
        category = column.category
        if category == "time":
            dimensions.append({"name": column.name, "type": "time", "granularity": "day"})
        elif category == "boolean":
            dimensions.append({"name": column.name, "type": "boolean"})
        elif category == "string":
            if lowered.endswith("_id"):
                continue
            distinct = table.distinct_counts.get(column.name)
            if distinct is not None and (
                distinct > CATEGORICAL_CARDINALITY_LIMIT
                or (table.row_count and distinct >= table.row_count and table.row_count > CATEGORICAL_CARDINALITY_LIMIT)
            ):
                continue
            dimensions.append({"name": column.name, "type": "categorical"})
        elif category == "numeric":
            metrics.append({"name": f"total_{lowered}", "agg": "sum", "sql": column.name})

    model: dict = {"name": model_name, "table": table.name}
    if primary_key:
        model["primary_key"] = primary_key
    if relationships:
        model["relationships"] = relationships
    if dimensions:
        model["dimensions"] = dimensions
    model["metrics"] = metrics
    return model


def bootstrap_models(connection: str, init_sql: list[str] | None = None, *, profile: bool = True) -> BootstrapResult:
    """Introspect a connection and infer a native model per table."""

    tables = [table for table in introspect_connection(connection, init_sql, profile=profile) if table.columns]
    all_model_names = {_model_name(table.name) for table in tables}
    model_dicts: dict[str, dict] = {}
    notes: list[str] = []
    for table in sorted(tables, key=lambda item: item.name):
        model = generate_model_dict(table, all_model_names)
        model_dicts[model["name"]] = model
        summary = f"{model['name']}: {len(model.get('dimensions', []))} dimension(s), {len(model['metrics'])} metric(s)"
        if model.get("relationships"):
            summary += f", joins {', '.join(rel['name'] for rel in model['relationships'])}"
        if not model.get("primary_key"):
            summary += " (no primary key detected; set one before joining)"
        notes.append(summary)
    return BootstrapResult(tables=tables, model_dicts=model_dicts, notes=notes)


def write_model_files(result: BootstrapResult, models_dir: Path, *, force: bool = False) -> list[Path]:
    """Write one native YAML file per generated model."""

    models_dir.mkdir(parents=True, exist_ok=True)
    conflicts = [f"{name}.yml" for name in result.model_dicts if (models_dir / f"{name}.yml").exists()]
    if conflicts and not force:
        listing = ", ".join(sorted(conflicts))
        raise FileExistsError(
            f"Refusing to overwrite existing model files in {models_dir}: {listing} (pass --force to replace)"
        )
    written: list[Path] = []
    for name, model in result.model_dicts.items():
        target = models_dir / f"{name}.yml"
        target.write_text(yaml.dump({"models": [model]}, sort_keys=False, default_flow_style=False))
        written.append(target)
    return written
