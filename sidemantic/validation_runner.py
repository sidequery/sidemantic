"""Noninteractive semantic layer validation."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp

from sidemantic import SemanticLayer, load_from_directory
from sidemantic.validation import validate_metric, validate_model, validate_model_warnings, validate_relationships

if TYPE_CHECKING:
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph


@dataclass
class ValidationReport:
    directory: Path
    # ``errors`` remains the structural/offline error list for API compatibility.
    errors: list[str] = field(default_factory=list)
    warehouse_errors: list[str] = field(default_factory=list)
    connection_errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.all_errors

    @property
    def all_errors(self) -> list[str]:
        return [*self.errors, *self.warehouse_errors, *self.connection_errors]


def validate_directory(
    directory: str | Path,
    *,
    connection: str | None = None,
    init_sql: list[str] | None = None,
    check_keys: bool = False,
    check_queries: bool = True,
) -> ValidationReport:
    """Load and validate semantic definitions, optionally against a warehouse.

    Structural validation is always completed first and does not require a connection. When a
    connection is supplied, warehouse errors and connection failures are collected separately so
    callers can distinguish an invalid model from an unavailable warehouse.
    """
    directory = Path(directory)
    report = ValidationReport(directory=directory)

    layer = SemanticLayer()
    load_from_directory(layer, str(directory))

    if not layer.graph.models:
        report.errors.append("No models found in directory")
        return report

    report.info.append(f"Loaded {len(layer.graph.models)} models")

    for model_name, model in layer.graph.models.items():
        report.errors.extend(validate_model(model))
        report.warnings.extend(validate_model_warnings(model))

        if not model.dimensions:
            report.warnings.append(f"Model '{model_name}' has no dimensions")
        if not model.metrics:
            report.warnings.append(f"Model '{model_name}' has no metrics")

        for metric in model.metrics:
            report.errors.extend(validate_metric(metric, layer.graph))

        # Hex ``view`` resources reference a base model by name and carry their
        # own ``contents``. Both are required by the Hex spec, but views are
        # exempt from the physical-source check in ``validate_model``, so a
        # missing/misspelled base or absent contents would otherwise pass
        # silently on the CLI validation path.
        model_meta = getattr(model, "meta", None) or {}
        if model_meta.get("hex_resource_type") == "view":
            base = model_meta.get("base")
            if not base:
                report.errors.append(f"Hex view '{model_name}' must have a 'base' model reference defined")
            elif base not in layer.graph.models:
                report.errors.append(f"Hex view '{model_name}' references base model '{base}' which doesn't exist")
            if not model_meta.get("contents"):
                report.errors.append(f"Hex view '{model_name}' must have non-empty 'contents' defined")

    for metric in layer.graph.metrics.values():
        report.errors.extend(validate_metric(metric, layer.graph))

    report.errors.extend(validate_relationships(layer.graph))

    if len(layer.graph.models) > 1:
        orphaned = []
        for model_name, model in layer.graph.models.items():
            has_outgoing = bool(model.relationships)
            has_incoming = any(
                any(rel.name == model_name for rel in other.relationships)
                for other_name, other in layer.graph.models.items()
                if other_name != model_name
            )
            if not has_outgoing and not has_incoming:
                orphaned.append(model_name)

        if orphaned:
            report.warnings.append(f"Orphaned models (no relationships): {', '.join(orphaned)}")

    total_dims = sum(len(model.dimensions) for model in layer.graph.models.values())
    total_metrics = sum(len(model.metrics) for model in layer.graph.models.values())
    total_rels = sum(len(model.relationships) for model in layer.graph.models.values())

    report.info.append(f"Total dimensions: {total_dims}")
    report.info.append(f"Total metrics: {total_metrics}")
    report.info.append(f"Total relationships: {total_rels}")

    if connection is not None:
        if report.errors:
            report.info.append("Warehouse validation skipped because structural validation failed")
        else:
            _validate_warehouse(
                directory,
                connection,
                report,
                init_sql=init_sql,
                check_keys=check_keys,
                check_queries=check_queries,
            )

    return report


@dataclass(frozen=True)
class _TableReference:
    name: str
    schema: str | None = None
    catalog: str | None = None
    name_quoted: bool = False
    schema_quoted: bool = False

    @property
    def qualified_name(self) -> str:
        return ".".join(part for part in (self.catalog, self.schema, self.name) if part)


def _split_table_reference(table: str, dialect: str) -> _TableReference | None:
    try:
        parsed = sqlglot.parse_one(table, into=exp.Table, dialect=dialect)
    except Exception:
        return None
    name_identifier = parsed.this
    schema_identifier = parsed.args.get("db")
    return _TableReference(
        name=parsed.name,
        schema=parsed.db or None,
        catalog=parsed.catalog or None,
        name_quoted=bool(getattr(name_identifier, "args", {}).get("quoted")),
        schema_quoted=bool(getattr(schema_identifier, "args", {}).get("quoted")),
    )


def _warehouse_identifier_matches(expected: str, actual: str, dialect: str, *, quoted: bool = False) -> bool:
    if dialect == "snowflake" and not quoted:
        expected = expected.upper()
    return expected == actual


def _warehouse_table_exists(
    table_ref: _TableReference,
    available: set[tuple[str, str]],
    dialect: str,
) -> bool:
    for known_schema, known_name in available:
        if not _warehouse_identifier_matches(table_ref.name, known_name, dialect, quoted=table_ref.name_quoted):
            continue
        if table_ref.schema is None or _warehouse_identifier_matches(
            table_ref.schema,
            known_schema,
            dialect,
            quoted=table_ref.schema_quoted,
        ):
            return True
    return False


def _warehouse_column_type(actual: dict[str, str], column: str, dialect: str) -> str | None:
    if column in actual:
        return actual[column]
    if dialect == "snowflake":
        return actual.get(column.upper())
    return None


def _single_column(expression: str | None, fallback: str | None = None) -> str | None:
    """Return a physical column for a simple column expression, not a computed expression."""
    candidate = expression or fallback
    if not candidate:
        return None
    candidate = candidate.replace("{model}.", "")
    try:
        parsed = sqlglot.parse_one(candidate)
    except Exception:
        return None
    if isinstance(parsed, exp.Column):
        return parsed.name
    return None


def _required_columns(model: "Model", graph: "SemanticGraph") -> dict[str, str | None]:
    """Collect physical source columns and any basic semantic type expectation."""
    columns: dict[str, str | None] = {}

    for column in model.primary_key_columns:
        columns[column] = None
    for key in model.unique_keys or []:
        for column in key:
            columns[column] = None
    for relationship in model.relationships:
        for column in relationship.foreign_key_columns:
            columns[column] = None
        if relationship.type in {"one_to_many", "one_to_one"}:
            for column in relationship.primary_key_columns:
                columns[column] = None

    # Include keys declared by relationships on other models but physically owned by this model.
    for source in graph.models.values():
        for relationship in source.relationships:
            if relationship.name == model.name:
                if relationship.type == "many_to_one" and relationship.primary_key is not None:
                    for column in relationship.primary_key_columns:
                        columns[column] = None
                elif relationship.type in {"one_to_many", "one_to_one"}:
                    for column in relationship.foreign_key_columns:
                        columns[column] = None
                elif relationship.type == "many_to_many" and relationship.primary_key is not None:
                    for column in relationship.primary_key_columns:
                        columns[column] = None
            if relationship.through == model.name:
                junction_source, junction_target = relationship.junction_key_columns()
                for column in junction_source + junction_target:
                    columns[column] = None

    for dimension in model.dimensions:
        column = _single_column(dimension.sql, dimension.name)
        if column:
            columns[column] = dimension.type

    numeric_aggs = {"sum", "avg", "median", "stddev", "stddev_pop", "variance", "variance_pop"}
    for metric in model.metrics:
        column = _single_column(metric.sql)
        if column:
            columns[column] = "numeric" if metric.agg in numeric_aggs else None

    return columns


def _warehouse_type_family(data_type: str) -> str | None:
    normalized = data_type.upper()
    if any(token in normalized for token in ("BOOL",)):
        return "boolean"
    if any(token in normalized for token in ("DATE", "TIME")):
        return "time"
    if any(
        token in normalized for token in ("INT", "DECIMAL", "NUMERIC", "NUMBER", "REAL", "DOUBLE", "FLOAT", "HUGEINT")
    ):
        return "numeric"
    if any(token in normalized for token in ("CHAR", "TEXT", "STRING")):
        return "categorical"
    return None


def _quote_identifier(identifier: str, dialect: str) -> str:
    return exp.to_identifier(identifier, quoted=True).sql(dialect=dialect)


def _model_source_sql(model: "Model", dialect: str) -> str:
    if model.sql:
        return f"({model.sql}) AS {_quote_identifier('_sidemantic_source', dialect)}"
    if not model.table:
        raise ValueError("model has no SQL-queryable table or sql source")
    parsed = sqlglot.parse_one(model.table, into=exp.Table, dialect=dialect)
    return parsed.sql(dialect=dialect)


def _check_key_columns(
    layer,
    model: "Model",
    report: ValidationReport,
    *,
    label: str,
    columns: list[str],
    require_non_null: bool = True,
    require_unique: bool = True,
) -> None:
    adapter = layer.adapter
    dialect = layer.dialect
    source = _model_source_sql(model, dialect)
    quoted = [_quote_identifier(column, dialect) for column in columns]

    if require_non_null:
        null_predicate = " OR ".join(f"{column} IS NULL" for column in quoted)
        null_sql = f"SELECT 1 FROM {source} WHERE {null_predicate} LIMIT 1"
        try:
            if adapter.fetchone(adapter.execute(null_sql)) is not None:
                report.warehouse_errors.append(f"Model '{model.name}' {label} {columns!r} contains NULL values")
        except Exception as exc:
            report.warehouse_errors.append(f"Model '{model.name}' {label} nullability check failed: {exc}")
            return

    if require_unique:
        group_columns = ", ".join(quoted)
        where_clause = ""
        if not require_non_null:
            non_null_predicate = " AND ".join(f"{column} IS NOT NULL" for column in quoted)
            where_clause = f" WHERE {non_null_predicate}"
        duplicate_sql = (
            f"SELECT {group_columns} FROM {source}{where_clause} GROUP BY {group_columns} HAVING COUNT(*) > 1 LIMIT 1"
        )
        try:
            if adapter.fetchone(adapter.execute(duplicate_sql)) is not None:
                report.warehouse_errors.append(f"Model '{model.name}' {label} {columns!r} is not unique")
        except Exception as exc:
            report.warehouse_errors.append(f"Model '{model.name}' {label} uniqueness check failed: {exc}")


def _check_declared_keys(layer, model: "Model", report: ValidationReport) -> None:
    if model.primary_key_columns:
        _check_key_columns(
            layer,
            model,
            report,
            label="primary_key",
            columns=model.primary_key_columns,
        )
    for columns in model.unique_keys or []:
        if columns:
            _check_key_columns(layer, model, report, label="unique_key", columns=columns)


def _check_relationship_cardinality(layer, graph: "SemanticGraph", report: ValidationReport) -> None:
    """Check data assumptions introduced by relationship-scoped keys/cardinality."""
    for source in graph.models.values():
        for relationship in source.relationships:
            target = graph.models.get(relationship.name)
            if target is None or relationship.type in {"cross", "many_to_many"}:
                continue

            # An explicit relationship primary_key is a scoped uniqueness declaration. Its side
            # follows the relationship direction: target for many_to_one, source otherwise.
            if relationship.primary_key is not None:
                key_model = target if relationship.type == "many_to_one" else source
                _check_key_columns(
                    layer,
                    key_model,
                    report,
                    label=f"relationship '{source.name}.{target.name}' primary_key",
                    columns=relationship.primary_key_columns,
                )

            # one_to_one additionally asserts that the target-side foreign key has at most one
            # row per source key. Nullable foreign keys are valid, but non-NULL values must be unique.
            if relationship.type == "one_to_one":
                _check_key_columns(
                    layer,
                    target,
                    report,
                    label=f"relationship '{source.name}.{target.name}' foreign_key",
                    columns=relationship.foreign_key_columns,
                    require_non_null=False,
                )


def _check_join_key_types(
    graph: "SemanticGraph",
    warehouse_types: dict[str, dict[str, str]],
    report: ValidationReport,
) -> None:
    def compare(label: str, left_model: str, left: list[str], right_model: str, right: list[str]) -> None:
        for left_column, right_column in zip(left, right, strict=False):
            left_type = warehouse_types.get(left_model, {}).get(left_column)
            right_type = warehouse_types.get(right_model, {}).get(right_column)
            if not left_type or not right_type:
                continue
            left_family = _warehouse_type_family(left_type)
            right_family = _warehouse_type_family(right_type)
            if left_family and right_family and left_family != right_family:
                report.warehouse_errors.append(
                    f"{label} joins incompatible warehouse types: "
                    f"{left_model}.{left_column} is {left_type}, "
                    f"{right_model}.{right_column} is {right_type}"
                )

    for source in graph.models.values():
        for relationship in source.relationships:
            target = graph.models.get(relationship.name)
            if target is None or relationship.type == "cross" or relationship.sql:
                continue
            label = f"Relationship '{source.name}.{target.name}'"
            if relationship.type == "many_to_one":
                compare(
                    label,
                    source.name,
                    relationship.foreign_key_columns,
                    target.name,
                    relationship.primary_key_columns or target.primary_key_columns,
                )
            elif relationship.type in {"one_to_many", "one_to_one"}:
                compare(
                    label,
                    source.name,
                    relationship.primary_key_columns or source.primary_key_columns,
                    target.name,
                    relationship.foreign_key_columns,
                )
            elif relationship.through and relationship.through in graph.models:
                junction_source, junction_target = relationship.junction_key_columns()
                compare(label, source.name, source.primary_key_columns, relationship.through, junction_source)
                compare(
                    label,
                    relationship.through,
                    junction_target,
                    target.name,
                    relationship.primary_key_columns or target.primary_key_columns,
                )
            else:
                compare(
                    label,
                    source.name,
                    relationship.foreign_key_columns,
                    target.name,
                    relationship.primary_key_columns,
                )


def _representative_query_specs(graph: "SemanticGraph") -> list[tuple[str, list[str], list[str]]]:
    specs: list[tuple[str, list[str], list[str]]] = []
    for model in graph.models.values():
        metrics = [f"{model.name}.{model.metrics[0].name}"] if model.metrics else []
        dimensions = [f"{model.name}.{model.dimensions[0].name}"] if model.dimensions else []
        if metrics or dimensions:
            specs.append((f"model '{model.name}'", metrics, dimensions))

    for source in graph.models.values():
        for relationship in source.relationships:
            target = graph.models.get(relationship.name)
            if target is None or relationship.type == "cross":
                continue
            metrics = [f"{source.name}.{source.metrics[0].name}"] if source.metrics else []
            dimensions = [f"{target.name}.{target.dimensions[0].name}"] if target.dimensions else []
            if not metrics and target.metrics:
                metrics = [f"{target.name}.{target.metrics[0].name}"]
            if not dimensions and source.dimensions:
                dimensions = [f"{source.name}.{source.dimensions[0].name}"]
            if metrics and dimensions:
                specs.append((f"relationship '{source.name}.{target.name}'", metrics, dimensions))
    return specs


def _validate_warehouse(
    directory: Path,
    connection: str,
    report: ValidationReport,
    *,
    init_sql: list[str] | None,
    check_keys: bool,
    check_queries: bool,
) -> None:
    try:
        layer = SemanticLayer(connection=connection, init_sql=init_sql, auto_register=False)
    except Exception as exc:
        report.connection_errors.append(f"Could not connect to warehouse: {exc}")
        return

    try:
        load_from_directory(layer, str(directory))
    except Exception as exc:
        report.connection_errors.append(f"Could not load models with the warehouse connection: {exc}")
        try:
            layer.adapter.close()
        except Exception:
            pass
        return

    try:
        available_tables = layer.adapter.get_tables()
    except Exception as exc:
        report.connection_errors.append(f"Could not inspect warehouse tables: {exc}")
        available_tables = []

    table_names = {
        (str(table.get("schema") or table.get("table_schema") or ""), str(table.get("table_name") or ""))
        for table in available_tables
    }
    warehouse_types: dict[str, dict[str, str]] = {}

    if not report.connection_errors:
        for model in layer.graph.models.values():
            if model.table:
                table_ref = _split_table_reference(model.table, layer.dialect)
                if table_ref is None:
                    report.warehouse_errors.append(
                        f"Model '{model.name}' table '{model.table}' is not a simple warehouse table reference"
                    )
                    continue
                # get_tables() is scoped to the adapter's current catalog on several
                # warehouses. For a catalog-qualified model, let get_columns() inspect
                # the fully qualified source instead of rejecting it against incomplete
                # current-catalog metadata.
                if (
                    table_ref.catalog is None
                    and table_names
                    and not _warehouse_table_exists(table_ref, table_names, layer.dialect)
                ):
                    report.warehouse_errors.append(
                        f"Model '{model.name}' table '{model.table}' does not exist in the warehouse"
                    )
                    continue
                try:
                    if table_ref.catalog and layer.dialect == "bigquery":
                        # BigQueryAdapter addresses tables as dataset + table; the
                        # project is already selected by the connection/client.
                        inspection_name = table_ref.name
                        inspection_schema = table_ref.schema
                    else:
                        inspection_name = table_ref.qualified_name if table_ref.catalog else table_ref.name
                        inspection_schema = None if table_ref.catalog else table_ref.schema
                    warehouse_columns = layer.adapter.get_columns(inspection_name, schema=inspection_schema)
                except Exception as exc:
                    report.warehouse_errors.append(
                        f"Model '{model.name}' table '{model.table}' could not be inspected: {exc}"
                    )
                    continue

                actual = {
                    str(column.get("column_name")): str(column.get("data_type") or "") for column in warehouse_columns
                }
                warehouse_types[model.name] = {}
                for column, expected_type in _required_columns(model, layer.graph).items():
                    actual_type = _warehouse_column_type(actual, column, layer.dialect)
                    if actual_type is None:
                        report.warehouse_errors.append(
                            f"Model '{model.name}' references missing column '{column}' in table '{model.table}'"
                        )
                        continue
                    warehouse_types[model.name][column] = actual_type
                    if expected_type in {"numeric", "time", "boolean"}:
                        actual_family = _warehouse_type_family(actual_type)
                        if actual_family is not None and actual_family != expected_type:
                            report.warehouse_errors.append(
                                f"Model '{model.name}' column '{column}' is declared {expected_type} but warehouse "
                                f"type is {actual_type}"
                            )

            if check_keys and (model.table or model.sql):
                _check_declared_keys(layer, model, report)

        if check_keys:
            _check_relationship_cardinality(layer, layer.graph, report)

        _check_join_key_types(layer.graph, warehouse_types, report)

        if check_queries:
            for label, metrics, dimensions in _representative_query_specs(layer.graph):
                try:
                    compiled = layer.compile(metrics=metrics, dimensions=dimensions).rstrip().rstrip(";")
                    # Keep the wrapper's closing parenthesis on a new line. Compiled SQL ends with
                    # an instrumentation comment, and placing ``)`` on that same line comments it out.
                    layer.adapter.execute(f"SELECT * FROM (\n{compiled}\n) AS _sidemantic_validation LIMIT 0")
                except Exception as exc:
                    report.warehouse_errors.append(f"Representative query for {label} failed: {exc}")

        report.info.append(
            "Warehouse validation: table/column/type/join/query checks"
            + (" plus declared key data checks" if check_keys else " (declared key data checks not requested)")
        )

    try:
        layer.adapter.close()
    except Exception:
        pass
