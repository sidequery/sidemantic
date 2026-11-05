"""Name resolution for the immutable November 2026 Ossie logical profile."""

from collections.abc import Mapping

import sqlglot
from sqlglot import exp

from sidemantic.interchange.ossie.expression_validation import scalar_sql_expression_error
from sidemantic.interchange.ossie.identifier import normalize_identifier

_DIALECTS = {
    "OSSIE_SQL_2026": "snowflake",
    "ANSI_SQL": None,
    "DUCKDB": "duckdb",
    "SPARK": "spark",
    "BIGQUERY": "bigquery",
    "DATABRICKS": "databricks",
    "SNOWFLAKE": "snowflake",
    "POSTGRES": "postgres",
    "REDSHIFT": "redshift",
    "TRINO": "trino",
    "CLICKHOUSE": "clickhouse",
}


def _items(value):
    return value if isinstance(value, (list, tuple)) else ()


def expression_variants(expression):
    if not isinstance(expression, Mapping):
        return
    if isinstance(expression.get("sql"), str):
        yield "OSSIE_SQL_2026", expression["sql"]
    for variant in _items(expression.get("dialects")):
        if not isinstance(variant, Mapping):
            continue
        label, text = variant.get("dialect"), variant.get("expression")
        if label == "OSSIE_SQL_2026" and "sql" in expression:
            continue
        if isinstance(label, str) and isinstance(text, str):
            yield label, text


def identifier_key(identifier, dialect):
    # BigQuery/Databricks quoting is syntactic and does not opt a logical name
    # into ANSI delimited-identifier case sensitivity (the lowering contract).
    quoted = identifier.args.get("quoted") and dialect not in {"BIGQUERY", "DATABRICKS", "SPARK"}
    return identifier.name if quoted else normalize_identifier(identifier.name)


def validate_latest_scope(validator, model, pointer, scope):
    """Validate known SQL dialects; unknown dialects remain opaque source data."""
    datasets = {
        normalize_identifier(dataset["name"]): dataset
        for dataset in _items(model.get("datasets"))
        if isinstance(dataset, Mapping) and isinstance(dataset.get("name"), str)
    }
    fields = {
        name: {
            normalize_identifier(field["name"])
            for field in _items(dataset.get("fields"))
            if isinstance(field, Mapping) and isinstance(field.get("name"), str)
        }
        for name, dataset in datasets.items()
    }

    def emit(message, location, code="ossie.semantic.metric.reference_invalid"):
        validator.emit(code, message, location, scope=scope)

    def check_portable_calls(definition, location):
        if not isinstance(definition, Mapping):
            return
        for label, text in expression_variants(definition.get("expression")):
            if label in _DIALECTS:
                error = scalar_sql_expression_error(text, sqlglot_dialect=_DIALECTS[label])
                if error:
                    emit(error, location, "ossie.semantic.expression.invalid_sql")
                    continue
            if label != "OSSIE_SQL_2026":
                continue
            try:
                tree = sqlglot.parse_one(text, read="snowflake")
            except sqlglot.errors.SqlglotError:
                continue
            if tree is not None and any(
                (call.name.upper() if isinstance(call, exp.Anonymous) else call.sql_name()) == "AGG"
                for call in tree.find_all(exp.Func)
            ):
                emit(
                    "AGG is reserved for measure evaluation and cannot be called in OSSIE_SQL_2026 model declarations.",
                    location,
                    "ossie.semantic.expression.agg_reserved",
                )

    def check_metric(metric, location, owner=None):
        check_portable_calls(metric, location)
        if not isinstance(metric, Mapping):
            return
        for label, text in expression_variants(metric.get("expression")):
            if label not in _DIALECTS:
                continue
            try:
                tree = sqlglot.parse_one(text, read=_DIALECTS[label])
            except sqlglot.errors.SqlglotError:
                continue  # Expression structural/lowering validation owns syntax errors.
            if tree is None:
                continue
            for column in tree.find_all(exp.Column):
                if not isinstance(column.this, exp.Identifier):
                    continue
                field = identifier_key(column.this, label)
                table = column.args.get("table")
                table_key = identifier_key(table, label) if isinstance(table, exp.Identifier) else None
                if owner is not None:
                    if len(column.parts) == 1 and field in fields[owner]:
                        continue
                    if len(column.parts) == 2 and table_key == "SOURCE" and "SOURCE" not in fields[owner]:
                        continue
                    emit(
                        f"Dataset metric references must name a declared local field or source.column; invalid reference {column.sql()!r}.",
                        location,
                    )
                elif len(column.parts) != 2 or table_key not in fields or field not in fields[table_key]:
                    emit(
                        f"Model metric references must name a declared dataset.field; metric references are not allowed: {column.sql()!r}.",
                        location,
                    )
            for filtered in tree.find_all(exp.Filter):
                if owner is not None:
                    continue
                argument_tables = {
                    identifier_key(c.args["table"], label)
                    for c in filtered.this.find_all(exp.Column)
                    if isinstance(c.args.get("table"), exp.Identifier)
                }
                predicate_tables = {
                    identifier_key(c.args["table"], label)
                    for c in filtered.expression.find_all(exp.Column)
                    if isinstance(c.args.get("table"), exp.Identifier)
                }
                if len(argument_tables | predicate_tables) > 1:
                    emit(
                        "FILTER predicate and aggregate arguments must refer to the same dataset.",
                        location,
                        "ossie.semantic.metric.filter_scope_invalid",
                    )

    for index, dataset in enumerate(_items(model.get("datasets"))):
        if not isinstance(dataset, Mapping) or not isinstance(dataset.get("name"), str):
            continue
        owner = normalize_identifier(dataset["name"])
        for field_index, field in enumerate(_items(dataset.get("fields"))):
            check_portable_calls(field, f"{pointer}/datasets/{index}/fields/{field_index}/expression")
        seen = set(fields[owner])
        for metric_index, metric in enumerate(_items(dataset.get("metrics"))):
            if not isinstance(metric, Mapping):
                continue
            location = f"{pointer}/datasets/{index}/metrics/{metric_index}"
            name = metric.get("name")
            if isinstance(name, str):
                validator.validate_identifier_length(name, location + "/name", scope=scope)
                normalized = normalize_identifier(name)
                if normalized in seen:
                    emit(
                        "Dataset fields and metrics share a namespace; this name is already declared.",
                        location + "/name",
                        "ossie.semantic.dataset.member_duplicate",
                    )
                seen.add(normalized)
            validator.validate_expression(
                metric.get("expression"), expression_pointer=location + "/expression", scope=scope
            )
            check_metric(metric, location + "/expression", owner)
    for index, metric in enumerate(_items(model.get("metrics"))):
        check_metric(metric, f"{pointer}/metrics/{index}/expression")
