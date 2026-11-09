"""PostgreSQL catalog emulation for the wire server.

Clients discover tables by querying ``information_schema`` and ``pg_catalog``.
Those queries are answered by running the client's own SQL against a private
in-memory DuckDB database that contains one empty table per relation the
session may see: every semantic model the session can access (schema
``semantic_layer``) and, when no security control is active, the physical
source tables. Requested columns, filters, joins and bound parameters are
therefore honored instead of being replaced by a fixed answer.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

CATALOG_DATABASE = "sidemantic"
SEMANTIC_SCHEMA = "semantic_layer"
# The PostgreSQL version reported to clients. Drivers parse it to pick features.
PG_VERSION = "17.4"

logger = logging.getLogger(__name__)

SourceTable = tuple[str, str, list[tuple[str, str]]]

# Functions and types clients use that DuckDB does not provide, or that must not
# report DuckDB's own identity. Each statement is attempted independently.
_COMPATIBILITY_OBJECTS = (
    "CREATE TYPE regclass AS VARCHAR",
    "CREATE TYPE regtype AS VARCHAR",
    "CREATE TYPE regproc AS VARCHAR",
    "CREATE TYPE regnamespace AS VARCHAR",
    "CREATE OR REPLACE MACRO pg_backend_pid() AS 1",
    "CREATE OR REPLACE MACRO pg_get_userbyid(role_oid) AS 'sidemantic'",
    "CREATE OR REPLACE MACRO pg_is_in_recovery() AS false",
    "CREATE OR REPLACE MACRO pg_encoding_to_char(encoding) AS 'UTF8'",
    "CREATE OR REPLACE MACRO pg_get_keywords() AS TABLE "
    "SELECT keyword_name AS word, 'U' AS catcode, 'unreserved' AS catdesc FROM duckdb_keywords()",
    # DuckDB's pg_attribute.atttypid holds DuckDB type ids, not PostgreSQL type OIDs,
    # so drivers that join it to pg_type (JDBC getColumns) lose or mistype columns.
    # A table macro, unlike a view, does not show up in the catalog it serves.
    """
    CREATE OR REPLACE MACRO pg_attribute_compat() AS TABLE
    SELECT a.* REPLACE (
        CASE
            WHEN c.data_type = 'BOOLEAN' THEN 16
            WHEN c.data_type IN ('TINYINT', 'SMALLINT', 'UTINYINT') THEN 21
            WHEN c.data_type IN ('INTEGER', 'USMALLINT') THEN 23
            WHEN c.data_type IN ('BIGINT', 'UINTEGER') THEN 20
            WHEN c.data_type = 'FLOAT' THEN 700
            WHEN c.data_type = 'DOUBLE' THEN 701
            WHEN c.data_type LIKE 'DECIMAL%' OR c.data_type IN ('HUGEINT', 'UBIGINT', 'UHUGEINT') THEN 1700
            WHEN c.data_type = 'DATE' THEN 1082
            WHEN c.data_type = 'TIME' THEN 1083
            WHEN c.data_type = 'TIMESTAMP WITH TIME ZONE' THEN 1184
            WHEN c.data_type LIKE 'TIMESTAMP%' THEN 1114
            WHEN c.data_type = 'UUID' THEN 2950
            WHEN c.data_type = 'BLOB' THEN 17
            ELSE 1043
        END AS atttypid)
    FROM pg_catalog.pg_attribute a
    JOIN duckdb_columns() c ON c.table_oid = a.attrelid AND c.column_index = a.attnum
    """,
    """
    CREATE OR REPLACE MACRO format_type(type_oid, typemod) AS CASE type_oid
        WHEN 16 THEN 'boolean' WHEN 21 THEN 'smallint' WHEN 23 THEN 'integer' WHEN 20 THEN 'bigint'
        WHEN 700 THEN 'real' WHEN 701 THEN 'double precision' WHEN 1082 THEN 'date'
        WHEN 1083 THEN 'time without time zone' WHEN 1114 THEN 'timestamp without time zone'
        WHEN 1184 THEN 'timestamp with time zone' WHEN 2950 THEN 'uuid' WHEN 17 THEN 'bytea'
        WHEN 1700 THEN CASE WHEN typemod > 4
            THEN 'numeric(' || ((typemod - 4) >> 16) || ',' || ((typemod - 4) & 65535) || ')' ELSE 'numeric' END
        ELSE 'character varying' END
    """,
)

# Catalog relations read through a "<name>_compat()" table macro defined above.
_OVERRIDDEN_RELATIONS = frozenset({"pg_attribute"})

# pg_catalog relations clients read without the schema qualifier.
_PG_CATALOG_RELATIONS = frozenset(
    {
        "pg_am",
        "pg_attrdef",
        "pg_attribute",
        "pg_class",
        "pg_constraint",
        "pg_database",
        "pg_depend",
        "pg_description",
        "pg_enum",
        "pg_index",
        "pg_indexes",
        "pg_namespace",
        "pg_proc",
        "pg_sequence",
        "pg_sequences",
        "pg_settings",
        "pg_tables",
        "pg_tablespace",
        "pg_type",
        "pg_views",
    }
)

# Table-free queries are answered by the catalog only when they call one of
# these session functions (or any pg_*/has_*_privilege function).
_SESSION_FUNCTIONS = frozenset(
    {
        "version",
        "current_version",
        "current_database",
        "current_catalog",
        "current_schema",
        "current_schemas",
        "current_user",
        "current_role",
        "session_user",
        "obj_description",
        "col_description",
        "shobj_description",
        "format_type",
        "txid_current",
    }
)


def semantic_column_type(field_type: str | None) -> str:
    """Map a semantic dimension type to the DuckDB type advertised in the catalog."""
    if field_type == "time":
        return "TIMESTAMP"
    if field_type in ("numeric", "number"):
        return "DOUBLE"
    if field_type == "boolean":
        return "BOOLEAN"
    return "VARCHAR"


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def is_catalog_query(statement: Any, semantic_names: set[str]) -> bool:
    """Return whether a parsed SELECT reads only the PostgreSQL catalog.

    A query qualifies when every relation it reads is a catalog relation, or
    when it reads no relation at all and calls a session/catalog function.
    Anything touching a semantic model or a physical table is not a catalog
    query and must go through semantic authorization.
    """
    from sqlglot import exp

    cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}
    saw_catalog_relation = False
    for table in statement.find_all(exp.Table):
        if isinstance(table.this, exp.Func):
            name = table.this.name.lower()
            if not name.startswith("pg_"):
                return False
            saw_catalog_relation = True
            continue
        name = table.name.lower()
        schema = table.db.lower()
        if not schema and name in cte_names:
            continue
        if schema in ("pg_catalog", "information_schema"):
            saw_catalog_relation = True
        elif not schema and name in _PG_CATALOG_RELATIONS and name not in semantic_names:
            saw_catalog_relation = True
        else:
            return False
    if saw_catalog_relation:
        return True

    for function in statement.find_all(exp.Func):
        name = (function.name if isinstance(function, exp.Anonymous) else function.sql_name()).lower()
        if name in _SESSION_FUNCTIONS or name.startswith("pg_") or (name.startswith("has_") and "privilege" in name):
            return True
    return False


def postgres_column_names(statement: Any, table: Any) -> Any:
    """Name unaliased function columns the way PostgreSQL does (``version``, not ``"version"()``)."""
    from sqlglot import exp

    if not isinstance(statement, exp.Select) or len(statement.expressions) != table.num_columns:
        return table
    names = list(table.column_names)
    for index, projection in enumerate(statement.expressions):
        while isinstance(projection, exp.Dot):  # pg_catalog.version()
            projection = projection.expression
        if isinstance(projection, exp.Func) and not isinstance(projection, exp.Cast):
            names[index] = projection.sql(dialect="duckdb").split("(")[0].split(".")[-1].strip('"').lower()
    return table.rename_columns(names)


def use_compatibility_objects(sql: str) -> str:
    """Point a client's catalog SQL at the compatibility objects.

    Drivers schema-qualify builtin functions: ``pg_catalog.func(...)`` becomes
    ``func(...)``, which resolves to a compatibility macro first and to DuckDB's
    own builtin otherwise. A reference to an overridden relation becomes a call
    to its table macro, keeping the relation's name as the alias. The SQL is
    edited by token so PostgreSQL syntax DuckDB accepts is left untouched.
    """
    lowered_sql = sql.lower()
    if "pg_catalog" not in lowered_sql and not any(name in lowered_sql for name in _OVERRIDDEN_RELATIONS):
        return sql
    import sqlglot
    from sqlglot.errors import SqlglotError
    from sqlglot.tokens import TokenType

    try:
        tokens = sqlglot.tokenize(sql, read="postgres")
    except SqlglotError:
        return sql

    def token_type(index: int):
        return tokens[index].token_type if index < len(tokens) else None

    pieces: list[str] = []
    position = 0
    for index, token in enumerate(tokens):
        if token.token_type is TokenType.STRING:
            continue
        name = token.text.lower()
        if name == "pg_catalog" and token_type(index + 1) is TokenType.DOT and index + 2 < len(tokens):
            is_call = token_type(index + 3) is TokenType.L_PAREN
            if is_call or tokens[index + 2].text.lower() in _OVERRIDDEN_RELATIONS:
                pieces.append(sql[position : token.start])
                position = tokens[index + 2].start
        elif name in _OVERRIDDEN_RELATIONS and token_type(index + 1) not in (TokenType.DOT, TokenType.L_PAREN):
            # A relation reference; "pg_attribute.attname" (a column qualifier) is left alone.
            pieces.append(sql[position : token.start])
            pieces.append(f"{name}_compat()")
            if token_type(index + 1) not in (TokenType.VAR, TokenType.IDENTIFIER, TokenType.ALIAS):
                pieces.append(f" AS {name}")
            position = token.end + 1
    pieces.append(sql[position:])
    return "".join(pieces)


class PgCatalog:
    """Builds and queries the per-visibility catalog databases."""

    def __init__(self, layer: Any, source_tables: list[SourceTable] | None = None):
        self.layer = layer
        self._source_tables = source_tables
        self._databases: dict[tuple[frozenset[str], bool], Any] = {}
        self._lock = threading.Lock()

    def query(self, sql: str, visible_models: list[str], *, include_sources: bool) -> Any:
        """Run a catalog query for a session and return a PyArrow table."""
        from sidemantic.db._duckdb_compat import fetch_arrow_reader

        cursor = self._database(frozenset(visible_models), include_sources).cursor()
        try:
            cursor.execute(f"USE {CATALOG_DATABASE}")
            return fetch_arrow_reader(cursor.execute(use_compatibility_objects(sql))).read_all()
        finally:
            cursor.close()

    def _database(self, visible_models: frozenset[str], include_sources: bool) -> Any:
        key = (visible_models, include_sources)
        with self._lock:
            database = self._databases.get(key)
            if database is None:
                database = self._build(visible_models, include_sources)
                self._databases[key] = database
            return database

    def _load_source_tables(self) -> list[SourceTable]:
        """Snapshot physical tables once; the catalog does not track later DDL."""
        if self._source_tables is None:
            tables: dict[tuple[str, str], list[tuple[str, str]]] = {}
            try:
                rows = self.layer.adapter.execute(
                    "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                    "ORDER BY table_schema, table_name, ordinal_position"
                ).fetchall()
                for schema_name, table_name, column_name, data_type in rows:
                    tables.setdefault((schema_name, table_name), []).append((column_name, data_type))
            except Exception:
                logger.warning("Could not read source tables for the PostgreSQL catalog", exc_info=True)
            self._source_tables = [(schema, table, columns) for (schema, table), columns in tables.items()]
        return self._source_tables

    def _build(self, visible_models: frozenset[str], include_sources: bool) -> Any:
        import duckdb

        import sidemantic

        database = duckdb.connect(":memory:")
        database.execute(f"ATTACH ':memory:' AS {CATALOG_DATABASE}")
        database.execute(f"USE {CATALOG_DATABASE}")
        version = f"PostgreSQL {PG_VERSION} (Sidemantic {sidemantic.__version__})"
        for statement in (
            f"CREATE OR REPLACE MACRO version() AS '{version}'",
            *_COMPATIBILITY_OBJECTS,
        ):
            try:
                database.execute(statement)
            except duckdb.Error:
                logger.debug("Skipped catalog compatibility object: %s", statement, exc_info=True)

        known_types: dict[str, str] = {}

        def column_type(data_type: str) -> str:
            # Source types come from any backend; advertise unknown ones as text.
            if data_type not in known_types:
                try:
                    database.execute(f"SELECT CAST(NULL AS {data_type})")
                    known_types[data_type] = data_type
                except duckdb.Error:
                    known_types[data_type] = "VARCHAR"
            return known_types[data_type]

        def create_table(schema_name: str, table_name: str, columns: list[tuple[str, str]]) -> None:
            if not columns:
                return
            columns = _unique_columns(columns)
            definitions = ", ".join(f"{_quote_identifier(name)} {data_type}" for name, data_type in columns)
            try:
                database.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema_name)}")
                database.execute(
                    f"CREATE TABLE {_quote_identifier(schema_name)}.{_quote_identifier(table_name)} ({definitions})"
                )
            except duckdb.Error:
                logger.warning("Could not expose %s.%s in the PostgreSQL catalog", schema_name, table_name)

        database.execute(f"CREATE SCHEMA {SEMANTIC_SCHEMA}")
        if include_sources:
            for schema_name, table_name, columns in self._load_source_tables():
                if schema_name == SEMANTIC_SCHEMA:
                    continue
                create_table(
                    schema_name, table_name, [(name, column_type(str(data_type))) for name, data_type in columns]
                )

        enforce_visibility = bool(getattr(self.layer, "enforce_visibility", False))

        def is_listed(field: Any) -> bool:
            return not enforce_visibility or getattr(field, "public", True)

        graph = self.layer.graph
        for model_name, model in graph.models.items():
            if model_name not in visible_models:
                continue
            columns = [(dim.name, semantic_column_type(dim.type)) for dim in model.dimensions if is_listed(dim)]
            columns += [(metric.name, "DOUBLE") for metric in model.metrics if is_listed(metric)]
            create_table(SEMANTIC_SCHEMA, model_name, columns)

        # The synthetic metrics relation spans the entire graph; do not advertise
        # its global field list to a partially authorized session.
        graph_metrics = getattr(graph, "metrics", None)
        if graph_metrics and len(visible_models) == len(graph.models) and "metrics" not in graph.models:
            columns = [(metric.name, "DOUBLE") for metric in graph_metrics.values() if is_listed(metric)]
            for model in graph.models.values():
                columns += [(dim.name, semantic_column_type(dim.type)) for dim in model.dimensions if is_listed(dim)]
            create_table(SEMANTIC_SCHEMA, "metrics", columns)

        # Catalog queries never need files or the network.
        database.execute("SET enable_external_access = false")
        return database


def _unique_columns(columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[str] = set()
    unique = []
    for name, data_type in columns:
        if name.lower() not in seen:
            seen.add(name.lower())
            unique.append((name, data_type))
    return unique
