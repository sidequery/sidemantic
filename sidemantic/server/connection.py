"""PostgreSQL wire protocol connection handler for semantic layer."""

import logging

import pyarrow as pa
import riffq

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter


class SemanticLayerConnection(riffq.BaseConnection):
    """Connection handler that translates PostgreSQL queries to semantic layer queries."""

    def __init__(
        self, connection_id, executor, layer: SemanticLayer, username: str | None = None, password: str | None = None
    ):
        super().__init__(connection_id, executor)
        self.layer = layer
        self.username = username
        self.password = password

    def handle_auth(self, user, pwd, host, database=None, callback=callable):
        """Handle authentication."""
        # If username/password are set, check them
        if self.username is not None and self.password is not None:
            callback(user == self.username and pwd == self.password)
        else:
            # No auth required
            callback(True)

    def handle_connect(self, ip, port, callback=callable):
        """Handle connection."""
        callback(True)

    def handle_disconnect(self, ip, port, callback=callable):
        """Handle disconnection."""
        callback(True)

    def _handle_query(self, sql, callback, **kwargs):
        """Handle a SQL query."""
        try:
            sql_lower = sql.lower().strip()

            # Check for DML commands first (before multi-statement check)
            # These are often PostgreSQL session config and should just succeed
            if sql_lower.startswith(("set ", "update ", "insert ", "delete ")):
                result = self.layer.conn.execute("SELECT 1 as ok WHERE FALSE")
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return

            # Try to handle PostgreSQL-specific system queries
            # Skip multi-statement queries to avoid response count mismatch
            if ";" not in sql:
                handled = self._try_handle_system_query(sql, sql_lower, callback)
                if handled:
                    return

            # Execute through semantic layer
            rewriter = QueryRewriter(self.layer.graph, dialect=self.layer.dialect)
            # Use non-strict mode to pass through system queries (SHOW, SET, etc.)
            rendered_sql = rewriter.rewrite(sql, strict=False)

            # Execute the query
            result = self.layer.conn.execute(rendered_sql)

            # Convert to Arrow record batch
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)

        except Exception as exc:
            logging.exception("Error executing query")
            # Return error to client
            batch = self.arrow_batch([pa.array(["ERROR"]), pa.array([str(exc)])], ["error", "message"])
            self.send_reader(batch, callback)

    def _try_handle_system_query(self, sql: str, sql_lower: str, callback) -> bool:
        """Try to handle PostgreSQL system queries. Returns True if handled."""

        # pg_get_keywords() - return DuckDB keywords instead
        if "pg_get_keywords" in sql_lower and ";" not in sql:
            result = self.layer.conn.execute("SELECT keyword_name as word, 'U' as catcode FROM duckdb_keywords()")
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # pg_my_temp_schema() - return NULL (DuckDB doesn't have temp schemas the same way)
        if "pg_my_temp_schema" in sql_lower:
            result = self.layer.conn.execute("SELECT NULL::INTEGER as oid")
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # information_schema queries - include semantic layer tables
        if "information_schema.tables" in sql_lower:
            schemas_tables = []
            for model_name in self.layer.graph.models.keys():
                schemas_tables.append(f"('semantic_layer', '{model_name}')")
            if self.layer.graph.metrics:
                schemas_tables.append("('semantic_layer', 'metrics')")

            union_sql = f"""
            SELECT table_schema, table_name, 'BASE TABLE' as table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT schema, table_name, 'BASE TABLE' as table_type
            FROM (VALUES {", ".join(schemas_tables)}) AS t(schema, table_name)
            """
            result = self.layer.conn.execute(union_sql)
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # pg_settings - PostgreSQL settings view
        if "pg_settings" in sql_lower:
            result = self.layer.conn.execute(
                "SELECT name, setting, NULL as source FROM (SELECT NULL as name, NULL as setting) WHERE FALSE"
            )
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # pg_catalog queries - map to DuckDB equivalents
        if "pg_catalog." in sql_lower:
            # pg_namespace - schemas
            if "pg_namespace" in sql_lower:
                result = self.layer.conn.execute(
                    "SELECT oid, schema_name as nspname, true as is_on_search_path, comment "
                    "FROM duckdb_schemas() "
                    "WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
                )
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return True

            # pg_class - tables and views
            elif "pg_class" in sql_lower:
                result = self.layer.conn.execute(
                    "SELECT table_name as relname, schema_name as relnamespace "
                    "FROM duckdb_tables() "
                    "WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
                )
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return True

            # Other pg_catalog queries - return empty result
            else:
                result = self.layer.conn.execute("SELECT NULL WHERE FALSE")
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return True

        # obj_description() - not supported, return NULL
        if "obj_description" in sql_lower:
            rendered_sql = sql.replace("obj_description(oid, 'pg_namespace')", "NULL")
            result = self.layer.conn.execute(rendered_sql)
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        return False

    def handle_query(self, sql, callback=callable, **kwargs):
        """Handle query in executor thread pool."""
        self.executor.submit(self._handle_query, sql, callback, **kwargs)
