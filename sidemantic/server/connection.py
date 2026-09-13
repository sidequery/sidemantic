"""PostgreSQL wire protocol connection handler for semantic layer."""

import logging

import riffq

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.core.transport_security import controls_are_active, rewrite_transport_sql


class SemanticLayerConnection(riffq.BaseConnection):
    """Connection handler that translates PostgreSQL queries to semantic layer queries."""

    def __init__(
        self,
        connection_id,
        executor,
        layer: SemanticLayer,
        username: str | None = None,
        password: str | None = None,
        user_attrs_map: dict[str, dict] | None = None,
    ):
        super().__init__(connection_id, executor)
        self.layer = layer
        self.username = username
        self.password = password
        # Maps the Postgres startup ``user`` to a security ``user_attributes``
        # dict (loaded from --user-attrs-file at startup). riffq does not cleanly
        # expose the full per-session startup parameters to the query handler, so
        # we key security attributes off the authenticated username only. The
        # connecting username is captured in handle_auth and looked up here.
        self.user_attrs_map = user_attrs_map or {}
        self.session_user: str | None = None

    def _user_attributes(self) -> dict | None:
        """Resolve security user attributes for the current session.

        Looks up the connecting Postgres username in the startup-loaded
        user-attrs map. Returns None when no mapping is configured for the user
        (the semantic layer then denies any query touching a secured model).
        """
        user_attrs_map = getattr(self, "user_attrs_map", None)
        session_user = getattr(self, "session_user", None)
        if not user_attrs_map or session_user is None:
            return None
        return user_attrs_map.get(session_user)

    def handle_auth(self, user, pwd, host, database=None, callback=callable):
        """Handle authentication."""
        # Capture the connecting username so query handling can map it to
        # security user attributes, regardless of whether password auth is on.
        self.session_user = user
        if self.username is None and self.password is None:
            # No auth required
            callback(True)
        elif self.username is not None and self.password is not None:
            callback(user == self.username and pwd == self.password)
        else:
            # Partial auth config must fail closed.
            callback(False)

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

            user_attributes = self._user_attributes()

            # Each executor thread gets its own cursor so concurrent reads do not
            # serialize on a single shared connection. For DuckDB this is an
            # independent handle over the same database; other adapters fall back
            # to a lock-guarded wrapper preserving today's behavior.
            cursor = self.layer.adapter.cursor()

            # Check for DML commands first (before multi-statement check)
            # These are often PostgreSQL session config and should just succeed
            if sql_lower.startswith(("set ", "update ", "insert ", "delete ")):
                result = cursor.execute("SELECT 1 as ok WHERE FALSE")
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return

            # Only parsed single statements qualify for compatibility responses.
            if self._try_handle_system_query(sql, sql_lower, callback, cursor):
                return

            rendered_sql = rewrite_transport_sql(
                self.layer,
                sql,
                strict=False,
                user_attributes=user_attributes,
                transport="PostgreSQL",
            )

            # Execute the query
            result = cursor.execute(rendered_sql)

            # Convert to Arrow record batch
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)

        except Exception:
            logging.exception("Error executing query")
            # Raise to let riffq send a proper PG ErrorResponse to the client.
            # Returning errors as data rows confuses BI tools that expect PG protocol errors.
            raise

    def _enforce_pg_access(self, rendered_sql: str, user_attributes: dict | None) -> None:
        """Validate SQL using the same parsed policy boundary as execution."""
        rewrite_transport_sql(
            self.layer,
            rendered_sql,
            strict=False,
            user_attributes=user_attributes,
            transport="PostgreSQL",
        )

    def _visible_model_names(self) -> list[str]:
        """Expose semantic names only when the session passes their access gate."""
        from sidemantic.core.security import evaluate_access

        attrs = self._user_attributes()
        return [
            name
            for name, model in self.layer.graph.models.items()
            if model.security is None or (attrs is not None and evaluate_access(model.security.access, attrs))
        ]

    def _try_handle_system_query(self, sql: str, sql_lower: str, callback, cursor) -> bool:
        """Try to handle PostgreSQL system queries. Returns True if handled."""

        import sqlglot
        from sqlglot import exp

        # Dispatch on parsed references, never on strings or comments in caller SQL.
        # Compatibility handlers execute only server-owned SQL, not modified input.
        statements = [statement for statement in sqlglot.parse(sql, dialect=self.layer.dialect) if statement]
        if len(statements) != 1:
            if controls_are_active(self.layer):
                from sidemantic.core.semantic_layer import SecurityError

                raise SecurityError("PostgreSQL requires a single statement while security controls are active.")
            return False
        if not isinstance(statements[0], exp.Select):
            return False
        statement = statements[0]
        tables = {(table.db.lower(), table.name.lower()) for table in statement.find_all(exp.Table)}
        functions = {node.name.lower() for node in statement.find_all(exp.Anonymous)}
        controlled = controls_are_active(self.layer)

        def send(query: str) -> bool:
            reader = cursor.execute(query).fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        if "pg_get_keywords" in functions:
            return send("SELECT keyword_name as word, 'U' as catcode FROM duckdb_keywords()")
        if "pg_my_temp_schema" in functions:
            return send("SELECT NULL::INTEGER as oid")

        if ("information_schema", "tables") in tables or ("pg_catalog", "pg_class") in tables:
            visible_models = self._visible_model_names()
            rows = ["('semantic_layer', '" + name.replace("'", "''") + "')" for name in visible_models]
            # The synthetic metrics relation spans the entire graph; do not advertise
            # its global field list to a partially authorized session.
            if self.layer.graph.metrics and len(visible_models) == len(self.layer.graph.models):
                rows.append("('semantic_layer', 'metrics')")
            semantic_sql = (
                "SELECT schema AS table_schema, table_name FROM (VALUES "
                + ", ".join(rows)
                + ") AS t(schema, table_name)"
                if rows
                else "SELECT NULL::VARCHAR AS table_schema, NULL::VARCHAR AS table_name WHERE FALSE"
            )
            catalog_sql = semantic_sql
            if not controlled:
                catalog_sql += (
                    " UNION ALL SELECT table_schema, table_name FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
                )
            if ("information_schema", "tables") in tables:
                return send(f"SELECT table_schema, table_name, 'BASE TABLE' AS table_type FROM ({catalog_sql})")
            return send(f"SELECT table_name AS relname, table_schema AS relnamespace FROM ({catalog_sql})")

        if any(name == "pg_settings" for _, name in tables):
            return send("SELECT NULL::VARCHAR AS name, NULL::VARCHAR AS setting, NULL::VARCHAR AS source WHERE FALSE")

        if ("pg_catalog", "pg_namespace") in tables:
            if controlled:
                return send(
                    "SELECT 0::BIGINT AS oid, 'semantic_layer' AS nspname, true AS is_on_search_path, "
                    "NULL::VARCHAR AS comment" + ("" if self._visible_model_names() else " WHERE FALSE")
                )
            return send(
                "SELECT oid, schema_name as nspname, true as is_on_search_path, comment "
                "FROM duckdb_schemas() WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
            )

        if any(schema == "pg_catalog" for schema, _ in tables):
            return send("SELECT NULL WHERE FALSE")
        if controlled and any(schema == "information_schema" for schema, _ in tables):
            return send("SELECT NULL WHERE FALSE")

        # A source-free description probe is safe. Queries involving real sources
        # must proceed through semantic authorization, including obj_description().
        if "obj_description" in functions and not tables:
            return send("SELECT NULL::VARCHAR AS obj_description")
        return False

    def handle_query(self, sql, callback=callable, **kwargs):
        """Handle query in executor thread pool."""
        self.executor.submit(self._handle_query, sql, callback, **kwargs)
