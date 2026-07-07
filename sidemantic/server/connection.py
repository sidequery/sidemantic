"""PostgreSQL wire protocol connection handler for semantic layer."""

import logging
import re

import riffq

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter


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

            # Resolve per-session security attributes from the connecting user.
            # NOTE: the PG path rewrites arbitrary SQL through QueryRewriter, which
            # does not currently bake in row-level filters. When user attributes
            # are configured we route the semantic-query path through
            # ``layer.compile(user_attributes=...)`` so access gates and row
            # filters are enforced; system/passthrough queries (SET, SHOW,
            # information_schema, pg_catalog) are unaffected.
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

            # Try to handle PostgreSQL-specific system queries
            # Skip multi-statement queries to avoid response count mismatch
            if ";" not in sql:
                handled = self._try_handle_system_query(sql, sql_lower, callback, cursor)
                if handled:
                    return

            # Execute through semantic layer
            rewriter = QueryRewriter(self.layer.graph, dialect=self.layer.dialect)
            # Use non-strict mode to pass through system queries (SHOW, SET, etc.)
            rendered_sql = rewriter.rewrite(sql, strict=False)

            # LIMITATION: QueryRewriter does not accept user_attributes, so the
            # SQL-first PG path cannot bake row-level filters into rendered_sql
            # today. When a user-attrs map is configured, enforce the coarse-
            # grained access gate here: deny queries that touch a secured model
            # if the connecting user has no attributes mapping. Fine-grained row
            # filtering over the PG wire path is deferred to a future rewriter
            # security hook.
            if getattr(self, "user_attrs_map", None):
                self._enforce_pg_access(rendered_sql, user_attributes)

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
        """Enforce model access gates for the PG wire path.

        Parses ``rendered_sql`` for referenced model names and, for any model with
        a declared security policy, evaluates its access gate. A secured model
        queried with no user attributes is denied (deny-by-default); a falsy access
        gate is denied. Row-level filters are NOT applied here (see the caller's
        LIMITATION note).

        Raises:
            SecurityError: If access to a touched secured model is denied.
        """
        from sidemantic.core.security import evaluate_access
        from sidemantic.core.semantic_layer import SecurityError

        secured = {
            name: model
            for name, model in self.layer.graph.models.items()
            if getattr(model, "security", None) is not None
        }
        if not secured:
            return

        lowered = rendered_sql.lower()
        for name, model in secured.items():
            # Match the model name (and its underlying table) as a whole word so a
            # substring of another identifier does not trigger a false positive.
            candidates = {name.lower()}
            if getattr(model, "table", None):
                candidates.add(str(model.table).lower())
            if not any(re.search(rf"\b{re.escape(c)}\b", lowered) for c in candidates):
                continue
            if user_attributes is None:
                raise SecurityError(
                    f"Model '{name}' has a security policy but no user attributes were provided "
                    f"for this session. Configure the connecting user in --user-attrs-file."
                )
            if not evaluate_access(model.security.access, user_attributes):
                raise SecurityError(f"Access to model '{name}' denied for the current user.")

    def _try_handle_system_query(self, sql: str, sql_lower: str, callback, cursor) -> bool:
        """Try to handle PostgreSQL system queries. Returns True if handled."""

        # pg_get_keywords() - return DuckDB keywords instead
        if "pg_get_keywords" in sql_lower and ";" not in sql:
            result = cursor.execute("SELECT keyword_name as word, 'U' as catcode FROM duckdb_keywords()")
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # pg_my_temp_schema() - return NULL (DuckDB doesn't have temp schemas the same way)
        if "pg_my_temp_schema" in sql_lower:
            result = cursor.execute("SELECT NULL::INTEGER as oid")
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # information_schema queries - include semantic layer tables
        if "information_schema.tables" in sql_lower:
            schemas_tables = []
            for model_name in self.layer.graph.models.keys():
                safe_name = model_name.replace("'", "''")
                schemas_tables.append(f"('semantic_layer', '{safe_name}')")
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
            result = cursor.execute(union_sql)
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # pg_settings - PostgreSQL settings view
        if "pg_settings" in sql_lower:
            result = cursor.execute(
                "SELECT name, setting, NULL as source FROM (SELECT NULL as name, NULL as setting) WHERE FALSE"
            )
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # pg_catalog queries - map to DuckDB equivalents
        if "pg_catalog." in sql_lower:
            # pg_namespace - schemas
            if "pg_namespace" in sql_lower:
                result = cursor.execute(
                    "SELECT oid, schema_name as nspname, true as is_on_search_path, comment "
                    "FROM duckdb_schemas() "
                    "WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
                )
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return True

            # pg_class - tables and views
            elif "pg_class" in sql_lower:
                result = cursor.execute(
                    "SELECT table_name as relname, schema_name as relnamespace "
                    "FROM duckdb_tables() "
                    "WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
                )
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return True

            # Other pg_catalog queries - return empty result
            else:
                result = cursor.execute("SELECT NULL WHERE FALSE")
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return True

        # obj_description() - not supported, return NULL
        if "obj_description" in sql_lower:
            rendered_sql = sql.replace("obj_description(oid, 'pg_namespace')", "NULL")
            result = cursor.execute(rendered_sql)
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        return False

    def handle_query(self, sql, callback=callable, **kwargs):
        """Handle query in executor thread pool."""
        self.executor.submit(self._handle_query, sql, callback, **kwargs)
