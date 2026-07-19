"""PostgreSQL wire protocol connection handler for semantic layer."""

import logging
import re

import riffq

from sidemantic.core.semantic_layer import SemanticLayer


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
            import sqlglot
            from sqlglot import exp

            sql_lower = sql.lower().strip()

            try:
                statements = sqlglot.parse(sql, dialect=self.layer.dialect)
            except Exception as exc:
                raise ValueError(f"PostgreSQL wire refused invalid SQL: {exc}") from exc
            if len(statements) != 1:
                raise ValueError("PostgreSQL wire accepts exactly one read-only statement")
            statement = statements[0]

            # Resolve per-session security attributes from the connecting user.
            # The policy-aware semantic rewriter receives these attributes, so
            # access gates and row filters match structured query transports.
            user_attributes = self._user_attributes()

            # Each executor thread gets its own cursor so concurrent reads do not
            # serialize on a single shared connection. For DuckDB this is an
            # independent handle over the same database; other adapters fall back
            # to a lock-guarded wrapper preserving today's behavior.
            cursor = self.layer.adapter.cursor()

            # PostgreSQL clients commonly send session and transaction setup.
            # Acknowledge those statements without forwarding them. SHOW is
            # treated the same way because DuckDB cannot answer every PG setting.
            safe_session_statement = isinstance(
                statement,
                (exp.Set, exp.Transaction, exp.Commit, exp.Rollback),
            ) or sql_lower.startswith("show ")
            if safe_session_statement:
                result = cursor.execute("SELECT 1 as ok WHERE FALSE")
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return

            if not isinstance(statement, exp.Query):
                raise ValueError(
                    "PostgreSQL wire queries are read-only; mutating, DDL, and command statements are not supported."
                )

            # Try to handle PostgreSQL-specific system queries
            # Skip multi-statement queries to avoid response count mismatch
            if ";" not in sql:
                handled = self._try_handle_system_query(sql, sql_lower, callback, cursor)
                if handled:
                    return

            # Rewrite through the same policy-aware generator used by structured
            # transports. Under active controls, an unrecognized source read is
            # rejected instead of being passed through to the underlying database.
            rendered_sql = self._rewrite_query(sql, user_attributes)

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

    def _rewrite_query(self, sql: str, user_attributes: dict | None) -> str:
        """Apply the shared transport policy and return executable SQL."""
        from sidemantic.core.transport_security import rewrite_transport_sql

        return rewrite_transport_sql(
            self.layer,
            sql,
            user_attributes=user_attributes,
            transport="PostgreSQL wire",
            strict=False,
        )

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

        if "information_schema.columns" in sql_lower:
            import sqlglot
            from sqlglot import exp

            parsed = sqlglot.parse_one(sql, dialect=self.layer.dialect)
            referenced_tables = list(parsed.find_all(exp.Table))
            if not referenced_tables or any(
                table.name.lower() != "columns" or table.db.lower() != "information_schema"
                for table in referenced_tables
            ):
                return False

            source_sql = self._information_schema_columns_source()
            rendered_sql = re.sub(
                r"\binformation_schema\.columns\b",
                lambda _match: f"({source_sql})",
                sql,
                flags=re.IGNORECASE,
            )
            result = cursor.execute(rendered_sql)
            reader = result.fetch_record_batch()
            self.send_reader(reader, callback)
            return True

        # information_schema queries - include semantic layer tables
        if "information_schema.tables" in sql_lower:
            from sidemantic.core.transport_security import controls_are_active

            schemas_tables = []
            for model_name in self.layer.graph.models.keys():
                safe_name = model_name.replace("'", "''")
                schemas_tables.append(f"('semantic_layer', '{safe_name}')")
            if self.layer.graph.metrics:
                schemas_tables.append("('semantic_layer', 'metrics')")

            semantic_catalog_sql = (
                "SELECT schema, table_name, 'BASE TABLE' as table_type "
                f"FROM (VALUES {', '.join(schemas_tables)}) AS t(schema, table_name)"
            )
            if controls_are_active(self.layer):
                union_sql = semantic_catalog_sql
            else:
                union_sql = f"""
                SELECT table_schema, table_name, 'BASE TABLE' as table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                UNION ALL
                {semantic_catalog_sql}
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

    def _information_schema_columns_source(self) -> str:
        """Return a visibility-aware relation for PostgreSQL column discovery."""
        from sidemantic.core.catalog import get_postgres_type_for_dimension, get_postgres_type_for_metric
        from sidemantic.core.transport_security import controls_are_active

        def literal(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        rows: list[str] = []

        def add_column(table_name: str, column_name: str, ordinal: int, data_type: str) -> None:
            char_length = "255" if data_type == "VARCHAR" else "NULL::INTEGER"
            numeric_precision = "38" if data_type == "NUMERIC" else "NULL::INTEGER"
            numeric_scale = "10" if data_type == "NUMERIC" else "NULL::INTEGER"
            rows.append(
                "("
                + ", ".join(
                    [
                        "'sidemantic'",
                        "'semantic_layer'",
                        literal(table_name),
                        literal(column_name),
                        str(ordinal),
                        "NULL::VARCHAR",
                        "'YES'",
                        literal(data_type),
                        char_length,
                        numeric_precision,
                        numeric_scale,
                    ]
                )
                + ")"
            )

        for model in self.layer.graph.models.values():
            ordinal = 1
            for dimension in model.dimensions:
                if self.layer.enforce_visibility and not getattr(dimension, "public", True):
                    continue
                add_column(
                    model.name,
                    dimension.name,
                    ordinal,
                    get_postgres_type_for_dimension(dimension.type, dimension.granularity),
                )
                ordinal += 1
            for metric in model.metrics:
                if self.layer.enforce_visibility and not getattr(metric, "public", True):
                    continue
                add_column(model.name, metric.name, ordinal, get_postgres_type_for_metric(metric.agg))
                ordinal += 1

        if self.layer.graph.metrics:
            ordinal = 1
            for metric in self.layer.graph.metrics.values():
                if self.layer.enforce_visibility and not getattr(metric, "public", True):
                    continue
                add_column("metrics", metric.name, ordinal, get_postgres_type_for_metric(metric.agg))
                ordinal += 1
            seen_dimensions: set[str] = set()
            for model in self.layer.graph.models.values():
                for dimension in model.dimensions:
                    if dimension.name in seen_dimensions:
                        continue
                    if self.layer.enforce_visibility and not getattr(dimension, "public", True):
                        continue
                    seen_dimensions.add(dimension.name)
                    add_column(
                        "metrics",
                        dimension.name,
                        ordinal,
                        get_postgres_type_for_dimension(dimension.type, dimension.granularity),
                    )
                    ordinal += 1

        column_names = (
            "table_catalog, table_schema, table_name, column_name, ordinal_position, "
            "column_default, is_nullable, data_type, character_maximum_length, "
            "numeric_precision, numeric_scale"
        )
        if rows:
            semantic_sql = f"SELECT * FROM (VALUES {', '.join(rows)}) AS semantic_columns({column_names})"
        else:
            semantic_sql = (
                "SELECT NULL::VARCHAR AS table_catalog, NULL::VARCHAR AS table_schema, "
                "NULL::VARCHAR AS table_name, NULL::VARCHAR AS column_name, "
                "NULL::INTEGER AS ordinal_position, NULL::VARCHAR AS column_default, "
                "NULL::VARCHAR AS is_nullable, NULL::VARCHAR AS data_type, "
                "NULL::INTEGER AS character_maximum_length, NULL::INTEGER AS numeric_precision, "
                "NULL::INTEGER AS numeric_scale WHERE FALSE"
            )

        if controls_are_active(self.layer):
            return semantic_sql
        return (
            f"SELECT {column_names} FROM information_schema.columns "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') UNION ALL "
            f"{semantic_sql}"
        )

    def handle_query(self, sql, callback=callable, **kwargs):
        """Handle query in executor thread pool."""
        self.executor.submit(self._handle_query, sql, callback, **kwargs)
