"""PostgreSQL wire protocol connection handler for semantic layer."""

import logging
import queue
import re
import threading
import time
import uuid

import riffq

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.server.query_execution import (
    QueryAdmission,
    QueryExecutionControl,
    QueryLimits,
    execute_bounded,
    limit_query_sql,
)
from sidemantic.sql.query_rewriter import QueryRewriter

_TRANSACTION_CONTROL = re.compile(
    r"^\s*(?:begin(?:\s+(?:work|transaction))?|start\s+transaction|commit(?:\s+work)?|rollback(?:\s+work)?)\s*;?\s*$",
    re.IGNORECASE,
)


def _is_transaction_control(sql: str) -> bool:
    """Return whether SQL is one standalone transaction-control statement."""
    return _TRANSACTION_CONTROL.fullmatch(sql) is not None


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
        query_limits: QueryLimits | None = None,
        query_admission: QueryAdmission | None = None,
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
        self.query_limits = query_limits or QueryLimits()
        self.query_admission = query_admission or QueryAdmission(
            self.query_limits.max_concurrent_queries,
            self.query_limits.max_queued_queries,
        )
        self._active_controls: set[QueryExecutionControl] = set()
        self._active_controls_lock = threading.Lock()

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
        controls_lock = getattr(self, "_active_controls_lock", None)
        if controls_lock is None:
            controls = ()
        else:
            with controls_lock:
                controls = tuple(self._active_controls)
        for control in controls:
            outcome = control.cancel()
            logging.info("Cancelled disconnected PostgreSQL query: %s", outcome.diagnostic)
        callback(True)

    def _handle_query(self, sql, callback, **kwargs):
        """Handle a SQL query."""
        cursor = None
        control = None
        query_admission = None
        admission_acquired = False
        worker_started = False
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

            # PG clients commonly bracket read-only SELECTs in transactions. The
            # server executes each query on its own database handle, so acknowledge
            # these controls without opening a disposable warehouse transaction or
            # trying to fetch rows from a command result.
            if _is_transaction_control(sql):
                import pyarrow as pa

                reader = pa.table({"ok": pa.array([], type=pa.bool_())}).to_reader()
                self.send_reader(reader, callback)
                return

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

            # SHOW is a row-producing database command, but not a SELECT that can
            # be wrapped in a derived table. Pass a single command through so
            # startup probes and metadata clients receive the database result.
            statement = sql.strip().removesuffix(";")
            if statement.lower().startswith("show ") and ";" not in statement:
                result = cursor.execute(statement)
                reader = result.fetch_record_batch()
                self.send_reader(reader, callback)
                return

            # Try to handle PostgreSQL-specific system queries
            # Skip multi-statement queries to avoid response count mismatch
            if ";" not in sql:
                handled = self._try_handle_system_query(sql, sql_lower, callback, cursor)
                if handled:
                    return

            # Enforce the coarse-grained access gate BEFORE rewriting. The raw SQL already
            # names the semantic models, so we can deny deny-by-default (no attributes), a
            # failing access gate, and any secured model with row_filters (which this SQL-first
            # path cannot inject) here, before the rewriter's generator runs. Doing it first also
            # lets an authorized user through: we then thread the attributes into the rewrite so
            # the generator's deny-by-default does not re-reject an access-only secured model.
            self._enforce_pg_access(sql, user_attributes)

            # Execute through semantic layer
            rewriter = QueryRewriter(self.layer.graph, dialect=self.layer.dialect)
            # Use non-strict mode to pass through system queries (SHOW, SET, etc.)
            rendered_sql = rewriter.rewrite(sql, strict=False, user_attributes=user_attributes)

            query_limits = getattr(self, "query_limits", None) or QueryLimits()
            query_admission = getattr(self, "query_admission", None)
            if query_admission is None:
                query_admission = QueryAdmission(
                    query_limits.max_concurrent_queries,
                    query_limits.max_queued_queries,
                )
                self.query_admission = query_admission

            control = QueryExecutionControl()
            if not hasattr(self, "_active_controls_lock"):
                self._active_controls_lock = threading.Lock()
                self._active_controls = set()
            with self._active_controls_lock:
                self._active_controls.add(control)

            admission_status = query_admission.acquire(query_limits.queue_timeout_seconds)
            if admission_status == "full":
                raise RuntimeError(
                    f"Sidemantic query queue is full (maximum {query_limits.max_queued_queries} waiting)"
                )
            if admission_status == "timeout":
                raise TimeoutError(
                    f"Sidemantic query waited more than {query_limits.queue_timeout_seconds:g}s for an execution slot"
                )
            admission_acquired = True
            if control.cancel_requested:
                raise ConnectionAbortedError("PostgreSQL client disconnected before query execution started")

            from sidemantic.core.query_telemetry import QueryEvent, sanitize_sql

            query_id = uuid.uuid4().hex
            started = time.monotonic()
            timed_out = threading.Event()
            result_queue = queue.Queue(maxsize=1)
            execution_cursor = cursor

            def execute_in_worker() -> None:
                bounded = None
                error = None
                row_count = None
                response_bytes = None
                execution_started = False
                try:
                    bounded_sql = limit_query_sql(rendered_sql, query_limits.max_rows, self.layer.dialect)
                    execution_started = True
                    bounded = execute_bounded(
                        self.layer,
                        bounded_sql,
                        limits=query_limits,
                        control=control,
                        cursor=execution_cursor,
                    )
                    row_count = bounded.row_count
                    response_bytes = int(bounded.table.nbytes)
                except Exception as exc:
                    error = exc
                finally:
                    try:
                        sanitized_sql, fingerprint = sanitize_sql(rendered_sql, self.layer.dialect)
                        outcome = control.cancellation_outcome
                        self.layer.query_telemetry.record(
                            QueryEvent(
                                query_id=query_id,
                                request_id=str(getattr(self, "connection_id", "unknown")),
                                duration_ms=(time.monotonic() - started) * 1000,
                                dialect=self.layer.dialect,
                                row_count=row_count,
                                response_bytes=response_bytes,
                                used_preaggregation="used_preagg=true" in rendered_sql,
                                cancelled=bool(outcome and outcome.cancelled),
                                timed_out=timed_out.is_set(),
                                error=(
                                    type(error).__name__
                                    if error is not None
                                    else "TimeoutError"
                                    if timed_out.is_set()
                                    else None
                                ),
                                sql=sanitized_sql,
                                sql_fingerprint=fingerprint,
                                plan_metadata={"source": "postgres_wire", "timeout": control.timeout_diagnostic},
                                cancellation_diagnostic=outcome.diagnostic if outcome else None,
                            )
                        )
                    except Exception:
                        logging.exception("Error recording query telemetry")
                    finally:
                        # execute_bounded owns cursor cleanup once entered. Only
                        # close here if SQL limiting failed before execution began.
                        if not execution_started:
                            close = getattr(execution_cursor, "close", None)
                            if callable(close):
                                close()
                        with self._active_controls_lock:
                            self._active_controls.discard(control)
                        query_admission.release()
                        result_queue.put((bounded, error))

            worker = threading.Thread(target=execute_in_worker, daemon=True)
            worker.start()
            worker_started = True
            # The worker owns this cursor through execution and cleanup. Do not let
            # the outer finally close it when a local deadline returns first.
            cursor = None
            try:
                bounded, error = result_queue.get(timeout=query_limits.execution_timeout_seconds)
            except queue.Empty:
                timed_out.set()
                outcome = control.cancel()
                raise TimeoutError(
                    f"Query exceeded the {query_limits.execution_timeout_seconds:g}s deadline. {outcome.diagnostic}"
                ) from None
            if error is not None:
                raise error
            if bounded is not None:
                self.send_reader(bounded.table.to_reader(), callback)

        except Exception:
            logging.exception("Error executing query")
            # Raise to let riffq send a proper PG ErrorResponse to the client.
            # Returning errors as data rows confuses BI tools that expect PG protocol errors.
            raise
        finally:
            if control is not None and not worker_started:
                with self._active_controls_lock:
                    self._active_controls.discard(control)
            if admission_acquired and not worker_started and query_admission is not None:
                query_admission.release()
            # File-backed DuckDB returns an independent connection per query.
            # Close it for system-query and early-return paths too; the bounded
            # execution path may already have closed it, which drivers tolerate.
            close = getattr(cursor, "close", None)
            if callable(close):
                close()

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
            # The SQL-first PG path cannot inject per-user row filters into the rewritten SQL,
            # so a model with row_filters would otherwise return unfiltered rows to any user who
            # passes the access gate. Deny it here (matching /sql, MCP run_sql, and .sql()).
            if model.security.row_filters:
                raise SecurityError(
                    f"Model '{name}' has row-level filters that the SQL-first PostgreSQL path "
                    "cannot apply. Query it through the structured HTTP /query API, which enforces "
                    "row filters per user."
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
