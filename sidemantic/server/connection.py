"""PostgreSQL wire protocol connection handler for semantic layer."""

import logging
import re

import riffq

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.core.transport_security import controls_are_active, rewrite_transport_sql
from sidemantic.server.common import result_to_record_batch_reader, to_json_compatible
from sidemantic.server.pg_catalog import (
    PG_VERSION,
    SEMANTIC_SCHEMA,
    PgCatalog,
    is_catalog_query,
    postgres_column_names,
)

# Transaction and session statements that clients send but that have nothing to
# do on a read-only, autocommit server. They are acknowledged with their tag.
_SESSION_COMMAND = re.compile(
    r"^\s*(begin|start\s+transaction|commit|end|rollback|abort|savepoint|release|discard|deallocate|reset)\b",
    re.IGNORECASE,
)
_SESSION_COMMAND_TAGS = {
    "begin": "BEGIN",
    "start transaction": "START TRANSACTION",
    "commit": "COMMIT",
    "end": "COMMIT",
    "rollback": "ROLLBACK",
    "abort": "ROLLBACK",
    "savepoint": "SAVEPOINT",
    "release": "RELEASE",
    "discard": "DISCARD ALL",
    "deallocate": "DEALLOCATE",
    "reset": "RESET",
}

# Settings drivers read with SHOW while connecting.
_SHOW_STATEMENT = re.compile(r"^\s*show\s+(.+?)\s*;?\s*$", re.IGNORECASE | re.DOTALL)
_SESSION_SETTINGS = {
    "server_version": PG_VERSION,
    "server_encoding": "UTF8",
    "client_encoding": "UTF8",
    "standard_conforming_strings": "on",
    "integer_datetimes": "on",
    "datestyle": "ISO, MDY",
    "max_identifier_length": "63",
    "search_path": SEMANTIC_SCHEMA,
    "transaction_isolation": "read committed",
    "default_transaction_isolation": "read committed",
}
_SESSION_SETTING_ALIASES = {"transaction isolation level": "transaction_isolation"}

# SQLSTATE codes by exception class name, so clients can tell failures apart.
_SQLSTATE_BY_EXCEPTION = {
    "SecurityError": "42501",
    "ReadOnlyError": "25006",
    "ParameterError": "08P01",
    "UnsupportedStatementError": "42601",
    "ParseError": "42601",
    "TokenError": "42601",
    "ParserException": "42601",
    "CatalogException": "42P01",
    "BinderException": "42703",
}


class ReadOnlyError(ValueError):
    """The statement would change data or database state."""


class UnsupportedStatementError(ValueError):
    """The text is not a statement this server recognizes."""


class ParameterError(ValueError):
    """A statement placeholder has no usable bound value."""


def error_response(exc: BaseException) -> tuple[str, str, str]:
    """Describe an exception as a PostgreSQL ``(severity, sqlstate, message)`` error."""
    sqlstate = "XX000"
    for cls in type(exc).__mro__:
        if cls.__name__ in _SQLSTATE_BY_EXCEPTION:
            sqlstate = _SQLSTATE_BY_EXCEPTION[cls.__name__]
            break
    else:
        if isinstance(exc, (ValueError, KeyError)) and "not found" in str(exc):
            sqlstate = "42703"
    return ("ERROR", sqlstate, str(exc) or type(exc).__name__)


def _parameter_literal(value, dialect: str) -> str:
    """Render one bound parameter as a SQL literal in the executing dialect."""
    import math
    from datetime import date, datetime
    from decimal import Decimal

    from sqlglot import exp

    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, Decimal)) or (isinstance(value, float) and math.isfinite(value)):
        # Parenthesized so "x-$1" cannot turn into a "--" comment.
        return f"({value})" if value < 0 else str(value)
    if isinstance(value, float):
        return exp.cast(exp.Literal.string(str(value)), "double").sql(dialect=dialect)
    if isinstance(value, datetime):
        target = "timestamptz" if value.tzinfo is not None else "timestamp"
        return exp.cast(exp.Literal.string(value.isoformat(sep=" ")), target).sql(dialect=dialect)
    if isinstance(value, date):
        return exp.cast(exp.Literal.string(value.isoformat()), "date").sql(dialect=dialect)
    if isinstance(value, str):
        if "\x00" in value:
            # Text never contains NUL: riffq handed over the raw bytes of a binary-format
            # value (date, numeric, uuid, ...) whose type it does not decode.
            raise ParameterError("A parameter was sent in a binary format this server cannot decode; send it as text")
        return exp.Literal.string(value).sql(dialect=dialect)
    raise ParameterError(f"Unsupported parameter type: {type(value).__name__}")


def inline_parameters(sql: str, query_args, *, dialect: str, describe: bool = False) -> str:
    """Replace ``$n`` placeholders with the values a client bound to them.

    Extended-protocol clients (JDBC, psycopg, most BI drivers) send the statement
    and its parameters separately. Values are inlined as escaped literals so the
    semantic rewriter and security checks see one ordinary statement. While a
    statement is only being described no values exist yet; NULL stands in.
    """
    if "$" not in sql:
        return sql
    import sqlglot
    from sqlglot.errors import SqlglotError
    from sqlglot.tokens import TokenType

    try:
        tokens = sqlglot.tokenize(sql, read="postgres")
    except SqlglotError:
        return sql
    values = list(query_args or [])
    pieces: list[str] = []
    position = 0
    for marker, number in zip(tokens, tokens[1:]):
        if (
            marker.token_type is not TokenType.PARAMETER
            or number.token_type is not TokenType.NUMBER
            or number.start != marker.end + 1
            or not number.text.isdigit()
        ):
            continue
        index = int(number.text)
        if 1 <= index <= len(values):
            literal = _parameter_literal(values[index - 1], dialect)
        elif describe:
            literal = "NULL"
        else:
            raise ParameterError(f"No value was bound for parameter ${index}")
        pieces.append(sql[position : marker.start])
        pieces.append(literal)
        position = number.end + 1
    pieces.append(sql[position:])
    return "".join(pieces)


def _is_read_statement(statement) -> bool:
    from sqlglot import exp

    if isinstance(statement, exp.Command):
        keyword = statement.name.upper()
        if keyword == "SHOW":
            return True
        if keyword == "EXPLAIN":
            import sqlglot
            from sqlglot.errors import SqlglotError

            explained = re.sub(r"^\s*analyze\b", "", statement.expression.name, flags=re.IGNORECASE)
            try:
                inner = [parsed for parsed in sqlglot.parse(explained) if parsed is not None]
            except SqlglotError:
                return False
            return len(inner) == 1 and isinstance(inner[0], exp.Query) and _is_read_statement(inner[0])
        return False
    if not isinstance(statement, (exp.Query, exp.Values, exp.Describe, exp.Show, exp.Summarize)):
        return False
    return not any(isinstance(node, (exp.DML, exp.DDL, exp.Into, exp.Command)) for node in statement.walk())


def require_read_only(sql: str, dialect: str) -> None:
    """Reject statements that could change the database behind the semantic layer.

    The server exposes a semantic layer for querying. Like the HTTP API it does
    not run DDL, DML, or administrative statements against the user's database.
    """
    import sqlglot
    from sqlglot import exp

    statements = [statement for statement in sqlglot.parse(sql, dialect=dialect) if statement is not None]
    for statement in statements:
        if _is_read_statement(statement):
            continue
        words = statement.sql(dialect=dialect).split(None, 1)
        keyword = words[0].upper() if words else "statement"
        if isinstance(statement, (exp.Condition, exp.Alias)):
            # sqlglot found an expression rather than a statement: a typo or a command it does not know.
            raise UnsupportedStatementError(
                f'syntax error or unsupported statement at "{keyword}": '
                "the Sidemantic PostgreSQL server only runs SELECT queries"
            )
        raise ReadOnlyError(
            f"cannot execute {keyword}: the Sidemantic PostgreSQL server is read-only and only runs SELECT queries"
        )


def wire_safe_table(table):
    """Convert columns riffq cannot encode into text.

    riffq sends NULL for Arrow types it has no PostgreSQL encoding for (time,
    interval, enum/dictionary, struct, map, UUID). Text keeps the value visible.
    """
    import json

    import pyarrow as pa
    import pyarrow.types as pat

    def encodable(data_type) -> bool:
        if isinstance(data_type, pa.ExtensionType):
            return False
        if pat.is_list(data_type) or pat.is_large_list(data_type) or pat.is_fixed_size_list(data_type):
            return encodable(data_type.value_type)
        return (
            pat.is_null(data_type)
            or pat.is_boolean(data_type)
            or pat.is_integer(data_type)
            or pat.is_float32(data_type)
            or pat.is_float64(data_type)
            or pat.is_decimal(data_type)
            or pat.is_string(data_type)
            or pat.is_large_string(data_type)
            or pat.is_binary(data_type)
            or pat.is_large_binary(data_type)
            or pat.is_fixed_size_binary(data_type)
            or pat.is_date(data_type)
            or pat.is_timestamp(data_type)
        )

    def as_text(value):
        if value is None:
            return None
        # Exact types only: named tuples such as Arrow intervals read better as str().
        if type(value) in (dict, list, tuple):
            return json.dumps(to_json_compatible(value), default=str)
        return str(value)

    for index, field in enumerate(table.schema):
        if encodable(field.type):
            continue
        column = table.column(index)
        if pat.is_dictionary(field.type) and encodable(field.type.value_type):
            converted = column.cast(field.type.value_type)
        else:
            converted = pa.array([as_text(value) for value in column.to_pylist()], type=pa.string())
        table = table.set_column(index, pa.field(field.name, converted.type, field.nullable), converted)
    return table


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
        # start_server replaces this with one catalog shared by all connections.
        self.catalog: PgCatalog | None = None

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

    def handle_connect(self, ip, port, callback=callable, **kwargs):
        """Handle connection.

        riffq adds keyword arguments here across releases (``server_name``,
        ``metadata``); accept and ignore the ones this server does not use.
        """
        callback(True)

    def handle_disconnect(self, ip, port, callback=callable, **kwargs):
        """Handle disconnection."""
        callback(True)

    def _send_table(self, table, callback):
        """Send a fully fetched result, so late execution errors reach the client."""
        import pyarrow as pa

        table = wire_safe_table(table)
        self.send_reader(pa.RecordBatchReader.from_batches(table.schema, table.to_batches()), callback)

    def _send_result(self, result, callback):
        reader = result_to_record_batch_reader(result, self.layer.adapter)
        self._send_table(reader.read_all(), callback)

    def _handle_query(self, sql, callback, **kwargs):
        """Handle a SQL query."""
        try:
            query_args = kwargs.get("query_args")
            describe = bool(kwargs.get("do_describe"))
            sql_lower = sql.lower().strip()

            user_attributes = self._user_attributes()

            # Each executor thread gets its own cursor so concurrent reads do not
            # serialize on a single shared connection. For DuckDB this is an
            # independent handle over the same database; other adapters fall back
            # to a lock-guarded wrapper preserving today's behavior.
            cursor = self.layer.adapter.cursor()

            # Session configuration is acknowledged without touching the database.
            if sql_lower.startswith("set "):
                result = cursor.execute("SELECT 1 as ok WHERE FALSE")
                self._send_result(result, callback)
                return

            session_command = _SESSION_COMMAND.match(sql)
            if session_command:
                keyword = " ".join(session_command.group(1).lower().split())
                callback(_SESSION_COMMAND_TAGS[keyword], is_tag=True)
                return

            show_statement = _SHOW_STATEMENT.match(sql)
            if show_statement:
                setting = " ".join(show_statement.group(1).lower().split())
                setting = _SESSION_SETTING_ALIASES.get(setting, setting)
                if setting in _SESSION_SETTINGS:
                    import pyarrow as pa

                    self._send_table(pa.table({setting: [_SESSION_SETTINGS[setting]]}), callback)
                    return

            # Only parsed single statements qualify for compatibility responses.
            if self._try_handle_system_query(
                sql, sql_lower, callback, cursor, query_args=query_args, describe=describe
            ):
                return

            sql = inline_parameters(sql, query_args, dialect=self.layer.dialect, describe=describe)

            # Active controls already restrict the transport to one SELECT.
            if not controls_are_active(self.layer):
                require_read_only(sql, self.layer.dialect)

            rendered_sql = rewrite_transport_sql(
                self.layer,
                sql,
                strict=False,
                user_attributes=user_attributes,
                transport="PostgreSQL",
            )

            # Execute the query. A Describe runs the same statement to learn its
            # columns: rewriting it to fetch no rows would regenerate the SQL, and a
            # query that only fails after regeneration could not be told apart from
            # one that really fails.
            result = cursor.execute(rendered_sql)

            # Convert to Arrow and send
            self._send_result(result, callback)

        except Exception:
            logging.exception("Error executing query")
            # Raise so _run_query reports a PG ErrorResponse to the client.
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

    def _catalog(self) -> PgCatalog:
        catalog = getattr(self, "catalog", None)
        if catalog is None:
            catalog = self.catalog = PgCatalog(self.layer)
        return catalog

    def _try_handle_system_query(
        self, sql: str, sql_lower: str, callback, cursor, query_args=None, describe: bool = False
    ) -> bool:
        """Answer PostgreSQL catalog queries from the session's catalog. Returns True if handled."""

        import sqlglot
        from sqlglot import exp

        # Dispatch on parsed references, never on strings or comments in caller SQL.
        statements = [statement for statement in sqlglot.parse(sql, dialect=self.layer.dialect) if statement]
        if len(statements) != 1:
            if controls_are_active(self.layer):
                from sidemantic.core.semantic_layer import SecurityError

                raise SecurityError("PostgreSQL requires a single statement while security controls are active.")
            return False
        statement = statements[0]
        if not isinstance(statement, (exp.Select, exp.SetOperation)):
            return False
        semantic_names = {name.lower() for name in self.layer.graph.models}
        if not is_catalog_query(statement, semantic_names):
            return False

        # The catalog holds only what this session may discover: its accessible
        # semantic models, plus physical tables when no control is active.
        table = self._catalog().query(
            inline_parameters(sql, query_args, dialect="duckdb", describe=describe),
            self._visible_model_names(),
            include_sources=not controls_are_active(self.layer),
        )
        self._send_table(postgres_column_names(statement, table), callback)
        return True

    def _run_query(self, sql, callback, **kwargs):
        """Run a query on a worker thread and report failures as PostgreSQL errors.

        An exception raised on the executor thread never reaches riffq, which
        would then answer with an empty result. The callback carries the error.
        """
        try:
            self._handle_query(sql, callback, **kwargs)
        except Exception as exc:
            callback(error_response(exc), is_error=True)

    def handle_query(self, sql, callback=callable, **kwargs):
        """Handle query in executor thread pool."""
        self.executor.submit(self._run_query, sql, callback, **kwargs)
