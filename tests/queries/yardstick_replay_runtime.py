"""Explicit SQLLogicTest batch execution for the Yardstick compatibility suite.

The public query API remains single-statement. This runner owns the setup catalog,
transaction, and temporary measure metadata needed to replay native test records.
Query expansion itself always uses Sidemantic's production rewriter.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.tokens import TokenType

from sidemantic import SemanticLayer
from sidemantic.adapters.yardstick import YardstickAdapter
from sidemantic.core.registry import reset_current_layer, set_current_layer
from sidemantic.sql.query_rewriter import QueryRewriter, YardstickBindingError, YardstickWarning


@dataclass
class ReplayStatement:
    sql: str
    parsed: exp.Expression | None
    has_aggregate: bool


def split_replay_sql(sql: str, dialect: str) -> list[str]:
    """Split SQLLogicTest batches without splitting comments or quoted literals."""
    tokens = sqlglot.tokenize(sql, read=dialect)
    ends = [token.end + 1 for token in tokens if token.token_type == TokenType.SEMICOLON]
    if not ends or ends[-1] < len(sql):
        ends.append(len(sql))
    statements = []
    start = 0
    for end in ends:
        text = sql[start:end].strip()
        start = end
        statement_tokens = sqlglot.tokenize(text, read=dialect)
        if not statement_tokens:
            continue
        if statement_tokens[0].text.upper() == "SEMANTIC":
            text = text[statement_tokens[0].end + 1 :].strip()
        statements.append(text)
    return statements


class YardstickReplayRuntime:
    def __init__(self, layer: SemanticLayer, adapter: YardstickAdapter):
        self.layer = layer
        self.adapter = adapter
        self.permanent = {name.lower(): model for name, model in layer.graph.models.items()}
        self.temporary = {}
        self.warnings_as_errors = False
        self.prepared = {}

    def _install_models(self, parsed: exp.Expression | None = None):
        models = {**self.permanent, **self.temporary}
        if parsed is not None:
            for table in parsed.find_all(exp.Table):
                if table.db.lower() == "main" and table.name.lower() in self.permanent:
                    models[table.name.lower()] = self.permanent[table.name.lower()]
        self.layer.graph.models = {model.name: model for model in models.values()}
        self.layer.graph._mark_dirty()

    def _statements(self, sql: str) -> list[ReplayStatement]:
        statements = []
        for text in split_replay_sql(sql, self.adapter.dialect):
            statement_tokens = sqlglot.tokenize(text, read=self.adapter.dialect)
            has_aggregate = any(
                token.text.upper() == "AGGREGATE" and following.token_type == TokenType.L_PAREN
                for token, following in zip(statement_tokens, statement_tokens[1:])
            )
            try:
                parsed = self.adapter._parse_statements(text)[0]
            except SqlglotError as exc:
                has_modifier = any(
                    token.text.upper() == "AT" and following.token_type == TokenType.L_PAREN
                    for token, following in zip(statement_tokens, statement_tokens[1:])
                )
                if not has_aggregate and not has_modifier:
                    raise duckdb.ParserException(f"Parser Error: {exc}") from exc
                transformed, _calls = QueryRewriter(
                    self.layer.graph, dialect=self.adapter.dialect
                )._replace_yardstick_aggregate_calls(text)
                try:
                    parsed = sqlglot.parse_one(transformed, read=self.adapter.dialect)
                except SqlglotError as exc:
                    raise duckdb.ParserException(f"Parser Error: {exc}") from exc
            if has_aggregate and isinstance(parsed, exp.Command):
                transformed, _calls = QueryRewriter(
                    self.layer.graph, dialect=self.adapter.dialect
                )._replace_yardstick_aggregate_calls(text)
                parsed = sqlglot.parse_one(transformed, read=self.adapter.dialect)
            statements.append(ReplayStatement(text, parsed, has_aggregate))
        return statements

    def _rewrite(self, statement: ReplayStatement) -> tuple[str, list[warnings.WarningMessage]]:
        self._install_models(statement.parsed)
        if statement.has_aggregate and statement.parsed is not None:
            for scope in traverse_scope(statement.parsed):
                for source in scope.sources.values():
                    if isinstance(source, exp.Table) and source.name.lower() not in {
                        name.lower() for name in self.layer.graph.models
                    }:
                        self.layer.adapter.execute(f"SELECT * FROM {source.sql(dialect=self.adapter.dialect)} LIMIT 0")
        with warnings.catch_warnings(record=True) as emitted:
            warnings.simplefilter("always")
            try:
                sql = QueryRewriter(self.layer.graph, dialect=self.adapter.dialect, use_rust_rewriter=False).rewrite(
                    statement.sql
                )
            except YardstickBindingError as exc:
                raise duckdb.BinderException(f"Binder Error: {exc}") from exc
        return sql, [warning for warning in emitted if issubclass(warning.category, YardstickWarning)]

    def _execute(self, sql: str, emitted: list[warnings.WarningMessage] | None = None):
        if self.warnings_as_errors and emitted:
            raise RuntimeError("\n".join(str(warning.message) for warning in emitted))
        result = self.layer.adapter.execute(sql)
        return result.fetchall() if result.description is not None else []

    def _temporary_reads(self, statement: ReplayStatement, names: set[str]) -> set[str]:
        if statement.parsed is None or not statement.has_aggregate:
            return set()
        return {
            source.name.lower()
            for scope in traverse_scope(statement.parsed)
            for source in scope.sources.values()
            if isinstance(source, exp.Table) and not source.db and source.name.lower() in names
        }

    def execute(self, sql: str) -> list[tuple]:
        statements = self._statements(sql)
        permanent_before = self.permanent.copy()
        prepared_before = self.prepared.copy()
        warning_setting_before = self.warnings_as_errors
        pending_temporary = set()
        used_temporary = set()
        catalog_seen = False
        executed_since_catalog = False
        self.layer.adapter.execute("BEGIN TRANSACTION")
        rows = []
        try:
            for statement_index, statement in enumerate(statements):
                parsed = statement.parsed
                if isinstance(parsed, exp.Create) and str(parsed.args.get("kind", "")).upper() == "VIEW":
                    select = parsed.expression
                    model = None
                    if isinstance(select, exp.Select):
                        token = set_current_layer(None)
                        try:
                            model = self.adapter._model_from_create_view(parsed, select)
                        finally:
                            reset_current_layer(token)
                    if model is not None:
                        if catalog_seen and executed_since_catalog:
                            raise RuntimeError(
                                "AS MEASURE batches cannot apply catalog changes after executable statements"
                            )
                        catalog_seen = True
                        executed_since_catalog = False
                        temporary = any(
                            isinstance(prop, exp.TemporaryProperty) for prop in parsed.find_all(exp.TemporaryProperty)
                        )
                        target = self.temporary if temporary else self.permanent
                        name = model.name.lower()
                        create_view = (
                            "CREATE "
                            + ("OR REPLACE " if parsed.args.get("replace") else "")
                            + ("TEMP " if temporary else "")
                            + "VIEW"
                        )
                        view_name = parsed.this.sql(dialect=self.adapter.dialect)
                        source = model.sql or (
                            f"SELECT * FROM {model.table}" if model.table != model.name else "SELECT 1"
                        )
                        self._execute(f"{create_view} {view_name} AS {source}")
                        target[name] = model
                        if temporary:
                            pending_temporary.add(name)
                        self._install_models()
                        continue
                if isinstance(parsed, exp.Drop) and str(parsed.args.get("kind", "")).upper() == "VIEW":
                    table = parsed.this
                    name = table.name.lower()
                    if table.db.lower() == "main" and not table.catalog:
                        drop = parsed.copy()
                        catalog = self.layer.adapter.execute("SELECT current_database()").fetchone()[0]
                        drop.this.set("catalog", exp.to_identifier(catalog, quoted=True))
                        self._execute(drop.sql(dialect=self.adapter.dialect))
                    else:
                        self._execute(statement.sql)
                    if not table.db and name in self.temporary:
                        self.temporary.pop(name, None)
                        pending_temporary.discard(name)
                    else:
                        self.permanent.pop(name, None)
                    self._install_models()
                    catalog_seen = True
                    executed_since_catalog = False
                    continue
                tokens = sqlglot.tokenize(statement.sql, read=self.adapter.dialect)
                command = tokens[0].text.upper() if tokens else ""
                if command == "PREPARE":
                    arguments = statement.sql[tokens[0].end + 1 :].strip()
                    prepare_tokens = sqlglot.tokenize(arguments, read=self.adapter.dialect)
                    name = prepare_tokens[0].text.lower()
                    if len(prepare_tokens) < 3 or prepare_tokens[1].text.upper() != "AS":
                        raise duckdb.ParserException("PREPARE requires AS before its statement")
                    body = self._statements(arguments[prepare_tokens[1].end + 1 :])[0]
                    rewritten, emitted = self._rewrite(body)
                    name_sql = arguments[prepare_tokens[0].start : prepare_tokens[0].end + 1]
                    self._execute(f"PREPARE {name_sql} AS {rewritten}")
                    self.prepared[name] = emitted
                    continue
                if command == "EXECUTE":
                    rows = self._execute(statement.sql, self.prepared.get(tokens[1].text.lower()))
                    continue
                if command == "DEALLOCATE":
                    rows = self._execute(statement.sql)
                    self.prepared.pop(tokens[1].text.lower(), None)
                    continue
                if (
                    tokens
                    and tokens[0].text.upper() == "SET"
                    and any(t.text.lower() == "warnings_as_errors" for t in tokens)
                ):
                    self.warnings_as_errors = any(t.text.lower() == "true" for t in tokens)
                    continue
                reads_temporary = self._temporary_reads(statement, pending_temporary)
                remaining_uses = used_temporary.copy()
                for remaining in statements[statement_index:]:
                    remaining_uses.update(self._temporary_reads(remaining, pending_temporary))
                    if isinstance(remaining.parsed, exp.Drop) and not remaining.parsed.this.db:
                        remaining_uses.add(remaining.parsed.this.name.lower())
                if pending_temporary - remaining_uses:
                    raise RuntimeError(
                        "TEMPORARY AS MEASURE views must be used in the same statement batch as AGGREGATE()"
                    )
                if statement.has_aggregate and reads_temporary and isinstance(parsed, (exp.Select, exp.SetOperation)):
                    raise RuntimeError(
                        "TEMPORARY AS MEASURE views must be used in the same statement batch as AGGREGATE() and cannot be returned directly"
                    )
                if statement.has_aggregate or isinstance(parsed, (exp.Select, exp.SetOperation)):
                    rewritten, emitted = self._rewrite(statement)
                    rows = self._execute(rewritten, emitted)
                    used_temporary.update(reads_temporary)
                else:
                    rows = self._execute(statement.sql)
                if catalog_seen:
                    executed_since_catalog = True
            if pending_temporary - used_temporary:
                raise RuntimeError("TEMPORARY AS MEASURE views must be used in the same statement batch as AGGREGATE()")
            for name in self.temporary:
                self._execute(f"DROP VIEW temp.{exp.to_identifier(name, quoted=True).sql()}")
            self.temporary.clear()
            self._install_models()
            self.layer.adapter.execute("COMMIT")
            return rows
        except Exception:
            self.layer.adapter.execute("ROLLBACK")
            self.permanent = permanent_before
            self.temporary.clear()
            self.prepared = prepared_before
            self.warnings_as_errors = warning_setting_before
            self._install_models()
            raise


def replay_runtime(layer: SemanticLayer, adapter: YardstickAdapter) -> YardstickReplayRuntime:
    runtime = getattr(layer, "_yardstick_replay_runtime", None)
    if runtime is None:
        runtime = YardstickReplayRuntime(layer, adapter)
        layer._yardstick_replay_runtime = runtime
    return runtime
