"""Persist Layer 2 view recipes alongside their database view definitions."""

from __future__ import annotations

from copy import copy, deepcopy

import sqlglot
from sqlglot import exp

from sidemantic.interchange.ossie.identifier import normalize_identifier
from sidemantic.semantic_handoff import RustBackendUnavailableError, UnsupportedSemanticFeaturesError


def _view_key(table):
    return tuple(normalize_identifier(part.sql()) for part in table.parts)


def execute_ossie_view_ddl(layer, sql, *, user_attributes=None):
    """Return (handled, result), registering a recipe only after successful DDL."""
    if "VIEW" not in sql.upper():
        return False, None
    if not any((model.metadata or {}).get("ossie_scope_name") for model in layer.graph.models.values()):
        return False, None
    from sidemantic.sql.ossie_relational import _measure_syntax

    statements = sqlglot.parse(_measure_syntax(sql, layer.dialect), read=layer.dialect)
    if len(statements) != 1:
        return False, None
    statement = statements[0]
    if not isinstance(statement, (exp.Create, exp.Drop)) or str(statement.args.get("kind", "")).upper() != "VIEW":
        return False, None
    from sidemantic.core.transport_security import controls_are_active

    if controls_are_active(layer) or user_attributes is not None:
        from sidemantic.core.semantic_layer import SecurityError

        raise SecurityError("Ossie view DDL cannot enforce active semantic security controls")
    registry = (layer.graph.metadata or {}).get("ossie_views", {})
    if isinstance(statement, exp.Drop):
        tables = statement.args.get("tables") or [statement.this]
        keys = {_view_key(table) for table in tables if isinstance(table, exp.Table)}
        removed = [
            name for name in registry if _view_key(sqlglot.parse_one(name, read=layer.dialect, into=exp.Table)) in keys
        ]
        result = layer.adapter.execute(sql)
        for name in removed:
            layer.graph.metadata["ossie_views"].pop(name)
            layer.graph.metadata.get("ossie_view_columns", {}).pop(name, None)
        if removed:
            layer.graph._version += 1
        return True, result
    table = statement.this
    aliases = []
    if isinstance(table, exp.Schema):
        aliases = table.expressions
        table = table.this
    if not isinstance(table, exp.Table):
        raise ValueError("Ossie CREATE VIEW requires a relation name")
    key = _view_key(table)
    existing = next(
        (name for name in registry if _view_key(sqlglot.parse_one(name, read=layer.dialect, into=exp.Table)) == key),
        None,
    )
    if statement.args.get("exists"):
        catalog_query = (
            exp.select("table_name")
            .from_("information_schema.tables")
            .where(
                exp.column("table_name").eq(table.name),
                exp.column("table_schema").eq(exp.convert(table.db) if table.db else exp.CurrentSchema()),
                exp.column("table_catalog").eq(exp.convert(table.catalog) if table.catalog else exp.CurrentCatalog()),
            )
        )
        if layer.adapter.execute(catalog_query.sql(dialect=layer.dialect)).fetchall():
            # Preserve the database's no-op semantics and never register a recipe
            # that did not replace the existing physical definition.
            statement.set("expression", exp.select("1"))
            return True, layer.adapter.execute(statement.sql(dialect=layer.dialect))
    definition = statement.args.get("expression")
    if definition is None:
        raise ValueError("Ossie CREATE VIEW requires a query")
    if aliases:
        definition = exp.select("*").from_(
            exp.Subquery(
                this=definition.copy(),
                alias=exp.TableAlias(
                    this=exp.to_identifier("__ossie_view_columns"), columns=[alias.copy() for alias in aliases]
                ),
            )
        )
    from sidemantic.sql.ossie_relational import RelationalCompiler

    if not RelationalCompiler(layer.graph, layer.dialect).relevant(definition):
        result = layer.adapter.execute(sql)
        if existing is not None:
            layer.graph.metadata["ossie_views"].pop(existing)
            layer.graph.metadata.get("ossie_view_columns", {}).pop(existing, None)
            layer.graph._version += 1
        return True, result
    definition_sql = definition.sql(dialect=layer.dialect)
    name = table.sql(dialect=layer.dialect)
    # Validate against the proposed registry, including self/transitive references,
    # without exposing a failed replacement to concurrent readers or cached queries.
    compilation_graph = copy(layer.graph)
    compilation_graph.metadata = deepcopy(layer.graph.metadata or {})
    proposed_views = compilation_graph.metadata.setdefault("ossie_views", {})
    if existing is not None:
        proposed_views.pop(existing)
    proposed_views[name] = definition_sql
    layer.last_engine_selection = None
    physical = None
    if layer._use_rust_sql_generator:
        from sidemantic.rust_bridge import compile_ossie_view_semantic_input

        try:
            physical, columns = compile_ossie_view_semantic_input(
                compilation_graph, definition_sql, dialect=layer.dialect
            )
        except (RustBackendUnavailableError, UnsupportedSemanticFeaturesError) as error:
            if layer._rust_no_fallback:
                raise
            layer.last_engine_selection = {"engine": "python", "reason": str(error)}
        else:
            layer.last_engine_selection = {"engine": "rust", "reason": None}
    if physical is None:
        from sidemantic.sql.ossie_relational import compile_ossie_view_definition

        physical, columns = compile_ossie_view_definition(definition_sql, compilation_graph, layer.dialect)
        if layer.last_engine_selection is None:
            layer.last_engine_selection = {"engine": "python", "reason": None}
    statement.set("expression", sqlglot.parse_one(physical, read=layer.dialect))
    result = layer.adapter.execute(statement.sql(dialect=layer.dialect))
    if layer.graph.metadata is None:
        layer.graph.metadata = {}
    if existing is not None:
        layer.graph.metadata.setdefault("ossie_views", {}).pop(existing, None)
        layer.graph.metadata.setdefault("ossie_view_columns", {}).pop(existing, None)
    layer.graph.metadata.setdefault("ossie_views", {})[name] = definition_sql
    layer.graph.metadata.setdefault("ossie_view_columns", {})[name] = columns
    layer.graph._version += 1
    return True, result
