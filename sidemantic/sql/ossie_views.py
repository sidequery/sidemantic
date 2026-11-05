"""Persist Layer 2 view recipes alongside their database view definitions."""

from __future__ import annotations

from copy import copy, deepcopy

import sqlglot
from sqlglot import exp

from sidemantic.interchange.ossie.identifier import normalize_identifier
from sidemantic.semantic_handoff import RustBackendUnavailableError, UnsupportedSemanticFeaturesError


def _view_key(table, dialect="duckdb"):
    if dialect == "duckdb":
        # DuckDB compares identifiers case insensitively, including quoted ones.
        return tuple(part.name.lower() for part in table.parts)
    return tuple(normalize_identifier(part.sql()) for part in table.parts)


def resolve_view_name(graph, table, dialect="duckdb"):
    """Resolve a view using the session's database bindings, when available."""
    metadata = graph.metadata or {}
    key = _view_key(table, dialect)
    bindings = metadata.get("ossie_view_bindings")
    if bindings is not None:
        return next((item["name"] for item in bindings if tuple(item["parts"]) == key), None)
    for name in metadata.get("ossie_views", {}):
        if _view_key(sqlglot.parse_one(name, read=dialect, into=exp.Table), dialect) == key:
            return name
    return None


def _qualified(identity):
    catalog, schema, name = identity
    return exp.Table(
        this=exp.to_identifier(name, quoted=True),
        db=exp.to_identifier(schema, quoted=True),
        catalog=exp.to_identifier(catalog, quoted=True),
    )


class _AmbiguousViewNameError(ValueError):
    pass


class _ViewCatalog:
    """A database snapshot used for DDL identity and query name resolution."""

    def __init__(self, layer):
        self.dialect = layer.dialect
        context = "SELECT CURRENT_CATALOG, CURRENT_SCHEMA"
        if self.dialect == "duckdb":
            context += ", current_setting('search_path')"
        row = layer.adapter.execute(context).fetchone()
        self.catalog, self.schema = row[:2]
        self.schemas = set(
            layer.adapter.execute("SELECT catalog_name, schema_name FROM information_schema.schemata").fetchall()
        )
        self.relations = {
            tuple(row)
            for row in layer.adapter.execute(
                "SELECT table_catalog, table_schema, table_name FROM information_schema.tables"
            ).fetchall()
        }
        self.paths = []
        if self.dialect == "duckdb":
            self.paths.append(("temp", "main"))
            if row[2]:
                # Parsing identifiers keeps quoted dots and commas intact.
                for item in sqlglot.parse_one("SELECT " + row[2], read=self.dialect).expressions:
                    parts = [part.name for part in item.parts]
                    self.paths.append((self.catalog, parts[0]) if len(parts) == 1 else tuple(parts))
        if len(self.paths) <= (1 if self.dialect == "duckdb" else 0):
            self.paths.append((self.catalog, self.schema))
        if self.dialect == "duckdb":
            self.paths.extend([(self.catalog, "main"), ("system", "main"), ("system", "pg_catalog")])

    def key(self, identity):
        return _view_key(_qualified(identity), self.dialect)

    def candidates(self, table, *, create=False, temporary=False):
        if temporary:
            return [("temp", "main", table.name)]
        if table.catalog:
            return [(table.catalog, table.db, table.name)]
        if table.db:
            # DuckDB allows catalog.table as shorthand for catalog.main.table.
            schema = (self.catalog, table.db)
            if self.dialect == "duckdb":
                schema_exists = any(
                    c.lower() == self.catalog.lower() and s.lower() == table.db.lower() for c, s in self.schemas
                )
                catalog = next((c for c, _ in self.schemas if c.lower() == table.db.lower()), None)
                if schema_exists and catalog is not None:
                    raise _AmbiguousViewNameError(
                        f"Ambiguous catalog or schema {table.db!r}; use a fully qualified name"
                    )
                if catalog is not None:
                    schema = (catalog, "main")
                elif not create and table.db.lower() == "main":
                    return [("temp", "main", table.name), (*schema, table.name)]
            return [(*schema, table.name)]
        paths = [(self.catalog, self.schema)] if create else self.paths
        return [(*path, table.name) for path in paths]

    def resolve(self, table, *, create=False, temporary=False):
        candidates = self.candidates(table, create=create, temporary=temporary)
        for candidate in candidates:
            existing = next((item for item in self.relations if self.key(item) == self.key(candidate)), None)
            if existing is not None:
                return existing
        return candidates[0]

    def bindings(self, registry):
        bindings = []
        live = {self.key(identity) for identity in self.relations}
        for name in registry:
            table = sqlglot.parse_one(name, read=self.dialect, into=exp.Table)
            identity = (table.catalog, table.db, table.name)
            if self.key(identity) not in live:
                continue
            aliases = [
                table,
                exp.Table(this=table.this.copy(), db=table.args["db"].copy()),
                exp.Table(this=table.this.copy()),
            ]
            if self.dialect == "duckdb" and table.db == "main":
                aliases.append(exp.Table(this=table.this.copy(), db=table.args["catalog"].copy()))
            for alias in aliases:
                try:
                    resolved = self.resolve(alias)
                except _AmbiguousViewNameError:
                    continue
                if self.key(resolved) == self.key(identity):
                    entry = {"parts": list(_view_key(alias, self.dialect)), "name": name}
                    if entry not in bindings:
                        bindings.append(entry)
        return bindings


def _relation_references(query):
    from sqlglot.optimizer.scope import traverse_scope

    for scope in traverse_scope(query):
        for _, source in scope.selected_sources.values():
            if isinstance(source, exp.Table):
                yield source


def _set_identity(table, identity):
    qualified = _qualified(identity)
    for part in ("catalog", "db", "this"):
        table.set(part, qualified.args[part])


def _refresh_bindings(layer, snapshot):
    registry = (layer.graph.metadata or {}).get("ossie_views", {})
    live = {snapshot.key(identity) for identity in snapshot.relations}
    removed = [
        name
        for name in registry
        if _view_key(sqlglot.parse_one(name, read=layer.dialect, into=exp.Table), layer.dialect) not in live
    ]
    for name in removed:
        registry.pop(name)
        layer.graph.metadata.get("ossie_view_columns", {}).pop(name, None)
    bindings = snapshot.bindings(registry)
    if removed or layer.graph.metadata.get("ossie_view_bindings") != bindings:
        layer.graph.metadata["ossie_view_bindings"] = bindings
        layer.graph._version += 1


def execute_ossie_view_ddl(layer, sql, *, user_attributes=None):
    """Return (handled, result), registering a recipe only after successful DDL."""
    if not any((model.metadata or {}).get("ossie_scope_name") for model in layer.graph.models.values()):
        return False, None
    snapshot = None
    if (layer.graph.metadata or {}).get("ossie_views"):
        snapshot = _ViewCatalog(layer)
        _refresh_bindings(layer, snapshot)
    if "VIEW" not in sql.upper():
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
    snapshot = snapshot or _ViewCatalog(layer)
    registry = (layer.graph.metadata or {}).get("ossie_views", {})
    if isinstance(statement, exp.Drop):
        tables = statement.args.get("tables") or [statement.this]
        keys = {snapshot.key(snapshot.resolve(table)) for table in tables if isinstance(table, exp.Table)}
        removed = [
            name
            for name in registry
            if _view_key(sqlglot.parse_one(name, read=layer.dialect, into=exp.Table), layer.dialect) in keys
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
    temporary = statement.find(exp.TemporaryProperty) is not None
    identity = snapshot.resolve(table, create=True, temporary=temporary)
    key = snapshot.key(identity)
    existing = next(
        (
            name
            for name in registry
            if _view_key(sqlglot.parse_one(name, read=layer.dialect, into=exp.Table), layer.dialect) == key
        ),
        None,
    )
    if statement.args.get("exists"):
        catalog_query = (
            exp.select("table_name")
            .from_("information_schema.tables")
            .where(
                exp.column("table_name").eq(identity[2]),
                exp.column("table_schema").eq(identity[1]),
                exp.column("table_catalog").eq(identity[0]),
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

    definition_sql = definition.sql(dialect=layer.dialect)
    name = _qualified(identity).sql(dialect=layer.dialect)
    # Validate against the proposed registry, including self/transitive references,
    # without exposing a failed replacement to concurrent readers or cached queries.
    compilation_graph = copy(layer.graph)
    compilation_graph.metadata = deepcopy(layer.graph.metadata or {})
    proposed_views = compilation_graph.metadata.setdefault("ossie_views", {})
    if existing is not None:
        proposed_views.pop(existing)
    proposed_views[name] = definition_sql
    snapshot.relations.add(identity)
    definition_catalog = copy(snapshot)
    if layer.dialect == "duckdb":
        definition_catalog.catalog, definition_catalog.schema = identity[:2]
        definition_catalog.paths = [identity[:2], *snapshot.paths]
    compilation_graph.metadata["ossie_view_bindings"] = definition_catalog.bindings(proposed_views)
    # Database views bind their referenced relations at creation time. Preserve
    # that binding when a later caller changes schema or search_path.
    for reference in _relation_references(definition):
        bound = resolve_view_name(compilation_graph, reference, layer.dialect)
        if bound is not None:
            qualified = sqlglot.parse_one(bound, read=layer.dialect, into=exp.Table)
            resolved = (qualified.catalog, qualified.db, qualified.name)
        else:
            if RelationalCompiler(compilation_graph, layer.dialect).model_for(reference) is not None:
                continue
            resolved = definition_catalog.resolve(reference)
            if resolved not in definition_catalog.relations:
                continue
        if not reference.args.get("alias"):
            reference.set("alias", exp.TableAlias(this=reference.this.copy()))
        _set_identity(reference, resolved)
    definition_sql = definition.sql(dialect=layer.dialect)
    proposed_views[name] = definition_sql
    if not RelationalCompiler(compilation_graph, layer.dialect).relevant(definition):
        result = layer.adapter.execute(sql)
        if existing is not None:
            layer.graph.metadata["ossie_views"].pop(existing)
            layer.graph.metadata.get("ossie_view_columns", {}).pop(existing, None)
            layer.graph._version += 1
        return True, result
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
    physical_query = sqlglot.parse_one(physical, read=layer.dialect)
    for reference in _relation_references(physical_query):
        resolved = snapshot.resolve(reference)
        if resolved in snapshot.relations:
            _set_identity(reference, resolved)
    statement.set("expression", physical_query)
    result = layer.adapter.execute(statement.sql(dialect=layer.dialect))
    if layer.graph.metadata is None:
        layer.graph.metadata = {}
    if existing is not None:
        layer.graph.metadata.setdefault("ossie_views", {}).pop(existing, None)
        layer.graph.metadata.setdefault("ossie_view_columns", {}).pop(existing, None)
    layer.graph.metadata.setdefault("ossie_views", {})[name] = definition_sql
    layer.graph.metadata.setdefault("ossie_view_columns", {})[name] = columns
    _refresh_bindings(layer, snapshot)
    layer.graph._version += 1
    return True, result
