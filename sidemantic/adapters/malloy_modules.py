"""Malloy-specific module binding and local import resolution."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import Literal
from urllib.parse import urlsplit

from sidemantic.core.inheritance import merge_model
from sidemantic.core.model import Model
from sidemantic.core.schema_exposure import SchemaExposure
from sidemantic.fidelity import record_import_feature


@dataclass(frozen=True)
class MalloyLocation:
    """Source location for a module statement."""

    path: Path
    line: int
    column: int

    @property
    def display(self) -> str:
        return f"{self.line}:{self.column}"


@dataclass(frozen=True)
class MalloyImportItem:
    """One selective import using Malloy's ``local is exported`` order."""

    local_name: str
    exported_name: str


@dataclass(frozen=True)
class MalloyImportStatement:
    specifier: str
    items: tuple[MalloyImportItem, ...] | None
    location: MalloyLocation


@dataclass(frozen=True)
class MalloyExportStatement:
    names: tuple[str, ...]
    location: MalloyLocation


@dataclass(frozen=True)
class MalloySourceStatement:
    models: tuple[Model, ...]
    location: MalloyLocation


@dataclass(frozen=True)
class MalloyNamedStatement:
    """A non-source declaration which participates in module visibility."""

    kind: Literal["given", "type", "query"]
    values: tuple[tuple[str, str], ...]
    location: MalloyLocation


MalloyStatement = MalloyImportStatement | MalloyExportStatement | MalloySourceStatement | MalloyNamedStatement


@dataclass(frozen=True)
class MalloyModule:
    path: Path
    statements: tuple[MalloyStatement, ...]
    user_types: dict[str, str] = field(default_factory=dict)
    given: dict[str, str] = field(default_factory=dict)


class MalloyResolutionError(ValueError):
    """A source-backed Malloy module resolution failure."""

    def __init__(
        self,
        code: str,
        message: str,
        location: MalloyLocation,
        *,
        chain: tuple[Path, ...] = (),
    ):
        self.code = code
        self.location = location
        self.chain = chain
        chain_text = ""
        if chain:
            chain_text = f" ({' -> '.join(str(path) for path in chain)})"
        super().__init__(f"{location.path}:{location.display}: {message}{chain_text}")


@dataclass(frozen=True)
class _Symbol:
    module_path: Path
    declared_name: str
    kind: Literal["source", "given", "type", "query"] = "source"


@dataclass
class _Definition:
    symbol: _Symbol
    model: Model
    scope: dict[str, _Symbol]
    location: MalloyLocation


@dataclass(frozen=True)
class _QueryDefinition:
    """A retained query plus the source binding visible at its declaration."""

    raw_definition: str
    scope: dict[str, _Symbol]
    location: MalloyLocation
    source_name: str | None
    source_symbol: _Symbol | None


@dataclass
class _BoundModule:
    module: MalloyModule
    namespace: dict[str, _Symbol]
    exports: dict[str, _Symbol]
    local_symbols: list[_Symbol]


@dataclass(frozen=True)
class MalloyResolvedQuery:
    """A visible query bound to its defining scope's emitted graph source."""

    raw_definition: str
    location: MalloyLocation
    source_model: str | None

    def __iter__(self):
        """Preserve the legacy raw-definition/location unpacking contract."""
        yield self.raw_definition
        yield self.location


@dataclass(frozen=True)
class MalloyResolution:
    models: dict[str, Model]
    exports_by_file: dict[Path, tuple[str, ...]]
    diagnostics: tuple[dict[str, object], ...]
    user_types: dict[str, str]
    given: dict[str, str]
    queries: dict[str, MalloyResolvedQuery]


class MalloyModuleResolver:
    """Resolve a graph of local Malloy modules into Sidemantic models."""

    def __init__(
        self,
        parse_module: Callable[[Path], MalloyModule],
        *,
        strict: bool,
        import_root: Path,
    ):
        self._parse_module = parse_module
        self.strict = strict
        self.import_root = import_root.resolve()
        self._modules: dict[Path, MalloyModule] = {}
        self._bound: dict[Path, _BoundModule] = {}
        self._binding: list[Path] = []
        self._definitions: dict[_Symbol, _Definition] = {}
        self._query_definitions: dict[_Symbol, _QueryDefinition] = {}
        self._symbol_values: dict[_Symbol, str] = {}
        self._symbol_locations: dict[_Symbol, MalloyLocation] = {}
        self._diagnostics: list[dict[str, object]] = []
        self._invalid_symbols: set[_Symbol] = set()

    def resolve(self, entries: list[Path]) -> MalloyResolution:
        """Resolve entry modules and their reachable local dependencies."""
        canonical_entries = [entry.resolve() for entry in entries]
        for entry in canonical_entries:
            self._bind_module(entry, self._synthetic_location(entry))

        requested: list[tuple[str, _Symbol, MalloyLocation]] = []
        for entry in canonical_entries:
            bound = self._bound.get(entry)
            if bound is None:
                continue
            # A root file contributes its visible sources. Non-source declarations
            # remain module-scoped metadata and do not become graph models.
            for name, symbol in bound.namespace.items():
                if symbol.kind == "source":
                    requested.append((name, symbol, self._definitions[symbol].location))

        visible_symbols: dict[str, _Symbol] = {}
        blocked_visible_names: set[str] = set()
        for entry in canonical_entries:
            bound = self._bound.get(entry)
            if bound is not None:
                for name, symbol in bound.namespace.items():
                    if symbol.kind == "source" or name in blocked_visible_names:
                        continue
                    previous = visible_symbols.get(name)
                    if previous is not None and previous != symbol:
                        self._problem(
                            "malloy_flat_symbol_conflict",
                            f"distinct Malloy declarations both require metadata name '{name}'",
                            self._symbol_locations[symbol],
                            status="rejected",
                        )
                        visible_symbols.pop(name, None)
                        blocked_visible_names.add(name)
                        continue
                    visible_symbols[name] = symbol

        valid_query_symbols: dict[str, _Symbol] = {}
        for visible_name, symbol in visible_symbols.items():
            if symbol.kind != "query":
                continue
            query = self._query_definitions[symbol]
            if query.source_name is None:
                # The query mapper owns diagnostics for unsupported source/query
                # shapes. Only direct named sources can be safely module-bound.
                valid_query_symbols[visible_name] = symbol
                continue
            if query.source_symbol is None or query.source_symbol.kind != "source":
                self._problem(
                    "malloy_query_source_reference_not_found",
                    (
                        f"query '{symbol.declared_name}' references unavailable source "
                        f"'{query.source_name}' in its defining scope"
                    ),
                    query.location,
                    status="rejected",
                )
                continue
            requested.append((query.source_name, query.source_symbol, query.location))
            valid_query_symbols[visible_name] = symbol

        emitted: dict[str, Model] = {}
        owners: dict[str, _Symbol] = {}
        blocked_names: set[str] = set()
        dependents: dict[str, set[str]] = {}
        resolving: list[_Symbol] = []
        resolved: dict[_Symbol, Model] = {}

        def drop_with_dependents(name: str) -> None:
            pending = [name]
            dropped_names: set[str] = set()
            while pending:
                dropped = pending.pop()
                if dropped in dropped_names:
                    continue
                dropped_names.add(dropped)
                emitted.pop(dropped, None)
                owners.pop(dropped, None)
                pending.extend(dependents.get(dropped, ()))

        request_index = 0
        processed_requests: set[tuple[str, _Symbol]] = set()
        while request_index < len(requested):
            output_name, symbol, location = requested[request_index]
            request_index += 1
            request_identity = (output_name, symbol)
            if request_identity in processed_requests:
                continue
            processed_requests.add(request_identity)
            if output_name in blocked_names:
                continue
            flattened = self._resolve_symbol(symbol, resolving, resolved)
            if flattened is None:
                continue
            definition = self._definitions[symbol]
            model = definition.model
            if model.extends and flattened.schema_exposure is not None:
                # Exposure controls are a pre-introspection safety boundary, so an
                # inherited physical source must carry them even while the adapter
                # otherwise preserves raw inheritance fields for downstream
                # resolution.
                model = model.model_copy(
                    update={
                        "schema_exposure": flattened.schema_exposure,
                        "auto_dimensions": flattened.auto_dimensions,
                    }
                )
            previous = owners.get(output_name)
            if previous is not None and previous != symbol:
                self._problem(
                    "malloy_flat_name_conflict",
                    f"distinct Malloy sources both require graph name '{output_name}'",
                    location,
                    status="rejected",
                )
                blocked_names.add(output_name)
                drop_with_dependents(output_name)
                continue
            owners[output_name] = symbol
            emitted[output_name] = self._with_provenance(model, output_name, symbol.module_path)

            # Raw adapter models deliberately retain ``extends``. Ensure the
            # referenced parent is emitted under the exact local binding name so
            # downstream inheritance resolution can flatten it later.
            dependency_names: list[str] = []
            if model.extends:
                dependency_names.append(model.extends)
            dependency_names.extend(relationship.related_model for relationship in model.relationships)
            dependency_names.extend(
                relationship.through for relationship in model.relationships if relationship.through
            )
            for dependency_name in dependency_names:
                dependency_symbol = definition.scope.get(dependency_name)
                if dependency_symbol is not None:
                    dependents.setdefault(dependency_name, set()).add(output_name)
                    requested.append((dependency_name, dependency_symbol, definition.location))

        exports_by_file = {
            path: tuple(bound.exports) for path, bound in sorted(self._bound.items(), key=lambda item: str(item[0]))
        }
        return MalloyResolution(
            models=emitted,
            exports_by_file=exports_by_file,
            diagnostics=tuple(self._diagnostics),
            user_types={
                name: self._symbol_values[symbol] for name, symbol in visible_symbols.items() if symbol.kind == "type"
            },
            given={
                name: self._symbol_values[symbol] for name, symbol in visible_symbols.items() if symbol.kind == "given"
            },
            queries={
                name: MalloyResolvedQuery(
                    raw_definition=self._query_definitions[symbol].raw_definition,
                    location=self._query_definitions[symbol].location,
                    source_model=self._query_definitions[symbol].source_name,
                )
                for name, symbol in valid_query_symbols.items()
            },
        )

    def _bind_module(self, path: Path, import_location: MalloyLocation) -> _BoundModule | None:
        path = path.resolve()
        if path in self._bound:
            return self._bound[path]
        if path in self._binding:
            start = self._binding.index(path)
            chain = tuple(self._binding[start:] + [path])
            self._problem(
                "malloy_import_cycle",
                "circular Malloy import",
                import_location,
                chain=chain,
                status="unsupported",
            )
            return None

        module = self._load_module(path, import_location)
        if module is None:
            return None

        self._binding.append(path)
        namespace: dict[str, _Symbol] = {}
        local_symbols: list[_Symbol] = []
        explicit_exports: dict[str, _Symbol] = {}
        invalid_names: set[str] = set()
        saw_export = False
        try:
            for statement in module.statements:
                if isinstance(statement, MalloyImportStatement):
                    imported_path = self._resolve_import(path, statement)
                    if imported_path is None:
                        continue
                    imported = self._bind_module(imported_path, statement.location)
                    if imported is None:
                        continue
                    if statement.items is None:
                        imports = [(name, name, symbol) for name, symbol in imported.exports.items()]
                    else:
                        imports = []
                        for item in statement.items:
                            symbol = imported.exports.get(item.exported_name)
                            if symbol is None:
                                self._problem(
                                    "malloy_selective_import_not_found",
                                    f"'{item.exported_name}' is not exported by {imported_path}",
                                    statement.location,
                                    status="unsupported",
                                )
                                continue
                            imports.append((item.local_name, item.exported_name, symbol))
                    for local_name, _, symbol in imports:
                        existing = namespace.get(local_name)
                        if existing == symbol:
                            continue
                        if existing is not None or local_name in invalid_names:
                            self._problem(
                                "malloy_import_name_conflict",
                                f"import cannot redefine '{local_name}'",
                                statement.location,
                                status="rejected",
                            )
                            namespace.pop(local_name, None)
                            invalid_names.add(local_name)
                            continue
                        namespace[local_name] = symbol
                    continue

                if isinstance(statement, MalloySourceStatement):
                    for model in statement.models:
                        if model.name in namespace or model.name in invalid_names:
                            self._problem(
                                "malloy_duplicate_source",
                                f"source definition cannot redefine '{model.name}'",
                                statement.location,
                                status="rejected",
                            )
                            namespace.pop(model.name, None)
                            invalid_names.add(model.name)
                            continue
                        symbol = _Symbol(path, model.name, "source")
                        namespace[model.name] = symbol
                        local_symbols.append(symbol)
                        self._definitions[symbol] = _Definition(
                            symbol=symbol,
                            model=model,
                            scope=dict(namespace),
                            location=statement.location,
                        )
                    continue

                if isinstance(statement, MalloyNamedStatement):
                    for name, value in statement.values:
                        if name in namespace or name in invalid_names:
                            self._problem(
                                "malloy_duplicate_symbol",
                                f"{statement.kind} definition cannot redefine '{name}'",
                                statement.location,
                                status="rejected",
                            )
                            namespace.pop(name, None)
                            invalid_names.add(name)
                            continue
                        symbol = _Symbol(path, name, statement.kind)
                        namespace[name] = symbol
                        local_symbols.append(symbol)
                        self._symbol_values[symbol] = value
                        self._symbol_locations[symbol] = statement.location
                        if statement.kind == "query":
                            source_name = self._direct_query_source(value)
                            self._query_definitions[symbol] = _QueryDefinition(
                                raw_definition=value,
                                scope=dict(namespace),
                                location=statement.location,
                                source_name=source_name,
                                source_symbol=namespace.get(source_name) if source_name is not None else None,
                            )
                    continue

                saw_export = True
                for name in statement.names:
                    symbol = namespace.get(name)
                    if symbol is None:
                        self._problem(
                            "malloy_export_forward_reference",
                            f"exported name '{name}' has not been defined or imported yet",
                            statement.location,
                            status="unsupported",
                        )
                        continue
                    explicit_exports[name] = symbol
        finally:
            self._binding.pop()

        if saw_export:
            exports = {name: symbol for name, symbol in explicit_exports.items() if name not in invalid_names}
        else:
            # Malloy's default export set contains definitions declared in this
            # document, not names which the document merely imported.
            exports = {
                symbol.declared_name: symbol for symbol in local_symbols if symbol.declared_name not in invalid_names
            }
        bound = _BoundModule(module, namespace, exports, local_symbols)
        self._bound[path] = bound
        return bound

    def _load_module(self, path: Path, location: MalloyLocation) -> MalloyModule | None:
        cached = self._modules.get(path)
        if cached is not None:
            return cached
        if not path.exists() or not path.is_file():
            self._problem(
                "malloy_import_missing",
                f"Malloy module does not exist: {path}",
                location,
                status="unsupported",
            )
            return None
        module = self._parse_module(path)
        self._modules[path] = module
        return module

    def _resolve_import(self, importer: Path, statement: MalloyImportStatement) -> Path | None:
        specifier = statement.specifier.strip()
        parsed = urlsplit(specifier)
        windows_absolute = PureWindowsPath(specifier).is_absolute() or bool(re.match(r"^[A-Za-z]:", specifier))
        if (
            not specifier
            or parsed.scheme
            or parsed.netloc
            or specifier.startswith("//")
            or Path(specifier).is_absolute()
            or windows_absolute
        ):
            self._problem(
                "malloy_nonlocal_import",
                f"only project-relative local Malloy imports are supported: {specifier!r}",
                statement.location,
                status="rejected",
            )
            return None
        candidate = (importer.parent / specifier).resolve()
        try:
            candidate.relative_to(self.import_root)
        except ValueError:
            self._problem(
                "malloy_import_root_escape",
                f"import resolves outside Malloy project root {self.import_root}: {specifier!r}",
                statement.location,
                status="rejected",
            )
            return None
        if not candidate.exists() or not candidate.is_file():
            self._problem(
                "malloy_import_missing",
                f"imported Malloy file does not exist: {candidate}",
                statement.location,
                status="unsupported",
            )
            return None
        return candidate

    def _resolve_symbol(
        self,
        symbol: _Symbol,
        resolving: list[_Symbol],
        resolved: dict[_Symbol, Model],
    ) -> Model | None:
        if symbol in resolved:
            return resolved[symbol]
        if symbol in self._invalid_symbols:
            return None
        if symbol in resolving:
            start = resolving.index(symbol)
            cycle = resolving[start:] + [symbol]
            definition = self._definitions[symbol]
            self._problem(
                "malloy_inheritance_cycle",
                "circular Malloy source inheritance",
                definition.location,
                chain=tuple(item.module_path for item in cycle),
                status="rejected",
            )
            self._invalid_symbols.update(cycle)
            return None

        definition = self._definitions.get(symbol)
        if definition is None:
            return None
        model = definition.model
        resolving.append(symbol)
        try:
            if model.extends:
                parent_symbol = definition.scope.get(model.extends)
                if parent_symbol is None:
                    self._problem(
                        "malloy_source_reference_not_found",
                        f"source '{model.name}' references unavailable source '{model.extends}'",
                        definition.location,
                        status="rejected",
                    )
                    self._invalid_symbols.add(symbol)
                    return None
                parent = self._resolve_symbol(parent_symbol, resolving, resolved)
                if parent is None:
                    self._invalid_symbols.add(symbol)
                    return None
                # Validate that the bound inheritance chain can be represented by
                # the generic model merger, but preserve the raw child in the
                # adapter graph. Existing compile/export paths own flattening.
                model = merge_model(model, parent)
                model = model.model_copy(
                    update={
                        "schema_exposure": self._merge_schema_exposure(
                            parent.schema_exposure, definition.model.schema_exposure
                        )
                    }
                )
        finally:
            resolving.pop()
        resolved[symbol] = model
        return model

    @staticmethod
    def _merge_schema_exposure(parent: SchemaExposure | None, child: SchemaExposure | None) -> SchemaExposure | None:
        """Compose inherited Malloy visibility edits as a narrowing operation."""
        if parent is None:
            return child
        if child is None:
            return parent

        private = list(dict.fromkeys([*parent.private, *child.private]))
        private_set = set(private)
        excluded = set(parent.except_fields) | set(child.except_fields) | private_set
        if parent.accept is not None and child.accept is not None:
            child_accept = set(child.accept)
            accept = [name for name in parent.accept if name in child_accept and name not in excluded]
        elif parent.accept is not None:
            accept = [name for name in parent.accept if name not in excluded]
        elif child.accept is not None:
            accept = [name for name in child.accept if name not in excluded]
        else:
            accept = None

        data: dict[str, object] = {
            "strict": parent.strict or child.strict,
            "include_primary_key": parent.include_primary_key or child.include_primary_key,
            "private": private,
        }
        if accept is not None:
            data["accept"] = accept
        else:
            data["except"] = [
                name for name in dict.fromkeys([*parent.except_fields, *child.except_fields]) if name not in private_set
            ]
        return SchemaExposure.model_validate(data)

    def _diagnose_unsupported_query(self, location: MalloyLocation, name: str) -> None:
        """Record query visibility without treating a valid declaration as missing."""
        message = f"Malloy query '{name}' is visible, but query execution is not mapped"
        diagnostic = {
            "feature": "malloy_query_execution_unsupported",
            "status": "unsupported",
            "detail": message,
            "source": str(location.path),
            "location": location.display,
        }
        self._diagnostics.append(diagnostic)
        record_import_feature(
            "malloy_query_execution_unsupported",
            "unsupported",
            detail=message,
            source=str(location.path),
            location=location.display,
        )

    @staticmethod
    def _with_provenance(model: Model, name: str, path: Path) -> Model:
        target = model.model_copy(update={"name": name})
        target._source_format = "Malloy"
        target._source_file = str(path)
        return target

    def _problem(
        self,
        code: str,
        message: str,
        location: MalloyLocation,
        *,
        status: Literal["unsupported", "rejected"],
        chain: tuple[Path, ...] = (),
    ) -> None:
        error = MalloyResolutionError(code, message, location, chain=chain)
        if self.strict:
            raise error
        diagnostic = {
            "feature": code,
            "status": status,
            "detail": message,
            "source": str(location.path),
            "location": location.display,
        }
        if chain:
            diagnostic["chain"] = [str(path) for path in chain]
        self._diagnostics.append(diagnostic)
        record_import_feature(
            code,
            status,
            detail=message,
            source=str(location.path),
            location=location.display,
        )

    @staticmethod
    def _direct_query_source(raw_definition: str) -> str | None:
        """Return the leftmost direct source reference from a retained query.

        Imports stay optional for users who do not install Malloy support. The
        original adapter parse has already syntax-checked this definition; this
        small second parse exists only to retain its module binding rather than
        resolving the raw source name later in an importing document's scope.
        """
        try:
            from antlr4 import CommonTokenStream, InputStream

            from sidemantic.adapters.malloy_grammar import MalloyLexer, MalloyParser
        except ImportError:
            return None

        try:
            lexer = MalloyLexer(InputStream(f"query: {raw_definition}"))
            lexer.removeErrorListeners()
            parser = MalloyParser(CommonTokenStream(lexer))
            parser.removeErrorListeners()
            tree = parser.malloyDocument()
        except Exception:
            return None

        definitions = []

        def walk(context) -> None:
            if isinstance(context, MalloyParser.TopLevelQueryDefContext):
                definitions.append(context)
                return
            for child in context.getChildren():
                if hasattr(child, "getChildren"):
                    walk(child)

        walk(tree)
        if len(definitions) != 1:
            return None
        expression = definitions[0].sqExpr()
        while isinstance(expression, MalloyParser.SQArrowContext):
            expression = expression.sqExpr()
        if not isinstance(expression, MalloyParser.SQIDContext):
            return None
        return expression.id_().getText()

    @staticmethod
    def _synthetic_location(path: Path) -> MalloyLocation:
        return MalloyLocation(path=path, line=1, column=0)
