"""Apache Ossie importer backed by preserved documents and scoped lowering."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.semantic_catalog import (
    AmbiguousSemanticScopeError,
    CompiledSemanticScope,
    DuplicateSemanticScopeError,
    SemanticCatalog,
    SemanticScopeNotFoundError,
)
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie import (
    OssieConsumerProfile,
    OssieDiagnostic,
    OssieDocument,
    OssieImportPolicy,
    OssieLoweringResult,
    OssieParseOptions,
    OssieParseResult,
    OssiePreservationPolicy,
    OssieSerialization,
    OssieSerializationResult,
    lower_ossie_document,
    parse_ossie_document,
    require_synthesized_document,
    serialize_ossie_document,
    sort_diagnostics,
    synthesize_ossie_document,
)

_GENERATED_DIRECTORIES = frozenset({"dbt_packages", "target"})


class OssieImportError(ValueError):
    """Raised when an Ossie source cannot produce the requested executable scope."""

    def __init__(self, message: str, *, diagnostics: tuple[OssieDiagnostic, ...] = ()) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics


@dataclass(frozen=True, slots=True)
class OssieProjectResult:
    """Preserved per-document results and their isolated compiled scopes."""

    lowerings: tuple[OssieLoweringResult, ...]
    catalog: SemanticCatalog

    @property
    def diagnostics(self) -> tuple[OssieDiagnostic, ...]:
        return sort_diagnostics(diagnostic for result in self.lowerings for diagnostic in result.diagnostics)

    @property
    def valid(self) -> bool:
        return all(result.valid for result in self.lowerings)


class OssieAdapter(BaseAdapter):
    """Import Apache Ossie files without flattening document scopes."""

    def __init__(
        self,
        *,
        target_dialect: str = "duckdb",
        source_dialect: str | None = None,
        scope_id: str | None = None,
        import_policy: OssieImportPolicy | str = OssieImportPolicy.STRICT,
        preserve_source: bool = False,
        consumer_profile: OssieConsumerProfile | str = OssieConsumerProfile.OSSIE_CORE,
        export_scope_name: str | None = None,
        expression_dialect: str | None = None,
        schema_version: str = "0.2.0.dev0",
        serialization: OssieSerialization | str | None = None,
    ) -> None:
        if not target_dialect.strip():
            raise ValueError("target_dialect must be a non-empty string")
        self._target_dialect = target_dialect
        self._scope_id = scope_id
        self._export_scope_name = export_scope_name
        self._expression_dialect = expression_dialect
        self._schema_version = schema_version
        self._serialization = OssieSerialization(serialization) if serialization is not None else None
        self._parse_options = OssieParseOptions(
            consumer_profile=consumer_profile,
            import_policy=import_policy,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            preservation_policy=(
                OssiePreservationPolicy.SOURCE_BYTES if preserve_source else OssiePreservationPolicy.CANONICAL_DATA
            ),
            validate_schema=True,
        )

    def parse_document(self, source: str | Path) -> OssieLoweringResult:
        """Parse, validate, preserve, and lower one exact Ossie file."""
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")
        if not source_path.is_file():
            raise ValueError("parse_document requires one exact Ossie file")
        parsed = parse_ossie_document(
            source_path.read_bytes(),
            source_identifier=str(source_path),
            options=self._parse_options,
        )
        return lower_ossie_document(parsed, target_dialect=self._target_dialect)

    def parse_catalog(self, source: str | Path) -> OssieProjectResult:
        """Load one file or a deterministic directory of isolated documents."""
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")

        if source_path.is_file():
            lowerings = (self.parse_document(source_path),)
            return OssieProjectResult(lowerings=lowerings, catalog=lowerings[0].catalog)

        candidates = sorted(
            path
            for path in source_path.rglob("*")
            if path.is_file()
            and path.suffix.lower() in {".json", ".yaml", ".yml"}
            and not any(part in _GENERATED_DIRECTORIES for part in path.relative_to(source_path).parts[:-1])
        )
        lowerings = tuple(self.parse_document(path) for path in candidates)
        scopes: list[CompiledSemanticScope] = []
        for lowering, path in zip(lowerings, candidates, strict=True):
            relative = path.relative_to(source_path).as_posix()
            for scope in lowering.catalog.scopes.values():
                qualified_id = f"{relative}::{scope.scope_id}"
                scopes.append(
                    CompiledSemanticScope(
                        scope_id=qualified_id,
                        document_id=scope.document_id,
                        content_id=scope.content_id,
                        compilation_id=scope.compilation_id,
                        revision_id=scope.revision_id,
                        runtime=scope.runtime,
                        target_dialect=scope.target_dialect,
                        lowering_policy=scope.lowering_policy,
                        valid=scope.valid,
                        diagnostics=scope.diagnostics,
                        provenance={**dict(scope.provenance), "unqualified_scope_id": scope.scope_id},
                    )
                )
        try:
            catalog = SemanticCatalog(scopes)
        except DuplicateSemanticScopeError as exc:  # defensive: qualified IDs should already be unique
            raise OssieImportError(str(exc)) from exc
        return OssieProjectResult(lowerings=lowerings, catalog=catalog)

    def parse(self, source: str | Path) -> SemanticGraph:
        """Return one explicitly selected graph for the legacy adapter surface."""
        project = self.parse_catalog(source)
        if any(not lowering.valid for lowering in project.lowerings):
            raise OssieImportError(
                "Apache Ossie import failed validation; inspect diagnostics for the exact source paths.",
                diagnostics=project.diagnostics,
            )
        try:
            return project.catalog.resolve_scope(self._scope_id).graph
        except (AmbiguousSemanticScopeError, SemanticScopeNotFoundError) as exc:
            raise OssieImportError(str(exc), diagnostics=project.diagnostics) from exc

    @staticmethod
    def _output_serialization(output_path: Path, explicit: OssieSerialization | str | None) -> OssieSerialization:
        if explicit is not None:
            return OssieSerialization(explicit)
        return OssieSerialization.JSON if output_path.suffix.lower() == ".json" else OssieSerialization.YAML

    def export_document(
        self,
        source: OssieDocument | OssieParseResult | OssieLoweringResult,
        output_path: str | Path,
        *,
        serialization: OssieSerialization | str | None = None,
        exact_source: bool = False,
    ) -> OssieSerializationResult:
        """Validate and write one preserved Ossie document."""
        if isinstance(source, OssieLoweringResult):
            document = source.document
            profile = source.parse_result.profile
        elif isinstance(source, OssieParseResult):
            document = source.document
            profile = source.profile
        else:
            document = source
            profile = None
        destination = Path(output_path)
        output_serialization = self._output_serialization(destination, serialization or self._serialization)
        result = serialize_ossie_document(
            document,
            output_serialization,
            exact_source=exact_source,
            profile=profile,
            consumer_profile=self._parse_options.consumer_profile,
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(result.data)
        return result

    def export(
        self,
        graph: SemanticGraph,
        output_path: str | Path,
        *,
        scope_name: str | None = None,
        expression_dialect: str | None = None,
        schema_version: str | None = None,
        serialization: OssieSerialization | str | None = None,
        portable_only: bool = False,
    ) -> None:
        """Synthesize and write one schema-valid logical Ossie document.

        Graph synthesis is intentionally explicit: a lowered runtime graph does
        not retain enough information to guess its semantic-model container or
        the dialect of its SQL strings.
        """
        destination = Path(output_path)
        output_serialization = self._output_serialization(destination, serialization or self._serialization)
        selected_scope = scope_name or self._export_scope_name
        selected_dialect = expression_dialect or self._expression_dialect
        if selected_scope is None:
            raise ValueError("Ossie graph export requires an explicit scope_name")
        if selected_dialect is None:
            raise ValueError("Ossie graph export requires an explicit expression_dialect")
        synthesis = synthesize_ossie_document(
            graph,
            scope_name=selected_scope,
            expression_dialect=selected_dialect,
            schema_version=schema_version or self._schema_version,
            serialization=output_serialization,
            consumer_profile=self._parse_options.consumer_profile,
            portable_only=portable_only,
        )
        document = require_synthesized_document(synthesis)
        if synthesis.diagnostics:
            import warnings

            for diagnostic in synthesis.diagnostics:
                warnings.warn(diagnostic.message, UserWarning, stacklevel=2)
        self.export_document(document, destination, serialization=output_serialization)
