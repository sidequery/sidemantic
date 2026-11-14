"""Apache Ossie importer backed by preserved documents and scoped lowering."""

from __future__ import annotations

from collections.abc import Mapping
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
from sidemantic.fidelity import record_export_feature, record_import_note
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
from sidemantic.interchange.ossie.profiles import LATEST_OSSIE_SCHEMA_COMMIT

_GENERATED_DIRECTORIES = frozenset({"dbt_packages", "target"})


class OssieImportError(ValueError):
    """Raised when an Ossie source cannot produce the requested executable scope."""

    def __init__(self, message: str, *, diagnostics: tuple[OssieDiagnostic, ...] = ()) -> None:
        self.diagnostics = diagnostics
        errors = [diagnostic for diagnostic in diagnostics if diagnostic.severity.value == "error"]
        if errors:
            details = [
                f"{diagnostic.json_pointer or '/'}: {diagnostic.message} ({diagnostic.code})"
                for diagnostic in errors[:5]
            ]
            if len(errors) > 5:
                details.append(f"{len(errors) - 5} more errors; inspect the structured diagnostics for all details.")
            message = "\n".join([message, *details])
        super().__init__(message)


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
        schema_revision: str | None = None,
        serialization: OssieSerialization | str | None = None,
    ) -> None:
        if not target_dialect.strip():
            raise ValueError("target_dialect must be a non-empty string")
        self._target_dialect = target_dialect
        self._scope_id = scope_id
        self._export_scope_name = export_scope_name
        self._expression_dialect = expression_dialect
        self._schema_version = schema_version
        self._schema_revision = schema_revision
        self._serialization = OssieSerialization(serialization) if serialization is not None else None
        self._parse_options = OssieParseOptions(
            schema_revision=schema_revision,
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
            graph = project.catalog.resolve_scope(self._scope_id).graph
            for lowering in project.lowerings:
                self._report_projection_losses(lowering.document.canonical_data, source=str(source))
            return graph
        except (AmbiguousSemanticScopeError, SemanticScopeNotFoundError) as exc:
            raise OssieImportError(str(exc), diagnostics=project.diagnostics) from exc

    @staticmethod
    def _report_projection_losses(value: object, *, source: str, pointer: str = "") -> None:
        """Document preservation is lossless; a runtime graph omits vendor annotations."""
        if isinstance(value, Mapping):
            for key, child in value.items():
                location = f"{pointer}/{key}"
                if key == "ai_context" and child:
                    record_import_note(
                        "ossie.ai_context",
                        "AI context is retained only by document preservation, not graph conversion.",
                        source=source,
                        location=location,
                    )
                elif key == "custom_extensions" and isinstance(child, (list, tuple)):
                    foreign = [
                        item for item in child if isinstance(item, Mapping) and item.get("vendor_name") != "SIDEMANTIC"
                    ]
                    if foreign:
                        record_import_note(
                            "ossie.custom_extensions",
                            "Foreign vendor extensions are retained only by document preservation, not graph conversion.",
                            source=source,
                            location=location,
                        )
                else:
                    OssieAdapter._report_projection_losses(child, source=source, pointer=location)
        elif isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                OssieAdapter._report_projection_losses(child, source=source, pointer=f"{pointer}/{index}")

    def preserve_document(self, source: str | Path, output_path: str | Path) -> OssieSerializationResult:
        """Validate and copy a logical document without lowering into a runtime graph.

        Original bytes are reused when source and output serializations agree.
        Changing JSON/YAML serialization preserves the canonical document data.
        """
        source_path = Path(source)
        if not source_path.is_file():
            raise ValueError("Document preservation requires one exact Ossie file")
        from dataclasses import replace

        options = replace(self._parse_options, preservation_policy=OssiePreservationPolicy.SOURCE_BYTES)
        parsed = parse_ossie_document(source_path.read_bytes(), source_identifier=str(source_path), options=options)
        if any(d.severity.value == "error" for d in parsed.diagnostics):
            raise OssieImportError("Ossie document failed validation", diagnostics=parsed.diagnostics)
        from sidemantic.interchange.ossie.semantic_validation import validate_ossie_semantics

        semantics = validate_ossie_semantics(parsed.document, profile=parsed.profile)
        if not semantics.valid:
            raise OssieImportError("Ossie document failed semantic validation", diagnostics=semantics.diagnostics)
        output_serialization = self._output_serialization(Path(output_path), self._serialization)
        return self.export_document(
            parsed, output_path, exact_source=output_serialization == parsed.document.serialization
        )

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
        schema_revision: str | None = None,
        serialization: OssieSerialization | str | None = None,
        portable_only: bool = False,
        official_compatible: bool = False,
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
        selected_revision = schema_revision or self._schema_revision or LATEST_OSSIE_SCHEMA_COMMIT
        selected_version = schema_version or self._schema_version
        if official_compatible:
            if selected_version != "0.2.0.dev0":
                raise ValueError("Official converter compatibility requires Ossie 0.2.0.dev0")
            if (schema_revision or self._schema_revision) not in (
                None,
                LATEST_OSSIE_SCHEMA_COMMIT,
                LATEST_OSSIE_SCHEMA_COMMIT[:7],
            ):
                raise ValueError("Official converter compatibility cannot be combined with a different schema revision")
            selected_revision = LATEST_OSSIE_SCHEMA_COMMIT
            portable_only = True
        synthesis = synthesize_ossie_document(
            graph,
            scope_name=selected_scope,
            expression_dialect=selected_dialect,
            schema_version=selected_version,
            schema_revision=selected_revision,
            serialization=output_serialization,
            consumer_profile=self._parse_options.consumer_profile,
            portable_only=portable_only,
        )
        document = require_synthesized_document(synthesis)
        if official_compatible:
            from sidemantic.interchange.ossie.compatibility import for_official_converters

            document = for_official_converters(document)
        if synthesis.diagnostics:
            for diagnostic in synthesis.diagnostics:
                record_export_feature(
                    diagnostic.code,
                    "partial",
                    detail=diagnostic.message,
                    source="ossie",
                    location=diagnostic.json_pointer,
                )
            import warnings

            for diagnostic in synthesis.diagnostics:
                warnings.warn(diagnostic.message, UserWarning, stacklevel=2)
        self.export_document(document, destination, serialization=output_serialization)
        if official_compatible:
            record_export_feature(
                "ossie.official_compatible",
                "exact",
                detail="Portable flat core with model-scoped metrics; vendor-specific feature support still varies.",
                source="ossie",
            )
