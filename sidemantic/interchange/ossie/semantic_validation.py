"""Semantic validation for parsed Apache Ossie documents.

This module is the second validation stage. JSON Schema owns document shape,
required properties, primitive types, and enum membership; these checks own
cross-object identity, references, and the invariants required to lower a
logical scope safely. Callers decide whether diagnostics block lowering.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

from sidemantic.interchange.ossie.diagnostics import (
    OssieDiagnostic,
    OssieDiagnosticSeverity,
    OssieSourceLocation,
    sort_diagnostics,
)
from sidemantic.interchange.ossie.documents import (
    OssieLogicalDocument,
    OssieOntologyDocument,
)
from sidemantic.interchange.ossie.identifier import (
    OSSIE_IDENTIFIER_MAX_LENGTH,
    identifier_length,
    identifier_within_limit,
    normalize_identifier,
)
from sidemantic.interchange.ossie.profiles import (
    OssieConsumerProfile,
    OssieProfile,
    OssieProfileError,
    resolve_ossie_profile,
)

SemanticDocumentKind = Literal["logical", "ontology", "unsupported"]
SemanticFailureStage = Literal["semantic"]
JSONObject: TypeAlias = Mapping[str, object]


@dataclass(frozen=True, slots=True)
class SemanticValidationResult:
    """Immutable result of the semantic validation stage."""

    valid: bool
    document_kind: SemanticDocumentKind
    checked_scopes: tuple[str, ...]
    diagnostics: tuple[OssieDiagnostic, ...]
    failure_stage: SemanticFailureStage | None = None
    stage: Literal["semantic"] = "semantic"

    def to_dict(self) -> dict[str, Any]:
        """Return a detached, serialization-friendly result."""

        return {
            "stage": self.stage,
            "failure_stage": self.failure_stage,
            "valid": self.valid,
            "document_kind": self.document_kind,
            "checked_scopes": list(self.checked_scopes),
            "diagnostics": [
                {
                    "code": diagnostic.code,
                    "severity": diagnostic.severity.value,
                    "message": diagnostic.message,
                    "json_pointer": diagnostic.json_pointer,
                    "scope": diagnostic.scope,
                    "profile": diagnostic.profile.identifier if diagnostic.profile else None,
                    "source": (
                        {
                            "identifier": diagnostic.source.identifier,
                            "line": diagnostic.source.line,
                            "column": diagnostic.source.column,
                            "end_line": diagnostic.source.end_line,
                            "end_column": diagnostic.source.end_column,
                        }
                        if diagnostic.source
                        else None
                    ),
                }
                for diagnostic in self.diagnostics
            ],
        }


def _escape_json_pointer_token(value: object) -> str:
    return str(value).replace("~", "~0").replace("/", "~1")


def _pointer(parent: str, *parts: object) -> str:
    suffix = "/".join(_escape_json_pointer_token(part) for part in parts)
    if not suffix:
        return parent
    return f"{parent}/{suffix}" if parent else f"/{suffix}"


def _mapping(value: object) -> JSONObject | None:
    return value if isinstance(value, Mapping) else None


def _array(value: object) -> Sequence[object] | None:
    if isinstance(value, (list, tuple)):
        return value
    return None


def _name(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _profile_for(
    document: JSONObject,
    explicit_profile: OssieProfile | None = None,
) -> OssieProfile | None:
    if explicit_profile is not None:
        return explicit_profile
    version = document.get("version")
    if not isinstance(version, str):
        return None
    try:
        return resolve_ossie_profile(version, OssieConsumerProfile.OSSIE_CORE)
    except OssieProfileError:
        return None


class _SemanticValidator:
    def __init__(
        self,
        *,
        profile: OssieProfile | None,
        source: OssieSourceLocation | None,
    ) -> None:
        self.profile = profile
        self.source = source
        self.diagnostics: list[OssieDiagnostic] = []
        self.checked_scopes: list[str] = []

    def emit(
        self,
        code: str,
        message: str,
        json_pointer: str,
        *,
        scope: str | None = None,
    ) -> None:
        self.diagnostics.append(
            OssieDiagnostic(
                severity=OssieDiagnosticSeverity.ERROR,
                code=code,
                message=message,
                json_pointer=json_pointer,
                source=self.source,
                scope=scope,
                profile=self.profile,
            )
        )

    def duplicate_names(
        self,
        values: Sequence[object] | None,
        *,
        parent_pointer: str,
        namespace: str,
        code: str,
        scope: str | None = None,
    ) -> Counter[str]:
        counts: Counter[str] = Counter()
        if values is None:
            return counts

        first_pointer: dict[str, str] = {}
        for index, value in enumerate(values):
            item = _mapping(value)
            if item is None:
                continue
            item_name = _name(item.get("name"))
            if item_name is None:
                continue
            name_pointer = _pointer(parent_pointer, index, "name")
            self.validate_identifier_length(item_name, name_pointer, scope=scope)
            normalized_name = normalize_identifier(item_name)
            counts[normalized_name] += 1
            if normalized_name in first_pointer:
                self.emit(
                    code,
                    (
                        f"Duplicate {namespace} name {item_name!r} after Ossie identifier normalization; "
                        f"first declared at {first_pointer[normalized_name]}."
                    ),
                    name_pointer,
                    scope=scope,
                )
            else:
                first_pointer[normalized_name] = name_pointer
        return counts

    def validate_identifier_length(
        self,
        identifier: str,
        json_pointer: str,
        *,
        scope: str | None = None,
    ) -> bool:
        length = identifier_length(identifier)
        if length <= OSSIE_IDENTIFIER_MAX_LENGTH:
            return True
        self.emit(
            "ossie.semantic.identifier.length_exceeded",
            (
                f"Identifier {identifier!r} is {length} characters after quoted-identifier decoding; "
                f"Apache Ossie identifiers are limited to {OSSIE_IDENTIFIER_MAX_LENGTH} characters."
            ),
            json_pointer,
            scope=scope,
        )
        return False

    def validate_expression(
        self,
        expression: object,
        *,
        expression_pointer: str,
        scope: str,
    ) -> None:
        expression_object = _mapping(expression)
        if expression_object is None or "dialects" not in expression_object:
            return

        dialects_pointer = _pointer(expression_pointer, "dialects")
        dialects = _array(expression_object.get("dialects"))
        if dialects is None:
            return
        if not dialects:
            self.emit(
                "ossie.semantic.expression.dialects_empty",
                "An executable expression must provide at least one dialect variant.",
                dialects_pointer,
                scope=scope,
            )
            return

        first_dialect_pointer: dict[str, str] = {}
        for index, value in enumerate(dialects):
            variant = _mapping(value)
            if variant is None:
                continue
            variant_pointer = _pointer(dialects_pointer, index)
            dialect = variant.get("dialect")
            if isinstance(dialect, str):
                dialect_pointer = _pointer(variant_pointer, "dialect")
                if not dialect.strip():
                    self.emit(
                        "ossie.semantic.expression.dialect_empty",
                        "Expression dialect labels must not be empty.",
                        dialect_pointer,
                        scope=scope,
                    )
                elif dialect.strip().upper() in first_dialect_pointer:
                    normalized_dialect = dialect.strip().upper()
                    self.emit(
                        "ossie.semantic.expression.dialect_duplicate",
                        (
                            f"Duplicate expression dialect {dialect!r}; first declared at "
                            f"{first_dialect_pointer[normalized_dialect]}."
                        ),
                        dialect_pointer,
                        scope=scope,
                    )
                else:
                    first_dialect_pointer[dialect.strip().upper()] = dialect_pointer

            text = variant.get("expression")
            if isinstance(text, str) and not text.strip():
                self.emit(
                    "ossie.semantic.expression.text_empty",
                    "Expression text must not be empty.",
                    _pointer(variant_pointer, "expression"),
                    scope=scope,
                )

    def validate_semantic_models(
        self,
        semantic_models: Sequence[object] | None,
        *,
        parent_pointer: str,
    ) -> None:
        name_counts = self.duplicate_names(
            semantic_models,
            parent_pointer=parent_pointer,
            namespace="semantic-model",
            code="ossie.semantic.semantic_model.name_duplicate",
        )
        if semantic_models is None:
            return

        for index, value in enumerate(semantic_models):
            semantic_model = _mapping(value)
            if semantic_model is None:
                continue
            semantic_model_pointer = _pointer(parent_pointer, index)
            semantic_model_name = _name(semantic_model.get("name"))
            scope = semantic_model_name or f"semantic_model[{index}]"
            if semantic_model_name and name_counts[normalize_identifier(semantic_model_name)] > 1:
                scope = f"{semantic_model_name}@{index}"
            self.checked_scopes.append(scope)
            self.validate_scope(semantic_model, semantic_model_pointer, scope)

    def validate_scope(
        self,
        semantic_model: JSONObject,
        semantic_model_pointer: str,
        scope: str,
    ) -> None:
        datasets_pointer = _pointer(semantic_model_pointer, "datasets")
        metrics_pointer = _pointer(semantic_model_pointer, "metrics")
        relationships_pointer = _pointer(semantic_model_pointer, "relationships")
        datasets = _array(semantic_model.get("datasets"))
        metrics = _array(semantic_model.get("metrics"))
        relationships = _array(semantic_model.get("relationships"))

        dataset_name_counts = self.duplicate_names(
            datasets,
            parent_pointer=datasets_pointer,
            namespace="dataset",
            code="ossie.semantic.dataset.name_duplicate",
            scope=scope,
        )
        self.duplicate_names(
            metrics,
            parent_pointer=metrics_pointer,
            namespace="metric",
            code="ossie.semantic.metric.name_duplicate",
            scope=scope,
        )
        self.duplicate_names(
            relationships,
            parent_pointer=relationships_pointer,
            namespace="relationship",
            code="ossie.semantic.relationship.name_duplicate",
            scope=scope,
        )

        dataset_by_name: dict[str, tuple[JSONObject, str]] = {}
        if datasets is not None:
            for dataset_index, value in enumerate(datasets):
                dataset = _mapping(value)
                if dataset is None:
                    continue
                dataset_pointer = _pointer(datasets_pointer, dataset_index)
                dataset_name = _name(dataset.get("name"))
                fields_pointer = _pointer(dataset_pointer, "fields")
                fields = _array(dataset.get("fields"))
                self.duplicate_names(
                    fields,
                    parent_pointer=fields_pointer,
                    namespace="field",
                    code="ossie.semantic.field.name_duplicate",
                    scope=scope,
                )
                if fields is not None:
                    for field_index, field_value in enumerate(fields):
                        field = _mapping(field_value)
                        if field is not None and "expression" in field:
                            self.validate_expression(
                                field.get("expression"),
                                expression_pointer=_pointer(fields_pointer, field_index, "expression"),
                                scope=scope,
                            )
                if dataset_name is not None:
                    self.validate_declared_keys(
                        dataset,
                        dataset_pointer=dataset_pointer,
                        scope=scope,
                    )
                if (
                    dataset_name is not None
                    and identifier_within_limit(dataset_name)
                    and dataset_name_counts[normalize_identifier(dataset_name)] == 1
                ):
                    dataset_by_name[normalize_identifier(dataset_name)] = (dataset, dataset_pointer)

        if metrics is not None:
            for metric_index, metric_value in enumerate(metrics):
                metric = _mapping(metric_value)
                if metric is not None and "expression" in metric:
                    self.validate_expression(
                        metric.get("expression"),
                        expression_pointer=_pointer(metrics_pointer, metric_index, "expression"),
                        scope=scope,
                    )

        if relationships is not None:
            for relationship_index, relationship_value in enumerate(relationships):
                relationship = _mapping(relationship_value)
                if relationship is not None:
                    self.validate_relationship(
                        relationship,
                        relationship_pointer=_pointer(relationships_pointer, relationship_index),
                        dataset_by_name=dataset_by_name,
                        scope=scope,
                    )

    def validate_relationship(
        self,
        relationship: JSONObject,
        *,
        relationship_pointer: str,
        dataset_by_name: Mapping[str, tuple[JSONObject, str]],
        scope: str,
    ) -> None:
        from_name = _name(relationship.get("from"))
        to_name = _name(relationship.get("to"))
        from_valid = (
            self.validate_identifier_length(from_name, _pointer(relationship_pointer, "from"), scope=scope)
            if from_name is not None
            else False
        )
        to_valid = (
            self.validate_identifier_length(to_name, _pointer(relationship_pointer, "to"), scope=scope)
            if to_name is not None
            else False
        )
        from_dataset = dataset_by_name.get(normalize_identifier(from_name)) if from_name and from_valid else None
        to_dataset = dataset_by_name.get(normalize_identifier(to_name)) if to_name and to_valid else None

        if from_name is not None and from_dataset is None:
            self.emit(
                "ossie.semantic.relationship.from_dataset_unknown",
                f"Relationship source dataset {from_name!r} does not exist uniquely within scope {scope!r}.",
                _pointer(relationship_pointer, "from"),
                scope=scope,
            )
        if to_name is not None and to_dataset is None:
            self.emit(
                "ossie.semantic.relationship.to_dataset_unknown",
                f"Relationship target dataset {to_name!r} does not exist uniquely within scope {scope!r}.",
                _pointer(relationship_pointer, "to"),
                scope=scope,
            )

        has_from_columns = "from_columns" in relationship
        has_to_columns = "to_columns" in relationship
        if not has_from_columns or not has_to_columns:
            missing = [
                name
                for name, present in (
                    ("from_columns", has_from_columns),
                    ("to_columns", has_to_columns),
                )
                if not present
            ]
            self.emit(
                "ossie.semantic.relationship.keys_incomplete",
                f"Executable relationships require both key arrays; missing {', '.join(missing)}.",
                relationship_pointer,
                scope=scope,
            )
            return

        from_columns = _array(relationship.get("from_columns"))
        to_columns = _array(relationship.get("to_columns"))
        if from_columns is None or to_columns is None:
            return

        empty_keys = False
        if not from_columns:
            empty_keys = True
            self.emit(
                "ossie.semantic.relationship.keys_empty",
                "Relationship source keys must not be empty.",
                _pointer(relationship_pointer, "from_columns"),
                scope=scope,
            )
        if not to_columns:
            empty_keys = True
            self.emit(
                "ossie.semantic.relationship.keys_empty",
                "Relationship target keys must not be empty.",
                _pointer(relationship_pointer, "to_columns"),
                scope=scope,
            )
        if empty_keys:
            return

        if len(from_columns) != len(to_columns):
            self.emit(
                "ossie.semantic.relationship.key_arity_mismatch",
                (
                    "Relationship source and target key arrays must have equal length; "
                    f"received {len(from_columns)} and {len(to_columns)}."
                ),
                relationship_pointer,
                scope=scope,
            )

        self.validate_relationship_fields(
            from_columns,
            dataset=from_dataset,
            columns_pointer=_pointer(relationship_pointer, "from_columns"),
            side="from",
            scope=scope,
        )
        to_key = self.validate_relationship_fields(
            to_columns,
            dataset=to_dataset,
            columns_pointer=_pointer(relationship_pointer, "to_columns"),
            side="to",
            scope=scope,
        )

        if to_dataset is not None and to_key is not None:
            target, _ = to_dataset
            declared_keys = self.declared_unique_keys(target)
            if to_key not in declared_keys:
                self.emit(
                    "ossie.semantic.relationship.target_key_not_unique",
                    (
                        f"Relationship target key {list(to_key)!r} is not the target dataset's "
                        "declared primary key or one of its declared unique keys."
                    ),
                    _pointer(relationship_pointer, "to_columns"),
                    scope=scope,
                )

    def validate_relationship_fields(
        self,
        columns: Sequence[object],
        *,
        dataset: tuple[JSONObject, str] | None,
        columns_pointer: str,
        side: Literal["from", "to"],
        scope: str,
    ) -> tuple[str, ...] | None:
        if dataset is None or not all(isinstance(column, str) and column for column in columns):
            return None
        dataset_object, _ = dataset
        fields = _array(dataset_object.get("fields"))
        field_names = {
            normalize_identifier(field_name)
            for value in fields or ()
            if (field := _mapping(value)) is not None
            if (field_name := _name(field.get("name"))) is not None
            if identifier_within_limit(field_name)
        }

        valid = True
        for index, column in enumerate(columns):
            column_pointer = _pointer(columns_pointer, index)
            if not self.validate_identifier_length(column, column_pointer, scope=scope):
                valid = False
                continue
            if normalize_identifier(column) not in field_names:
                valid = False
                self.emit(
                    f"ossie.semantic.relationship.{side}_key_field_unknown",
                    f"Relationship {side} key field {column!r} does not exist in dataset {dataset_object.get('name')!r}.",
                    column_pointer,
                    scope=scope,
                )
        return tuple(normalize_identifier(column) for column in columns) if valid else None

    @staticmethod
    def declared_unique_keys(dataset: JSONObject) -> set[tuple[str, ...]]:
        keys: set[tuple[str, ...]] = set()
        primary_key = _array(dataset.get("primary_key"))
        if primary_key and all(
            isinstance(column, str) and column and identifier_within_limit(column) for column in primary_key
        ):
            keys.add(tuple(normalize_identifier(column) for column in primary_key))
        unique_keys = _array(dataset.get("unique_keys"))
        for value in unique_keys or ():
            key = _array(value)
            if key and all(isinstance(column, str) and column and identifier_within_limit(column) for column in key):
                keys.add(tuple(normalize_identifier(column) for column in key))
        return keys

    def validate_declared_keys(
        self,
        dataset: JSONObject,
        *,
        dataset_pointer: str,
        scope: str,
    ) -> None:
        fields = _array(dataset.get("fields"))
        field_names = {
            normalize_identifier(field_name)
            for value in fields or ()
            if (field := _mapping(value)) is not None
            if (field_name := _name(field.get("name"))) is not None
            if identifier_within_limit(field_name)
        }
        key_groups: list[tuple[str, Sequence[object]]] = []
        primary_key = _array(dataset.get("primary_key"))
        if primary_key is not None:
            key_groups.append(("primary_key", primary_key))
        unique_keys = _array(dataset.get("unique_keys"))
        for key_index, value in enumerate(unique_keys or ()):
            key = _array(value)
            if key is not None:
                key_groups.append((f"unique_keys/{key_index}", key))

        first_key_pointer: dict[tuple[str, ...], str] = {}
        for key_path, columns in key_groups:
            key_pointer = _pointer(dataset_pointer, *key_path.split("/"))
            if not columns:
                self.emit(
                    "ossie.semantic.dataset.key_empty",
                    "Declared primary and unique keys must contain at least one field.",
                    key_pointer,
                    scope=scope,
                )
                continue
            normalized_columns: list[str] = []
            seen_columns: set[str] = set()
            valid = True
            for column_index, column in enumerate(columns):
                if not isinstance(column, str) or not column:
                    valid = False
                    continue
                column_pointer = _pointer(dataset_pointer, *key_path.split("/"), column_index)
                if not self.validate_identifier_length(column, column_pointer, scope=scope):
                    valid = False
                    continue
                normalized = normalize_identifier(column)
                normalized_columns.append(normalized)
                if normalized not in field_names:
                    valid = False
                    self.emit(
                        "ossie.semantic.dataset.key_field_unknown",
                        f"Declared key field {column!r} does not exist uniquely in dataset {dataset.get('name')!r}.",
                        column_pointer,
                        scope=scope,
                    )
                if normalized in seen_columns:
                    valid = False
                    self.emit(
                        "ossie.semantic.dataset.key_column_duplicate",
                        f"Declared key repeats field {column!r} after Ossie identifier normalization.",
                        column_pointer,
                        scope=scope,
                    )
                seen_columns.add(normalized)
            normalized_key = tuple(normalized_columns)
            is_unique_key = key_path.startswith("unique_keys/")
            if valid and is_unique_key and normalized_key in first_key_pointer:
                self.emit(
                    "ossie.semantic.dataset.key_duplicate",
                    f"Declared key duplicates the key first declared at {first_key_pointer[normalized_key]}.",
                    key_pointer,
                    scope=scope,
                )
            elif valid and is_unique_key:
                first_key_pointer[normalized_key] = key_pointer

    def validate_ontology(self, document: JSONObject) -> None:
        ontology = _array(document.get("ontology"))
        concept_names = {
            concept
            for value in ontology or ()
            if (component := _mapping(value)) is not None
            if (concept := _name(component.get("concept"))) is not None
        }
        ontology_mappings = _array(document.get("ontology_mappings"))
        embedded_models: list[object] = []
        embedded_model_pointers: list[str] = []

        for mapping_index, value in enumerate(ontology_mappings or ()):
            ontology_mapping = _mapping(value)
            if ontology_mapping is None:
                continue
            mapping_pointer = _pointer("/ontology_mappings", mapping_index)
            if "semantic_model" in ontology_mapping:
                embedded_models.append(ontology_mapping.get("semantic_model"))
                embedded_model_pointers.append(_pointer(mapping_pointer, "semantic_model"))

            concept_mappings = _array(ontology_mapping.get("concept_mappings"))
            for concept_mapping_index, concept_value in enumerate(concept_mappings or ()):
                concept_mapping = _mapping(concept_value)
                if concept_mapping is None:
                    continue
                concept_mapping_pointer = _pointer(mapping_pointer, "concept_mappings", concept_mapping_index)
                self.validate_concept_reference(
                    concept_mapping.get("concept"),
                    pointer=_pointer(concept_mapping_pointer, "concept"),
                    concept_names=concept_names,
                )
                object_mappings = _array(concept_mapping.get("object_mappings"))
                for object_index, object_value in enumerate(object_mappings or ()):
                    self.validate_object_mapping(
                        object_value,
                        pointer=_pointer(concept_mapping_pointer, "object_mappings", object_index),
                        concept_names=concept_names,
                    )
                link_mappings = _array(concept_mapping.get("link_mappings"))
                for link_index, link_value in enumerate(link_mappings or ()):
                    self.validate_link_mapping(
                        link_value,
                        pointer=_pointer(concept_mapping_pointer, "link_mappings", link_index),
                        concept_names=concept_names,
                    )

        # Ontology maps embed complete logical SemanticModel objects. Validate
        # each as an isolated scope while keeping ontology itself preservation-only.
        self.validate_embedded_semantic_models(embedded_models, embedded_model_pointers)

    def validate_concept_reference(
        self,
        concept: object,
        *,
        pointer: str,
        concept_names: set[str],
    ) -> None:
        if isinstance(concept, str) and concept and concept not in concept_names:
            self.emit(
                "ossie.semantic.ontology.concept_unknown",
                f"Ontology mapping references unknown concept {concept!r}.",
                pointer,
            )

    def validate_object_mapping(
        self,
        value: object,
        *,
        pointer: str,
        concept_names: set[str],
    ) -> None:
        object_mapping = _mapping(value)
        if object_mapping is None:
            return
        if "concept" in object_mapping:
            self.validate_concept_reference(
                object_mapping.get("concept"),
                pointer=_pointer(pointer, "concept"),
                concept_names=concept_names,
            )

    def validate_link_mapping(
        self,
        value: object,
        *,
        pointer: str,
        concept_names: set[str],
    ) -> None:
        link_mapping = _mapping(value)
        if link_mapping is None:
            return
        if "object_mapping" in link_mapping:
            self.validate_object_mapping(
                link_mapping.get("object_mapping"),
                pointer=_pointer(pointer, "object_mapping"),
                concept_names=concept_names,
            )
        children = _array(link_mapping.get("children"))
        for child_index, child in enumerate(children or ()):
            self.validate_link_mapping(
                child,
                pointer=_pointer(pointer, "children", child_index),
                concept_names=concept_names,
            )

    def validate_embedded_semantic_models(
        self,
        semantic_models: Sequence[object],
        pointers: Sequence[str],
    ) -> None:
        name_counts: Counter[str] = Counter()
        first_pointer: dict[str, str] = {}
        for semantic_model, pointer in zip(semantic_models, pointers, strict=True):
            model = _mapping(semantic_model)
            if model is None:
                continue
            model_name = _name(model.get("name"))
            if model_name is None:
                continue
            name_pointer = _pointer(pointer, "name")
            self.validate_identifier_length(model_name, name_pointer)
            normalized_name = normalize_identifier(model_name)
            name_counts[normalized_name] += 1
            if normalized_name in first_pointer:
                self.emit(
                    "ossie.semantic.semantic_model.name_duplicate",
                    (
                        f"Duplicate semantic-model name {model_name!r} after Ossie identifier normalization; "
                        f"first declared at {first_pointer[normalized_name]}."
                    ),
                    name_pointer,
                )
            else:
                first_pointer[normalized_name] = name_pointer

        for index, (semantic_model, pointer) in enumerate(zip(semantic_models, pointers, strict=True)):
            model = _mapping(semantic_model)
            if model is None:
                continue
            model_name = _name(model.get("name"))
            scope = model_name or f"ontology_mapping[{index}]"
            if model_name and name_counts[normalize_identifier(model_name)] > 1:
                scope = f"{model_name}@{index}"
            self.checked_scopes.append(scope)
            self.validate_scope(model, pointer, scope)


def validate_ossie_semantics(
    document: Mapping[str, object] | OssieLogicalDocument | OssieOntologyDocument,
    *,
    profile: OssieProfile | None = None,
) -> SemanticValidationResult:
    """Validate scope-local Apache Ossie semantics after schema validation.

    The function intentionally does not run JSON Schema validation. It accepts
    schema-invalid mappings so permissive pipelines can collect independent
    semantic diagnostics, but skips checks whose prerequisite shape is absent.
    """

    source: OssieSourceLocation | None = None
    if isinstance(document, OssieLogicalDocument):
        canonical_data = document.canonical_data
        document_kind: SemanticDocumentKind = "logical"
        if document.source and document.source.identifier:
            source = OssieSourceLocation(identifier=document.source.identifier)
    elif isinstance(document, OssieOntologyDocument):
        canonical_data = document.canonical_data
        document_kind = "ontology"
        if document.source and document.source.identifier:
            source = OssieSourceLocation(identifier=document.source.identifier)
    elif isinstance(document, Mapping):
        canonical_data = document
        has_logical_root = "semantic_model" in canonical_data
        has_ontology_root = "ontology" in canonical_data or "ontology_mappings" in canonical_data
        if has_logical_root and not has_ontology_root:
            document_kind = "logical"
        elif has_ontology_root and not has_logical_root:
            document_kind = "ontology"
        else:
            document_kind = "unsupported"
    else:
        raise TypeError("Semantic validation requires a parsed mapping or a supported Ossie document")

    validator = _SemanticValidator(profile=_profile_for(canonical_data, profile), source=source)
    if document_kind == "logical":
        validator.validate_semantic_models(
            _array(canonical_data.get("semantic_model")),
            parent_pointer="/semantic_model",
        )
    elif document_kind == "ontology":
        validator.validate_ontology(canonical_data)

    diagnostics = sort_diagnostics(validator.diagnostics)
    valid = not any(diagnostic.severity is OssieDiagnosticSeverity.ERROR for diagnostic in diagnostics)
    return SemanticValidationResult(
        valid=valid,
        document_kind=document_kind,
        checked_scopes=tuple(validator.checked_scopes),
        diagnostics=diagnostics,
        failure_stage=None if valid else "semantic",
    )
