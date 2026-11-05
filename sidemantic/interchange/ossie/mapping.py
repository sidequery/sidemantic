"""Offline validation for standalone Ossie ontology mappings.

References are resolved only from documents supplied by the caller, keyed by
logical name or IRI. Parsing a mapping never performs network or filesystem IO.
Unresolved references are preserved and explicitly diagnosed rather than being
mistaken for successfully cross-validated documents.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp

from sidemantic.interchange.ossie.diagnostics import OssieDiagnostic, OssieDiagnosticSeverity, sort_diagnostics
from sidemantic.interchange.ossie.identifier import normalize_identifier
from sidemantic.interchange.ossie.profiles import OssieProfile

if TYPE_CHECKING:
    from sidemantic.interchange.ossie.semantic_validation import SemanticValidationResult

_BUILTINS = {"Any", "Boolean", "Date", "DateTime", "Decimal", "Float", "Integer", "String"}


def _data(document: object) -> Mapping:
    if isinstance(document, Mapping):
        return document
    convert = getattr(document, "to_parsed_data", None)
    value = convert() if callable(convert) else None
    return value if isinstance(value, Mapping) else {}


def _items(value: object) -> tuple | list:
    return value if isinstance(value, (list, tuple)) else ()


def _field_references(tree: exp.Expression):
    """Yield model references while respecting names bound by nested queries."""
    bound: dict[int, set[str]] = {}
    for node in tree.find_all(exp.Table, exp.TableAlias):
        if isinstance(node, exp.Table) and node.args.get("alias"):
            continue
        if isinstance(node.this, exp.Identifier):
            query = node.find_ancestor(exp.Select)
            bound.setdefault(id(query), set()).add(normalize_identifier(node.this.sql()))
    visible: dict[int, set[str]] = {}
    for query in tree.find_all(exp.Select):
        outer = query.find_ancestor(exp.Select)
        visible[id(query)] = bound.get(id(query), set()) | visible.get(id(outer), set())
    for column in tree.find_all(exp.Column):
        parts = column.parts
        if len(parts) != 2 or not all(isinstance(part, exp.Identifier) for part in parts):
            continue
        dataset, field = (normalize_identifier(part.sql()) for part in parts)
        query = column.find_ancestor(exp.Select)
        if query is None or dataset not in visible.get(id(query), set()):
            yield dataset, field, column.sql()


def validate_ossie_mapping(
    document: object,
    *,
    profile: OssieProfile | None = None,
    references: Mapping[str, object] | None = None,
) -> SemanticValidationResult:
    """Check a preserved mapping and optionally its explicitly supplied references.

    ``references`` accepts parsed mappings or immutable Ossie documents. IRI keys
    take precedence over name keys; the resolved document's own name must match
    its reference exactly. Unresolved documents produce warnings, not invented
    concepts or fields. Referenced schemas are checked against the latest pinned
    revision before their contents are used.
    """
    from sidemantic.interchange.ossie.semantic_validation import SemanticValidationResult
    from sidemantic.interchange.ossie.validation import validate_ossie_schema

    data = _data(document)
    diagnostics: list[OssieDiagnostic] = []

    def emit(code: str, message: str, pointer: str, *, warning: bool = False) -> None:
        diagnostics.append(
            OssieDiagnostic(
                code=f"ossie.semantic.mapping.{code}",
                severity=OssieDiagnosticSeverity.WARNING if warning else OssieDiagnosticSeverity.ERROR,
                message=message,
                json_pointer=pointer,
                profile=profile,
            )
        )

    def resolve(key: str, family: str) -> Mapping | None:
        reference = _data(data.get(key))
        supplied = references or {}
        iri, name = reference.get("iri"), reference.get("name")
        value = supplied.get(iri) if isinstance(iri, str) else None
        if value is None and isinstance(name, str):
            value = supplied.get(name)
        if value is None:
            emit(
                "reference_unresolved",
                f"No document supplied for {key} {name!r}; cross-document {family} checks were not performed.",
                f"/{key}",
                warning=True,
            )
            return None
        resolved = _data(value)
        if resolved.get("name") != name:
            emit(
                "reference_name_mismatch",
                f"{key} names {name!r}, but its document is named {resolved.get('name')!r}.",
                f"/{key}/name",
            )
        checked = validate_ossie_schema(resolved, profile=f"{family}-0.2.0.dev0-172a3e5")
        if not checked.valid:
            detail = checked.diagnostics[0].message if checked.diagnostics else "Invalid referenced document"
            emit("reference_schema", f"{key} fails its pinned {family} schema: {detail}", f"/{key}")
            return None
        return resolved

    ontology = resolve("ontology_ref", "ontology")
    model = resolve("semantic_model_ref", "logical")
    components = (
        {
            component["concept"]: component
            for component in _items(ontology.get("ontology"))
            if ontology
            if isinstance(component, Mapping) and isinstance(component.get("concept"), str)
        }
        if ontology
        else {}
    )
    concepts = set(components) | _BUILTINS
    datasets = (
        {
            normalize_identifier(dataset["name"]): {
                normalize_identifier(field["name"])
                for field in _items(dataset.get("fields"))
                if isinstance(field, Mapping) and isinstance(field.get("name"), str)
            }
            for dataset in _items(model.get("datasets"))
            if isinstance(dataset, Mapping) and isinstance(dataset.get("name"), str)
        }
        if model
        else {}
    )

    def ancestors(concept: str) -> list[str]:
        found: list[str] = []
        pending = [concept]
        while pending:
            name = pending.pop()
            if name not in found:
                found.append(name)
                pending.extend(
                    parent for parent in _items(components.get(name, {}).get("extends")) if isinstance(parent, str)
                )
        return found

    def relation(owner: str | None, name: object, pointer: str) -> Mapping | None:
        if ontology is None or owner is None or not isinstance(name, str):
            return None
        written = name
        if "." in name:
            prefix, name = name.split(".", 1)
            if prefix not in ancestors(owner):
                emit(
                    "relationship_owner",
                    f"Relationship {written!r} does not belong to {owner!r} or its supertypes.",
                    pointer,
                )
                return None
            owner = prefix
        for concept in ancestors(owner):
            for candidate in _items(components.get(concept, {}).get("relationships")):
                if isinstance(candidate, Mapping) and candidate.get("name") == name:
                    return candidate
        emit(
            "relationship_unknown", f"Relationship {written!r} is not declared on {owner!r} or its supertypes.", pointer
        )
        return None

    def role_target(relationship: Mapping | None) -> str | None:
        roles = _items(relationship.get("roles")) if relationship else ()
        return roles[-1].get("concept") if roles and isinstance(roles[-1], Mapping) else None

    def referents(value: object, owner: str | None, pointer: str) -> None:
        for index, item in enumerate(_items(value)):
            if not isinstance(item, Mapping):
                continue
            here = f"{pointer}/{index}"
            relationship = relation(owner, item.get("relationship"), f"{here}/relationship")
            target = role_target(relationship)
            nested = item.get("referent_mappings")
            if relationship is not None and _items(nested) and target is None:
                emit("relationship_role", "Nested referents require a relationship role to resolve on.", here)
            referents(nested, target, f"{here}/referent_mappings")

    def object_mapping(value: object, owner: str | None, pointer: str) -> None:
        if isinstance(value, Mapping):
            named = value.get("concept")
            if isinstance(named, str):
                owner = named
            referents(value.get("referent_mappings"), owner, f"{pointer}/referent_mappings")

    def link_mapping(value: object, owner: str, pointer: str, level: int) -> None:
        if not isinstance(value, Mapping):
            return
        relationship = relation(owner, value.get("relationship"), f"{pointer}/relationship")
        if relationship is not None and len(_items(relationship.get("roles"))) + 1 != level:
            emit("relationship_arity", f"Relationship arity does not match link level {level}.", pointer)
        object_mapping(
            value.get("object_mapping"), owner if level == 1 else role_target(relationship), f"{pointer}/object_mapping"
        )
        for index, child in enumerate(_items(value.get("children"))):
            link_mapping(child, owner, f"{pointer}/children/{index}", level + 1)

    def walk(value: object, pointer: str) -> None:
        if isinstance(value, Mapping):
            concept = value.get("concept")
            if ontology is not None and isinstance(concept, str) and concept not in concepts:
                emit(
                    "concept_unknown",
                    f"Concept {concept!r} is not declared in the referenced ontology.",
                    f"{pointer}/concept",
                )
            expression = value.get("expression")
            if isinstance(expression, str):
                try:
                    statements = sqlglot.parse(expression)
                    if not any(statement is not None for statement in statements):
                        emit(
                            "expression_empty",
                            "Mapping expression contains no SQL expression.",
                            f"{pointer}/expression",
                        )
                    elif model is not None:
                        tree = sqlglot.parse_one(expression, into=exp.Condition)
                        if isinstance(tree, exp.Block):
                            raise ValueError("Multiple expressions")
                        for dataset, field, written in _field_references(tree):
                            if dataset not in datasets or field not in datasets[dataset]:
                                emit(
                                    "field_unknown",
                                    f"Mapping expression references undeclared dataset field {written!r}.",
                                    f"{pointer}/expression",
                                )
                except (sqlglot.errors.ParseError, sqlglot.errors.TokenError, ValueError, RecursionError):
                    emit(
                        "expression_unchecked",
                        "Mapping expression could not be parsed; its field references were not checked.",
                        f"{pointer}/expression",
                        warning=True,
                    )
            for key, child in value.items():
                token = str(key).replace("~", "~0").replace("/", "~1")
                walk(child, f"{pointer}/{token}")
        elif isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                walk(child, f"{pointer}/{index}")

    mappings = _items(data.get("concept_mappings"))
    walk(mappings, "/concept_mappings")
    if ontology is not None:
        for index, item in enumerate(mappings):
            if not isinstance(item, Mapping) or item.get("concept") not in concepts:
                continue
            owner = item["concept"]
            for child_index, value in enumerate(_items(item.get("object_mappings"))):
                object_mapping(value, owner, f"/concept_mappings/{index}/object_mappings/{child_index}")
            for child_index, value in enumerate(_items(item.get("link_mappings"))):
                link_mapping(value, owner, f"/concept_mappings/{index}/link_mappings/{child_index}", 1)
    valid = not any(d.severity is OssieDiagnosticSeverity.ERROR for d in diagnostics)
    return SemanticValidationResult(
        valid=valid,
        document_kind="mapping",
        checked_scopes=(),
        diagnostics=sort_diagnostics(diagnostics),
        failure_stage=None if valid else "semantic",
    )
