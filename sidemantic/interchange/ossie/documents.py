"""Immutable Apache Ossie source-document contracts.

These contracts preserve the parsed JSON-compatible data model and optionally
the original source bytes. They do not claim to reconstruct YAML comments,
anchors, quoting, scalar style, whitespace, or other lexical formatting.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import ClassVar, TypeAlias

from sidemantic.interchange.ossie.profiles import OssieSerialization

JSONScalar: TypeAlias = str | int | float | bool | None
ParsedJSONValue: TypeAlias = JSONScalar | Mapping[str, object] | list[object] | tuple[object, ...]


@dataclass(frozen=True, slots=True)
class FrozenJSONObject(Mapping[str, "FrozenJSONValue"]):
    """An insertion-ordered, deeply immutable JSON object."""

    _entries: tuple[tuple[str, FrozenJSONValue], ...] = ()

    def __post_init__(self) -> None:
        keys = [key for key, _ in self._entries]
        if any(not isinstance(key, str) for key in keys):
            raise TypeError("JSON object keys must be strings")
        if len(keys) != len(set(keys)):
            raise ValueError("JSON object keys must be unique")
        object.__setattr__(self, "_entries", tuple((key, freeze_json(value)) for key, value in self._entries))

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> FrozenJSONObject:
        return cls(tuple((key, freeze_json(item)) for key, item in value.items()))

    def __getitem__(self, key: str) -> FrozenJSONValue:
        for candidate, value in self._entries:
            if candidate == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _ in self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def to_dict(self) -> dict[str, object]:
        """Return a mutable JSON-compatible copy for serialization."""

        return {key: thaw_json(value) for key, value in self._entries}


FrozenJSONValue: TypeAlias = JSONScalar | FrozenJSONObject | tuple["FrozenJSONValue", ...]


def freeze_json(value: object) -> FrozenJSONValue:
    """Copy JSON-compatible data into an immutable representation."""

    if isinstance(value, FrozenJSONObject):
        return value
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
    if isinstance(value, Mapping):
        return FrozenJSONObject.from_mapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(freeze_json(item) for item in value)
    raise TypeError(f"Unsupported parsed data type: {type(value).__name__}")


def thaw_json(value: FrozenJSONValue) -> object:
    """Return a detached mutable JSON-compatible copy."""

    if isinstance(value, FrozenJSONObject):
        return value.to_dict()
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def _escape_json_pointer_token(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def _present_fields(value: FrozenJSONValue, pointer: str = "") -> set[str]:
    fields: set[str] = set()
    if isinstance(value, FrozenJSONObject):
        for key, child in value._entries:
            child_pointer = f"{pointer}/{_escape_json_pointer_token(key)}"
            fields.add(child_pointer)
            fields.update(_present_fields(child, child_pointer))
    elif isinstance(value, tuple):
        for index, child in enumerate(value):
            fields.update(_present_fields(child, f"{pointer}/{index}"))
    return fields


@dataclass(frozen=True, slots=True)
class OssieDocumentSource:
    """Optional source identity and exact bytes retained with a parsed document."""

    identifier: str | None = None
    original_bytes: bytes | None = None
    media_type: str | None = None

    def __post_init__(self) -> None:
        if self.identifier is not None and not self.identifier:
            raise ValueError("source identifier must not be empty")
        if self.original_bytes is not None and not isinstance(self.original_bytes, bytes):
            raise TypeError("original_bytes must be bytes")

    @property
    def sha256(self) -> str | None:
        if self.original_bytes is None:
            return None
        return hashlib.sha256(self.original_bytes).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class _OssieDocumentBase:
    canonical_data: ParsedJSONValue | FrozenJSONObject
    serialization: OssieSerialization
    source: OssieDocumentSource | None = None

    _known_root_fields: ClassVar[frozenset[str]] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(self, "canonical_data", freeze_json(self.canonical_data))
        object.__setattr__(self, "serialization", OssieSerialization(self.serialization))

    @property
    def version(self) -> str | None:
        if not isinstance(self.canonical_data, FrozenJSONObject):
            return None
        value = self.canonical_data.get("version")
        return value if isinstance(value, str) else None

    @property
    def present_fields(self) -> frozenset[str]:
        """JSON pointers for fields explicitly present in the parsed data."""

        return frozenset(_present_fields(self.canonical_data))

    def is_field_present(self, json_pointer: str) -> bool:
        return json_pointer in self.present_fields

    @property
    def unknown_data(self) -> FrozenJSONObject:
        """Unknown root fields; nested unknown data remains in ``canonical_data``."""

        if not isinstance(self.canonical_data, FrozenJSONObject):
            return FrozenJSONObject()
        return FrozenJSONObject(
            tuple((key, value) for key, value in self.canonical_data._entries if key not in self._known_root_fields)
        )

    def to_parsed_data(self) -> object:
        return thaw_json(self.canonical_data)


@dataclass(frozen=True, slots=True, kw_only=True)
class OssieLogicalDocument(_OssieDocumentBase):
    """A logical-layer Ossie document, before validation or runtime lowering."""

    _known_root_fields: ClassVar[frozenset[str]] = frozenset({"version", "dialects", "vendors", "semantic_model"})

    def __post_init__(self) -> None:
        super(OssieLogicalDocument, self).__post_init__()
        if not isinstance(self.canonical_data, FrozenJSONObject):
            raise TypeError("An Ossie logical document must have an object root")

    @property
    def semantic_model_value(self) -> FrozenJSONValue | None:
        return self.canonical_data.get("semantic_model")

    @property
    def semantic_models(self) -> tuple[FrozenJSONValue, ...]:
        value = self.semantic_model_value
        return value if isinstance(value, tuple) else ()


@dataclass(frozen=True, slots=True, kw_only=True)
class OssieOntologyDocument(_OssieDocumentBase):
    """An ontology-layer Ossie document, preserved without reasoning semantics."""

    _known_root_fields: ClassVar[frozenset[str]] = frozenset(
        {"version", "name", "description", "ai_context", "ontology", "ontology_mappings"}
    )

    def __post_init__(self) -> None:
        super(OssieOntologyDocument, self).__post_init__()
        if not isinstance(self.canonical_data, FrozenJSONObject):
            raise TypeError("An Ossie ontology document must have an object root")

    @property
    def ontology(self) -> FrozenJSONValue | None:
        return self.canonical_data.get("ontology")

    @property
    def ontology_mappings(self) -> FrozenJSONValue | None:
        return self.canonical_data.get("ontology_mappings")


@dataclass(frozen=True, slots=True, kw_only=True)
class UnsupportedOssieDocument(_OssieDocumentBase):
    """Parsed data that cannot yet be classified as a supported Ossie document family."""

    reason: str | None = None


OssieDocument: TypeAlias = OssieLogicalDocument | OssieOntologyDocument | UnsupportedOssieDocument
