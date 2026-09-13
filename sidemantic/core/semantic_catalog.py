"""Immutable composition of explicitly identified semantic runtime scopes.

``SemanticCatalog`` is deliberately separate from ``SemanticGraph``. A graph
continues to represent one executable namespace, while a catalog can retain
multiple namespaces without merging their models or metrics.

Catalog membership and scope metadata are immutable defensive snapshots. The
wrapped ``SemanticGraph`` remains a mutable compatibility type, so each scope
captures a deep semantic snapshot, rejects cache reuse after nested mutation,
and gives bound ``SemanticLayer`` instances isolated runtime clones. A changed
graph must be recompiled under a new compilation identity.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from sidemantic.core.semantic_graph import SemanticGraph


class SemanticCatalogError(ValueError):
    """Base error for invalid or unresolved semantic catalog operations."""


class DuplicateSemanticScopeError(SemanticCatalogError):
    """Raised when a catalog receives the same scope identity more than once."""


class SemanticScopeNotFoundError(SemanticCatalogError):
    """Raised when an explicitly requested semantic scope does not exist."""


class AmbiguousSemanticScopeError(SemanticCatalogError):
    """Raised when scope inference is requested from a multi-scope catalog."""


class CompiledSemanticScopeMutationError(SemanticCatalogError):
    """Raised when a compiled runtime changed without recompilation."""


def _require_identity(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _freeze_value(value: object) -> object:
    """Recursively freeze common metadata containers without copying runtimes."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_value(item) for item in value)
    return value


def _runtime_state(runtime: SemanticGraph) -> object:
    """Capture all semantic graph state while excluding derived caches."""
    return (
        runtime.models,
        runtime.metrics,
        runtime.metric_owners,
        runtime.table_calculations,
        runtime.parameters,
        runtime.explores,
        runtime.saved_queries,
        runtime.import_warnings,
        runtime.metadata,
    )


@dataclass(frozen=True, slots=True)
class SemanticScopeCacheKey:
    """Stable, hashable runtime inputs that must partition compiled caches."""

    document_id: str
    scope_id: str
    content_id: str
    compilation_id: str
    revision_id: str | None
    target_dialect: str
    lowering_policy: str


@dataclass(frozen=True, slots=True, kw_only=True)
class CompiledSemanticScope:
    """One identified, compiled semantic namespace.

    The dataclass freezes identities, options, diagnostics, and provenance and
    snapshots all semantic runtime state. ``runtime`` remains the original
    ``SemanticGraph`` compatibility object, but mutation is detected before
    cache-key reuse or runtime cloning and requires recompilation.
    """

    scope_id: str
    document_id: str
    content_id: str
    compilation_id: str
    runtime: SemanticGraph
    target_dialect: str
    lowering_policy: str
    valid: bool
    revision_id: str | None = None
    diagnostics: tuple[object, ...] = ()
    provenance: Mapping[str, object] = field(default_factory=dict)
    _runtime_snapshot: object = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _require_identity(self.scope_id, "scope_id")
        _require_identity(self.document_id, "document_id")
        _require_identity(self.content_id, "content_id")
        _require_identity(self.compilation_id, "compilation_id")
        _require_identity(self.target_dialect, "target_dialect")
        _require_identity(self.lowering_policy, "lowering_policy")
        if not isinstance(self.valid, bool):
            raise TypeError("valid must be a boolean")
        if self.revision_id is not None:
            _require_identity(self.revision_id, "revision_id")

        frozen_diagnostics = tuple(_freeze_value(diagnostic) for diagnostic in self.diagnostics)
        frozen_provenance = _freeze_value(self.provenance)
        object.__setattr__(self, "diagnostics", frozen_diagnostics)
        object.__setattr__(self, "provenance", frozen_provenance)
        object.__setattr__(self, "_runtime_snapshot", copy.deepcopy(_runtime_state(self.runtime)))

    def assert_runtime_unchanged(self) -> None:
        """Reject stale compiled identity after any nested runtime mutation."""
        if _runtime_state(self.runtime) != self._runtime_snapshot:
            raise CompiledSemanticScopeMutationError(
                f"Compiled semantic scope '{self.scope_id}' was mutated; recompile it to derive a new identity"
            )

    def clone_runtime(self) -> SemanticGraph:
        """Return an isolated runtime clone after verifying the snapshot."""
        self.assert_runtime_unchanged()
        return copy.deepcopy(self.runtime)

    @property
    def graph(self) -> SemanticGraph:
        """Return the existing graph-compatible runtime for this scope."""
        return self.runtime

    @property
    def cache_key(self) -> SemanticScopeCacheKey:
        """Return the stable identity and compilation inputs for cache partitioning."""
        self.assert_runtime_unchanged()
        return SemanticScopeCacheKey(
            document_id=self.document_id,
            scope_id=self.scope_id,
            content_id=self.content_id,
            compilation_id=self.compilation_id,
            revision_id=self.revision_id,
            target_dialect=self.target_dialect,
            lowering_policy=self.lowering_policy,
        )


@dataclass(frozen=True, slots=True, init=False)
class SemanticCatalog:
    """An immutable collection of isolated compiled semantic scopes.

    Scope IDs are the only catalog keys. Model and metric dictionaries remain
    owned by each scope's runtime and are never aggregated by the catalog.
    Calling :meth:`resolve_scope` without an ID succeeds only when the catalog
    contains exactly one scope.
    """

    _scopes: Mapping[str, CompiledSemanticScope] = field(repr=False)

    def __init__(self, scopes: Iterable[CompiledSemanticScope] = ()) -> None:
        by_id: dict[str, CompiledSemanticScope] = {}
        for scope in scopes:
            if scope.scope_id in by_id:
                raise DuplicateSemanticScopeError(f"Semantic scope '{scope.scope_id}' is declared more than once")
            by_id[scope.scope_id] = scope

        deterministic = {scope_id: by_id[scope_id] for scope_id in sorted(by_id)}
        object.__setattr__(self, "_scopes", MappingProxyType(deterministic))

    @property
    def scopes(self) -> Mapping[str, CompiledSemanticScope]:
        """Return the immutable scope mapping, keyed only by explicit scope ID."""
        return self._scopes

    @property
    def scope_ids(self) -> tuple[str, ...]:
        """Return scope IDs in deterministic order."""
        return tuple(self._scopes)

    def __len__(self) -> int:
        return len(self._scopes)

    def __contains__(self, scope_id: object) -> bool:
        return scope_id in self._scopes

    def __getitem__(self, scope_id: str) -> CompiledSemanticScope:
        return self.get_scope(scope_id)

    def get_scope(self, scope_id: str) -> CompiledSemanticScope:
        """Return an explicitly identified scope or raise a focused error."""
        try:
            return self._scopes[scope_id]
        except KeyError as exc:
            available = ", ".join(self.scope_ids) or "(none)"
            raise SemanticScopeNotFoundError(
                f"Semantic scope '{scope_id}' was not found. Available scopes: {available}"
            ) from exc

    def resolve_scope(self, scope_id: str | None = None) -> CompiledSemanticScope:
        """Resolve an explicit scope, or infer it only when exactly one exists."""
        if scope_id is not None:
            return self.get_scope(scope_id)
        if not self._scopes:
            raise SemanticScopeNotFoundError("Semantic catalog contains no scopes")
        if len(self._scopes) > 1:
            available = ", ".join(self.scope_ids)
            raise AmbiguousSemanticScopeError(f"Semantic scope is ambiguous. Select one explicitly from: {available}")
        return next(iter(self._scopes.values()))
