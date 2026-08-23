from dataclasses import FrozenInstanceError

import pytest

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_catalog import (
    AmbiguousSemanticScopeError,
    CompiledSemanticScope,
    CompiledSemanticScopeMutationError,
    DuplicateSemanticScopeError,
    SemanticCatalog,
    SemanticScopeCacheKey,
    SemanticScopeNotFoundError,
)
from sidemantic.core.semantic_graph import SemanticGraph


def _graph(*, table: str) -> SemanticGraph:
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table=table, primary_key="id"))
    graph.add_metric(Metric(name="order_count", agg="count", sql="*"))
    return graph


def _scope(
    scope_id: str,
    *,
    table: str = "orders",
    content_id: str = "sha256:content",
    revision_id: str | None = "revision-1",
    target_dialect: str = "duckdb",
    lowering_policy: str = "strict",
    valid: bool = True,
    diagnostics: tuple[object, ...] = (),
    provenance: dict[str, object] | None = None,
) -> CompiledSemanticScope:
    return CompiledSemanticScope(
        scope_id=scope_id,
        document_id="document-1",
        content_id=content_id,
        compilation_id=f"compilation:{scope_id}:{target_dialect}:{lowering_policy}",
        revision_id=revision_id,
        runtime=_graph(table=table),
        target_dialect=target_dialect,
        lowering_policy=lowering_policy,
        valid=valid,
        diagnostics=diagnostics,
        provenance=provenance or {},
    )


def test_catalog_isolates_duplicate_model_and_metric_names_across_scopes() -> None:
    finance = _scope("finance", table="finance.orders")
    marketing = _scope("marketing", table="marketing.orders")

    catalog = SemanticCatalog([marketing, finance])

    assert catalog.scope_ids == ("finance", "marketing")
    assert catalog["finance"].runtime.get_model("orders").table == "finance.orders"
    assert catalog["marketing"].runtime.get_model("orders").table == "marketing.orders"
    assert catalog["finance"].runtime.get_metric("order_count") is not catalog["marketing"].runtime.get_metric(
        "order_count"
    )
    assert not hasattr(catalog, "models")
    assert not hasattr(catalog, "metrics")


def test_scope_resolution_requires_an_explicit_id_when_ambiguous() -> None:
    catalog = SemanticCatalog([_scope("marketing"), _scope("finance")])

    with pytest.raises(AmbiguousSemanticScopeError, match="finance, marketing"):
        catalog.resolve_scope()

    assert catalog.resolve_scope("marketing").scope_id == "marketing"


def test_scope_resolution_uniquely_infers_a_single_scope() -> None:
    only_scope = _scope("finance")
    catalog = SemanticCatalog([only_scope])

    assert catalog.resolve_scope() is only_scope


def test_scope_resolution_reports_empty_and_unknown_catalogs() -> None:
    with pytest.raises(SemanticScopeNotFoundError, match="contains no scopes"):
        SemanticCatalog().resolve_scope()

    catalog = SemanticCatalog([_scope("finance")])
    with pytest.raises(SemanticScopeNotFoundError, match="Available scopes: finance"):
        catalog.get_scope("marketing")


def test_catalog_rejects_duplicate_scope_ids() -> None:
    with pytest.raises(DuplicateSemanticScopeError, match="'finance'.*more than once"):
        SemanticCatalog([_scope("finance"), _scope("finance", table="other.orders")])


def test_catalog_membership_is_a_defensive_immutable_snapshot() -> None:
    source = [_scope("finance")]
    catalog = SemanticCatalog(source)

    source.append(_scope("marketing"))

    assert catalog.scope_ids == ("finance",)
    with pytest.raises(TypeError):
        catalog.scopes["marketing"] = source[-1]  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        catalog._scopes = {}  # type: ignore[misc]


def test_scope_metadata_is_defensively_frozen() -> None:
    diagnostic = {"code": "ossie.warning", "path": ["semantic_model", 0]}
    provenance = {"source": {"path": "model.yml", "indexes": [0, 2]}}
    scope = _scope("finance", diagnostics=(diagnostic,), provenance=provenance)

    diagnostic["code"] = "changed"
    provenance["source"] = {"path": "changed.yml"}

    assert scope.diagnostics[0]["code"] == "ossie.warning"  # type: ignore[index]
    assert scope.diagnostics[0]["path"] == ("semantic_model", 0)  # type: ignore[index]
    assert scope.provenance["source"]["path"] == "model.yml"  # type: ignore[index]
    with pytest.raises(TypeError):
        scope.provenance["source"] = {}  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        scope.scope_id = "changed"  # type: ignore[misc]


def test_cache_identity_is_deterministic_and_partitions_compilation_inputs() -> None:
    first = _scope("finance")
    same_identity = _scope(
        "finance",
        table="another_physical_copy",
        diagnostics=({"code": "informational"},),
        provenance={"source_path": "copy.yml"},
    )

    assert first.cache_key == same_identity.cache_key
    assert hash(first.cache_key) == hash(same_identity.cache_key)
    assert first.cache_key == SemanticScopeCacheKey(
        document_id="document-1",
        scope_id="finance",
        content_id="sha256:content",
        compilation_id="compilation:finance:duckdb:strict",
        revision_id="revision-1",
        target_dialect="duckdb",
        lowering_policy="strict",
    )
    assert _scope("finance", content_id="sha256:different").cache_key != first.cache_key
    assert _scope("finance", revision_id="revision-2").cache_key != first.cache_key
    assert _scope("finance", target_dialect="bigquery").cache_key != first.cache_key
    assert _scope("finance", lowering_policy="permissive").cache_key != first.cache_key


def test_scope_detects_nested_runtime_mutation_before_reusing_compiled_identity() -> None:
    scope = _scope("finance")

    scope.runtime.add_model(Model(name="customers", table="customers", primary_key="id"))

    with pytest.raises(CompiledSemanticScopeMutationError, match="recompile"):
        _ = scope.cache_key
    with pytest.raises(CompiledSemanticScopeMutationError, match="recompile"):
        scope.clone_runtime()
    with pytest.raises(FrozenInstanceError):
        scope.runtime = SemanticGraph()  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("scope_id", ""),
        ("document_id", " "),
        ("content_id", ""),
        ("compilation_id", ""),
        ("revision_id", " "),
        ("target_dialect", ""),
        ("lowering_policy", " "),
    ],
)
def test_scope_rejects_empty_cache_identity_fields(field_name: str, value: str) -> None:
    values = {
        "scope_id": "finance",
        "document_id": "document-1",
        "content_id": "sha256:content",
        "compilation_id": "sha256:compilation",
        "revision_id": "revision-1",
        "runtime": _graph(table="orders"),
        "target_dialect": "duckdb",
        "lowering_policy": "strict",
        "valid": True,
    }
    values[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        CompiledSemanticScope(**values)  # type: ignore[arg-type]
