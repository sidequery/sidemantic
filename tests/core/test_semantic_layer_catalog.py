import pytest

from sidemantic.core.model import Model
from sidemantic.core.registry import get_current_layer, reset_current_layer, set_current_layer
from sidemantic.core.semantic_catalog import (
    AmbiguousSemanticScopeError,
    CompiledSemanticScope,
    SemanticCatalog,
)
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SemanticLayer


def _scope(scope_id: str, table: str, *, target_dialect: str = "duckdb", valid: bool = True) -> CompiledSemanticScope:
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table=table, primary_key="id"))
    return CompiledSemanticScope(
        scope_id=scope_id,
        document_id="document",
        content_id=f"content:{scope_id}",
        compilation_id=f"compilation:{scope_id}:{target_dialect}",
        runtime=graph,
        target_dialect=target_dialect,
        lowering_policy="strict",
        valid=valid,
    )


def test_from_catalog_infers_only_scope_and_binds_its_graph() -> None:
    scope = _scope("finance", "finance.orders")
    catalog = SemanticCatalog([scope])

    layer = SemanticLayer.from_catalog(catalog, auto_register=False)

    assert layer.catalog is catalog
    assert layer.compiled_scope is scope
    assert layer.graph is not scope.graph
    assert layer.graph.get_model("orders").table == "finance.orders"


def test_catalog_bindings_receive_isolated_runtime_graphs() -> None:
    catalog = SemanticCatalog([_scope("finance", "finance.orders")])

    first = SemanticLayer.from_catalog(catalog, auto_register=False)
    second = SemanticLayer.from_catalog(catalog, auto_register=False)
    first.graph.add_model(Model(name="customers", table="finance.customers"))

    assert first.graph.get_model("customers") is not None
    with pytest.raises(KeyError):
        second.graph.get_model("customers")
    with pytest.raises(KeyError):
        catalog["finance"].graph.get_model("customers")


def test_from_catalog_requires_selection_for_multiple_scopes() -> None:
    catalog = SemanticCatalog(
        [
            _scope("finance", "finance.orders"),
            _scope("marketing", "marketing.orders"),
        ]
    )

    with pytest.raises(AmbiguousSemanticScopeError, match="Select one explicitly"):
        SemanticLayer.from_catalog(catalog, auto_register=False)

    layer = SemanticLayer.from_catalog(catalog, scope_id="marketing", auto_register=False)
    assert layer.graph.get_model("orders").table == "marketing.orders"


def test_from_catalog_rejects_runtime_dialect_mismatch() -> None:
    catalog = SemanticCatalog([_scope("warehouse", "analytics.orders", target_dialect="bigquery")])

    with pytest.raises(ValueError, match="targets 'bigquery'.*runtime dialect is 'duckdb'"):
        SemanticLayer.from_catalog(catalog, auto_register=False)


def test_from_catalog_dialect_mismatch_does_not_leak_current_layer_registration() -> None:
    previous = SemanticLayer(auto_register=False)
    token = set_current_layer(previous)
    catalog = SemanticCatalog([_scope("warehouse", "analytics.orders", target_dialect="bigquery")])

    try:
        with pytest.raises(ValueError, match="targets 'bigquery'.*runtime dialect is 'duckdb'"):
            SemanticLayer.from_catalog(catalog)
        assert get_current_layer() is previous
    finally:
        reset_current_layer(token)


def test_from_catalog_registers_only_after_successful_binding() -> None:
    catalog = SemanticCatalog([_scope("warehouse", "analytics.orders")])

    with SemanticLayer.from_catalog(catalog) as layer:
        assert get_current_layer() is layer
        assert layer.compiled_scope is catalog["warehouse"]


def test_from_catalog_rejects_non_catalog_values() -> None:
    with pytest.raises(TypeError, match="must be a SemanticCatalog"):
        SemanticLayer.from_catalog(object(), auto_register=False)


def test_from_catalog_requires_explicit_opt_in_for_invalid_permissive_scope() -> None:
    catalog = SemanticCatalog([_scope("unsafe", "analytics.orders", valid=False)])

    with pytest.raises(ValueError, match="allow_invalid=True"):
        SemanticLayer.from_catalog(catalog, auto_register=False)

    layer = SemanticLayer.from_catalog(catalog, allow_invalid=True, auto_register=False)
    assert layer.compiled_scope is catalog["unsafe"]
