"""Tests for Tableau adapter handling of extended relation types and attributes.

Covers physical-layer relation types beyond table/join/text/collection
(union, batch-union, pivot, subquery, stored-proc, project, text-transform),
the Tableau Semantics object-graph semantic-layer / is-legacy attributes, and
the spatial datatype mapping.
"""

from pathlib import Path

import pytest

from sidemantic.adapters.tableau import TableauAdapter

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "tableau"


@pytest.fixture
def adapter():
    return TableauAdapter()


# =============================================================================
# SET-OPERATION RELATIONS: union / batch-union
# =============================================================================


def test_union_relation_builds_union_all_sql(adapter):
    """union.tds: a union relation stacks members with UNION ALL."""
    graph = adapter.parse(FIXTURES / "union.tds")

    assert "union_sales" in graph.models
    model = graph.models["union_sales"]

    assert model.table is None
    assert model.sql is not None
    assert "UNION ALL" in model.sql
    assert "public.sales_2023" in model.sql
    assert "public.sales_2024" in model.sql

    # Columns still imported
    assert model.get_dimension("region") is not None
    assert model.get_metric("amount") is not None


def test_batch_union_relation_builds_union_all_sql(adapter):
    """batch_union.tds: a batch-union (wildcard) relation also unions members."""
    graph = adapter.parse(FIXTURES / "batch_union.tds")

    assert "monthly_logs" in graph.models
    model = graph.models["monthly_logs"]

    assert model.table is None
    assert model.sql is not None
    # Three members -> two UNION ALL separators
    assert model.sql.count("UNION ALL") == 2
    assert "jan.csv" in model.sql
    assert "feb.csv" in model.sql
    assert "mar.csv" in model.sql


# =============================================================================
# WRAPPER RELATIONS: subquery / stored-proc / pivot / project / text-transform
# =============================================================================


def test_subquery_relation_becomes_derived_sql(adapter):
    """subquery.tds: a subquery relation wraps its SQL as a derived source."""
    graph = adapter.parse(FIXTURES / "subquery.tds")

    assert "active_users" in graph.models
    model = graph.models["active_users"]

    assert model.sql is not None
    assert "SELECT * FROM users WHERE active = true" in model.sql
    assert model.get_dimension("user_id") is not None


def test_stored_proc_relation_resolves_actual_name(adapter):
    """spatial_proc.tds: a stored-proc relation resolves to its actual name."""
    graph = adapter.parse(FIXTURES / "spatial_proc.tds")

    assert "store_locations" in graph.models
    model = graph.models["store_locations"]

    # Stored procedures can't be joined/unioned; we resolve to the proc name.
    assert model.table == "dbo.get_store_locations"


def test_pivot_relation_resolves_to_child_table(adapter):
    """pivot.tds: a pivot relation resolves to its wrapped child table."""
    graph = adapter.parse(FIXTURES / "pivot.tds")

    assert "pivoted_sales" in graph.models
    model = graph.models["pivoted_sales"]

    assert model.table == "sales_wide$"
    # Pivot output columns still imported
    assert model.get_dimension("Pivot Field Names") is not None
    assert model.get_metric("Pivot Field Values") is not None


def test_project_relation_resolves_to_child_table(adapter):
    """project.tds: a project relation resolves to its wrapped child table."""
    graph = adapter.parse(FIXTURES / "project.tds")

    assert "projected_orders" in graph.models
    model = graph.models["projected_orders"]

    assert model.table == "public.orders"


def test_text_transform_relation_resolves_to_child_table(adapter):
    """text_transform.tds: a text-transform relation resolves to its child."""
    graph = adapter.parse(FIXTURES / "text_transform.tds")

    assert "parsed_logs" in graph.models
    model = graph.models["parsed_logs"]

    assert model.table == "raw_logs.txt"


# =============================================================================
# SPATIAL DATATYPE
# =============================================================================


def test_spatial_datatype_maps_to_categorical(adapter):
    """spatial_proc.tds: spatial datatype columns map to categorical dimensions."""
    graph = adapter.parse(FIXTURES / "spatial_proc.tds")
    model = graph.models["store_locations"]

    geometry = model.get_dimension("geometry")
    assert geometry is not None
    assert geometry.type == "categorical"


# =============================================================================
# TABLEAU SEMANTICS: object-graph semantic-layer / is-legacy attributes
# =============================================================================


def test_object_graph_semantic_layer_attributes_captured(adapter):
    """semantic_layer.tds: semantic-layer / is-legacy attributes -> model metadata."""
    graph = adapter.parse(FIXTURES / "semantic_layer.tds")

    assert "semantic_model" in graph.models
    model = graph.models["semantic_model"]

    assert model.metadata is not None
    assert model.metadata.get("tableau_semantic_layer") == "true"
    assert model.metadata.get("tableau_is_legacy") == "false"

    # Relationships still extracted from the object-graph.
    assert len(model.relationships) == 1
    assert model.relationships[0].name == "Customers"


def test_legacy_object_graph_has_no_semantic_metadata(adapter):
    """Object-graph without semantic attributes leaves model metadata unset."""
    # The false_object_graph inline fixture (see test_parsing) has no
    # semantic-layer / is-legacy attributes; assert via a real_world fixture
    # that lacks them too.
    real_world = FIXTURES / "real_world" / "thoughtspot_sf_trial.tds"
    if not real_world.exists():
        pytest.skip("real_world fixtures not present")

    graph = adapter.parse(real_world)
    model = graph.models.get("SF Trial")
    assert model is not None
    # No semantic-layer / is-legacy attributes on this object-graph.
    if model.metadata is not None:
        assert "tableau_semantic_layer" not in model.metadata
        assert "tableau_is_legacy" not in model.metadata
