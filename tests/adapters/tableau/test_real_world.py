"""Tests for Tableau adapter against real-world files from MIT-licensed repos."""

from pathlib import Path

import pytest

from sidemantic.adapters.tableau import TableauAdapter
from sidemantic.sql.generator import SQLGenerator

REAL_WORLD = Path(__file__).parent.parent.parent / "fixtures" / "tableau" / "real_world"


@pytest.fixture
def adapter():
    return TableauAdapter()


def _skip_if_missing():
    if not REAL_WORLD.exists():
        pytest.skip("real_world fixtures not present")


# =============================================================================
# PARSE-ALL SMOKE TEST: every file parses without error
# =============================================================================


@pytest.mark.parametrize(
    "filename",
    sorted(f.name for f in REAL_WORLD.iterdir() if f.is_file()) if REAL_WORLD.exists() else [],
)
def test_real_world_parse_no_errors(adapter, filename):
    """Every real-world fixture parses without exceptions."""
    graph = adapter.parse(REAL_WORLD / filename)
    assert len(graph.models) >= 0  # just assert it doesn't crash


# =============================================================================
# CONNECTOR SDK: CAST CALCS
# =============================================================================


def test_cast_calcs_table_and_fields(adapter):
    """connector_sdk_cast_calcs.tds: correct table, labeled dims+metrics."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "connector_sdk_cast_calcs.tds")
    assert len(graph.models) == 1
    model = next(iter(graph.models.values()))

    assert model.table == "public.Calcs"
    assert len(model.dimensions) == 18
    assert len(model.metrics) == 10

    # Check that labels are extracted
    labeled_dims = [d for d in model.dimensions if d.label]
    assert len(labeled_dims) >= 10


# =============================================================================
# DOCUMENT API: FILTERING (groups, calc fields, namespace handling)
# =============================================================================


def test_filtering_table_found(adapter):
    """document_api_filtering.twb: extracts table through namespace-prefixed tag."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_filtering.twb")
    assert len(graph.models) == 1
    model = next(iter(graph.models.values()))

    assert model.table == "dbo.TestData"


def test_filtering_groups_as_segments(adapter):
    """document_api_filtering.twb: group is converted to segment."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_filtering.twb")
    model = next(iter(graph.models.values()))

    assert len(model.segments) >= 1


def test_filtering_calculated_dimension(adapter):
    """document_api_filtering.twb: calculated dimension is extracted."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_filtering.twb")
    model = next(iter(graph.models.values()))

    calc_dims = [d for d in model.dimensions if d.sql is not None]
    assert len(calc_dims) >= 1


# =============================================================================
# DOCUMENT API: SHAPES (hidden fields, labels, multiple tables)
# =============================================================================


def test_shapes_table_found(adapter):
    """document_api_shapes.twb: table through namespace-prefixed tag."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_shapes.twb")
    model = graph.models.get("Sample - Superstore")
    assert model is not None
    assert model.sql is not None
    assert '"Orders$"' in model.sql


def test_shapes_hidden_fields(adapter):
    """document_api_shapes.twb: hidden fields marked public=False."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_shapes.twb")
    model = graph.models.get("Sample - Superstore")
    assert model is not None

    hidden_dims = [d for d in model.dimensions if not d.public]
    assert len(hidden_dims) >= 5


def test_shapes_labels(adapter):
    """document_api_shapes.twb: captions become labels."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_shapes.twb")
    model = graph.models.get("Sample - Superstore")
    assert model is not None

    labeled = [d for d in model.dimensions if d.label] + [m for m in model.metrics if m.label]
    assert len(labeled) >= 3


# =============================================================================
# DOCUMENT API: NESTED (groups, calc fields)
# =============================================================================


def test_nested_table_found(adapter):
    """document_api_nested.tds: table through namespace-prefixed tag."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_nested.tds")
    assert len(graph.models) == 1
    model = next(iter(graph.models.values()))

    assert model.table == "dbo.TestData"


def test_nested_calculated_fields(adapter):
    """document_api_nested.tds: calculated fields extracted."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_nested.tds")
    model = next(iter(graph.models.values()))

    calc_dims = [d for d in model.dimensions if d.sql is not None]
    assert len(calc_dims) >= 4


# =============================================================================
# DOCUMENT API: WORLD (24 metrics, extract)
# =============================================================================


def test_world_all_metrics(adapter):
    """document_api_world.tds: all 24 metrics with aggregations."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_world.tds")
    model = graph.models.get("World Indicators")
    assert model is not None

    assert len(model.metrics) == 24
    # All should have aggregations (from metadata-records)
    no_agg = [m for m in model.metrics if m.agg is None]
    assert len(no_agg) == 0, f"Metrics missing agg: {[m.name for m in no_agg]}"


# =============================================================================
# SERVER INSIGHTS: CONTENT (large file, 241 dims, 73 metrics)
# =============================================================================


def test_server_insights_content_scale(adapter):
    """server_insights_content.twb: parses large file with 300+ fields."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "server_insights_content.twb")
    assert len(graph.models) == 1
    model = next(iter(graph.models.values()))

    assert len(model.dimensions) >= 200
    assert len(model.metrics) >= 50


def test_server_insights_content_hidden(adapter):
    """server_insights_content.twb: many hidden fields."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "server_insights_content.twb")
    model = next(iter(graph.models.values()))

    hidden_dims = [d for d in model.dimensions if not d.public]
    hidden_mets = [m for m in model.metrics if not m.public]
    assert len(hidden_dims) >= 100
    assert len(hidden_mets) >= 30


def test_server_insights_content_calculated(adapter):
    """server_insights_content.twb: calculated fields translated or preserved."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "server_insights_content.twb")
    model = next(iter(graph.models.values()))

    calc_dims = [d for d in model.dimensions if d.sql is not None]
    calc_mets = [m for m in model.metrics if m.sql is not None]
    assert len(calc_dims) >= 20
    assert len(calc_mets) >= 10


def test_server_insights_content_relationships(adapter):
    """server_insights_content.twb: multi-table joins produce relationships."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "server_insights_content.twb")
    model = next(iter(graph.models.values()))

    assert len(model.relationships) >= 10


def test_server_insights_content_lod_preserved(adapter):
    """server_insights_content.twb: LOD expressions stored in metadata."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "server_insights_content.twb")
    model = next(iter(graph.models.values()))

    lod_dims = [d for d in model.dimensions if d.metadata and "tableau_formula" in d.metadata]
    lod_mets = [m for m in model.metrics if m.metadata and "tableau_formula" in m.metadata]
    # Should have at least some LOD expressions preserved (can be on dims or metrics)
    assert len(lod_dims) + len(lod_mets) >= 1


# =============================================================================
# SERVER INSIGHTS: DATA CONNECTIONS (largest file, 333 dims)
# =============================================================================


def test_data_connections_scale(adapter):
    """server_insights_data_connections.twb: parses largest file."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "server_insights_data_connections.twb")
    assert len(graph.models) == 1
    model = next(iter(graph.models.values()))

    assert len(model.dimensions) >= 300
    assert len(model.metrics) >= 50


def test_data_connections_relationships(adapter):
    """server_insights_data_connections.twb: join relationships extracted."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "server_insights_data_connections.twb")
    model = next(iter(graph.models.values()))

    assert len(model.relationships) >= 10
    # All relationships should have valid types
    for rel in model.relationships:
        assert rel.type in ("many_to_one", "one_to_many", "one_to_one", "many_to_many")


# =============================================================================
# THOUGHTSPOT SF TRIAL (Snowflake datasource, all labeled)
# =============================================================================


def test_sf_trial_collection_builds_join_sql(adapter):
    """thoughtspot_sf_trial.tds: logical-layer collection becomes joined model SQL."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "thoughtspot_sf_trial.tds")
    model = graph.models.get("SF Trial")
    assert model is not None

    assert model.sql is not None
    assert '"ORDERS"' in model.sql
    assert '"CUSTOMER"' in model.sql
    assert '"LINEITEM"' in model.sql
    assert "LEFT JOIN" in model.sql


def test_sf_trial_primary_key_projected(adapter):
    """thoughtspot_sf_trial.tds: inferred PK comes from projected collection SQL."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "thoughtspot_sf_trial.tds")
    model = graph.models.get("SF Trial")
    assert model is not None

    assert model.primary_key == "__tableau_pk"
    assert model.sql is not None
    assert '"__tableau_pk"' in model.sql


def test_sf_trial_all_labeled(adapter):
    """thoughtspot_sf_trial.tds: all fields have labels."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "thoughtspot_sf_trial.tds")
    model = graph.models.get("SF Trial")
    assert model is not None

    labeled_dims = [d for d in model.dimensions if d.label]
    assert len(labeled_dims) >= 30

    labeled_mets = [m for m in model.metrics if m.label]
    assert len(labeled_mets) >= 25


def test_sf_trial_object_graph_relationships(adapter):
    """thoughtspot_sf_trial.tds: object-graph relationships extracted."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "thoughtspot_sf_trial.tds")
    model = graph.models.get("SF Trial")
    assert model is not None

    # Should have relationships from the object-graph (8 tables, 7 relationships)
    assert len(model.relationships) >= 5
    # Verify relationship quality
    for rel in model.relationships:
        assert rel.type in ("many_to_one", "one_to_many", "one_to_one", "many_to_many")
        assert rel.foreign_key, f"Relationship {rel.name} missing foreign_key"
        assert rel.primary_key, f"Relationship {rel.name} missing primary_key"


def test_sf_trial_joined_metric_uses_collection_sql(adapter):
    """Joined-table metrics should compile against the logical-layer SQL, not a base table."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "thoughtspot_sf_trial.tds")

    sql = SQLGenerator(graph).generate(
        metrics=["SF Trial.L_DISCOUNT"],
        dimensions=[],
        limit=5,
        skip_default_time_dimensions=True,
    )
    assert "id AS id" not in sql
    assert "__tableau_pk AS __tableau_pk" in sql
    assert '"LINEITEM"' in sql


def test_shapes_object_graph_relationships(adapter):
    """document_api_shapes.twb: object-graph relationships extracted."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_shapes.twb")
    model = graph.models.get("Sample - Superstore")
    assert model is not None

    # Should have relationships from the object-graph (3 tables, 2 relationships)
    assert len(model.relationships) >= 2


def test_shapes_joined_dimension_uses_collection_sql(adapter):
    """Joined dimensions from secondary tables should compile from the collection SQL."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_shapes.twb")

    sql = SQLGenerator(graph).generate(
        metrics=[],
        dimensions=["Sample - Superstore.Regional Manager"],
        limit=5,
        skip_default_time_dimensions=True,
    )
    assert '"People$"' in sql
    assert '"Regional Manager"' in sql


def test_shapes_orphan_columns_imported(adapter):
    """document_api_shapes.twb: orphan metadata columns imported."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_shapes.twb")
    model = graph.models.get("Sample - Superstore")
    assert model is not None

    # Sales, Discount, Quantity should now be imported as metrics
    sales = model.get_metric("Sales")
    assert sales is not None
    assert sales.agg == "sum"

    # Segment, Sub-Category should be imported as dimensions
    segment = model.get_dimension("Segment")
    assert segment is not None
    assert segment.type == "categorical"


# =============================================================================
# DOCUMENT API: MULTIPLE CONNECTIONS (join with relationships)
# =============================================================================


def test_multiple_connections_join(adapter):
    """document_api_multiple_connections.twb: join produces relationship."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_multiple_connections.twb")

    # Should have a model with SQL (join) and relationships
    models_with_rels = [m for m in graph.models.values() if len(m.relationships) > 0]
    assert len(models_with_rels) >= 1


# =============================================================================
# EDGE CASES
# =============================================================================


def test_minimal_tableau10(adapter):
    """document_api_tableau10.tds: minimal file produces model with 0 fields."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_tableau10.tds")
    assert len(graph.models) == 1


def test_minimal_tableau93(adapter):
    """document_api_tableau93.tds: old format still parseable."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_tableau93.tds")
    assert len(graph.models) == 1


def test_unicode_content(adapter):
    """document_api_unicode.tds: unicode content handled correctly."""
    _skip_if_missing()
    graph = adapter.parse(REAL_WORLD / "document_api_unicode.tds")
    assert len(graph.models) == 1
    model = next(iter(graph.models.values()))
    assert len(model.dimensions) >= 2
