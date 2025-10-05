"""Test dimension hierarchies."""

from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_dimension_parent_field():
    """Test that dimensions can have a parent field."""
    country = Dimension(name="country", type="categorical")
    state = Dimension(name="state", type="categorical", parent="country")
    city = Dimension(name="city", type="categorical", parent="state")

    assert state.parent == "country"
    assert city.parent == "state"
    assert country.parent is None


def test_hierarchy_in_model():
    """Test hierarchical dimensions in a model."""
    layer = SemanticLayer()

    locations = Model(
        name="locations",
        table="locations_table",
        primary_key="location_id",
        dimensions=[
            Dimension(name="country", type="categorical", description="Country"),
            Dimension(
                name="state", type="categorical", parent="country", description="State/Province"
            ),
            Dimension(name="city", type="categorical", parent="state", description="City"),
        ],
        metrics=[
            Metric(name="population", agg="sum", sql="pop"),
        ],
    )

    layer.add_model(locations)

    # Should be able to query at any level
    sql_country = layer.compile(metrics=["locations.population"], dimensions=["locations.country"])

    sql_state = layer.compile(metrics=["locations.population"], dimensions=["locations.state"])

    sql_city = layer.compile(metrics=["locations.population"], dimensions=["locations.city"])

    assert "country" in sql_country
    assert "state" in sql_state
    assert "city" in sql_city


def test_hierarchy_drill_path():
    """Test getting drill path from hierarchy."""
    model = Model(
        name="geography",
        table="geo_table",
        dimensions=[
            Dimension(name="country", type="categorical"),
            Dimension(name="region", type="categorical", parent="country"),
            Dimension(name="state", type="categorical", parent="region"),
            Dimension(name="city", type="categorical", parent="state"),
        ],
        metrics=[
            Metric(name="sales", agg="sum", sql="amount"),
        ],
    )

    # Find the full hierarchy path
    city_dim = model.get_dimension("city")
    assert city_dim.parent == "state"

    state_dim = model.get_dimension(city_dim.parent)
    assert state_dim.parent == "region"

    region_dim = model.get_dimension(state_dim.parent)
    assert region_dim.parent == "country"


def test_hierarchy_adapter_roundtrip():
    """Test hierarchies survive adapter export/import."""
    import tempfile
    from pathlib import Path

    from sidemantic.adapters.sidemantic import SidemanticAdapter
    from sidemantic.core.semantic_graph import SemanticGraph

    original = Model(
        name="geography",
        table="geo",
        dimensions=[
            Dimension(name="continent", type="categorical"),
            Dimension(name="country", type="categorical", parent="continent"),
            Dimension(name="city", type="categorical", parent="country"),
        ],
        metrics=[
            Metric(name="population", agg="sum", sql="pop"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(original)

    adapter = SidemanticAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)
        imported_graph = adapter.parse(temp_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()

    imported = list(imported_graph.models.values())[0]

    # Verify hierarchy preserved
    country = imported.get_dimension("country")
    assert country.parent == "continent"

    city = imported.get_dimension("city")
    assert city.parent == "country"

    continent = imported.get_dimension("continent")
    assert continent.parent is None


def test_time_hierarchy():
    """Test hierarchical time dimensions."""
    model = Model(
        name="events",
        table="events_table",
        dimensions=[
            Dimension(name="event_date", type="time", granularity="day"),
            Dimension(name="event_week", type="time", granularity="week", parent="event_month"),
            Dimension(name="event_month", type="time", granularity="month", parent="event_year"),
            Dimension(name="event_year", type="time", granularity="year"),
        ],
        metrics=[
            Metric(name="event_count", agg="count"),
        ],
    )

    # Year -> Month -> Week -> Day hierarchy
    month = model.get_dimension("event_month")
    assert month.parent == "event_year"

    week = model.get_dimension("event_week")
    assert week.parent == "event_month"
