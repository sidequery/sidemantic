"""Test drill-down API helpers."""

from sidemantic import Dimension, Metric, Model


def test_get_hierarchy_path():
    """Test getting full hierarchy path."""
    model = Model(
        name="geography",
        table="geo",
        dimensions=[
            Dimension(name="country", type="categorical"),
            Dimension(name="state", type="categorical", parent="country"),
            Dimension(name="city", type="categorical", parent="state"),
        ],
        metrics=[
            Metric(name="population", agg="sum", sql="pop"),
        ],
    )

    # Get path from root to city
    path = model.get_hierarchy_path("city")
    assert path == ["country", "state", "city"]

    # Get path from root to state
    path = model.get_hierarchy_path("state")
    assert path == ["country", "state"]

    # Root has single-item path
    path = model.get_hierarchy_path("country")
    assert path == ["country"]


def test_get_drill_down():
    """Test getting next dimension in hierarchy."""
    model = Model(
        name="geography",
        table="geo",
        dimensions=[
            Dimension(name="country", type="categorical"),
            Dimension(name="state", type="categorical", parent="country"),
            Dimension(name="city", type="categorical", parent="state"),
        ],
        metrics=[
            Metric(name="population", agg="sum", sql="pop"),
        ],
    )

    # Drill from country -> state
    assert model.get_drill_down("country") == "state"

    # Drill from state -> city
    assert model.get_drill_down("state") == "city"

    # City is leaf, no drill down
    assert model.get_drill_down("city") is None


def test_get_drill_up():
    """Test getting parent dimension in hierarchy."""
    model = Model(
        name="geography",
        table="geo",
        dimensions=[
            Dimension(name="country", type="categorical"),
            Dimension(name="state", type="categorical", parent="country"),
            Dimension(name="city", type="categorical", parent="state"),
        ],
        metrics=[
            Metric(name="population", agg="sum", sql="pop"),
        ],
    )

    # Drill from city -> state
    assert model.get_drill_up("city") == "state"

    # Drill from state -> country
    assert model.get_drill_up("state") == "country"

    # Country is root, no drill up
    assert model.get_drill_up("country") is None


def test_time_hierarchy_drill():
    """Test drill-down with time dimensions."""
    model = Model(
        name="events",
        table="events",
        dimensions=[
            Dimension(name="event_year", type="time", granularity="year"),
            Dimension(name="event_month", type="time", granularity="month", parent="event_year"),
            Dimension(name="event_day", type="time", granularity="day", parent="event_month"),
        ],
        metrics=[
            Metric(name="event_count", agg="count"),
        ],
    )

    # Full path
    path = model.get_hierarchy_path("event_day")
    assert path == ["event_year", "event_month", "event_day"]

    # Drill down year -> month -> day
    assert model.get_drill_down("event_year") == "event_month"
    assert model.get_drill_down("event_month") == "event_day"
    assert model.get_drill_down("event_day") is None

    # Drill up day -> month -> year
    assert model.get_drill_up("event_day") == "event_month"
    assert model.get_drill_up("event_month") == "event_year"
    assert model.get_drill_up("event_year") is None


def test_hierarchy_cycle_detection():
    """Test that get_hierarchy_path terminates on cyclic parent references."""
    model = Model(
        name="bad",
        table="t",
        dimensions=[
            Dimension(name="a", type="categorical", parent="b"),
            Dimension(name="b", type="categorical", parent="a"),
        ],
        metrics=[
            Metric(name="c", agg="count"),
        ],
    )

    # Should terminate, not infinite loop
    path = model.get_hierarchy_path("a")
    # b -> a, but b's parent is a which is already visited, so path is [b, a]
    assert "a" in path
    assert "b" in path
    assert len(path) == 2


def test_hierarchy_self_referencing_cycle():
    """Test that get_hierarchy_path handles self-referencing parent."""
    model = Model(
        name="bad",
        table="t",
        dimensions=[
            Dimension(name="a", type="categorical", parent="a"),
        ],
        metrics=[
            Metric(name="c", agg="count"),
        ],
    )

    path = model.get_hierarchy_path("a")
    assert path == ["a"]


def test_multiple_children():
    """Test dimension with multiple children."""
    model = Model(
        name="org",
        table="org",
        dimensions=[
            Dimension(name="country", type="categorical"),
            Dimension(name="state", type="categorical", parent="country"),
            Dimension(name="region", type="categorical", parent="country"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
        ],
    )

    # Drill down returns one of the children (first found)
    child = model.get_drill_down("country")
    assert child in ["state", "region"]
