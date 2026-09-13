"""Two-event conversion populations against independent synthetic expectations."""

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_layer import SecurityError


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Conversion acceptance requires the real extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="id",
            dimensions=[
                Dimension(name="user_id", type="categorical"),
                Dimension(name="event_type", type="categorical"),
                Dimension(name="event_time", type="time", granularity="day"),
                Dimension(name="channel", type="categorical"),
            ],
            metrics=[
                Metric(
                    name="converted",
                    type="conversion",
                    entity="user_id",
                    base_event="signup",
                    conversion_event="buy",
                    conversion_window="7 days",
                )
            ],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    layer.adapter.execute("""
        create table events(id integer, user_id varchar, event_type varchar, event_time timestamp, channel varchar, tenant integer);
        insert into events values
          (1,'u1','signup','2024-01-01','a',1), (2,'u1','buy','2024-01-02','b',1),
          (3,'u2','signup','2024-01-01','a',1), (4,'u2','buy','2024-01-10','a',1),
          (5,'u3','signup','2024-01-01','b',1), (6,'u3','buy','2024-01-08','b',1),
          (7,'u4','buy','2023-12-31','b',1), (8,'u4','signup','2024-01-01','b',1),
          (9,null,'signup','2024-01-01','a',1),
          (10,'u1','signup','2024-01-01','a',1),
          (11,'u6','signup','2024-01-01','a',2), (12,'u6','buy','2024-01-02','a',2),
          (13,'u7','signup','2024-01-01','a',1), (14,'u7','buy','2024-01-02','a',2),
          (15,'u8','signup',null,'a',1);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def result(layer, **query):
    query.setdefault("metrics", ["events.converted"])
    query.setdefault("user_attributes", {"tenant": 1})
    cursor = layer.adapter.execute(layer.compile(**query))
    columns = ["channel", "converted"] if query.get("dimensions") else ["converted"]
    assert [field[0] for field in cursor.description] == columns
    return cursor.fetchall()


def test_unique_entities_and_inclusive_window(layer):
    # u1 and u3 convert; u2 is late, u4 early, u7 has only a forbidden target,
    # u8 has no timestamp; repeated u1 and NULL identities do not add denominator.
    assert result(layer)[0][0] == pytest.approx(2 / 6)


def test_attribution_uses_base_event_dimension(layer):
    rows = result(layer, dimensions=["events.channel"], order_by=["events.channel"])
    assert rows == [("a", 0.25), ("b", 0.5)]


def test_empty_base_population_is_null(layer):
    assert result(layer, filters=["events.event_type = 'absent'"]) == [(None,)]


def test_row_filter_scopes_both_event_populations(layer):
    assert result(layer, filters=["events.user_id != 'u2'"])[0][0] == pytest.approx(2 / 5)


def test_policy_scopes_target_and_denominator(layer):
    assert result(layer, user_attributes={"tenant": 2}) == [(1.0,)]


def test_conversion_policy_requires_caller_context(layer):
    with pytest.raises(SecurityError):
        layer.compile(metrics=["events.converted"])


def test_grouped_pagination_preserves_conversion_output(layer):
    assert result(layer, dimensions=["events.channel"], order_by=["events.channel DESC"], limit=1) == [("b", 0.5)]


def test_literal_policy_attribute_cannot_expand_base_population(layer):
    layer.graph.models["events"].security = SecurityPolicy(row_filters=["channel = {{ user.channel }}"])
    assert result(layer, user_attributes={"channel": "a' OR '1'='1"}) == [(None,)]


def test_invariant_scopes_base_and_target_events(layer):
    layer.graph.models["events"].invariant_filters = ["channel <> 'b'"]
    assert result(layer) == [(0.0,)]


def test_metric_filter_scopes_base_and_target_events(layer):
    layer.graph.models["events"].metrics[0].filters = ["user_id != 'u2'"]
    assert result(layer)[0][0] == pytest.approx(2 / 5)
