"""Graph-level population metrics infer a unique declared entity source."""

from datetime import date

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.semantic_handoff import graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="id",
            dimensions=[
                Dimension(name="user_id", type="categorical"),
                Dimension(name="event_type", type="categorical"),
                Dimension(name="day", type="time", granularity="day"),
            ],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    # Inference must use the entity declaration, not a one-model shortcut.
    layer.add_model(Model(name="unrelated", table="must_not_read", primary_key="id"))
    layer.adapter.execute("""
        create table events(id integer, user_id integer, event_type varchar, day date, tenant integer);
        insert into events values
            (1,1,'signup','2024-01-01',1), (2,1,'purchase','2024-01-02',1),
            (3,2,'signup','2024-01-01',1), (4,2,'browse','2024-01-02',1),
            (5,3,'signup','2024-01-01',1),
            (6,9,'signup','2024-01-01',2), (7,9,'purchase','2024-01-02',2);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def population_metric(kind):
    options = {
        "cohort": {
            "agg": "count",
            "inner_metrics": [{"name": "events_seen", "agg": "count_distinct", "sql": "event_type"}],
            "having": "events_seen >= 2",
        },
        "conversion": {"base_event": "signup", "conversion_event": "purchase", "conversion_window": "7 days"},
        "retention": {
            "cohort_event": "event_type = 'signup'",
            "activity_event": "true",
            "periods": 2,
            "retention_granularity": "day",
        },
    }
    return Metric(name="population", type=kind, entity="user_id", **options[kind])


@pytest.mark.parametrize("kind", ["cohort", "conversion", "retention"])
@pytest.mark.parametrize("ownership", ["inferred", "explicit"])
def test_graph_population_metric_infers_unique_entity_and_preserves_policy(layer, kind, ownership):
    if ownership == "explicit":
        layer.graph.models["unrelated"].dimensions.append(Dimension(name="user_id", type="categorical"))
    layer.graph.add_metric(population_metric(kind), model_name="events" if ownership == "explicit" else None)
    before = graph_to_semantic_input(layer.graph)
    result = layer.query(metrics=["population"], user_attributes={"tenant": 1})
    columns = [column[0] for column in result.description]
    rows = result.fetchall()
    assert graph_to_semantic_input(layer.graph) == before
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    if kind == "cohort":
        assert columns == ["population"]
        assert rows == [(2,)]
    elif kind == "conversion":
        assert dict(zip(columns, rows[0]))["population"] == pytest.approx(1 / 3)
        assert len(rows) == 1
    else:
        assert rows == [(date(2024, 1, 1), 0, 3, 3, 100.0), (date(2024, 1, 1), 1, 2, 3, 66.7)]


@pytest.mark.parametrize("kind", ["cohort", "conversion", "retention"])
def test_graph_population_metric_rejects_ambiguous_entity_sources(layer, kind):
    layer.graph.models["unrelated"].dimensions.append(Dimension(name="user_id", type="categorical"))
    layer.graph.add_metric(population_metric(kind))
    with pytest.raises(ValueError, match="[Aa]mbiguous"):
        layer.compile(metrics=["population"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("kind", ["cohort", "conversion", "retention"])
@pytest.mark.parametrize("prefix", ["analytics", "events"])
def test_dotted_graph_population_identity_preserves_source_policy(layer, kind, prefix):
    # The graph identity also wins when it shadows a real model-local metric.
    layer.graph.models["events"].metrics.append(Metric(name="population", agg="count"))
    metric = population_metric(kind)
    metric.name = f"{prefix}.population"
    layer.graph.add_metric(metric)
    result = layer.query(metrics=[metric.name], user_attributes={"tenant": 1})
    if kind == "cohort":
        assert result.fetchall() == [(2,)]
    elif kind == "conversion":
        assert result.fetchall() == [(pytest.approx(1 / 3),)]
    else:
        assert result.fetchall() == [(date(2024, 1, 1), 0, 3, 3, 100.0), (date(2024, 1, 1), 1, 2, 3, 66.7)]
    if kind != "retention":
        assert [column[0] for column in result.description] == [metric.name]


def test_local_dependency_precedes_global_name_during_policy_discovery(layer):
    # Bare dependencies resolve locally before graph names;
    # policy discovery must follow that same binding rather than visit a shadow.
    layer.graph.models["events"].metrics.extend(
        [Metric(name="total", agg="sum", sql="id"), Metric(name="double_total", type="derived", sql="total * 2")]
    )
    layer.graph.models["unrelated"].security = SecurityPolicy(access=False)
    layer.graph.add_metric(Metric(name="total", agg="sum", sql="unrelated.id"))
    assert layer.query(metrics=["events.double_total"], user_attributes={"tenant": 1}).fetchall() == [(30,)]


def test_dotted_graph_multistep_conversion_preserves_identity_and_policy(layer):
    layer.graph.add_metric(
        Metric(
            name="analytics.funnel",
            type="conversion",
            entity="user_id",
            steps=["event_type = 'signup'", "event_type = 'purchase'"],
        )
    )
    result = layer.query(metrics=["analytics.funnel"], user_attributes={"tenant": 1})
    assert [column[0] for column in result.description] == [
        "total_entities",
        "step_1_count",
        "step_2_count",
        "analytics.funnel",
    ]
    assert result.fetchall() == [(3, 3, 1, 1)]


def test_inferred_graph_cohort_approximate_count_uses_authorized_entities(layer):
    metric = population_metric("cohort")
    metric.agg = "approx_count_distinct"
    metric.sql = "user_id"
    layer.graph.add_metric(metric)
    assert layer.query(metrics=["population"], user_attributes={"tenant": 1}).fetchall() == [(2,)]


@pytest.mark.parametrize("source_state", ["missing", "ambiguous"])
def test_graph_cohort_approximate_count_requires_unambiguous_entity_source(layer, source_state):
    metric = population_metric("cohort")
    metric.agg = "approx_count_distinct"
    metric.sql = "user_id"
    if source_state == "missing":
        metric.entity = "missing_entity"
        error = "metric.cohort_owner" if layer.engine == "rust" else "No model found"
    else:
        layer.graph.models["unrelated"].dimensions.append(Dimension(name="user_id", type="categorical"))
        error = "[Aa]mbiguous"
    layer.graph.add_metric(metric)
    with pytest.raises(ValueError, match=error):
        layer.compile(metrics=["population"], user_attributes={"tenant": 1})
