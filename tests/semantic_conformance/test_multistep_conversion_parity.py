"""Sequential funnel populations from independently specified synthetic events."""

from datetime import date

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_layer import SecurityError


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Funnel acceptance requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="funnel_events",
            primary_key="id",
            dimensions=[
                Dimension(name="person", type="categorical"),
                Dimension(name="region", type="categorical"),
                Dimension(name="occurred", type="time", granularity="day"),
            ],
            metrics=[
                Metric(
                    name="funnel",
                    type="conversion",
                    entity="person",
                    steps=["kind = 'signup'", "kind = 'view'", "kind = 'purchase'"],
                )
            ],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    layer.adapter.execute("""
        create table funnel_events(id integer, person varchar, kind varchar, occurred timestamp, region varchar, tenant integer);
        insert into funnel_events values
          (1,'u1','signup','2024-01-01','a',1), (2,'u1','view','2023-12-31','b',1),
          (3,'u1','view','2024-01-02','b',1), (4,'u1','purchase','2024-01-03','c',1),
          (5,'u2','signup','2024-01-01','a',1), (6,'u2','view','2023-12-31','a',1), (7,'u2','purchase','2024-01-03','a',1),
          (8,'u3','signup','2024-01-01','b',1), (9,'u3','view','2024-01-01','b',1), (10,'u3','purchase','2024-01-01','b',1),
          (11,'u4','signup','2024-01-01',null,1), (12,'u4','view','2024-01-02','x',1),
          (13,'u5','signup',null,'a',1), (14,'u5','view','2024-01-02','a',1), (15,'u5','purchase','2024-01-03','a',1),
          (16,'u6','view','2024-01-02','a',1), (17,'u6','purchase','2024-01-03','a',1),
          (18,'u7','signup','2024-01-01','a',1), (19,'u7','view','2024-01-02','a',2), (20,'u7','purchase','2024-01-03','a',1),
          (21,null,'signup','2024-01-01','a',1), (22,null,'view','2024-01-02','a',1), (23,null,'purchase','2024-01-03','a',1),
          (24,'u8','signup','2024-01-01','b',2), (25,'u8','view','2024-01-02','b',2), (26,'u8','purchase','2024-01-03','b',2),
          (27,'u1','signup','2024-01-01','a',1);
    """)
    yield layer
    layer.adapter.close()


def result(layer, **query):
    query.setdefault("metrics", ["events.funnel"])
    query.setdefault("user_attributes", {"tenant": 1})
    sql = layer.compile(**query)
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    cursor = layer.adapter.execute(sql)
    return [column[0] for column in cursor.description], cursor.fetchall()


COLUMNS = ["total_entities", "step_1_count", "step_2_count", "step_3_count", "funnel"]


def test_entrants_chronology_repeated_actions_ties_and_nulls(layer):
    assert result(layer) == (COLUMNS, [(6, 6, 3, 2, 2)])


def test_later_steps_keep_first_step_dimension_attribution(layer):
    assert result(layer, dimensions=["events.region"], order_by=["events.region"]) == (
        ["region", *COLUMNS],
        [("a", 4, 4, 1, 1, 1), ("b", 1, 1, 1, 1, 1), (None, 1, 1, 1, 0, 0)],
    )


def test_repeated_entrant_has_independent_chronology_in_each_first_step_group(layer):
    layer.adapter.execute("""
        insert into funnel_events values
          (28,'u1','signup','2024-01-01 12:00:00','b',1),
          (29,'u1','signup','2024-01-04','c',1);
    """)
    # u1 enters a and b before the same view/purchase, and c after those events.
    # Group populations overlap; the ungrouped distinct population is unchanged.
    assert result(layer) == (COLUMNS, [(6, 6, 3, 2, 2)])
    assert result(layer, dimensions=["events.region"], order_by=["events.region"]) == (
        ["region", *COLUMNS],
        [("a", 4, 4, 1, 1, 1), ("b", 2, 2, 2, 2, 2), ("c", 1, 1, 0, 0, 0), (None, 1, 1, 1, 0, 0)],
    )


def test_query_filter_scopes_every_step(layer):
    assert result(layer, filters=["kind != 'view'"]) == (COLUMNS, [(6, 6, 0, 0, 0)])


def test_metric_filter_scopes_every_step_and_parenthesizes_or(layer):
    layer.graph.models["events"].metrics[0].filters = ["kind != 'view'"]
    assert result(layer, filters=["person = 'u1' OR person = 'u3'"]) == (COLUMNS, [(2, 2, 0, 0, 0)])


def test_policy_scopes_every_step_independently(layer):
    assert result(layer, user_attributes={"tenant": 2}) == (COLUMNS, [(1, 1, 1, 1, 1)])


def test_policy_context_is_required(layer):
    with pytest.raises(SecurityError):
        layer.compile(metrics=["events.funnel"])


def test_invariant_excludes_later_step_before_chronology(layer):
    layer.graph.models["events"].invariant_filters = ["kind != 'view'"]
    assert result(layer) == (COLUMNS, [(6, 6, 0, 0, 0)])


def test_two_step_list_has_its_existing_count_outputs(layer):
    layer.graph.models["events"].metrics[0].steps = ["kind = 'signup'", "kind = 'view'"]
    assert result(layer) == (["total_entities", "step_1_count", "step_2_count", "funnel"], [(6, 6, 3, 3)])


def test_empty_population_has_zero_counts(layer):
    assert result(layer, filters=["person = 'absent'"]) == (COLUMNS, [(0, 0, 0, 0, 0)])


def test_dimension_and_metric_ordering_with_limit(layer):
    assert result(
        layer, dimensions=["events.region"], order_by=["events.funnel DESC", "events.region DESC"], limit=1
    ) == (["region", *COLUMNS], [("b", 1, 1, 1, 1, 1)])


def test_mapped_entity_time_and_qualified_physical_steps(layer):
    model = layer.graph.models["events"]
    model.dimensions[0] = Dimension(name="identity", sql="{model}.person", type="categorical")
    model.dimensions[2] = Dimension(
        name="occurred", sql="CAST({model}.occurred AS TIMESTAMP)", type="time", granularity="day"
    )
    model.metrics[0].entity = "identity"
    model.metrics[0].steps = ["events.kind = 'signup'", "events.kind = 'view'", "events.kind = 'purchase'"]
    assert result(layer) == (COLUMNS, [(6, 6, 3, 2, 2)])


def test_step_literal_is_not_rewritten_as_model_reference(layer):
    layer.adapter.execute("update funnel_events set kind = 'events.signup' where kind = 'signup'")
    layer.graph.models["events"].metrics[0].steps[0] = "events.kind = 'events.signup'"
    assert result(layer) == (COLUMNS, [(6, 6, 3, 2, 2)])


def test_time_bucket_belongs_to_first_step(layer):
    assert result(layer, dimensions=["events.occurred__month"], order_by=["events.occurred__month"]) == (
        ["occurred__month", *COLUMNS],
        [(date(2024, 1, 1), 5, 5, 3, 2, 2), (None, 1, 1, 0, 0, 0)],
    )


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize(
    "expression",
    ["count(*) > 0", "sum(tenant) over () > 0", "(select count(*) from funnel_events) > 0", "other.kind = 'signup'"],
)
def test_step_cannot_change_source_scope(layer, expression):
    layer.graph.models["events"].metrics[0].steps[0] = expression
    with pytest.raises(ValueError):
        layer.compile(metrics=["events.funnel"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize("alias", ["TOTAL_ENTITIES", "STEP_2_COUNT", "FUNNEL", "ENTITY", "STEP_1_TS"])
def test_fixed_output_alias_collisions_are_explicit(layer, alias):
    layer.graph.models["events"].dimensions.append(Dimension(name=alias, sql="region", type="categorical"))
    with pytest.raises(ValueError, match="conversion_output_alias"):
        layer.compile(metrics=["events.funnel"], dimensions=[f"events.{alias}"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize("source", ["sum(tenant) over ()", "(select max(occurred) from funnel_events)"])
def test_mapped_time_cannot_change_source_scope(layer, source):
    layer.graph.models["events"].get_dimension("occurred").sql = source
    with pytest.raises(ValueError, match="conversion_non_row_expression"):
        layer.compile(metrics=["events.funnel"], user_attributes={"tenant": 1})
