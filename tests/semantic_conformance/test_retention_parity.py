"""Retention populations and denominators, executed against independently counted fixtures."""

from datetime import date, datetime
from pathlib import Path

import pytest

from sidemantic import Metric, SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError

FIXTURES = Path(__file__).parent / "fixtures"
DAY_COLUMNS = ["cohort_date", "days_since", "active_users", "cohort_size", "retention_pct"]
DAY_ROWS = [
    ("2024-01-01", 0, 1, 3, 33.3),
    ("2024-01-01", 1, 1, 3, 33.3),
    ("2024-01-01", 2, 1, 3, 33.3),
    ("2024-01-02", 2, 1, 1, 100.0),
]


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Retention parity requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    try:
        layer.graph = SidemanticAdapter().parse(FIXTURES / "retention.yml")
        layer.adapter.execute((FIXTURES / "retention.sql").read_text())
        yield layer
    finally:
        layer.adapter.close()


def result(layer, **query):
    query.setdefault("metrics", ["events.retained"])
    query.setdefault("user_attributes", {"tenant": 1})
    cursor = layer.adapter.execute(layer.compile(**query))
    columns = [field[0] for field in cursor.description]
    rows = [
        tuple(value.isoformat()[:10] if isinstance(value, (date, datetime)) else value for value in row)
        for row in cursor.fetchall()
    ]
    return columns, rows


def test_first_cohort_sparse_periods_nulls_and_inactive_denominator(layer):
    # Jan 1 has A/B/C; B never returns but remains in the denominator. A's
    # repeated signup cannot move its cohort, and duplicate activities count once.
    # NULL entities/dates, activity without signup, and deleted rows contribute none.
    assert result(layer) == (DAY_COLUMNS, DAY_ROWS)


def test_policy_scopes_cohort_and_activity_for_each_tenant(layer):
    # Tenant 2's earlier signup for A must never change tenant 1's A cohort.
    assert result(layer, user_attributes={"tenant": 2}) == (
        DAY_COLUMNS,
        [("2024-01-01", 0, 1, 1, 100.0), ("2024-01-01", 3, 1, 1, 100.0)],
    )
    assert result(layer) == (DAY_COLUMNS, DAY_ROWS)


def test_row_filter_scopes_denominator_as_well_as_activity(layer):
    assert result(layer, filters=["events.country = 'us'"]) == (
        DAY_COLUMNS,
        [("2024-01-01", day, 1, 2, 50.0) for day in (0, 1, 2)] + [("2024-01-02", 2, 1, 1, 100.0)],
    )


def test_or_filter_cannot_bypass_cohort_predicate_or_mandatory_policies(layer):
    # Both countries are already present, so this filter must not alter either
    # population. Ungrouped OR would admit deleted/foreign-tenant signup rows.
    assert result(layer, filters=["events.country = 'us' OR events.country = 'ca'"]) == (DAY_COLUMNS, DAY_ROWS)


def test_metric_filter_scopes_both_populations(layer):
    assert result(layer, metrics=["events.retained_us"]) == (
        DAY_COLUMNS,
        [("2024-01-01", day, 1, 2, 50.0) for day in (0, 1, 2)] + [("2024-01-02", 2, 1, 1, 100.0)],
    )


def test_time_filter_recomputes_first_qualifying_cohort(layer):
    # Filtering removes Jan 1 signups; A's remaining Jan 2 signup shares D's
    # cohort. The inclusive period boundary retains A's Jan 5 return at day 3.
    assert result(layer, filters=["events.occurred >= '2024-01-02'"]) == (
        DAY_COLUMNS,
        [("2024-01-02", day, 1, 2, 50.0) for day in (1, 2, 3)],
    )


def test_aliased_dimension_filter_can_empty_cohort_population(layer):
    assert result(layer, filters=["events.event_label != 'signup'"]) == (DAY_COLUMNS, [])


def test_default_activity_includes_signup_and_preserves_inactive_denominator(layer):
    layer.graph.get_model("events").get_metric("retained").activity_event = None
    assert result(layer) == (
        DAY_COLUMNS,
        [
            ("2024-01-01", 0, 3, 3, 100.0),
            ("2024-01-01", 1, 2, 3, 66.7),
            ("2024-01-01", 2, 1, 3, 33.3),
            ("2024-01-02", 0, 1, 1, 100.0),
            ("2024-01-02", 2, 1, 1, 100.0),
        ],
    )


@pytest.mark.parametrize(
    "grain,label,rows",
    [
        ("week", "weeks_since", [("2024-01-01", 0, 3, 4, 75.0), ("2024-01-01", 1, 1, 4, 25.0)]),
        ("month", "months_since", [("2024-01-01", 0, 3, 4, 75.0), ("2024-01-01", 1, 1, 4, 25.0)]),
    ],
)
def test_calendar_periods_have_fixed_output_labels(layer, grain, label, rows):
    layer.graph.get_model("events").get_metric("retained").retention_granularity = grain
    assert result(layer) == (["cohort_date", label, "active_users", "cohort_size", "retention_pct"], rows)


def test_order_and_pagination_apply_to_retention_outputs(layer):
    assert result(layer, order_by=["cohort_date DESC", "days_since DESC"], limit=2, offset=1) == (
        DAY_COLUMNS,
        [DAY_ROWS[2], DAY_ROWS[1]],
    )


def test_sql_source_and_model_placeholders_preserve_population(layer):
    model = layer.graph.get_model("events")
    model.table = None
    model.sql = "select * from retention_events"
    model.get_dimension("occurred").sql = "{model}.event_ts"
    metric = model.get_metric("retained")
    metric.cohort_event = "{model}.event_kind = 'signup'"
    metric.activity_event = "{model}.event_kind = 'active'"
    assert result(layer) == (DAY_COLUMNS, DAY_ROWS)


@pytest.mark.parametrize("attributes", [None, {}])
def test_missing_policy_context_denies_retention(layer, attributes):
    with pytest.raises(SecurityError):
        layer.compile(metrics=["events.retained"], user_attributes=attributes)


def test_retention_cannot_combine_with_additive_metric(layer):
    with pytest.raises(ValueError, match="cannot be combined"):
        layer.compile(metrics=["events.retained", "events.event_count"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("shape", ["dimension", "aggregate_filter", "wrapper"])
def test_rust_rejects_unpromoted_retention_shapes(layer, shape):
    if layer.engine != "rust":
        pytest.skip("Rust capability boundary; Python retains its existing feature behavior")
    query = {}
    if shape == "dimension":
        query["dimensions"] = ["events.country"]
    elif shape == "aggregate_filter":
        query["filters"] = ["events.event_count > 0"]
    else:
        layer.graph.get_model("events").metrics.append(Metric(name="wrapped", type="derived", sql="retained"))
        query["metrics"] = ["events.wrapped"]
    with pytest.raises(UnsupportedSemanticFeaturesError):
        result(layer, **query)


def test_selected_calculations_preserve_retention_fixed_projection(layer):
    from sidemantic.core.table_calculation import TableCalculation
    from sidemantic.sql.table_calc_processor import TableCalculationProcessor

    calculations = [
        TableCalculation(name="sequence", type="row_number"),
        TableCalculation(name="running", type="running_total", field="active_users"),
    ]
    for calculation in calculations:
        layer.graph.add_table_calculation(calculation)
    query = {"order_by": ["cohort_date DESC", "days_since DESC"], "limit": 3, "offset": 1}
    columns, rows = result(layer, **query)
    expected_rows, expected_columns = TableCalculationProcessor(calculations).process(rows, columns)
    assert result(layer, **query, table_calculations=[c.name for c in calculations]) == (
        expected_columns,
        expected_rows,
    )
    if layer.engine == "rust":
        import json

        import sidemantic_rs

        from sidemantic.semantic_handoff import graph_to_semantic_input

        # Reference validation keeps the complete output contract even without
        # caller attributes; compile separately performs authorization.
        payload = {**query, "metrics": ["events.retained"], "table_calculations": [c.name for c in calculations]}
        assert (
            json.loads(
                sidemantic_rs.validate_with_semantic_input(
                    json.dumps(graph_to_semantic_input(layer.graph)), json.dumps(payload)
                )
            )
            == []
        )


@pytest.mark.parametrize("fill", [0, -99, "missing"])
def test_retention_accepts_fill_metadata_without_changing_its_fixed_output_schema(layer, fill):
    # Python retention has a fixed table output and does not apply metric fills.
    layer.graph.models["events"].metrics[0].fill_nulls_with = fill
    assert result(layer) == (DAY_COLUMNS, DAY_ROWS)
    assert result(layer, filters=["events.event_label != 'signup'"]) == (DAY_COLUMNS, [])
