"""Snapshot values against independent, adversarial expected results in both engines."""

from datetime import date, datetime
from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import SecurityError

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Snapshot parity requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, auto_register=False)
    try:
        layer.graph = SidemanticAdapter().parse(FIXTURES / "snapshots.yml")
        layer.adapter.execute((FIXTURES / "snapshots.sql").read_text())
        yield layer
    finally:
        layer.adapter.close()


def result(layer, **query):
    query.setdefault("user_attributes", {"tenant": 1})
    query.setdefault("use_preaggregations", False)
    cursor = layer.adapter.execute(layer.compile(**query))
    return [
        tuple(value.isoformat()[:10] if isinstance(value, (date, datetime)) else value for value in row)
        for row in cursor.fetchall()
    ]


def test_opening_closing_and_additive_sibling_keep_separate_populations(layer):
    # Per-account last balances are A=170, B=80, C=NULL, D=NULL: 250.
    # Opening balances are 100+50+30=180. All eight authorized activity rows survive.
    assert result(layer, metrics=["snapshots.closing", "snapshots.opening", "snapshots.activity"]) == [(250, 180, 36)]


def test_declared_snapshot_groups_do_not_become_global_latest_date(layer):
    # The global latest authorized date belongs to C and has a NULL balance.
    assert result(layer, metrics=["snapshots.closing", "snapshots.global_closing"]) == [(250, None)]


def test_snapshot_groups_preserve_all_null_time_and_latest_null_value(layer):
    assert result(
        layer,
        metrics=["snapshots.closing", "snapshots.opening", "snapshots.activity"],
        dimensions=["snapshots.account"],
        order_by=["snapshots.account"],
    ) == [("A", 170, 100, 6), ("B", 80, 50, 9), ("C", None, 30, 13), ("D", None, None, 8)]


def test_declared_groups_roll_up_into_selected_region(layer):
    assert result(
        layer,
        metrics=["snapshots.closing", "snapshots.opening", "snapshots.activity"],
        dimensions=["snapshots.region"],
        order_by=["snapshots.region"],
    ) == [("east", 250, 150, 15), ("west", None, 30, 21)]


def test_declared_grouping_keeps_latest_account_when_region_changes(layer):
    layer.adapter.execute("insert into snapshot_rows values (10, 'A', 'west', '2024-02-03', 200, 9, true, 1)")
    # Declared account partition wins over the query's region: A's older east
    # balance is not another snapshot. Additive activity still includes both regions.
    assert result(
        layer,
        metrics=["snapshots.closing", "snapshots.opening", "snapshots.activity"],
        dimensions=["snapshots.region"],
        order_by=["snapshots.region"],
    ) == [("east", 80, 150, 15), ("west", 200, 30, 30)]


def test_equal_latest_dates_keep_all_tied_rows(layer):
    layer.adapter.execute("insert into snapshot_rows values (10, 'B', 'east', '2024-01-25', 5, 9, true, 1)")
    # Snapshot selection is equality with MAX(day), not one arbitrary ROW_NUMBER.
    assert result(layer, metrics=["snapshots.closing", "snapshots.activity"]) == [(255, 45)]


def test_null_grouping_key_is_one_snapshot_partition(layer):
    layer.adapter.execute(
        "insert into snapshot_rows values "
        "(10, null, 'west', '2024-01-01', 10, 9, true, 1), "
        "(11, null, 'west', '2024-01-02', 20, 10, true, 1)"
    )
    assert result(layer, metrics=["snapshots.closing", "snapshots.opening", "snapshots.activity"]) == [(270, 190, 55)]


def test_coarse_time_uses_snapshot_within_each_month(layer):
    rows = result(
        layer,
        metrics=["snapshots.closing", "snapshots.opening", "snapshots.activity"],
        dimensions=["snapshots.day__month"],
    )
    assert set(rows) == {(None, None, None, 8), ("2024-01-01", 260, 180, 18), ("2024-02-01", 170, 170, 10)}


def test_raw_time_grouping_bypasses_snapshot_mask_including_null_time(layer):
    rows = result(layer, metrics=["snapshots.closing"], dimensions=["snapshots.day"])
    assert set(rows) == {
        ("2024-01-01", 100),
        ("2024-01-03", 50),
        ("2024-01-10", 30),
        ("2024-01-20", 150),
        ("2024-01-25", 80),
        ("2024-02-01", 170),
        ("2024-02-02", None),
        (None, 900),
    }


def test_row_filter_changes_snapshot_candidates_before_window(layer):
    assert result(
        layer,
        metrics=["snapshots.closing", "snapshots.opening", "snapshots.activity"],
        filters=["snapshots.day < '2024-02-01'"],
    ) == [(260, 180, 18)]


def test_metric_filter_does_not_pick_an_earlier_eligible_snapshot(layer):
    # A's latest January row is ineligible; its earlier 100 must not replace it.
    assert result(
        layer,
        metrics=["snapshots.closing", "snapshots.eligible_closing", "snapshots.activity"],
        filters=["snapshots.day < '2024-02-01'"],
    ) == [(260, 110, 18)]


def test_policy_changes_snapshot_candidates_before_window(layer):
    assert result(
        layer, metrics=["snapshots.closing", "snapshots.opening", "snapshots.activity"], user_attributes={"tenant": 2}
    ) == [(999, 999, 100)]


def test_snapshot_policy_requires_context(layer):
    with pytest.raises(SecurityError):
        layer.compile(metrics=["snapshots.closing"], use_preaggregations=False)


@pytest.mark.parametrize("granularity,expected", [("day", 430), (None, 260)])
def test_intraday_snapshot_uses_declared_dimension_grain(layer, granularity, expected):
    layer.graph.get_model("snapshots").get_dimension("day").granularity = granularity
    layer.adapter.execute("alter table snapshot_rows alter column day type timestamp")
    layer.adapter.execute("insert into snapshot_rows values (10, 'A', 'east', '2024-02-01 15:00:00', 180, 9, true, 1)")
    # Day grain preserves both of A's latest-day values (170+180), while a raw
    # timestamp selects only 180. B contributes 80; C/D contribute no value.
    assert result(layer, metrics=["snapshots.closing"]) == [(expected,)]


def test_snapshot_count_and_distinct_count_mask_rows_not_entities(layer):
    metrics = ["snapshots.closing_rows", "snapshots.closing_accounts", "snapshots.row_count"]
    # C's latest NULL balance still counts as a row; D's NULL date cannot match MAX.
    assert result(layer, metrics=metrics) == [(3, 3, 8)]
    layer.adapter.execute("insert into snapshot_rows values (10, 'B', 'east', '2024-01-25', 5, 9, true, 1)")
    # A tied latest row increases COUNT but not COUNT(DISTINCT account).
    assert result(layer, metrics=metrics) == [(4, 3, 9)]
