"""Tests for semi-additive (non_additive_dimension) metric handling.

A measure with ``non_additive_dimension`` must be aggregated over only the rows at
the last (or first) value of that time dimension per group -- e.g. an account-balance
snapshot summed across accounts but NOT double-counted across days. The generator
implements this by injecting a ``QUALIFY`` into the owning model's CTE for QUALIFY
dialects (DuckDB, Snowflake, BigQuery, ...). It raises ``UnsupportedMetricError`` only
for cases it does not implement: a dialect without QUALIFY, or the combination of
semi-additive handling with fan-out symmetric aggregation on the same model.

``allow_non_additive_unsafe=True`` skips the rewrite entirely and aggregates naively
(over ALL snapshots, i.e. the old, incorrect behavior).
"""

import tempfile
from pathlib import Path

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import UnsupportedMetricError
from sidemantic.sql.generator import SQLGenerator


def _model(non_additive: bool, window: str = "max"):
    metrics = [
        Metric(name="revenue", agg="sum", sql="amount"),
    ]
    if non_additive:
        # A daily account balance snapshot is not additive across time: summing
        # a balance across days double-counts it.
        metrics.append(
            Metric(
                name="balance",
                agg="sum",
                sql="balance",
                non_additive_dimension="snapshot_date",
                non_additive_window=window,
            )
        )
    else:
        metrics.append(Metric(name="balance", agg="sum", sql="balance"))
    return Model(
        name="accounts",
        table="accounts",
        primary_key="id",
        dimensions=[
            Dimension(name="snapshot_date", type="time", granularity="day", sql="snapshot_date"),
            Dimension(name="account_id", type="categorical", sql="account_id"),
            Dimension(name="region", type="categorical", sql="region"),
        ],
        metrics=metrics,
    )


def _seed(layer: SemanticLayer):
    """Daily per-account balance snapshots.

    Naive SUM(balance) double-counts every day; the correct semi-additive SUM keeps
    only the last snapshot per account. The two give different totals by construction.

        account A (east): 2024-01-01 -> 100, 2024-01-02 -> 150   (last = 150)
        account B (east): 2024-01-01 ->  50, 2024-01-02 ->  70   (last =  70)
        account C (west): 2024-01-01 ->  10, 2024-01-03 ->  33   (last =  33)

    naive total  = 100+150+50+70+10+33 = 413
    semi total   = 150+70+33           = 253
    by region    east = 220, west = 33   (naive east 370, west 43)
    """
    layer.adapter.execute(
        """
        CREATE TABLE accounts (
            id INTEGER, account_id VARCHAR, region VARCHAR,
            snapshot_date DATE, balance DOUBLE, amount DOUBLE
        );
        INSERT INTO accounts VALUES
            (1, 'A', 'east', DATE '2024-01-01', 100, 1),
            (2, 'A', 'east', DATE '2024-01-02', 150, 1),
            (3, 'B', 'east', DATE '2024-01-01',  50, 1),
            (4, 'B', 'east', DATE '2024-01-02',  70, 1),
            (5, 'C', 'west', DATE '2024-01-01',  10, 1),
            (6, 'C', 'west', DATE '2024-01-03',  33, 1);
        """
    )


def test_semi_additive_value_is_last_snapshot():
    """The crux: correct semi-additive SUM != naive SUM, verified against DuckDB."""
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=True))
    _seed(layer)

    # Semi-additive: sum of the last snapshot per account (global last-date window
    # collapses to the single latest snapshot when no other grouping is requested).
    sql = layer.compile(metrics=["accounts.balance"])
    assert "QUALIFY" in sql

    # Grouped by account: last balance per account, summed -> 150 + 70 + 33 = 253.
    rows = layer.query(
        metrics=["accounts.balance"], dimensions=["accounts.account_id"], order_by=["accounts.account_id"]
    ).fetchall()
    assert rows == [("A", 150.0), ("B", 70.0), ("C", 33.0)]
    assert sum(r[1] for r in rows) == 253.0

    # Naive (a plain additive sum of the same column) double-counts across days = 413.
    naive_model = _model(non_additive=False)
    naive_layer = SemanticLayer()
    naive_layer.add_model(naive_model)
    _seed(naive_layer)
    naive_rows = naive_layer.query(
        metrics=["accounts.balance"], dimensions=["accounts.account_id"], order_by=["accounts.account_id"]
    ).fetchall()
    assert naive_rows == [("A", 250.0), ("B", 120.0), ("C", 43.0)]
    assert sum(r[1] for r in naive_rows) == 413.0


def test_semi_additive_grouped_by_other_dimension():
    """Partition is the query's non-time grouping dimensions (here: region)."""
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=True))
    _seed(layer)

    sql = layer.compile(metrics=["accounts.balance"], dimensions=["accounts.region"])
    assert "PARTITION BY" in sql

    rows = layer.query(
        metrics=["accounts.balance"], dimensions=["accounts.region"], order_by=["accounts.region"]
    ).fetchall()
    # east: last of A (150) + last of B (70) = 220; west: last of C (33) = 33.
    assert rows == [("east", 220.0), ("west", 33.0)]


def test_semi_additive_min_window():
    """window='min' keeps the FIRST snapshot per group."""
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=True, window="min"))
    _seed(layer)

    rows = layer.query(
        metrics=["accounts.balance"], dimensions=["accounts.account_id"], order_by=["accounts.account_id"]
    ).fetchall()
    # First snapshot per account: A -> 100, B -> 50, C -> 10.
    assert rows == [("A", 100.0), ("B", 50.0), ("C", 10.0)]


def test_grouping_by_non_additive_dimension_is_additive():
    """Grouping by the non-additive dim itself is a no-op: each bucket is additive."""
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=True))
    _seed(layer)

    # No QUALIFY should be emitted -- each snapshot_date bucket stands on its own.
    sql = layer.compile(metrics=["accounts.balance"], dimensions=["accounts.snapshot_date"])
    assert "QUALIFY" not in sql

    rows = layer.query(
        metrics=["accounts.balance"], dimensions=["accounts.snapshot_date"], order_by=["accounts.snapshot_date"]
    ).fetchall()
    # Per-day totals: Jan 1 = 100+50+10 = 160; Jan 2 = 150+70 = 220; Jan 3 = 33.
    assert [r[1] for r in rows] == [160.0, 220.0, 33.0]


def test_escape_hatch_reverts_to_naive():
    """allow_non_additive_unsafe skips the semi-additive rewrite (naive aggregation)."""
    layer = SemanticLayer(allow_non_additive_unsafe=True)
    layer.add_model(_model(non_additive=True))
    _seed(layer)

    sql = layer.compile(metrics=["accounts.balance"], dimensions=["accounts.account_id"])
    # No QUALIFY -> aggregates naively over all snapshots (over-counted, unsafe).
    assert "QUALIFY" not in sql
    rows = layer.query(
        metrics=["accounts.balance"], dimensions=["accounts.account_id"], order_by=["accounts.account_id"]
    ).fetchall()
    assert rows == [("A", 250.0), ("B", 120.0), ("C", 43.0)]


def test_query_without_non_additive_metric_unaffected():
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=True))
    # revenue has no non_additive_dimension, so querying it alone must not raise.
    sql = layer.compile(metrics=["accounts.revenue"], dimensions=["accounts.region"])
    assert "accounts" in sql.lower()
    assert "QUALIFY" not in sql


def test_model_without_non_additive_metric_unaffected():
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=False))
    sql = layer.compile(metrics=["accounts.balance"], dimensions=["accounts.region"])
    assert "accounts" in sql.lower()
    assert "QUALIFY" not in sql


def test_non_qualify_dialect_raises_cleanly():
    """A dialect without QUALIFY (e.g. postgres) is rejected, not served wrong SQL."""
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=True))
    gen = SQLGenerator(layer.graph, dialect="postgres")
    with pytest.raises(UnsupportedMetricError) as exc:
        gen.generate(metrics=["accounts.balance"], dimensions=["accounts.region"])
    msg = str(exc.value)
    assert "postgres" in msg
    assert "QUALIFY" in msg
    assert "allow_non_additive_unsafe" in msg


def test_semi_additive_plus_fanout_symmetric_aggregate_raises():
    """Semi-additive combined with fan-out symmetric aggregation is unsupported.

    The two rewrites operate at different grains and do not compose: semi-additive
    filters rows before the join (INNER, pre-fan-out), while symmetric aggregation
    deduplicates after it (OUTER). Composing them would filter on a post-fan-out max
    and silently interfere, so the generator refuses rather than emit wrong SQL.
    """
    layer = SemanticLayer()
    layer.add_model(
        Model(
            name="accounts",
            table="acc",
            primary_key="account_id",
            dimensions=[
                Dimension(name="snapshot_date", type="time", granularity="day", sql="snapshot_date"),
                Dimension(name="region", type="categorical", sql="region"),
            ],
            metrics=[
                Metric(
                    name="balance",
                    agg="sum",
                    sql="balance",
                    non_additive_dimension="snapshot_date",
                )
            ],
            relationships=[Relationship(name="transactions", type="one_to_many", foreign_key="account_id")],
        )
    )
    layer.add_model(
        Model(
            name="transactions",
            table="txn",
            primary_key="txn_id",
            dimensions=[Dimension(name="account_id", type="categorical", sql="account_id")],
            metrics=[Metric(name="amount", agg="sum", sql="amt")],
            relationships=[Relationship(name="accounts", type="many_to_one", foreign_key="account_id")],
        )
    )
    with pytest.raises(UnsupportedMetricError) as exc:
        layer.compile(metrics=["accounts.balance", "transactions.amount"], dimensions=["accounts.region"])
    msg = str(exc.value).lower()
    assert "symmetric" in msg or "fan-out" in msg
    assert "compose" in msg


def test_adapter_round_trips_non_additive_dimension():
    graph_layer = SemanticLayer()
    graph_layer.add_model(_model(non_additive=True))

    adapter = SidemanticAdapter()
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "models.yml"
        adapter.export(graph_layer.graph, out)
        reparsed = adapter.parse(out)

    metric = reparsed.models["accounts"].get_metric("balance")
    assert metric is not None
    assert metric.non_additive_dimension == "snapshot_date"
