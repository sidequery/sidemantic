"""Guard tests for semi-additive (non_additive_dimension) metrics.

The SQL generator has no semi-additive handling. Querying a metric that declares
``non_additive_dimension`` would silently over-aggregate it (wrong results), so it
must raise ``UnsupportedMetricError`` unless the caller opts into the unsafe path.
"""

import tempfile
from pathlib import Path

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import UnsupportedMetricError


def _model(non_additive: bool):
    metrics = [
        Metric(name="revenue", agg="sum", sql="amount"),
    ]
    if non_additive:
        # Average balance is not additive across time: summing daily balances is wrong.
        metrics.append(
            Metric(
                name="balance",
                agg="sum",
                sql="balance",
                non_additive_dimension="snapshot_date",
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
            Dimension(name="region", type="categorical", sql="region"),
        ],
        metrics=metrics,
    )


def test_querying_non_additive_metric_raises():
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=True))
    with pytest.raises(UnsupportedMetricError) as exc:
        layer.compile(metrics=["accounts.balance"], dimensions=["accounts.region"])
    msg = str(exc.value)
    assert "accounts.balance" in msg
    assert "snapshot_date" in msg
    assert "allow_non_additive_unsafe" in msg


def test_escape_hatch_allows_query():
    layer = SemanticLayer(allow_non_additive_unsafe=True)
    layer.add_model(_model(non_additive=True))
    # Should compile without raising; behaves as before (over-aggregated, unsafe).
    sql = layer.compile(metrics=["accounts.balance"], dimensions=["accounts.region"])
    assert "accounts" in sql.lower()


def test_query_without_non_additive_metric_unaffected():
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=True))
    # revenue has no non_additive_dimension, so querying it alone must not raise.
    sql = layer.compile(metrics=["accounts.revenue"], dimensions=["accounts.region"])
    assert "accounts" in sql.lower()


def test_model_without_non_additive_metric_unaffected():
    layer = SemanticLayer()
    layer.add_model(_model(non_additive=False))
    sql = layer.compile(metrics=["accounts.balance"], dimensions=["accounts.region"])
    assert "accounts" in sql.lower()


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
