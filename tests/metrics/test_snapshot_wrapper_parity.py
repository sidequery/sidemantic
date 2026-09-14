"""Execute snapshot calculations through both compiler entrypoints."""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("window, expected", [("min", 20), ("max", 30)])
def test_snapshot_wrappers_preserve_leaf_populations(engine, window, expected):
    if engine == "rust":
        pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    layer.adapter.execute("create table snapshots (id integer, account varchar, day date, amount integer)")
    layer.adapter.execute("insert into snapshots values (1, 'a', '2026-01-01', 10), (2, 'a', '2026-01-02', 15)")
    layer.add_model(
        Model(
            name="snapshots",
            table="snapshots",
            primary_key="id",
            dimensions=[
                Dimension(name="account", type="categorical"),
                Dimension(name="day", type="time", granularity="day"),
            ],
            metrics=[
                Metric(
                    name="balance",
                    agg="sum",
                    sql="amount",
                    non_additive_dimension="day",
                    non_additive_window=window,
                ),
                Metric(name="activity", agg="sum", sql="amount"),
                Metric(name="twice_balance", type="derived", sql="balance * 2"),
                Metric(name="share", type="ratio", numerator="balance", denominator="activity"),
            ],
        )
    )
    layer.add_metric(Metric(name="wrapped", type="derived", sql="snapshots.twice_balance"))
    layer.add_metric(Metric(name="nested", type="derived", sql="wrapped + snapshots.activity"))
    rows = layer.query(
        metrics=["wrapped", "nested", "snapshots.share", "snapshots.activity"],
        dimensions=["snapshots.account"],
        order_by=["wrapped desc"],
        limit=1,
    ).fetchall()
    assert rows == [("a", expected, expected + 25, expected / 2 / 25, 25)]


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_snapshot_wrapper_defaults_apply_after_leaf_selection(engine):
    if engine == "rust":
        pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    layer.adapter.execute("create table snapshots (id integer, day date, amount integer)")
    layer.adapter.execute("insert into snapshots values (1, '2026-01-01', 10), (2, '2026-01-02', null)")
    layer.add_model(
        Model(
            name="snapshots",
            table="snapshots",
            primary_key="id",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[Metric(name="balance", agg="sum", sql="amount", non_additive_dimension="day", fill_nulls_with=3)],
        )
    )
    layer.add_metric(Metric(name="wrapped", type="derived", sql="snapshots.balance * 2", fill_nulls_with=99))
    assert layer.query(metrics=["wrapped"]).fetchall() == [(6,)]
