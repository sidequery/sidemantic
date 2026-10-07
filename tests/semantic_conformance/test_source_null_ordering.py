"""Source reconciliation preserves public NULL ordering before pagination."""

from pathlib import Path

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires real native extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="facts",
            table="facts",
            primary_key="id",
            dimensions=[Dimension(name="label", type="categorical")],
            metrics=[Metric(name="total", agg="sum", sql="amount")],
            relationships=[Relationship(name="items", type="one_to_many", foreign_key="fact_id")],
        )
    )
    layer.add_model(
        Model(
            name="items",
            table="items",
            primary_key="id",
            dimensions=[Dimension(name="label", type="categorical")],
            metrics=[Metric(name="value", agg="sum", sql="amount")],
        )
    )
    layer.adapter.execute("create table facts(id integer, label varchar, amount integer)")
    layer.adapter.execute("insert into facts values (1, NULL, 10), (2, 'a', NULL), (3, 'b', 30)")
    layer.adapter.execute("create table items(id integer, fact_id integer, label varchar, amount integer)")
    layer.adapter.execute(
        "insert into items values (1, 1, NULL, 40), (2, 1, NULL, 60), (3, 2, 'a', 200), (4, 3, 'b', 300)"
    )
    yield layer
    layer.adapter.close()


@pytest.mark.parametrize("plan", ["ordinary", "fanout", "multi_source"])
@pytest.mark.parametrize("field", ["dimension", "metric"])
@pytest.mark.parametrize("suffix", ["", " ASC", " DESC", " ASC NULLS LAST", " DESC NULLS FIRST"])
@pytest.mark.parametrize("page", [False, True])
def test_source_ordering_matches_independent_rows(layer, plan, field, suffix, page):
    dimension = "facts.label" if plan == "ordinary" else "items.label"
    metrics = ["facts.total", "items.value"] if plan == "multi_source" else ["facts.total"]
    rows = [(None, 10, 100), ("a", None, 200), ("b", 30, 300)]
    index = 0 if field == "dimension" else 1
    descending = suffix.strip().startswith("DESC")
    nulls_first = suffix.endswith("NULLS FIRST") or ("NULLS" not in suffix and not descending)
    nonnull = sorted((row for row in rows if row[index] is not None), key=lambda row: row[index], reverse=descending)
    null = [row for row in rows if row[index] is None]
    expected = null + nonnull if nulls_first else nonnull + null
    if plan != "multi_source":
        expected = [row[:2] for row in expected]
    if page:
        expected = expected[1:2]
    result = layer.query(
        metrics=metrics,
        dimensions=[dimension],
        order_by=[(dimension if field == "dimension" else "facts.total") + suffix],
        **({"limit": 1, "offset": 1} if page else {}),
    )
    assert [column[0] for column in result.description] == [
        "label",
        "total",
        *(["value"] if plan == "multi_source" else []),
    ]
    assert result.fetchall() == expected


def test_minimized_nullable_fanout_case():
    from tests.semantic_conformance.differential.harness import Case, DifferentialRunner

    pytest.importorskip("sidemantic_rs", reason="Requires real native extension")
    case = Case.read(Path(__file__).parent / "fixtures" / "source_null_ordering.json")
    runner = DifferentialRunner()
    try:
        outcome = runner.evaluate(case)
    finally:
        runner.close()
    assert outcome.failure_class is None, outcome
    for result in [outcome.python, outcome.rust]:
        assert result.columns == ["status", "label", "rows"]
        assert result.rows == [("paid", None, 1), ("paid", "x", 1)]


@pytest.mark.parametrize("dialect", ["tsql", "mysql"])
@pytest.mark.parametrize("suffix", [" ASC", " DESC", " ASC NULLS LAST", " DESC NULLS FIRST"])
def test_multi_source_ordering_renders_target_dialect(layer, dialect, suffix):
    import sqlglot

    sql = layer.compile(
        metrics=["facts.total", "items.value"],
        dimensions=["items.label"],
        order_by=["items.label" + suffix],
        dialect=dialect,
    )
    # Check the newly emitted clause independently of unrelated source-join
    # dialect capabilities. Neither SQL Server nor MySQL accepts NULLS syntax.
    order = sql.rsplit("ORDER BY", 1)[1].split("-- sidemantic:", 1)[0].strip()
    assert "NULLS FIRST" not in order and "NULLS LAST" not in order
    probe = sqlglot.parse_one("select label from facts order by " + order, read=dialect)
    actual = layer.adapter.execute(probe.sql(dialect="duckdb")).fetchall()
    descending = suffix.strip().startswith("DESC")
    nulls_first = suffix.endswith("NULLS FIRST") or ("NULLS" not in suffix and not descending)
    expected = [("b",), ("a",)] if descending else [("a",), ("b",)]
    expected.insert(0 if nulls_first else len(expected), (None,))
    assert actual == expected
