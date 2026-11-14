"""Execute count round trips and reject unsupported SQL before emitting a project."""

import duckdb
import pytest

from sidemantic.adapters.tmdl import TMDLAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


def graph_with_metrics(metrics):
    graph = SemanticGraph()
    graph.add_model(
        Model(name="Sales", table="Sales", dimensions=[Dimension(name="flag", type="boolean")], metrics=metrics)
    )
    return graph


@pytest.mark.parametrize(
    "values,expected", [([True, False, None, True], (3, 2, 4)), ([None, None], (0, 0, 2)), ([], (0, 0, 0))]
)
def test_exported_counts_preserve_boolean_null_and_empty_semantics(tmp_path, values, expected):
    graph = graph_with_metrics(
        [
            Metric(name="NonNull", agg="count", sql="flag"),
            Metric(name="Unique", agg="count_distinct", sql="flag"),
            Metric(name="Rows", agg="count", sql="*"),
        ]
    )
    adapter = TMDLAdapter()
    destination = tmp_path / "counts.tmdl"
    adapter.export(graph, destination)
    content = destination.read_text()
    assert "COALESCE(COUNTA(Sales[flag]), 0)" in content
    assert "COALESCE(DISTINCTCOUNTNOBLANK(Sales[flag]), 0)" in content
    assert "COALESCE(COUNTROWS(Sales), 0)" in content
    # The native DAX parser validates generated syntax; execute its imported
    # semantics against NULL, Boolean and empty populations (no live engine).
    imported = adapter.parse(destination)
    assert imported.import_warnings == []
    sql = SQLGenerator(imported).generate(metrics=["Sales.NonNull", "Sales.Unique", "Sales.Rows"])
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE Sales(flag BOOLEAN)")
        if values:
            conn.executemany("INSERT INTO Sales VALUES (?)", [(value,) for value in values])
        assert conn.execute(sql).fetchone() == expected


@pytest.mark.parametrize(
    "metric",
    [
        Metric(name="Derived", type="derived", sql="COALESCE(SUM(amount), 0)"),
        Metric(name="Expression", agg="sum", sql="amount * quantity"),
        Metric(name="Foreign", agg="sum", sql="other.amount"),
        Metric(name="Approximate", agg="approx_count_distinct", sql="flag"),
        Metric(name="Filtered", agg="sum", sql="flag", filters=["flag IS NOT NULL"]),
        Metric(name="Filled", agg="sum", sql="flag", fill_nulls_with=0),
        Metric(name="Snapshot", agg="sum", sql="flag", non_additive_dimension="date"),
        Metric(name="Running", type="cumulative", agg="sum", sql="flag"),
    ],
)
def test_untranslated_sql_and_modifiers_fail_before_writing(tmp_path, metric):
    destination = tmp_path / "project"
    with pytest.raises(ValueError, match="TMDL export cannot"):
        TMDLAdapter().export(graph_with_metrics([metric]), destination)
    assert not destination.exists()


def test_preserved_dax_requires_unchanged_sql_and_explicit_dax_edits_win(tmp_path):
    source = tmp_path / "native.tmdl"
    source.write_text(
        "table Sales\n    column flag\n        dataType: boolean\n    measure Unique = DISTINCTCOUNT(Sales[flag])\n"
    )
    adapter = TMDLAdapter()
    graph = adapter.parse(source)
    metric = graph.models["Sales"].get_metric("Unique")
    destination = tmp_path / "output.tmdl"
    adapter.export(graph, destination)
    assert "DISTINCTCOUNT(Sales[flag])" in destination.read_text()
    metric.sql = "other_column"
    with pytest.raises(ValueError, match="edited SQL semantics"):
        adapter.export(graph, destination)
    metric.dax = "COUNTROWS(Sales)"
    adapter.export(graph, destination)
    assert "measure Unique = COUNTROWS(Sales)" in destination.read_text()


def test_sql_column_resolves_to_dax_display_name(tmp_path):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="Sales",
            table="Sales",
            dimensions=[Dimension(name="Revenue", type="numeric", sql="amount")],
            metrics=[Metric(name="Total", agg="sum", sql="amount")],
        )
    )
    destination = tmp_path / "aliased.tmdl"
    adapter = TMDLAdapter()
    adapter.export(graph, destination)
    assert "SUM(Sales[Revenue])" in destination.read_text()
    imported = adapter.parse(destination)
    sql = SQLGenerator(imported).generate(metrics=["Sales.Total"])
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE Sales AS SELECT * FROM (VALUES (3), (5), (NULL)) t(amount)")
        assert conn.execute(sql).fetchone() == (8,)


def test_missing_column_is_not_exported_as_a_dangling_dax_reference(tmp_path):
    with pytest.raises(ValueError, match="cannot translate SQL"):
        TMDLAdapter().export(
            graph_with_metrics([Metric(name="Missing", agg="sum", sql="amount")]), tmp_path / "invalid.tmdl"
        )


def test_multiple_sql_fragments_are_rejected(tmp_path):
    with pytest.raises(ValueError, match="cannot translate SQL"):
        TMDLAdapter().export(
            graph_with_metrics([Metric(name="Extra", agg="sum", sql="flag; amount")]), tmp_path / "invalid.tmdl"
        )


def test_graph_level_metrics_are_not_silently_dropped(tmp_path):
    graph = graph_with_metrics([])
    graph.add_metric(Metric(name="Total", agg="count"))
    with pytest.raises(ValueError, match="graph-level metrics"):
        TMDLAdapter().export(graph, tmp_path / "out.tmdl")
