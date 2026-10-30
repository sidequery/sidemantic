"""Execute native and fallback TMDL translations against physical source data."""

import duckdb
import pytest

from sidemantic.adapters import tmdl
from sidemantic.sql.generator import SQLGenerator


@pytest.fixture(params=["native", "fallback"])
def parser_mode(request, monkeypatch):
    if request.param == "native":
        pytest.importorskip("sidemantic_dax")
    else:

        def unavailable(*args):
            raise tmdl.DaxRuntimeUnavailableError("Native parser disabled for fallback regression")

        monkeypatch.setattr(tmdl, "_parse_dax_expression", unavailable)
    return request.param


@pytest.mark.parametrize("physical_name", ["amount", "source amount", "select", "amount.raw", 'amount"quoted'])
@pytest.mark.parametrize("reference", ["sAlEs[rEvEnUe]", "[rEvEnUe]"])
def test_aggregations_use_physical_source_column(tmp_path, parser_mode, physical_name, reference):
    path = tmp_path / "Sales.tmdl"
    # TMDL property values may contain spaces, dots and double quotes verbatim.
    path.write_text(
        f"table Sales\n"
        f"    column Revenue\n"
        f"        dataType: double\n"
        f"        sourceColumn: {physical_name}\n"
        f"    measure Total = SUM({reference})\n"
    )
    graph = tmdl.TMDLAdapter().parse(path)
    if parser_mode == "native":
        assert graph.import_warnings == []
    sql = SQLGenerator(graph).generate(metrics=["Sales.Total"])
    quoted = '"' + physical_name.replace('"', '""') + '"'
    with duckdb.connect() as conn:
        # A same-named logical column must not silently supply the wrong value.
        conn.execute(f"CREATE TABLE Sales AS SELECT 10 AS {quoted}, 999 AS Revenue")
        assert conn.execute(sql).fetchone() == (10,)
        conn.execute("DROP TABLE Sales")
        conn.execute(f"CREATE TABLE Sales AS SELECT 10 AS {quoted}")
        assert conn.execute(sql).fetchone() == (10,)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([1, 1, None], (2, 1)),
        ([None, None], (1, 0)),
        ([1, 1, 2], (2, 2)),
        ([], (None, 0)),
    ],
)
def test_distinctcount_preserves_blank_and_empty_input(tmp_path, parser_mode, values, expected):
    path = tmp_path / "Sales.tmdl"
    path.write_text(
        "table Sales\n"
        "    column Amount\n"
        "        dataType: double\n"
        "        sourceColumn: source_amount\n"
        "    measure Distinct = DISTINCTCOUNT(Sales[Amount])\n"
        "    measure Nonblank = DISTINCTCOUNTNOBLANK(Sales[Amount])\n"
    )
    graph = tmdl.TMDLAdapter().parse(path)
    if parser_mode == "native":
        assert graph.import_warnings == []
    metric = graph.models["Sales"].get_metric("Distinct")
    assert metric.sql_is_complete
    assert metric.agg is None
    sql = SQLGenerator(graph).generate(metrics=["Sales.Distinct", "Sales.Nonblank"])
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE Sales(source_amount DOUBLE)")
        if values:
            conn.executemany("INSERT INTO Sales VALUES (?)", [(value,) for value in values])
        assert conn.execute(sql).fetchone() == expected


def test_distinctcount_applies_to_each_group_and_preserves_dax_on_export(tmp_path, parser_mode):
    path = tmp_path / "Sales.tmdl"
    path.write_text(
        "table Sales\n"
        "    column Amount\n"
        "        dataType: double\n"
        "    column segment\n"
        "        dataType: string\n"
        "    measure Distinct = DISTINCTCOUNT([Amount])\n"
    )
    adapter = tmdl.TMDLAdapter()
    graph = adapter.parse(path)
    sql = SQLGenerator(graph).generate(metrics=["Sales.Distinct"], dimensions=["Sales.segment"])
    with duckdb.connect() as conn:
        conn.execute(
            "CREATE TABLE Sales AS SELECT * FROM (VALUES (1, 'a'), (NULL, 'a'), (NULL, 'b')) t(Amount, segment)"
        )
        assert sorted(conn.execute(sql).fetchall()) == [("a", 2), ("b", 1)]
    output = tmp_path / "exported"
    adapter.export(graph, output)
    assert "DISTINCTCOUNT([Amount])" in next(output.rglob("Sales.tmdl")).read_text()
