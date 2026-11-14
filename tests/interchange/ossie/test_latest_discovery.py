"""Public discovery, persistent view recipes, and strict CLI query routes."""

import json

import duckdb
import pytest
from typer.testing import CliRunner

from sidemantic import SemanticLayer
from sidemantic.cli import app
from sidemantic.interchange.ossie import lower_ossie_document, parse_ossie_document
from tests.interchange.ossie.test_latest_query_interface import FIXTURES, ROUTES, _execute


@pytest.mark.parametrize("route", ROUTES)
def test_describe_marks_measures_and_keeps_scalar_fields(route):
    rows = _execute(route, "DESCRIBE scott_model.emp")
    assert [row[0] for row in rows][-4:] == ["sum_sal", "emp_count", "comm_count", "sum_comm"]
    assert not any(row[0].startswith("__ossie") for row in rows)
    columns = {row[0]: row[1] for row in rows}
    assert columns["empno"] == "INTEGER"
    assert columns["sum_sal"] == "DECIMAL MEASURE"
    assert columns["emp_count"] == "INTEGER MEASURE"


@pytest.mark.parametrize("route", ROUTES)
def test_information_schema_preserves_physical_catalog_and_adds_measures(route):
    query = """SELECT table_schema, table_name, column_name, data_type
    FROM information_schema.columns
    WHERE (table_schema='scott_model' AND table_name='emp' AND column_name='sum_sal')
       OR (table_schema='scott' AND table_name='emp' AND column_name='empno')
    ORDER BY table_schema"""
    assert _execute(route, query) == [
        ("scott", "emp", "empno", "INTEGER"),
        ("scott_model", "emp", "sum_sal", "DECIMAL MEASURE"),
    ]
    assert _execute(
        route, "SELECT table_name FROM information_schema.tables WHERE table_schema='scott_model' ORDER BY table_name"
    ) == [("dept",), ("emp",), ("salgrade",)]


def _layer(engine):
    parsed = parse_ossie_document((FIXTURES / "scott/scott_model.ossie.yaml").read_bytes())
    lowered = lower_ossie_document(parsed, target_dialect="duckdb")
    layer = SemanticLayer.from_catalog(lowered.catalog, engine=engine, fallback=False, auto_register=False)
    layer.adapter.execute((FIXTURES / "scott/scott.sql").read_text())
    return layer


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_view_filters_and_renames_preserve_recipes_and_invalidate_cached_queries(engine):
    layer = _layer(engine)
    try:
        layer.sql("CREATE VIEW salary_view AS SELECT deptno, sum_sal AS pay FROM scott_model.emp WHERE sal>1000")
        query = "SELECT AGG(pay) FROM salary_view"
        expected = layer.adapter.execute("SELECT SUM(sal) FROM scott.emp WHERE sal>1000").fetchall()
        assert layer.sql(query).fetchall() == expected
        layer.sql(
            "CREATE OR REPLACE VIEW salary_view AS SELECT deptno, sum_sal AS pay FROM scott_model.emp WHERE sal>2000"
        )
        expected = layer.adapter.execute("SELECT SUM(sal) FROM scott.emp WHERE sal>2000").fetchall()
        assert layer.sql(query).fetchall() == expected
        assert layer.last_engine_selection == {"engine": engine, "reason": None}
        assert dict((row[0], row[1]) for row in layer.sql("DESCRIBE salary_view").fetchall())["pay"].endswith("MEASURE")
        layer.sql("DROP VIEW salary_view")
        assert not layer.graph.metadata["ossie_views"]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("command", ["query", "rewrite"])
def test_strict_wide_cli(engine, command, tmp_path):
    database = tmp_path / "data.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute((FIXTURES / "scott/scott.sql").read_text())
    query = {"metrics": ["emp.sum_sal", "emp.emp_count"]}
    args = [
        command,
        "--ossie-wide",
        "--models",
        str(FIXTURES / "scott/scott_model.ossie.yaml"),
        "--engine",
        engine,
        "--no-fallback",
        "--connection",
        f"duckdb:///{database}",
    ]
    # Stdin exercises the same strict JSON contract.
    args.append("-")
    result = CliRunner().invoke(app, args, input=json.dumps(query))
    assert result.exit_code == 0, result.output
    if command == "rewrite":
        with duckdb.connect(str(database)) as connection:
            assert connection.execute(result.stdout).fetchall() == [(29025, 14)]
    else:
        assert "29025" in result.stdout
        assert ",14" in result.stdout


@pytest.mark.parametrize("route", ROUTES)
def test_describe_projected_query_preserves_measure_marker(route):
    rows = _execute(route, "DESCRIBE SELECT deptno, sum_sal AS pay FROM scott_model.emp WHERE sal>1000")
    assert [row[0] for row in rows] == ["deptno", "pay"]
    assert rows[1][1].endswith("MEASURE")


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_view_column_aliases_and_failed_create_do_not_replace_registry(engine):
    layer = _layer(engine)
    try:
        layer.sql("CREATE VIEW salary_view(department, pay) AS SELECT deptno, sum_sal FROM scott_model.emp")
        assert layer.sql("SELECT AGG(pay) FROM salary_view").fetchall() == [(29025,)]
        original = dict(layer.graph.metadata["ossie_views"])
        with pytest.raises(Exception, match="already exists"):
            layer.sql("CREATE VIEW salary_view AS SELECT sum_sal AS different FROM scott_model.emp WHERE sal>5000")
        assert layer.graph.metadata["ossie_views"] == original
        assert layer.sql("SELECT AGG(pay) FROM salary_view").fetchall() == [(29025,)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("indirect", [False, True])
def test_cyclic_view_replacement_is_rejected_before_ddl(engine, indirect):
    layer = _layer(engine)
    try:
        layer.sql("CREATE VIEW salaries AS SELECT sum_sal AS pay FROM scott_model.emp")
        source = "salaries"
        if indirect:
            layer.sql("CREATE VIEW salaries_copy AS SELECT pay FROM salaries")
            source = "salaries_copy"
        registry = dict(layer.graph.metadata["ossie_views"])
        version = layer.graph._version
        with pytest.raises(ValueError, match="(?i)recursive|cyclic|cycle"):
            layer.sql(f"CREATE OR REPLACE VIEW salaries AS SELECT pay FROM {source}")
        assert layer.graph.metadata["ossie_views"] == registry
        assert layer.graph._version == version
        assert layer.sql("SELECT AGG(pay) FROM salaries").fetchall() == [(29025,)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("registered", [False, True])
def test_create_view_if_not_exists_keeps_existing_recipe(engine, registered):
    layer = _layer(engine)
    try:
        if registered:
            layer.sql("CREATE VIEW salaries AS SELECT sum_sal AS pay FROM scott_model.emp")
        else:
            layer.adapter.execute("CREATE VIEW salaries AS SELECT 7 AS pay")
        registry = dict(layer.graph.metadata.get("ossie_views", {}))
        layer.sql("CREATE VIEW IF NOT EXISTS salaries AS SELECT sum_sal AS pay FROM scott_model.emp WHERE sal>5000")
        assert layer.graph.metadata.get("ossie_views", {}) == registry
        if registered:
            assert layer.sql("SELECT AGG(pay) FROM salaries").fetchall() == [(29025,)]
        else:
            assert layer.adapter.execute("SELECT pay FROM salaries").fetchall() == [(7,)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_view_metadata_does_not_duplicate_physical_placeholder_columns(engine):
    layer = _layer(engine)
    try:
        layer.sql("CREATE VIEW salaries AS SELECT deptno, sum_sal AS pay FROM scott_model.emp")
        result = layer.sql(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog=current_catalog AND table_schema='main' AND table_name='salaries' ORDER BY ordinal_position"
        ).fetchall()
        assert [row[0] for row in result] == ["deptno", "pay"]
        assert result[1][1].endswith("MEASURE")
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_zero_row_projection_exposes_measure_type_to_driver(engine):
    layer = _layer(engine)
    try:
        result = layer.sql("SELECT deptno, sum_sal AS pay FROM scott_model.emp LIMIT 0")
        assert result.fetchall() == []
        assert [column[0] for column in result.description] == ["deptno", "pay"]
        assert "MEASURE" in str(result.description[1][1]).upper()
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_delimited_view_name_is_distinct_from_schema_qualification(engine):
    layer = _layer(engine)
    try:
        layer.adapter.execute("CREATE SCHEMA v")
        layer.sql('CREATE VIEW "v.salaries" AS SELECT sum_sal AS pay FROM scott_model.emp WHERE sal>1000')
        layer.sql("CREATE VIEW v.salaries AS SELECT sum_sal AS pay FROM scott_model.emp WHERE sal>2000")
        assert len(layer.graph.metadata["ossie_views"]) == 2
        for name, minimum in [('"v.salaries"', 1000), ("v.salaries", 2000)]:
            expected = layer.adapter.execute(f"SELECT SUM(sal) FROM scott.emp WHERE sal>{minimum}").fetchall()
            assert layer.sql(f"SELECT AGG(pay) FROM {name}").fetchall() == expected
        layer.sql('DROP VIEW "v.salaries"')
        assert len(layer.graph.metadata["ossie_views"]) == 1
        assert (
            layer.sql("SELECT AGG(pay) FROM v.salaries").fetchall()
            == layer.adapter.execute("SELECT SUM(sal) FROM scott.emp WHERE sal>2000").fetchall()
        )
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_ordinary_physical_view_remains_a_database_view(engine):
    layer = _layer(engine)
    try:
        layer.sql("CREATE VIEW plain_view AS SELECT * FROM scott.emp")
        assert layer.sql("SELECT COUNT(*) FROM plain_view").fetchall() == [(14,)]
        assert "plain_view" not in layer.graph.metadata.get("ossie_views", {})
        layer.sql("CREATE OR REPLACE VIEW plain_view AS SELECT sum_sal AS pay FROM scott_model.emp")
        assert len(layer.graph.metadata["ossie_views"]) == 1
        layer.sql("CREATE OR REPLACE VIEW plain_view AS SELECT * FROM scott.emp")
        assert not layer.graph.metadata["ossie_views"]
        assert layer.sql("SELECT COUNT(*) FROM plain_view").fetchall() == [(14,)]
    finally:
        layer.adapter.close()
