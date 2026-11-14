"""Physical DESCRIBE queries retain database column expansion and type inference."""

import duckdb
import pytest

from tests.interchange.ossie.test_latest_query_interface import FIXTURES, ROUTES, _execute


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query",
    [
        "DESCRIBE SELECT * FROM scott.emp",
        "DESCRIBE SELECT empno, sal AS salary, hiredate FROM scott.emp",
        "DESCRIBE SELECT COUNT(*) AS employees, SUM(sal) AS salary FROM scott.emp",
        "DESCRIBE WITH physical AS (SELECT empno, sal FROM scott.emp) SELECT * FROM physical",
        "DESCRIBE SELECT * FROM (VALUES (1::BIGINT, '2026-11-05'::DATE)) AS physical(id, day)",
    ],
)
def test_physical_describe_uses_database_columns_and_types(route, query):
    with duckdb.connect() as connection:
        connection.execute((FIXTURES / "scott/scott.sql").read_text())
        expected = connection.execute(query).fetchall()
    assert _execute(route, query) == expected


@pytest.mark.parametrize("route", ROUTES)
def test_semantic_describe_still_marks_measure_projections(route):
    rows = _execute(route, "DESCRIBE SELECT deptno, sum_sal AS pay FROM scott_model.emp")
    assert [(row[0], row[1].upper()) for row in rows] == [("deptno", "INTEGER"), ("pay", "DECIMAL MEASURE")]
