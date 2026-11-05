"""QUALIFY filters the windowed relation without dropping measure lineage."""

import duckdb
import pytest

from tests.interchange.ossie.test_latest_query_interface import FIXTURES, ROUTES, _execute


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT empno FROM scott_model.emp QUALIFY ROW_NUMBER() OVER(ORDER BY empno)=1",
        "SELECT empno,ROW_NUMBER() OVER(ORDER BY empno) AS rn FROM scott_model.emp QUALIFY rn=1",
        "SELECT -empno AS empno,ROW_NUMBER() OVER(ORDER BY empno) AS rn FROM scott_model.emp QUALIFY empno>0 ORDER BY rn",
        "SELECT empno FROM scott_model.emp QUALIFY ROW_NUMBER() OVER(ORDER BY empno)<=3 ORDER BY empno DESC LIMIT 1",
        "SELECT DISTINCT deptno FROM scott_model.emp QUALIFY ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY empno)=1 ORDER BY deptno",
        "SELECT empno FROM scott_model.emp WHERE sal>1000 QUALIFY ROW_NUMBER() OVER(ORDER BY empno)=1",
    ],
)
def test_qualify_preserves_ordinary_window_filtering(route, query):
    with duckdb.connect() as connection:
        connection.execute((FIXTURES / "scott/scott.sql").read_text())
        expected = connection.execute(query.replace("scott_model.", "scott.")).fetchall()
    assert _execute(route, query) == expected


@pytest.mark.parametrize("route", ROUTES)
def test_qualify_preserves_measure_lineage_through_cte(route):
    query = """WITH first_employee AS (
        SELECT sum_sal, emp_count FROM scott_model.emp
        QUALIFY ROW_NUMBER() OVER(ORDER BY empno)=1
    ) SELECT AGG(sum_sal),AGG(emp_count) FROM first_employee"""
    assert _execute(route, query) == [(800, 1)]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT deptno,AGG(sum_sal) FROM scott_model.emp GROUP BY deptno QUALIFY ROW_NUMBER() OVER(ORDER BY deptno)=1",
        "SELECT empno FROM scott_model.emp WINDOW w AS(ORDER BY empno) QUALIFY ROW_NUMBER() OVER w=1",
    ],
)
def test_unimplemented_qualify_shapes_fail_explicitly(route, query):
    with pytest.raises(ValueError, match="QUALIFY"):
        _execute(route, query)


@pytest.mark.parametrize("route", ROUTES)
def test_qualify_cannot_treat_measure_alias_as_scalar(route):
    query = """SELECT sum_sal AS m FROM scott_model.emp
        QUALIFY m>0 AND ROW_NUMBER() OVER(ORDER BY empno)=1"""
    with pytest.raises(ValueError, match="measure"):
        _execute(route, query)
