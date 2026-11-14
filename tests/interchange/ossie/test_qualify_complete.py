"""Window stages over scalar/grouped relations agree with independent SQL."""

import duckdb
import pytest

from tests.interchange.ossie.test_latest_query_interface import FIXTURES, ROUTES, _execute

CASES = [
    (
        "order_alias_precedence",
        "SELECT -deptno AS deptno,AGG(sum_sal) AS total FROM scott_model.emp GROUP BY deptno QUALIFY ROW_NUMBER() OVER(ORDER BY deptno)>0 ORDER BY deptno",
    ),
    (
        "dynamic_frame",
        "SELECT deptno,SUM(SUM(sal)) OVER w AS running FROM scott_model.emp GROUP BY deptno WINDOW w AS(ORDER BY deptno ROWS BETWEEN deptno PRECEDING AND CURRENT ROW) QUALIFY ROW_NUMBER() OVER(ORDER BY deptno)>0 ORDER BY deptno",
    ),
    (
        "grouped_measure",
        "SELECT deptno,AGG(sum_sal) AS total FROM scott_model.emp GROUP BY deptno QUALIFY ROW_NUMBER() OVER(ORDER BY AGG(sum_sal) DESC)=1",
    ),
    (
        "hidden_measure",
        "SELECT deptno FROM scott_model.emp GROUP BY deptno QUALIFY ROW_NUMBER() OVER(ORDER BY AGG(sum_sal) DESC)=1",
    ),
    (
        "window_alias_having",
        "SELECT deptno,AGG(sum_sal) AS total,RANK() OVER(ORDER BY AGG(sum_sal) DESC) AS rn FROM scott_model.emp GROUP BY deptno HAVING AGG(sum_sal)>5000 QUALIFY rn<=2 ORDER BY total DESC",
    ),
    (
        "group_ordinal",
        "SELECT deptno AS d,AGG(sum_sal) AS total FROM scott_model.emp GROUP BY 1 QUALIFY ROW_NUMBER() OVER(ORDER BY total DESC)<=2 ORDER BY d",
    ),
    (
        "group_alias",
        "SELECT deptno AS d,AGG(sum_sal) AS total FROM scott_model.emp GROUP BY d QUALIFY ROW_NUMBER() OVER(ORDER BY total DESC)<=2 ORDER BY d",
    ),
    (
        "group_all",
        "SELECT deptno,AGG(sum_sal) AS total FROM scott_model.emp GROUP BY ALL QUALIFY ROW_NUMBER() OVER(ORDER BY total DESC)<=2 ORDER BY deptno",
    ),
    (
        "grouped_window_sum",
        "SELECT deptno,AGG(sum_sal) AS total,SUM(AGG(sum_sal)) OVER(ORDER BY deptno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM scott_model.emp GROUP BY deptno QUALIFY running>9000 ORDER BY deptno",
    ),
    (
        "rollup_window",
        "SELECT deptno,AGG(sum_sal) AS total,SUM(AGG(sum_sal)) OVER() AS overall FROM scott_model.emp GROUP BY ROLLUP(deptno) QUALIFY ROW_NUMBER() OVER(ORDER BY AGG(sum_sal) DESC)<=2 ORDER BY total DESC",
    ),
    (
        "grouping_sets",
        "SELECT deptno,job,GROUPING_ID(deptno,job) AS gid,AGG(sum_sal) AS total FROM scott_model.emp GROUP BY GROUPING SETS((deptno),(job),()) QUALIFY ROW_NUMBER() OVER(PARTITION BY GROUPING_ID(deptno,job) ORDER BY AGG(sum_sal) DESC,deptno,job)=1 ORDER BY gid",
    ),
    (
        "global_nested_aggregate",
        "SELECT SUM(SUM(sal)) OVER() AS total FROM scott_model.emp QUALIFY ROW_NUMBER() OVER()=1",
    ),
    (
        "empty_global_aggregate",
        "SELECT SUM(COUNT(*)) OVER() AS total FROM scott_model.emp WHERE FALSE QUALIFY ROW_NUMBER() OVER()=1",
    ),
    (
        "grouped_qualify_before_limit",
        "SELECT DISTINCT deptno,AGG(sum_sal) AS total FROM scott_model.emp GROUP BY deptno QUALIFY ROW_NUMBER() OVER(ORDER BY total DESC)<=2 ORDER BY deptno DESC LIMIT 1",
    ),
    ("named_window", "SELECT empno FROM scott_model.emp WINDOW w AS(ORDER BY empno) QUALIFY ROW_NUMBER() OVER w=1"),
    (
        "inherited_window",
        "SELECT empno,deptno,ROW_NUMBER() OVER ranked AS rn FROM scott_model.emp WINDOW base AS(PARTITION BY deptno),ranked AS(base ORDER BY sal DESC,empno) QUALIFY rn=1 ORDER BY deptno",
    ),
    (
        "inherited_frame",
        "SELECT empno,deptno,SUM(sal) OVER framed AS running FROM scott_model.emp WINDOW base AS(PARTITION BY deptno),ranked AS(base ORDER BY empno),framed AS(ranked ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) QUALIFY running>4000 ORDER BY empno",
    ),
    (
        "inline_inherited_frame",
        "SELECT empno,SUM(sal) OVER(w ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running FROM scott_model.emp WINDOW w AS(ORDER BY empno) QUALIFY running>5000 ORDER BY empno",
    ),
    (
        "grouped_named_window",
        "SELECT deptno,AGG(sum_sal) AS total,ROW_NUMBER() OVER ranked AS rn FROM scott_model.emp GROUP BY deptno WINDOW ranked AS(ORDER BY AGG(sum_sal) DESC) QUALIFY rn<=2 ORDER BY total DESC",
    ),
    (
        "named_scopes",
        "WITH picked AS(SELECT empno,deptno,sal FROM scott_model.emp WINDOW w AS(PARTITION BY deptno ORDER BY empno) QUALIFY ROW_NUMBER() OVER w=1) SELECT empno FROM picked WINDOW w AS(ORDER BY sal DESC,empno) QUALIFY ROW_NUMBER() OVER w=1",
    ),
    (
        "source_alias_precedence",
        "SELECT -deptno AS deptno,AGG(sum_sal) AS total,ROW_NUMBER() OVER(ORDER BY AGG(sum_sal) DESC) AS rn FROM scott_model.emp GROUP BY deptno QUALIFY deptno>0 AND rn<=2 ORDER BY total DESC",
    ),
]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("case,query", CASES, ids=[case for case, _ in CASES])
def test_qualify_complete_matches_physical_sql(route, case, query):
    reference = query.replace("scott_model.emp", "scott.emp").replace("AGG(sum_sal)", "SUM(sal)")
    if case == "group_all":
        # DuckDB rejects the syntactic GROUP BY ALL + QUALIFY combination;
        # the explicit grouping list has identical relational semantics.
        reference = reference.replace("GROUP BY ALL", "GROUP BY deptno")
    if case == "inherited_frame":
        # DuckDB's multi-hop named-window expansion loses the grandparent's
        # partition. Write the fully inherited window in the physical oracle.
        reference = reference.replace(
            "OVER framed", "OVER(PARTITION BY deptno ORDER BY empno ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
        )
    with duckdb.connect() as connection:
        connection.execute((FIXTURES / "scott/scott.sql").read_text())
        expected = connection.execute(reference).fetchall()
    assert _execute(route, query) == expected


@pytest.mark.parametrize("route", ROUTES)
def test_named_qualify_keeps_hidden_measure_identity(route):
    query = """WITH picked AS(
        SELECT sum_sal,emp_count FROM scott_model.emp
        WINDOW w AS(PARTITION BY deptno ORDER BY sal DESC,empno)
        QUALIFY ROW_NUMBER() OVER w=1
    ) SELECT AGG(sum_sal),AGG(emp_count) FROM picked"""
    with duckdb.connect() as connection:
        connection.execute((FIXTURES / "scott/scott.sql").read_text())
        expected = connection.execute("""SELECT SUM(sal),COUNT(*) FROM(
            SELECT sal FROM scott.emp
            QUALIFY ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY sal DESC,empno)=1
        )""").fetchall()
    assert _execute(route, query) == expected


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT empno FROM scott_model.emp WINDOW a AS(b),b AS(a) QUALIFY ROW_NUMBER() OVER a=1",
        "SELECT empno FROM scott_model.emp QUALIFY ROW_NUMBER() OVER missing=1",
        "SELECT empno FROM scott_model.emp WINDOW a AS(ORDER BY empno),b AS(a ORDER BY sal) QUALIFY ROW_NUMBER() OVER b=1",
    ],
)
def test_invalid_window_definitions_are_rejected(route, query):
    with pytest.raises(ValueError, match="window"):
        _execute(route, query)
