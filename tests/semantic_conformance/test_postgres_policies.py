"""Execute policy SQL from the built Rust extension on PostgreSQL, without fallback."""

import os

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.rust_bridge import compile_semantic_input, rewrite_semantic_input
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError


@pytest.fixture(scope="module")
def postgres():
    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL policy execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    # A configured acceptance job must fail, not skip, if its driver or matching
    # extension is missing. Ordinary local Python suites need neither dependency.
    import psycopg
    import sidemantic_rs

    assert callable(sidemantic_rs.rewrite_with_semantic_input_context)
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute("set standard_conforming_strings = on")
        connection.execute(
            "create temporary table policy_population "
            "(id integer, tenant integer, subject text, enabled boolean, amount integer, discarded boolean)"
        )
        with connection.cursor() as cursor:
            cursor.executemany(
                "insert into policy_population values (%s, %s, %s, %s, %s, %s)",
                [
                    (1, 1, "alice", True, 10, False),
                    (2, 1, "alice", True, 90, True),
                    (3, 2, "alice", True, 400, False),
                    (4, 1, "O'Brien", True, 7, False),
                    (5, 1, "x' OR '1'='1", True, 11, False),
                    (6, 1, "back\\slash' OR TRUE --", True, 13, False),
                    (7, 1, None, False, 17, False),
                    (8, 1, "alice", False, 50, False),
                ],
            )
        yield connection


@pytest.fixture
def graph():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="secured",
            table="policy_population",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric"), Dimension(name="subject", public=False)],
            metrics=[
                Metric(name="total", agg="sum", sql="amount"),
                Metric(name="private_total", agg="sum", sql="amount", public=False),
            ],
            invariant_filters=["discarded = false"],
            security=SecurityPolicy(
                access="user.role == 'analyst'",
                row_filters=[
                    "tenant = {{ user.tenant }}",
                    "enabled = {{ user.enabled }}",
                    "subject IS NOT DISTINCT FROM {{ user.subject }}",
                ],
            ),
        )
    )
    return graph


def policy_sql(graph, mode, attributes, *, field="total", visibility=True, output="postgres"):
    if mode == "compile":
        return compile_semantic_input(
            graph,
            {
                "metrics": [f"secured.{field}"],
                "user_attributes": attributes,
                "enforce_visibility": visibility,
                "dialect": output,
            },
        )
    return rewrite_semantic_input(
        graph,
        f"select secured.{field} from metrics",
        user_attributes=attributes,
        enforce_visibility=visibility,
        output_dialect=output,
    )


@pytest.mark.parametrize("mode", ["compile", "rewrite"])
@pytest.mark.parametrize(
    "subject,enabled,expected",
    [
        ("alice", True, 10),
        ("O'Brien", True, 7),
        ("x' OR '1'='1", True, 11),
        ("back\\slash' OR TRUE --", True, 13),
        (None, False, 17),
    ],
)
def test_postgres_preserves_typed_policy_literals_and_exact_population(
    postgres, graph, mode, subject, enabled, expected
):
    sql = policy_sql(graph, mode, {"role": "analyst", "tenant": 1, "subject": subject, "enabled": enabled})
    assert postgres.execute(sql).fetchall() == [(expected,)]
    assert postgres.execute("select count(*) from policy_population").fetchone() == (8,)


@pytest.mark.parametrize("mode", ["compile", "rewrite"])
@pytest.mark.parametrize(
    "attributes",
    [None, {}, {"role": "viewer"}, {"role": "analyst", "tenant": [1, 2], "enabled": True, "subject": "alice"}],
)
def test_postgres_policy_denial_produces_no_sql(postgres, graph, mode, attributes):
    with pytest.raises(SecurityError):
        policy_sql(graph, mode, attributes)


@pytest.mark.parametrize("mode", ["compile", "rewrite"])
def test_postgres_visibility_is_enforced_only_when_requested(postgres, graph, mode):
    attributes = {"role": "analyst", "tenant": 1, "enabled": True, "subject": "alice"}
    with pytest.raises(SecurityError):
        policy_sql(graph, mode, attributes, field="private_total")
    sql = policy_sql(graph, mode, attributes, field="private_total", visibility=False)
    assert postgres.execute(sql).fetchall() == [(10,)]


@pytest.mark.parametrize("mode", ["compile", "rewrite"])
def test_other_policy_output_dialects_remain_unsupported(postgres, graph, mode):
    attributes = {"role": "analyst", "tenant": 1, "enabled": True, "subject": "alice"}
    with pytest.raises(UnsupportedSemanticFeaturesError):
        policy_sql(graph, mode, attributes, output="bigquery")


@pytest.mark.parametrize("mode", ["compile", "rewrite"])
def test_postgres_translates_date_diff_in_policy_and_invariant(postgres, graph, mode):
    postgres.execute("drop table if exists pg_policy_dates")
    postgres.execute(
        "create temporary table pg_policy_dates (id integer, tenant integer, amount integer, occurred date)"
    )
    postgres.execute(
        "insert into pg_policy_dates values "
        "(1, 1, 400, '2024-12-31'), (2, 1, 10, '2025-01-01'), "
        "(3, 1, 20, '2025-01-02'), (4, 1, 90, '2025-01-03'), (5, 2, 1000, '2025-01-01')"
    )
    model = graph.models["secured"]
    model.table = "pg_policy_dates"
    model.invariant_filters = ["date_diff('day', DATE '2025-01-01', occurred) >= 0"]
    model.security.row_filters = [
        "tenant = {{ user.tenant }}",
        "date_diff('day', DATE '2025-01-01', occurred) <= {{ user.days }}",
    ]
    sql = policy_sql(graph, mode, {"role": "analyst", "tenant": 1, "days": 1})
    assert "DATE_DIFF(" not in sql.upper(), sql
    assert "EXTRACT" in sql.upper(), sql
    assert postgres.execute(sql).fetchall() == [(30,)]
