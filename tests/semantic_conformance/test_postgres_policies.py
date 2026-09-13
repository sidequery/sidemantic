"""Execute policy SQL from the built Rust extension on PostgreSQL, without fallback."""

import os

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.db.postgres import PostgreSQLAdapter
from sidemantic.rust_bridge import compile_semantic_input, rewrite_semantic_input
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError
from sidemantic.sql.query_rewriter import QueryRewriter


@pytest.fixture(scope="module")
def postgres():
    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL policy execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    # A configured acceptance job must fail, not skip, if its driver or matching
    # extension is missing. Ordinary local Python suites need neither dependency.
    import sidemantic_rs

    assert callable(sidemantic_rs.rewrite_with_semantic_input_context)
    adapter = PostgreSQLAdapter.from_url(dsn)
    try:
        connection = adapter.raw_connection
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
        yield adapter
    finally:
        adapter.close()


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
def test_postgres_translates_year_in_policy_and_invariant(postgres, graph, mode):
    postgres.execute("drop table if exists pg_policy_dates")
    postgres.execute(
        "create temporary table pg_policy_dates (id integer, tenant integer, amount integer, occurred date)"
    )
    postgres.execute(
        "insert into pg_policy_dates values "
        "(1, 1, 400, '2024-12-31'), (2, 1, 10, '2025-01-01'), "
        "(3, 1, 20, '2025-01-02'), (4, 1, 90, '2026-01-03'), (5, 2, 1000, '2025-01-01'), (6, 1, 500, NULL)"
    )
    model = graph.models["secured"]
    model.table = "pg_policy_dates"
    model.invariant_filters = ["year(occurred) >= 2025"]
    model.security.row_filters = [
        "tenant = {{ user.tenant }}",
        "year(occurred) <= {{ user.year }}",
    ]
    sql = policy_sql(graph, mode, {"role": "analyst", "tenant": 1, "year": 2025})
    assert "YEAR(" not in sql.upper(), sql
    assert "EXTRACT" in sql.upper(), sql
    assert postgres.execute(sql).fetchall() == [(30,)]


@pytest.mark.parametrize("mode", ["compile", "rewrite"])
def test_postgres_date_diff_policy_is_not_claimed_equivalent(postgres, graph, mode):
    graph.models["secured"].invariant_filters = [
        "date_diff('day', TIMESTAMP '2025-01-01 23:59:00', TIMESTAMP '2025-01-02 00:01:00') = 1"
    ]
    with pytest.raises(UnsupportedSemanticFeaturesError, match="policy.unqualified_output_expression"):
        policy_sql(graph, mode, {"role": "analyst", "tenant": 1, "enabled": True, "subject": "alice"})


@pytest.mark.parametrize("mode", ["compile", "query", "sql", "rewriter"])
def test_postgres_public_paths_reach_rust_with_transport_syntax(postgres, graph, mode):
    graph.models["secured"].dimensions.append(Dimension(name="CaseSubject", sql="subject"))
    attributes = {"role": "analyst", "tenant": 1, "enabled": True, "subject": "O'Brien"}
    layer = SemanticLayer(
        connection=postgres, engine="rust", fallback=False, enforce_visibility=True, auto_register=False
    )
    layer.graph = graph
    semantic_sql = r"""select secured."CaseSubject", secured.total from metrics
        where secured."CaseSubject" = E'O\'Brien' order by secured."CaseSubject" DESC NULLS LAST"""
    query = {
        "metrics": ["secured.total"],
        "dimensions": ["secured.CaseSubject"],
        "filters": [r"""secured."CaseSubject" = E'O\'Brien' """],
        "order_by": ["secured.CaseSubject DESC NULLS LAST"],
        "user_attributes": attributes,
    }
    if mode == "compile":
        result = postgres.execute(layer.compile(**query))
    elif mode == "query":
        result = layer.query(**query)
    elif mode == "sql":
        result = layer.sql(semantic_sql, user_attributes=attributes)
    else:
        rewriter = QueryRewriter(
            graph, dialect=postgres.dialect, use_rust_rewriter=True, rust_no_fallback=True, enforce_visibility=True
        )
        result = postgres.execute(rewriter.rewrite(semantic_sql, user_attributes=attributes))
        assert rewriter.last_engine_selection == {"engine": "rust", "reason": None}
    assert result.fetchall() == [("O'Brien", 7)]
    if mode != "rewriter":
        assert layer.last_engine_selection == {"engine": "rust", "reason": None}


@pytest.mark.parametrize("mode", ["compile", "sql"])
def test_postgres_public_denial_does_not_fall_back(postgres, graph, mode):
    layer = SemanticLayer(connection=postgres, engine="auto", enforce_visibility=True, auto_register=False)
    layer.graph = graph
    with pytest.raises(SecurityError):
        if mode == "compile":
            layer.compile(metrics=["secured.total"], user_attributes={"role": "viewer"})
        else:
            layer.sql("select secured.total from metrics", user_attributes={"role": "viewer"})
    assert not layer.last_engine_selection or layer.last_engine_selection["engine"] != "python"


def test_postgres_transport_does_not_relabel_declared_graph_dialect(postgres, graph):
    graph.models["secured"].metrics[0].metadata = {"ossie_target_dialect": "POSTGRESQL"}
    layer = SemanticLayer(connection=postgres, engine="rust", fallback=False, auto_register=False)
    layer.graph = graph
    with pytest.raises(UnsupportedSemanticFeaturesError, match="input_dialect"):
        layer.compile(
            metrics=["secured.total"],
            user_attributes={"role": "analyst", "tenant": 1, "enabled": True, "subject": "alice"},
        )


def test_explicit_postgres_graph_input_stays_unsupported(postgres, graph):
    with pytest.raises(UnsupportedSemanticFeaturesError, match="input_dialect.postgres"):
        compile_semantic_input(graph, {"metrics": ["secured.total"], "dialect": "postgres"}, input_dialect="postgres")


@pytest.mark.parametrize("mode", ["query", "sql"])
def test_postgres_public_null_ordering_matches_adapter(postgres, graph, mode):
    model = graph.models["secured"]
    model.dimensions.append(Dimension(name="CaseSubject", sql="subject"))
    model.security.row_filters = ["tenant = {{ user.tenant }}"]
    layer = SemanticLayer(connection=postgres, engine="rust", auto_register=False)
    layer.graph = graph
    attributes = {"role": "analyst", "tenant": 1}
    if mode == "query":
        result = layer.query(
            dimensions=["secured.CaseSubject"],
            metrics=["secured.total"],
            order_by=["secured.CaseSubject DESC"],
            user_attributes=attributes,
        )
    else:
        result = layer.sql(
            'select secured."CaseSubject", secured.total from metrics order by secured."CaseSubject" DESC',
            user_attributes=attributes,
        )
    assert result.fetchone() == (None, 17)
    assert layer.last_engine_selection == {"engine": "rust", "reason": None}
