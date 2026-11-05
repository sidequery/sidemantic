"""Relational scope regressions outside the shared independent conformance corpus."""

import duckdb
import pytest

from sidemantic.sql.ossie_relational import compile_ossie_view_definition
from tests.interchange.ossie.test_layer2_conformance import (
    ROUTES,
    assert_rows,
    execute,
    model,
    setup,
    shared_catalog,
)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query,reference",
    [
        (
            "SELECT AGG(revenue) FILTER (WHERE amount > (SELECT AVG(amount) FROM commerce.orders)),AGG(order_count) FROM commerce.orders",
            "SELECT SUM(amount) FILTER(WHERE amount>(SELECT AVG(amount) FROM raw_orders)),COUNT(*) FROM raw_orders",
        ),
        (
            "SELECT AGG(o.revenue) FILTER (WHERE amount>(SELECT AVG(t.cost) FROM commerce.tickets t WHERE t.customer_id=o.customer_id)) FROM commerce.orders o",
            "SELECT SUM(o.amount) FILTER(WHERE amount>(SELECT AVG(t.cost) FROM raw_tickets t WHERE t.customer_id=o.customer_id)) FROM raw_orders o",
        ),
        (
            "SELECT o.customer_id+1 k,AGG(o.revenue),(SELECT COUNT(*) FROM commerce.tickets t WHERE t.customer_id=o.customer_id+1) n FROM commerce.orders o GROUP BY o.customer_id+1",
            "SELECT g.k,g.total,(SELECT COUNT(*) FROM raw_tickets t WHERE t.customer_id=g.k) n FROM (SELECT customer_id+1 k,SUM(amount) total FROM raw_orders GROUP BY customer_id+1) g",
        ),
        (
            "SELECT AGG(o.revenue) FROM commerce.orders o JOIN (SELECT * FROM raw_tickets) t ON o.customer_id=t.customer_id",
            "SELECT SUM(amount) FROM raw_orders o WHERE EXISTS (SELECT 1 FROM raw_tickets t WHERE t.customer_id=o.customer_id)",
        ),
        (
            "WITH t(tid,cid,cost) AS (SELECT * FROM raw_tickets) SELECT AGG(o.revenue) FROM commerce.orders o JOIN t ON o.customer_id=t.cid",
            "SELECT SUM(amount) FROM raw_orders o WHERE EXISTS (SELECT 1 FROM raw_tickets t WHERE t.customer_id=o.customer_id)",
        ),
        (
            "SELECT AGG(o.revenue) FROM commerce.orders o JOIN (SELECT * FROM raw_tickets) t(tid,cid,cost) ON o.customer_id=t.cid",
            "SELECT SUM(amount) FROM raw_orders o WHERE EXISTS (SELECT 1 FROM raw_tickets t WHERE t.customer_id=o.customer_id)",
        ),
    ],
)
def test_scope_preserving_relational_edges(route, query, reference):
    with duckdb.connect() as connection:
        connection.execute(setup())
        expected = connection.execute(reference).fetchall()
    rows, _ = execute(route, query)
    assert_rows(rows, expected)


def test_view_projection_descriptors_preserve_declared_types():
    document = model()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    for item in orders["fields"]:
        if item["name"] == "amount":
            item["datatype"] = "Integer"
    # Metrics and fields whose declarations omit types remain explicitly UNKNOWN.
    graph = next(iter(shared_catalog(document).scopes.values())).graph
    _, columns = compile_ossie_view_definition(
        "SELECT amount AS value, revenue AS total, TO_MEASURE(COUNT(*)) AS n FROM commerce.orders", graph
    )
    assert columns[0]["data_type"].upper() == "INTEGER"
    assert columns[1]["is_measure"]
    assert columns[2] == {"name": "n", "is_measure": True, "data_type": "BIGINT"}


@pytest.mark.parametrize("route", ROUTES)
def test_zero_row_metadata_preserves_measure_columns_and_hides_internal_inputs(route):
    rows, names = execute(route, "SELECT * FROM commerce.orders LIMIT 0")
    orders = next(dataset for dataset in model()["datasets"] if dataset["name"] == "orders")
    assert rows == []
    assert names == [item["name"] for item in [*orders["fields"], *orders["metrics"]]]
    from tests.interchange.ossie.test_layer2_conformance import compile_query

    with duckdb.connect() as connection:
        connection.execute(setup())
        result = connection.execute(compile_query(route, "SELECT revenue FROM commerce.orders WHERE FALSE"))
        assert result.fetchall() == []
        assert "MEASURE" in str(result.description[0][1]).upper()


@pytest.mark.parametrize("use_rust", [False, True])
@pytest.mark.parametrize(
    "query",
    [
        "SELECT AGG(revenue) FROM commerce.orders",
        "DESCRIBE commerce.orders",
        "SELECT column_name FROM information_schema.columns",
    ],
)
def test_direct_rewriter_rejects_unsupported_security_controls(use_rust, query):
    from sidemantic.sql.query_rewriter import QueryRewriter

    graph = next(iter(shared_catalog(model()).scopes.values())).graph
    rewriter = QueryRewriter(graph, enforce_visibility=True, use_rust_rewriter=use_rust, rust_no_fallback=True)
    with pytest.raises(ValueError, match="security controls"):
        rewriter.rewrite(query)


@pytest.mark.parametrize("route", ROUTES)
def test_agg_rejects_scalar_field_from_physical_relation_before_execution(route):
    from tests.interchange.ossie.test_layer2_conformance import compile_query

    with pytest.raises(ValueError, match="measure"):
        compile_query(route, "SELECT AGG(amount) FROM raw_orders")


@pytest.mark.parametrize("use_rust", [False, True])
def test_sql_measure_definitions_need_no_imported_model(use_rust):
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.query_rewriter import QueryRewriter

    query = "SELECT AGG(m) FROM (SELECT TO_MEASURE(SUM(amount)) AS m FROM raw_orders) q CROSS JOIN (VALUES(1),(2)) x(n)"
    sql = QueryRewriter(SemanticGraph(), use_rust_rewriter=use_rust, rust_no_fallback=True).rewrite(query)
    with duckdb.connect() as connection:
        connection.execute(setup())
        assert connection.execute(sql).fetchall() == [(280,)]


@pytest.mark.parametrize("use_rust", [False, True])
def test_qualified_physical_relation_does_not_bind_same_named_semantic_model(use_rust):
    from sidemantic.sql.query_rewriter import QueryRewriter
    from tests.interchange.ossie.test_latest_discovery import _layer

    layer = _layer("python")
    try:
        query = "SELECT * FROM scott.emp"
        rewriter = QueryRewriter(layer.graph, use_rust_rewriter=use_rust, rust_no_fallback=True)
        assert rewriter.rewrite(query) == query
        assert len(layer.adapter.execute(rewriter.rewrite(query)).fetchall()) == 14
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("route", ROUTES)
def test_semantic_join_to_same_named_physical_relation(route):
    from tests.interchange.ossie.test_latest_query_interface import _execute

    assert _execute(
        route, "SELECT AGG(e.sum_sal),SUM(p.sal) FROM scott_model.emp e JOIN scott.emp p ON e.empno=p.empno"
    ) == [(29025, 29025)]
