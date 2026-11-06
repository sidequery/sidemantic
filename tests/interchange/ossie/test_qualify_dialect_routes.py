"""Public semantic-input calls preserve windows across dialect boundaries."""

import json

import duckdb
import pytest

from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from sidemantic.rust_bridge import _compiler_graph_json, get_rust_module
from tests.interchange.ossie.test_layer2_conformance import LATEST, model, setup


@pytest.mark.parametrize(
    "graph_dialect,query_dialect,output_dialect",
    [
        ("duckdb", "duckdb", "postgres"),
        ("duckdb", "snowflake", "snowflake"),
        ("snowflake", "snowflake", "snowflake"),
    ],
)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT status,AGG(revenue) AS total FROM commerce.orders GROUP BY status "
        "WINDOW ranked AS(ORDER BY AGG(revenue) DESC) QUALIFY ROW_NUMBER() OVER ranked=1",
        "SELECT DISTINCT status,AGG(revenue) AS total FROM commerce.orders GROUP BY status "
        "WINDOW ranked AS(ORDER BY AGG(revenue) DESC) QUALIFY ROW_NUMBER() OVER ranked<=2 "
        "ORDER BY total LIMIT 1",
        "SELECT DISTINCT status,ROW_NUMBER() OVER(ORDER BY amount DESC NULLS LAST,id) AS rn FROM commerce.orders "
        "QUALIFY rn<=3 ORDER BY rn DESC LIMIT 1",
        "SELECT id,ROW_NUMBER() OVER(ORDER BY amount DESC NULLS LAST,id) AS rn FROM commerce.orders "
        "QUALIFY rn<=3 AND amount>0 ORDER BY id DESC LIMIT 1",
    ],
)
def test_direct_semantic_entrypoint_qualify_dialect_boundary(graph_dialect, query_dialect, output_dialect, query):
    document = parse_ossie_document(
        json.dumps(model()).encode(),
        options=OssieParseOptions(schema_revision=LATEST, target_dialect=graph_dialect),
    )
    catalog = lower_ossie_document(document).catalog
    graph = next(iter(catalog.scopes.values())).graph
    compiled = get_rust_module().rewrite_with_semantic_input_context(
        _compiler_graph_json(graph, input_dialect=graph_dialect),
        query,
        json.dumps({"sql_dialect": query_dialect, "output_dialect": output_dialect}),
    )
    if output_dialect == "postgres":
        assert "QUALIFY" not in compiled
    reference = query.replace("commerce.orders", "raw_orders").replace("AGG(revenue)", "SUM(amount)")
    with duckdb.connect() as connection:
        connection.execute(setup())
        expected = connection.execute(reference).fetchall()
        # Window NULL placement is explicit: Snowflake's DESC default puts
        # NULL first, unlike DuckDB. Execute emitted SQL directly, without
        # any test transpilation, under the same authored ordering contract.
        assert connection.execute(compiled).fetchall() == expected
