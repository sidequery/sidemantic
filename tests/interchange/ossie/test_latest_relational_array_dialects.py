"""Array indexing retains its source dialect through Layer 2 normalization."""

import json

import duckdb
import pytest
import sqlglot

from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from sidemantic.sql.query_rewriter import QueryRewriter
from tests.interchange.ossie.test_layer2_conformance import LATEST, ROUTES
from tests.rust_layer_adapter import _rust_request


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(("source", "target"), [("snowflake", "duckdb"), ("duckdb", "snowflake")])
@pytest.mark.parametrize("location", ["field", "metric", "source", "query", "correlated"])
def test_layer2_array_indexes_keep_source_and_query_dialects(route, source, target, location):
    index = 0 if source == "snowflake" else 1
    array = "ARRAY_CONSTRUCT(10,20)" if source == "snowflake" else "[10,20]"

    def variant(expression):
        return {
            "dialects": [{"dialect": "ANSI_SQL" if source == "duckdb" else source.upper(), "expression": expression}]
        }

    source_query = f"SELECT {array} AS xs"
    fields = [{"name": "first", "expression": variant(f"xs[{index}]")}]
    metric = "SUM(first)"
    if location in {"metric", "query"}:
        fields = [{"name": "xs", "expression": variant("xs")}]
        metric = f"SUM(xs[{index}])"
    if location == "source":
        source_query = f"SELECT ({array})[{index}] AS first"
        fields = [{"name": "first", "expression": variant("first")}]
    document = {
        "version": "0.2.0.dev0",
        "name": "arrays",
        "datasets": [
            {
                "name": "rows",
                "source": source_query,
                "fields": fields,
                "metrics": [{"name": "total", "expression": variant(metric)}],
            }
        ],
    }
    query = "SELECT AGG(total) FROM arrays.rows"
    if location == "query":
        query = f"SELECT xs[{0 if target == 'snowflake' else 1}] FROM arrays.rows"
    if location == "correlated":
        query = "SELECT (SELECT AGG(b.total) FROM arrays.rows b WHERE b.first=a.first) FROM arrays.rows a"
    if route == "native_rust":
        response = _rust_request(
            {
                "action": "ossie_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": source.upper(),
                "dialect": target,
                "schema_revision": LATEST,
                "sql": query,
            }
        )
        assert response["status"] == "ok", response
        compiled = response["sql"]
    else:
        parsed = parse_ossie_document(
            json.dumps(document).encode(),
            options=OssieParseOptions(
                validate_schema=True, schema_revision=LATEST, target_dialect=source, source_dialect=source
            ),
        )
        lowered = lower_ossie_document(parsed)
        assert lowered.valid, lowered.diagnostics
        graph = next(iter(lowered.catalog.scopes.values())).graph
        compiled = QueryRewriter(
            graph, dialect=target, use_rust_rewriter=route == "public_rust", rust_no_fallback=True
        ).rewrite(query)
    with duckdb.connect() as connection:
        executable = sqlglot.transpile(compiled, read=target, write="duckdb")[0]
        assert connection.execute(executable).fetchall() == [(10,)]
