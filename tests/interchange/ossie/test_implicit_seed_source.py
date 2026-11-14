"""Warehouse-valid implicit SEED aliases survive native source parsing."""

import duckdb
import pytest

from tests.interchange.ossie.test_layer2_conformance import ROUTES, compile_query


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("alias", ["seed", "AS seed", '"seed"', "/* é seed */ seed"])
@pytest.mark.parametrize("empty", [False, True])
def test_implicit_seed_dataset_source_preserves_empty_and_nonempty_populations(route, alias, empty):
    source = f"SELECT amount FROM (SELECT 1 AS amount) {alias}"
    if empty:
        source += " WHERE FALSE"
    document = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": source,
                "fields": [{"name": "amount", "expression": {"sql": "amount"}}],
                "metrics": [
                    {"name": "total", "expression": {"sql": "SUM(amount)"}},
                    {"name": "row_count", "expression": {"sql": "COUNT(*)"}},
                ],
            }
        ],
    }
    with duckdb.connect() as connection:
        # The real warehouse, independently of either compiler, establishes
        # that the original implicit spelling is valid and defines its rows.
        expected = connection.execute(f"SELECT SUM(amount), COUNT(*) FROM ({source})").fetchall()
        assert expected == ([(None, 0)] if empty else [(1, 1)])
        compiled = compile_query(route, "SELECT AGG(total), AGG(row_count) FROM sales.orders", document)
        assert connection.execute(compiled).fetchall() == expected


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("target", ["duckdb", "postgres", "snowflake", "clickhouse", "bigquery"])
def test_wide_seed_source_retains_where_across_target_normalization(route, target):
    import json

    import sqlglot

    from sidemantic import SemanticLayer
    from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
    from tests.interchange.ossie.test_layer2_conformance import LATEST
    from tests.interchange.ossie.test_review_import_boundaries import _document
    from tests.rust_layer_adapter import _rust_request

    document = _document()
    document["datasets"][0]["source"] = "SELECT amount FROM (SELECT 1 AS amount) seed WHERE FALSE"
    document["datasets"][0]["metrics"].append({"name": "rows", "expression": {"sql": "COUNT(*)"}})
    query = {"metrics": ["orders.total", "orders.rows"]}
    if route == "native_rust":
        result = _rust_request(
            {
                "action": "ossie_wide_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": target.upper(),
                "dialect": target,
                "schema_revision": LATEST,
                "query": query,
            }
        )
        assert result["status"] == "ok", result
        compiled = result["sql"]
    else:
        parsed = parse_ossie_document(
            json.dumps(document).encode(),
            options=OssieParseOptions(validate_schema=True, target_dialect=target, schema_revision=LATEST),
        )
        lowered = lower_ossie_document(parsed)
        assert lowered.valid, lowered.diagnostics
        layer = SemanticLayer.from_catalog(
            lowered.catalog,
            dialect=target,
            engine="rust" if route == "public_rust" else "python",
            fallback=False,
            auto_register=False,
        )
        try:
            compiled = layer.compile_ossie_wide(query, dialect=target)
        finally:
            layer.adapter.close()
    executable = sqlglot.transpile(compiled, read=target, write="duckdb")[0]
    with duckdb.connect() as connection:
        assert connection.execute(executable).fetchall() == [(None, 0)]
