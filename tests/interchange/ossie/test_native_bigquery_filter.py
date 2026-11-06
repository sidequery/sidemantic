"""GoogleSQL FILTER lowering preserves values, DISTINCT, and empty populations."""

import json
import re

import duckdb
import pytest
import sqlglot

from sidemantic import SemanticLayer
from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from tests.interchange.ossie.test_layer2_conformance import LATEST, ROUTES
from tests.rust_layer_adapter import _rust_request


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("population", ["matches", "no_matches", "empty"])
@pytest.mark.parametrize(
    "aggregate,matched,unmatched",
    [
        ("SUM(amount)", 22, None),
        ("COUNT(*)", 2, 0),
        ("COUNT(DISTINCT amount)", 1, 0),
        ("COUNT(DISTINCT NULL)", 0, 0),
    ],
)
def test_bigquery_portable_aggregate_filter_uses_conditional_inputs(route, population, aggregate, matched, unmatched):
    source = "SELECT 11 AS amount UNION ALL SELECT 11 UNION ALL SELECT NULL UNION ALL SELECT -1"
    if population != "matches":
        source = f"SELECT amount FROM ({source}) AS rows WHERE {'FALSE' if population == 'empty' else 'amount < 0'}"
    document = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": source,
                "fields": [{"name": "amount", "expression": {"sql": "amount"}}],
                "metrics": [{"name": "filtered", "expression": {"sql": f"{aggregate} FILTER (WHERE amount > 5)"}}],
            }
        ],
    }
    query = {"metrics": ["orders.filtered"]}
    if route == "native_rust":
        result = _rust_request(
            {
                "action": "ossie_wide_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": "BIGQUERY",
                "dialect": "bigquery",
                "schema_revision": LATEST,
                "query": query,
            }
        )
        assert result["status"] == "ok", result
        compiled = result["sql"]
    else:
        parsed = parse_ossie_document(
            json.dumps(document).encode(),
            options=OssieParseOptions(
                validate_schema=True,
                schema_revision=LATEST,
                target_dialect="bigquery",
            ),
        )
        lowered = lower_ossie_document(parsed)
        assert lowered.valid and lowered.executable, lowered.diagnostics
        layer = SemanticLayer.from_catalog(
            lowered.catalog,
            dialect="bigquery",
            engine="rust" if route == "public_rust" else "python",
            fallback=False,
            auto_register=False,
        )
        try:
            compiled = layer.compile_ossie_wide(query)
        finally:
            layer.adapter.close()
    # FILTER is not GoogleSQL, even if a tolerant parser accepts it.
    assert not re.search(r"\bFILTER\s*\(", compiled, re.IGNORECASE), compiled
    tree = sqlglot.parse_one(compiled, read="bigquery")
    # BigQuery COUNTIF(empty)=0, whereas DuckDB COUNT_IF(empty)=NULL. Use
    # the standard conditional COUNT oracle rather than SQLGlot's spelling-only
    # translation for this one aggregate.
    tree = tree.transform(
        lambda node: sqlglot.exp.Count(this=sqlglot.exp.Case().when(node.this.copy(), sqlglot.exp.Literal.number(1)))
        if isinstance(node, sqlglot.exp.CountIf)
        else node
    )
    executable = tree.sql(dialect="duckdb")
    with duckdb.connect() as connection:
        assert connection.execute(executable).fetchall() == [(matched if population == "matches" else unmatched,)]
