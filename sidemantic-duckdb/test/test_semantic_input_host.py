# /// script
# dependencies = ["pytest"]
# ///
"""Run SemanticInput through the built DuckDB SQL host, without ABI substitution."""

import copy
import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SOURCE = json.loads((ROOT / "sidemantic-rs/tests/fixtures/semantic_host.json").read_text())
CONTEXT = {"user_attributes": {"tenant": "a"}, "enforce_visibility": True}
QUERY = {"metrics": ["orders.revenue"], **CONTEXT}


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def execute(sql, error=False):
    binary = Path(os.environ["SIDEMANTIC_DUCKDB_BINARY"]).resolve()
    extension = Path(os.environ["SIDEMANTIC_DUCKDB_EXTENSION"]).resolve()
    assert binary.is_file(), f"Missing built DuckDB shell: {binary}"
    assert extension.is_file(), f"Missing built extension: {extension}"
    setup = f"""
        load {literal(str(extension))};
        create table orders(id integer, tenant varchar, amount integer, deleted boolean);
        insert into orders values (1, 'a', 10, false), (2, 'a', 50, true), (3, 'b', 100, false);
    """
    result = subprocess.run(
        [str(binary), "-unsigned", "-json", "-bail", ":memory:"],
        input=setup + sql,
        text=True,
        capture_output=True,
    )
    if error:
        assert result.returncode != 0, result.stdout
        assert "SemanticInput" in result.stderr, result.stderr
        return result.stderr
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def call(query=None, source=None, sql=None, context=None, error=False):
    arguments = [literal(json.dumps(SOURCE if source is None else source))]
    if sql is None:
        function = "sidemantic_compile_semantic_input"
        arguments.append(literal(json.dumps(QUERY if query is None else query)))
    else:
        function = "sidemantic_rewrite_semantic_input"
        arguments.extend([literal(sql), literal(json.dumps(CONTEXT if context is None else context))])
    result = execute(f"select {function}({', '.join(arguments)}) as sql;", error=error)
    return result if error else result[0]["sql"]


@pytest.mark.parametrize("rewrite", [False, True])
def test_population_and_injection_value(rewrite):
    for tenant, expected in [("a", 10), ("b", 100), ("a' OR 1=1 --", None)]:
        context = {"user_attributes": {"tenant": tenant}, "enforce_visibility": True}
        if rewrite:
            sql = call(sql="select orders.revenue as total from metrics", context=context)
        else:
            sql = call(query={"metrics": ["orders.revenue"], **context})
        rows = execute(sql)
        assert len(rows) == 1
        # DuckDB's shell encodes HUGEINT aggregate results as JSON strings.
        assert list(rows[0].values()) == [None if expected is None else str(expected)]


@pytest.mark.parametrize("mutation", ["version", "envelope", "field", "key", "scope", "capability", "policy"])
@pytest.mark.parametrize("rewrite", [False, True])
def test_invalid_contract_is_rejected(mutation, rewrite):
    source = copy.deepcopy(SOURCE)
    if mutation == "version":
        source["version"] = 999
    elif mutation == "envelope":
        source["unknown"] = True
    elif mutation == "field":
        source["models"][0]["metrics"][0]["unknown"] = True
    elif mutation == "key":
        source["models"][0]["primary_key"] = {"sql": "id"}
    elif mutation == "scope":
        source["metric_owners"] = {"ghost": "missing"}
    elif mutation == "capability":
        source["required_capabilities"] = ["future_capability"]
    else:
        source["models"][0]["security"]["unknown"] = True
    call(source=source, sql="select orders.revenue from metrics" if rewrite else None, error=True)


@pytest.mark.parametrize(
    "query",
    [
        {"metrics": ["orders.revenue"]},
        {"metrics": ["orders.secret"], **CONTEXT},
        {"metrics": ["orders.revenue"], "prepared_policies": {}},
    ],
)
def test_compile_denials(query):
    call(query=query, error=True)


def test_rewrite_denials():
    call(sql="select orders.revenue from metrics", context={}, error=True)
    call(sql="select orders.secret from metrics", error=True)
    call(sql="select orders.revenue from metrics", context={"prepared_policies": {}}, error=True)


def test_nulls_and_nul_bytes():
    source = literal(json.dumps(SOURCE))
    assert execute(f"select sidemantic_compile_semantic_input({source}, NULL) as sql;") == [{"sql": None}]
    assert execute(f"select sidemantic_rewrite_semantic_input({source}, NULL, '{{}}') as sql;") == [{"sql": None}]
    execute(f"select sidemantic_compile_semantic_input({source}, '{{}}' || chr(0));", error=True)
    execute(f"select sidemantic_rewrite_semantic_input({source}, 'select 1' || chr(0), '{{}}');", error=True)


def test_vectorized_callers_are_isolated():
    source = literal(json.dumps(SOURCE))
    query_a = literal(json.dumps(QUERY))
    query_b = literal(json.dumps({**QUERY, "user_attributes": {"tenant": "b"}}))
    compiled = execute(
        f"select sidemantic_compile_semantic_input({source}, query) as sql "
        f"from (values (1, {query_a}), (2, {query_b}), (3, {query_a})) requests(id, query) order by id;"
    )
    assert [list(execute(row["sql"])[0].values()) for row in compiled] == [["10"], ["100"], ["10"]]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q", "-o", "addopts="]))
