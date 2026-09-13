# /// script
# dependencies = ["duckdb", "pytest"]
# ///
"""Execute SQL from the generated WASM module; absence of the host is a failure."""

import copy
import json
import os
import subprocess
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent
SOURCE = json.loads((ROOT / "fixtures/semantic_host.json").read_text())


def call(method, source=None, query=None, sql=None, context=None):
    module = Path(os.environ["SIDEMANTIC_WASM_MODULE"]).resolve()
    assert module.is_file(), f"Missing generated WASM module: {module}"
    args = [json.dumps(SOURCE if source is None else source)]
    if sql is not None:
        args.append(sql)
        if context is not None:
            args.append(json.dumps(context))
    else:
        args.append(json.dumps(query))
    result = subprocess.run(
        ["node", str(ROOT / "wasm_semantic_host.cjs"), str(module)],
        input=json.dumps({"method": f"wasm_{method}_with_semantic_input", "args": args}),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def rows(sql):
    with duckdb.connect() as connection:
        connection.execute("create table orders(id integer, tenant varchar, amount integer, deleted boolean)")
        connection.execute("insert into orders values (1, 'a', 10, false), (2, 'a', 50, true), (3, 'b', 100, false)")
        return connection.execute(sql).fetchall()


def test_compile_population_and_injection_value():
    query = {"metrics": ["orders.revenue"], "user_attributes": {"tenant": "a"}, "enforce_visibility": True}
    assert rows(call("compile", query=query)["result"]) == [(10,)]
    query["user_attributes"]["tenant"] = "a' OR 1=1 --"
    assert rows(call("compile", query=query)["result"]) == [(None,)]


@pytest.mark.parametrize("tenant,expected", [("a", [(10,)]), ("b", [(100,)]), ("a' OR 1=1 --", [(None,)])])
def test_rewrite_population(tenant, expected):
    # Call the context-bearing public export using the same JSON IPC harness.
    module = str(Path(os.environ["SIDEMANTIC_WASM_MODULE"]).resolve())
    request = {
        "method": "wasm_rewrite_with_semantic_input_context",
        "args": [
            json.dumps(SOURCE),
            "select orders.revenue as total from metrics",
            json.dumps({"user_attributes": {"tenant": tenant}, "enforce_visibility": True}),
        ],
    }
    result = subprocess.run(
        ["node", str(ROOT / "wasm_semantic_host.cjs"), module],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=True,
    )
    response = json.loads(result.stdout)
    assert "error" not in response, response
    assert rows(response["result"]) == expected


@pytest.mark.parametrize("mutation", ["version", "envelope", "field", "key", "scope", "capability", "policy"])
def test_invalid_contract_is_rejected(mutation):
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
    result = call("compile", source, {"metrics": ["orders.revenue"], "user_attributes": {"tenant": "a"}})
    expected = {
        "version": "supported semantic input version",
        "envelope": "unknown field",
        "field": "unknown semantic field",
        "key": "key",
        "scope": "Invalid metric owner",
        "capability": "Unsupported semantic features",
        "policy": "unknown field",
    }
    assert expected[mutation] in result.get("error", ""), result


@pytest.mark.parametrize(
    "query,expected",
    [
        ({"metrics": ["orders.revenue"]}, "no user_attributes"),
        ({"metrics": ["orders.secret"], "user_attributes": {"tenant": "a"}, "enforce_visibility": True}, "not public"),
        ({"metrics": ["orders.revenue"], "prepared_policies": {}}, "unknown field"),
    ],
)
def test_query_denials(query, expected):
    assert expected in call("compile", query=query).get("error", "")


def test_reference_validation_is_not_authorization():
    assert json.loads(call("validate", query={"metrics": ["orders.revenue"]})["result"]) == []
    assert "no user_attributes" in call("compile", query={"metrics": ["orders.revenue"]}).get("error", "")
    assert "no user_attributes" in call("rewrite", sql="select orders.revenue from metrics").get("error", "")


@pytest.mark.parametrize("method", ["compile", "rewrite"])
@pytest.mark.parametrize(
    "expression,diagnostic",
    [
        ("(" * 1000 + "1" + ")" * 1000, "nesting limit"),
        ("NOT " * 1000 + "true", "operator-chain limit"),
    ],
)
def test_parser_work_limits_return_errors_not_traps(method, expression, diagnostic):
    model = copy.deepcopy(SOURCE)
    del model["models"][0]["security"]
    if method == "compile":
        model["models"][0]["metrics"][0]["sql"] = expression
        result = call(method, source=model, query={"metrics": ["orders.revenue"]})
    else:
        result = call(method, source=model, sql=f"select {expression}")
    assert "SQL parse error: WASM SQL parser" in result.get("error", ""), result
    assert diagnostic in result["error"]


def test_near_nesting_limit_and_literal_delimiters_are_accepted():
    model = copy.deepcopy(SOURCE)
    del model["models"][0]["security"]
    model["models"][0]["metrics"][0]["sql"] = "(" * 14 + "amount" + ")" * 14
    assert rows(call("compile", source=model, query={"metrics": ["orders.revenue"]})["result"]) == [(110,)]
    result = call("rewrite", source=model, sql="select " + "NOT " * 30 + "true")
    assert "result" in result, result
    # Flat projections can exceed 256 tokens without recursive expression depth.
    wide_sql = "select " + ", ".join(f"{i} as c{i}" for i in range(100))
    assert "result" in call("rewrite", source=model, sql=wide_sql)
    # Parentheses in literals and comments do not consume a nesting budget.
    literal = "(" * 100
    for expression in [f"'{literal}'", f"$tag${literal}$tag$"]:
        result = call("rewrite", source=model, sql=f"select {expression} /* {literal} */ -- {literal}\n")
        assert "result" in result, result


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q", "-o", "addopts="]))
