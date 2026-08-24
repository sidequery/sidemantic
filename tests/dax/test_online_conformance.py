from __future__ import annotations

import json
import os
import urllib.request

import pytest

dax_ast = pytest.importorskip("sidemantic_dax.ast")


pytestmark = pytest.mark.skipif(
    os.environ.get("SIDEMANTIC_DAX_ONLINE_CONFORMANCE") != "1",
    reason="set SIDEMANTIC_DAX_ONLINE_CONFORMANCE=1 to probe DAX Formatter",
)

_FORMATTER_URL = "https://api.daxformatter.com/api/daxtextformat"


def _formatter_result(dax: str) -> dict[str, object]:
    payload = json.dumps(
        {
            "Dax": dax,
            "ListSeparator": ",",
            "DecimalSeparator": ".",
            "CallerApp": "sidemantic-dax-conformance",
            "CallerVersion": "0",
        }
    ).encode()
    request = urllib.request.Request(
        _FORMATTER_URL,
        data=payload,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return dict(json.load(response))


def _normalized(text: str) -> str:
    return text.replace("\r\n", "\n").rstrip("\n")


@pytest.mark.parametrize(
    ("kind", "dax"),
    [
        (
            "expression",
            "=sumx(filter('Sales','Sales'[Amount]>100),'Sales'[Amount])",
        ),
        ("query", "EVALUATE {(1,2),(3,4)}"),
        (
            "query",
            "DEFINE MEASURE 'S'[M]=SUM('S'[A]) VAR threshold=10 EVALUATE ROW(\"m\",[M])",
        ),
        (
            "query",
            "DEFINE FUNCTION myUdf=(x:NUMERIC)=>x EVALUATE {myUdf(1)}",
        ),
        (
            "query",
            'DEFINE TABLE data=ROW("Year",2000,"IsTotal",FALSE()) '
            "WITH VISUAL SHAPE AXIS ROWS GROUP [Year] TOTAL [IsTotal] "
            'ORDER BY [Year] DENSIFY "IsDensified" EVALUATE data',
        ),
    ],
)
def test_stable_syntax_matches_dax_formatter(kind: str, dax: str):
    if kind == "expression":
        local = dax_ast.format_expression(dax, sqlbi=True)
    else:
        local = dax_ast.format_query(dax, sqlbi=True)
    result = _formatter_result(dax)
    assert result.get("errors") == []
    assert _normalized(str(result["formatted"])) == local


@pytest.mark.parametrize(
    "dax",
    [
        '=dt"2020-12-15T12:30:59"',
        'EVALUATE DATATABLE("A", INTEGER, "B", STRING, {{1, "x"}, {2,}})',
        "EVALUATE FILTER('Product', ('Product'[Color], 'Product'[Brand]) IN {(\"Red\", \"Contoso\")})",
        """
        DEFINE
          TABLE data = ROW("Year", 2000, "IsTotal", FALSE())
          WITH VISUAL SHAPE
            AXIS ROWS GROUP [Year] TOTAL [IsTotal] ORDER BY [Year]
            DENSIFY "IsDensified"
        EVALUATE data
        """,
    ],
)
def test_stable_syntax_is_accepted_by_dax_formatter(dax: str):
    if dax.lstrip().startswith("="):
        dax_ast.parse_expression(dax)
    else:
        dax_ast.parse_query(dax)
    assert _formatter_result(dax).get("errors") == []


def test_udf_defaults_are_known_sqlbi_lag():
    dax = "DEFINE FUNCTION AddTax=(amount:NUMERIC,taxRate:NUMERIC=0.1)=>amount*(1+taxRate) EVALUATE {AddTax(10)}"
    local = dax_ast.format_query(dax, sqlbi=True)
    assert "taxRate : NUMERIC = 0.1" in local

    # Microsoft compatibility level 1702 supports defaults, while SQLBI's
    # formatter still rejects the default-expression `=`.
    result = _formatter_result(dax)
    assert result.get("formatted") == ""
    errors = list(result.get("errors", []))
    assert len(errors) == 1
    assert "Syntax error" in str(errors[0].get("message"))


def test_local_semantic_checks_are_stricter_than_formatter():
    dax = 'EVALUATE DATATABLE("A", INTEGER, "B", STRING, {{1}})'
    with pytest.raises(ValueError, match="schema defines 2 columns"):
        dax_ast.parse_query(dax)

    # SQLBI currently accepts this structurally invalid row width. Keep this
    # probe explicit so the service is never mistaken for a semantic oracle.
    assert _formatter_result(dax).get("errors") == []
