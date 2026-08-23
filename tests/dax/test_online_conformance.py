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


def _formatter_errors(dax: str) -> list[dict[str, object]]:
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
        result = json.load(response)
    return list(result.get("errors", []))


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
def test_stable_syntax_matches_dax_formatter(dax: str):
    if dax.lstrip().startswith("="):
        dax_ast.parse_expression(dax)
    else:
        dax_ast.parse_query(dax)
    assert _formatter_errors(dax) == []


def test_local_semantic_checks_are_stricter_than_formatter():
    dax = 'EVALUATE DATATABLE("A", INTEGER, "B", STRING, {{1}})'
    with pytest.raises(ValueError, match="schema defines 2 columns"):
        dax_ast.parse_query(dax)

    # SQLBI currently accepts this structurally invalid row width. Keep this
    # probe explicit so the service is never mistaken for a semantic oracle.
    assert _formatter_errors(dax) == []
