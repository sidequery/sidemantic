"""Constant DISTINCT aggregates retain their modifier when bound to source rows."""

import pytest

from tests.interchange.ossie.test_ossie_engine_conformance import _query


def _document(empty):
    return {
        "version": "0.2.0.dev0",
        "name": "constants",
        "datasets": [
            {
                "name": "items",
                "source": (
                    "SELECT 1 AS id WHERE FALSE" if empty else "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3"
                ),
                "fields": [{"name": "id", "expression": {"sql": "id"}}],
                "metrics": [
                    {"name": "distinct_one", "expression": {"sql": "COUNT(DISTINCT 1)"}},
                    {"name": "distinct_null", "expression": {"sql": "COUNT(DISTINCT NULL)"}},
                    {
                        "name": "filtered_one",
                        "expression": {"sql": "COUNT(DISTINCT 1) FILTER (WHERE id > 99)"},
                    },
                ],
            }
        ],
    }


@pytest.mark.parametrize("route", ["python", "public_rust", "native_rust"])
@pytest.mark.parametrize("empty", [False, True], ids=["nonempty", "empty"])
@pytest.mark.parametrize("surface", ["structured", "relational"])
def test_distinct_constant_counts_keep_source_population(route, empty, surface):
    document = _document(empty)
    if surface == "structured":
        rows = _query(
            document,
            route,
            {"metrics": ["items.distinct_one", "items.distinct_null", "items.filtered_one"]},
        )
    else:
        from tests.interchange.ossie.test_layer2_conformance import execute

        rows, _ = execute(
            route,
            "SELECT AGG(distinct_one), AGG(distinct_null), AGG(filtered_one) FROM constants.items",
            document,
        )
    assert rows == [(0 if empty else 1, 0, 0)]
