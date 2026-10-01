"""Embedding compiled SQL must terminate trailing line comments."""

import pytest

from sidemantic import SemanticLayer


@pytest.mark.parametrize(
    "suffix",
    [
        "\n-- used_preagg=true",
        "\n-- used_preagg=true\n-- sidemantic: models=orders",
        " -- source comment",
        "\r\n-- source comment",
    ],
)
def test_post_process_preserves_query_before_trailing_comment(suffix):
    layer = SemanticLayer(auto_register=False)
    try:
        sql = layer._apply_post_process(
            "SELECT '-- literal text' AS label, 41 AS value" + suffix,
            "SELECT label, value + 1 AS result FROM ({inner}) AS scoped",
        )
        assert layer.adapter.execute(sql).fetchall() == [("-- literal text", 42)]
    finally:
        layer.adapter.close()
