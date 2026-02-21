"""Tests for MCP server error paths and helpers."""

import pytest

pytest.importorskip("mcp")  # Skip if mcp extra not installed

from sidemantic import mcp_server


def test_get_layer_requires_init():
    mcp_server._layer = None
    with pytest.raises(RuntimeError, match="not initialized"):
        mcp_server.get_layer()


def test_format_field_name():
    assert mcp_server._format_field_name("orders.total_revenue") == "Total Revenue"
    assert mcp_server._format_field_name("created_at__month") == "Created At (Month)"


def test_generate_chart_title():
    title = mcp_server._generate_chart_title(["orders.created_at__month"], ["orders.total_revenue"])
    assert "Total Revenue" in title
    assert "Created At" in title


def test_validate_filter_accepts_valid_filters():
    mcp_server._validate_filter("status = 'active'")
    mcp_server._validate_filter("amount > 100 AND region = 'US'")
    mcp_server._validate_filter("created_at >= '2024-01-01'")


def test_validate_filter_rejects_drop_table():
    with pytest.raises(ValueError, match="disallowed SQL"):
        mcp_server._validate_filter("1=1; DROP TABLE users")


def test_validate_filter_rejects_insert():
    with pytest.raises(ValueError, match="disallowed SQL"):
        mcp_server._validate_filter("1=1; INSERT INTO users VALUES (1)")


def test_validate_filter_rejects_delete():
    with pytest.raises(ValueError, match="disallowed SQL"):
        mcp_server._validate_filter("1=1; DELETE FROM users")


def test_validate_filter_rejects_invalid_sql():
    with pytest.raises(ValueError, match="Invalid filter"):
        mcp_server._validate_filter(")))invalid((( sql garbage")
