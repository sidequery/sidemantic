"""Tests for MCP server error paths and helpers."""

import pytest

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
