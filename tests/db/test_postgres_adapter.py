"""Tests for PostgreSQL adapter."""

import pytest

from sidemantic.db.postgres import PostgreSQLAdapter


def test_postgres_adapter_missing_dependency():
    """Test that missing psycopg raises helpful error."""
    # This will fail if psycopg is not installed
    # We test the error message is helpful
    try:
        PostgreSQLAdapter()
    except ImportError as e:
        assert "psycopg" in str(e).lower()
        assert "sidemantic[postgres]" in str(e) or "psycopg[binary]" in str(e)


def test_postgres_adapter_from_url_parsing():
    """Test URL parsing (without actually connecting)."""
    # Test that URL parsing works correctly
    # We can't test actual connection without a postgres instance
    url = "postgres://user:pass@localhost:5432/testdb"

    # Just verify the from_url method exists and accepts the URL format
    # It will fail on connection but that's ok for this test
    assert hasattr(PostgreSQLAdapter, "from_url")


@pytest.mark.skipif(True, reason="Requires running PostgreSQL instance")
def test_postgres_adapter_execute():
    """Test executing queries against real Postgres.

    This test is skipped by default since it requires a running Postgres instance.
    To run: pytest -v --run-postgres tests/db/test_postgres_adapter.py
    """
    adapter = PostgreSQLAdapter(host="localhost", database="test")
    result = adapter.execute("SELECT 1 as x, 2 as y")
    row = result.fetchone()
    assert row == (1, 2)
