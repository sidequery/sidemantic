"""Tests for PostgreSQL adapter."""

import pytest


def test_postgres_adapter_import():
    """Test that PostgreSQL adapter can be imported."""
    try:
        from sidemantic.db.postgres import PostgreSQLAdapter

        assert hasattr(PostgreSQLAdapter, "from_url")
        assert hasattr(PostgreSQLAdapter, "execute")
    except ImportError as e:
        # If psycopg is not installed, should get helpful error
        assert "psycopg" in str(e).lower()


def test_postgres_url_parsing():
    """Test URL parsing logic (without connecting)."""
    try:
        from sidemantic.db.postgres import PostgreSQLAdapter
    except ImportError:
        pytest.skip("psycopg not installed")

    # Test that from_url method exists
    assert hasattr(PostgreSQLAdapter, "from_url")
    # URL parsing is tested in integration tests where we can actually connect


@pytest.mark.skipif(True, reason="Requires running PostgreSQL instance")
def test_postgres_adapter_execute():
    """Test executing queries against real Postgres.

    This test is skipped by default since it requires a running Postgres instance.
    Use integration tests instead.
    """
    from sidemantic.db.postgres import PostgreSQLAdapter

    adapter = PostgreSQLAdapter(host="localhost", database="test")
    result = adapter.execute("SELECT 1 as x, 2 as y")
    row = result.fetchone()
    assert row == (1, 2)
