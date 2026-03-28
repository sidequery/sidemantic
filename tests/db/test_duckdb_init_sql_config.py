"""Tests for init_sql config parsing and plumbing."""

from sidemantic.config import (
    DuckDBConnection,
    SidemanticConfig,
    build_connection_string,
    get_init_sql,
    load_config,
)


def test_duckdb_connection_init_sql_field():
    """Test DuckDBConnection accepts init_sql."""
    conn = DuckDBConnection(path=":memory:", init_sql=["LOAD httpfs", "LOAD iceberg"])
    assert conn.init_sql == ["LOAD httpfs", "LOAD iceberg"]


def test_duckdb_connection_init_sql_none_by_default():
    """Test DuckDBConnection init_sql defaults to None."""
    conn = DuckDBConnection(path=":memory:")
    assert conn.init_sql is None


def test_get_init_sql_returns_statements():
    """Test get_init_sql extracts init_sql from config."""
    config = SidemanticConfig(
        connection=DuckDBConnection(
            path=":memory:",
            init_sql=["INSTALL httpfs", "LOAD httpfs"],
        )
    )
    result = get_init_sql(config)
    assert result == ["INSTALL httpfs", "LOAD httpfs"]


def test_get_init_sql_returns_none_when_no_init_sql():
    """Test get_init_sql returns None when no init_sql configured."""
    config = SidemanticConfig(connection=DuckDBConnection(path=":memory:"))
    assert get_init_sql(config) is None


def test_get_init_sql_returns_none_for_non_duckdb():
    """Test get_init_sql returns None for non-DuckDB connections."""
    config = SidemanticConfig(connection=None)
    assert get_init_sql(config) is None


def test_load_config_with_init_sql(tmp_path):
    """Test loading YAML config with init_sql."""
    config_path = tmp_path / "sidemantic.yaml"
    config_path.write_text(
        """
models_dir: .
connection:
  type: duckdb
  path: ":memory:"
  init_sql:
    - "INSTALL httpfs"
    - "LOAD httpfs"
    - "ATTACH 's3://bucket/db.duckdb' AS remote"
"""
    )
    config = load_config(config_path)
    assert config.connection is not None
    init_sql = get_init_sql(config)
    assert init_sql == [
        "INSTALL httpfs",
        "LOAD httpfs",
        "ATTACH 's3://bucket/db.duckdb' AS remote",
    ]


def test_build_connection_string_ignores_init_sql():
    """Test that build_connection_string produces URL without init_sql."""
    config = SidemanticConfig(
        connection=DuckDBConnection(
            path=":memory:",
            init_sql=["LOAD httpfs"],
        )
    )
    url = build_connection_string(config)
    assert url == "duckdb:///:memory:"
    assert "init_sql" not in url


def test_resolve_paths_preserves_init_sql(tmp_path):
    """Test that resolve_paths keeps init_sql when resolving DuckDB paths."""
    config = SidemanticConfig(
        connection=DuckDBConnection(
            path="data/warehouse.db",
            init_sql=["LOAD httpfs"],
        )
    )
    resolved = config.resolve_paths(tmp_path)
    assert isinstance(resolved.connection, DuckDBConnection)
    assert resolved.connection.init_sql == ["LOAD httpfs"]
