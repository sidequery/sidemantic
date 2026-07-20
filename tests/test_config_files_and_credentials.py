"""Tests for the ``files`` connection type and password_file credential handling."""

from __future__ import annotations

from pathlib import Path

import pytest

from sidemantic.config import (
    ClickHouseConnection,
    FilesConnection,
    PostgreSQLConnection,
    SidemanticConfig,
    SnowflakeConnection,
    SparkConnection,
    _connection_password,
    build_connection_string,
    get_init_sql,
    load_config,
)

# --- files connection ---------------------------------------------------------


def test_files_connection_expands_globs_relative_to_config_dir(tmp_path: Path):
    (tmp_path / "b.csv").write_text("id\n1\n")
    (tmp_path / "a.csv").write_text("id\n1\n")

    config = SidemanticConfig(connection=FilesConnection(type="files", paths=["*.csv"]))
    resolved = config.resolve_paths(tmp_path)

    assert isinstance(resolved.connection, FilesConnection)
    # Glob matches are absolute, sorted, and rooted at the config directory.
    names = [Path(path).name for path in resolved.connection.paths]
    assert names == ["a.csv", "b.csv"]
    assert all(Path(path).is_absolute() for path in resolved.connection.paths)


def test_files_connection_resolves_plain_relative_paths(tmp_path: Path):
    (tmp_path / "orders.csv").write_text("id\n1\n")

    config = SidemanticConfig(connection=FilesConnection(type="files", paths=["orders.csv"]))
    resolved = config.resolve_paths(tmp_path)

    assert Path(resolved.connection.paths[0]) == (tmp_path / "orders.csv").resolve()


def test_files_connection_empty_glob_is_an_error(tmp_path: Path):
    config = SidemanticConfig(connection=FilesConnection(type="files", paths=["missing_*.csv"]))
    with pytest.raises(ValueError, match="matched nothing"):
        config.resolve_paths(tmp_path)


def test_files_connection_missing_plain_path_is_an_error(tmp_path: Path):
    config = SidemanticConfig(connection=FilesConnection(type="files", paths=["orders.csv"]))
    with pytest.raises(ValueError, match="path not found"):
        config.resolve_paths(tmp_path)


def test_files_connection_get_init_sql_returns_create_views(tmp_path: Path):
    (tmp_path / "orders.csv").write_text("id\n1\n")
    (tmp_path / "customers.csv").write_text("id\n1\n")

    config = SidemanticConfig(connection=FilesConnection(type="files", paths=["*.csv"]))
    resolved = config.resolve_paths(tmp_path)

    statements = get_init_sql(resolved)
    assert statements is not None
    assert len(statements) == 2
    assert all(stmt.startswith("CREATE OR REPLACE VIEW") for stmt in statements)


def test_files_connection_string_is_in_memory_duckdb(tmp_path: Path):
    (tmp_path / "orders.csv").write_text("id\n1\n")
    config = SidemanticConfig(connection=FilesConnection(type="files", paths=["orders.csv"]))
    resolved = config.resolve_paths(tmp_path)
    assert build_connection_string(resolved) == "duckdb:///:memory:"


def test_files_connection_loads_from_yaml(tmp_path: Path):
    (tmp_path / "orders.csv").write_text("id\n1\n")
    config_path = tmp_path / "sidemantic.yaml"
    config_path.write_text("models_dir: models\nconnection:\n  type: files\n  paths:\n    - orders.csv\n")

    config = load_config(config_path)

    assert isinstance(config.connection, FilesConnection)
    assert build_connection_string(config) == "duckdb:///:memory:"
    assert len(get_init_sql(config)) == 1


# --- password_file credentials ------------------------------------------------


def test_password_file_is_read_and_stripped(tmp_path: Path):
    secret = tmp_path / "pg.pw"
    secret.write_text("s3cr3t\n")
    connection = PostgreSQLConnection(type="postgres", host="h", database="db", username="u", password_file=str(secret))
    assert _connection_password(connection) == "s3cr3t"


def test_inline_password_used_when_no_file():
    connection = PostgreSQLConnection(type="postgres", host="h", database="db", username="u", password="inline")
    assert _connection_password(connection) == "inline"


@pytest.mark.parametrize(
    "connection_factory",
    [
        lambda pf: PostgreSQLConnection(
            type="postgres", host="h", database="db", username="u", password="x", password_file=pf
        ),
        lambda pf: ClickHouseConnection(type="clickhouse", host="h", password="x", password_file=pf),
        lambda pf: SnowflakeConnection(type="snowflake", account="a", username="u", password="x", password_file=pf),
        lambda pf: SparkConnection(type="spark", host="h", username="u", password="x", password_file=pf),
    ],
)
def test_password_and_password_file_together_is_an_error(tmp_path: Path, connection_factory):
    secret = tmp_path / "pw"
    secret.write_text("s")
    with pytest.raises(ValueError, match="both password and password_file"):
        _connection_password(connection_factory(str(secret)))


def test_missing_password_file_is_an_error(tmp_path: Path):
    connection = PostgreSQLConnection(
        type="postgres", host="h", database="db", username="u", password_file=str(tmp_path / "nope")
    )
    with pytest.raises(ValueError, match="password_file not found"):
        _connection_password(connection)


def test_postgres_connection_string_uses_password_file(tmp_path: Path):
    secret = tmp_path / "pg.pw"
    secret.write_text("hunter2\n")
    config = SidemanticConfig(
        connection=PostgreSQLConnection(
            type="postgres",
            host="db.example",
            port=5432,
            database="analytics",
            username="reader",
            password_file=str(secret),
        )
    )
    assert build_connection_string(config) == "postgres://reader:hunter2@db.example:5432/analytics"


def test_snowflake_requires_password_or_password_file():
    config = SidemanticConfig(connection=SnowflakeConnection(type="snowflake", account="acct", username="u"))
    with pytest.raises(ValueError, match="requires password or password_file"):
        build_connection_string(config)


def test_snowflake_connection_string_uses_password_file(tmp_path: Path):
    secret = tmp_path / "snow.pw"
    secret.write_text("topsecret\n")
    config = SidemanticConfig(
        connection=SnowflakeConnection(type="snowflake", account="acct", username="svc", password_file=str(secret))
    )
    assert build_connection_string(config) == "snowflake://svc:topsecret@acct"


def test_resolve_paths_makes_password_file_absolute(tmp_path: Path):
    secret = tmp_path / "pg.pw"
    secret.write_text("s3cr3t\n")
    config = SidemanticConfig(
        connection=PostgreSQLConnection(type="postgres", host="h", database="db", username="u", password_file="pg.pw")
    )
    resolved = config.resolve_paths(tmp_path)
    assert Path(resolved.connection.password_file) == secret.resolve()
    # And the resolved credential still reads correctly end-to-end.
    assert build_connection_string(resolved) == "postgres://u:s3cr3t@h:5432/db"
