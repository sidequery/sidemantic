"""Tests for raw-data-file project resolution in sidemantic/project.py.

Covers ``--db`` pointing at a data file or directory, auto-discovery of a
``data/*.csv`` project, unchanged ``.db`` behavior, and the conventional-root
marker for a ``data/`` directory that only contains data files.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from sidemantic.project import ProjectContext


@pytest.mark.parametrize("suffix", ["csv", "json", "parquet", "duckdb", "db"])
@pytest.mark.parametrize("directory_link", [False, True])
def test_implicit_discovery_excludes_external_symlinks(tmp_path, suffix, directory_link):
    project = tmp_path / "project"
    (project / "models").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / f"private.{suffix}"
    target.write_text("id\n1\n")
    data = project / "data"
    if directory_link:
        data.symlink_to(outside, target_is_directory=True)
    else:
        data.mkdir()
        (data / target.name).symlink_to(target)
    context = ProjectContext.discover(project)
    assert context.resolve_connection() is None
    # Explicitly choosing the same external file remains authorized.
    assert context.resolve_connection(database=str(target)) is not None


def test_internal_data_symlink_remains_queryable(tmp_path):
    (tmp_path / "models").mkdir()
    data = tmp_path / "data"
    data.mkdir()
    source = tmp_path / "source.csv"
    source.write_text("id\n42\n")
    (data / "orders.csv").symlink_to(source)
    resolved = ProjectContext.discover(tmp_path).resolve_connection(required=True)
    with duckdb.connect() as connection:
        for statement in resolved.init_sql:
            connection.execute(statement)
        assert connection.execute("select id from orders").fetchall() == [(42,)]


def test_explicit_external_models_keeps_its_conventional_data(tmp_path):
    local = tmp_path / "local"
    (local / "models").mkdir(parents=True)
    external = tmp_path / "external"
    (external / "models").mkdir(parents=True)
    (external / "data").mkdir()
    (external / "data" / "orders.csv").write_text("id\n1\n")
    resolved = ProjectContext.discover(local).resolve_connection(models=str(external / "models"), required=True)
    assert resolved.source == "project data files"


def test_db_option_pointing_at_csv_file_builds_in_memory_view(tmp_path: Path):
    csv = tmp_path / "orders.csv"
    csv.write_text("id,amount\n1,10\n2,20\n")

    resolved = ProjectContext.discover(tmp_path).resolve_connection(database=str(csv))

    assert resolved is not None
    assert resolved.connection == "duckdb:///:memory:"
    assert resolved.source == "--db"
    assert resolved.init_sql and resolved.init_sql[0].startswith("CREATE OR REPLACE VIEW")


def test_db_option_pointing_at_directory_of_csvs_builds_views(tmp_path: Path):
    data = tmp_path / "raw"
    data.mkdir()
    (data / "orders.csv").write_text("id\n1\n")
    (data / "customers.csv").write_text("id\n1\n")

    resolved = ProjectContext.discover(tmp_path).resolve_connection(database=str(data))

    assert resolved is not None
    assert resolved.connection == "duckdb:///:memory:"
    assert resolved.source == "--db"
    assert len(resolved.init_sql) == 2


def test_auto_discovery_of_data_csvs_when_no_database(tmp_path: Path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "orders.csv").write_text("id\n1\n")

    resolved = ProjectContext.discover(tmp_path).resolve_connection(required=True)

    assert resolved is not None
    assert resolved.connection == "duckdb:///:memory:"
    assert resolved.source == "project data files"
    assert resolved.init_sql and len(resolved.init_sql) == 1


def test_existing_db_file_still_wins_over_data_files(tmp_path: Path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "orders.csv").write_text("id\n1\n")
    db_path = data / "warehouse.duckdb"
    duckdb.connect(str(db_path)).close()

    resolved = ProjectContext.discover(tmp_path).resolve_connection(required=True)

    assert resolved is not None
    assert resolved.connection == f"duckdb:///{db_path}"
    assert resolved.source == "project data"
    assert resolved.database == db_path


def test_data_dir_with_csvs_is_a_conventional_root_marker(tmp_path: Path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "orders.csv").write_text("id\n1\n")
    nested = tmp_path / "work" / "nested"
    nested.mkdir(parents=True)

    context = ProjectContext.discover(nested)

    # Walking up from a deep subdir, the data/ CSVs mark tmp_path as the root.
    assert context.root == tmp_path
    resolved = context.resolve_connection(required=True)
    assert resolved is not None
    assert resolved.source == "project data files"
