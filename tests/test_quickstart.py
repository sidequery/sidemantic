"""Tests for project scaffolding (sidemantic/quickstart.py)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from sidemantic.quickstart import (
    create_demo_database,
    scaffold_demo_project,
    scaffold_starter_project,
)


def test_scaffold_starter_project_creates_expected_files(tmp_path: Path):
    result = scaffold_starter_project(tmp_path)

    created = {path.relative_to(tmp_path).as_posix() for path in result.created}
    assert created == {"sidemantic.yaml", "models/orders.yml", "tests/orders.yml"}
    for name in created:
        assert (tmp_path / name).is_file()


def test_scaffold_starter_project_rerun_without_force_raises(tmp_path: Path):
    scaffold_starter_project(tmp_path)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        scaffold_starter_project(tmp_path)


def test_scaffold_starter_project_force_overwrites(tmp_path: Path):
    scaffold_starter_project(tmp_path)
    (tmp_path / "models" / "orders.yml").write_text("models: []\n")

    result = scaffold_starter_project(tmp_path, force=True)

    assert (tmp_path / "models" / "orders.yml") in result.created
    assert "orders" in (tmp_path / "models" / "orders.yml").read_text()


def test_scaffold_demo_project_creates_files_and_database(tmp_path: Path):
    result = scaffold_demo_project(tmp_path)

    created = {path.relative_to(tmp_path).as_posix() for path in result.created}
    assert "sidemantic.yaml" in created
    assert "tests/demo.yml" in created
    assert "models/customers.yml" in created
    assert "models/products.yml" in created
    assert "models/orders.yml" in created
    assert "data/demo.duckdb" in created
    assert (tmp_path / "data" / "demo.duckdb").is_file()


def test_scaffold_demo_project_rerun_without_force_raises(tmp_path: Path):
    scaffold_demo_project(tmp_path)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        scaffold_demo_project(tmp_path)


def test_scaffold_demo_project_force_overwrites(tmp_path: Path):
    scaffold_demo_project(tmp_path)
    # Should not raise and should rebuild the database file.
    result = scaffold_demo_project(tmp_path, force=True)
    assert (tmp_path / "data" / "demo.duckdb") in result.created


def test_demo_database_has_expected_tables_and_rows(tmp_path: Path):
    db_path = tmp_path / "demo.duckdb"
    create_demo_database(db_path)

    con = duckdb.connect(str(db_path))
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        assert {"customers", "products", "orders"} <= tables
        assert con.execute("SELECT count(*) FROM customers").fetchone()[0] == 8
        assert con.execute("SELECT count(*) FROM products").fetchone()[0] == 8
        # Orders are generated deterministically (seeded RNG) so there is data.
        assert con.execute("SELECT count(*) FROM orders").fetchone()[0] > 0
    finally:
        con.close()


def test_demo_database_is_deterministic(tmp_path: Path):
    first = tmp_path / "a.duckdb"
    second = tmp_path / "b.duckdb"
    create_demo_database(first)
    create_demo_database(second)

    con_a = duckdb.connect(str(first))
    con_b = duckdb.connect(str(second))
    try:
        count_a = con_a.execute("SELECT count(*) FROM orders").fetchone()[0]
        count_b = con_b.execute("SELECT count(*) FROM orders").fetchone()[0]
        assert count_a == count_b
    finally:
        con_a.close()
        con_b.close()
