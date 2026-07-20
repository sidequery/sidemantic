"""Unit tests for the raw data-file helpers (sidemantic/datafiles.py)."""

from __future__ import annotations

from pathlib import Path

import pytest

from sidemantic.datafiles import (
    DATA_FILE_SUFFIXES,
    build_file_views,
    discover_data_files,
    is_data_file,
    reader_sql,
    table_name_for,
)


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("orders.csv", "orders"),
        ("Orders.CSV", "orders"),
        ("weird--name.csv", "weird__name"),
        ("2020 orders!.csv", "t_2020_orders"),
        ("__.csv", "data"),
    ],
)
def test_table_name_for_sanitizes_and_lowercases(filename: str, expected: str):
    assert table_name_for(Path(filename)) == expected


def test_table_name_leading_digit_gets_prefix():
    # Bare "9" would strip to an empty then digit-leading identifier.
    assert table_name_for(Path("9.csv")) == "t_9"


@pytest.mark.parametrize(
    ("filename", "reader"),
    [
        ("a.csv", "read_csv_auto"),
        ("a.tsv", "read_csv_auto"),
        ("a.parquet", "read_parquet"),
        ("a.json", "read_json_auto"),
        ("a.jsonl", "read_json_auto"),
        ("a.ndjson", "read_json_auto"),
    ],
)
def test_reader_sql_picks_reader_and_escapes_quotes(filename: str, reader: str):
    assert reader_sql(Path(filename)).startswith(f"{reader}('")
    # Single quotes in a path are doubled so the generated SQL stays valid.
    assert reader_sql(Path("o'brien.csv")) == "read_csv_auto('o''brien.csv')"


def test_is_data_file_matches_known_suffixes():
    assert is_data_file(Path("x.parquet"))
    assert not is_data_file(Path("x.txt"))
    assert not is_data_file(Path("x"))


def test_build_file_views_emits_create_view_per_file():
    views = build_file_views([Path("/data/orders.csv"), Path("/data/customers.parquet")])
    assert len(views) == 2
    assert views[0].startswith('CREATE OR REPLACE VIEW "orders" AS SELECT * FROM read_csv_auto(')
    assert views[1].startswith('CREATE OR REPLACE VIEW "customers" AS SELECT * FROM read_parquet(')


def test_build_file_views_rejects_duplicate_table_names():
    with pytest.raises(ValueError, match="both map to table 'orders'"):
        build_file_views([Path("/a/orders.csv"), Path("/b/orders.parquet")])


def test_build_file_views_rejects_unsupported_suffix():
    with pytest.raises(ValueError, match="Unsupported data file type"):
        build_file_views([Path("/a/notes.txt")])

    # The error lists the supported suffixes so the user can self-correct.
    with pytest.raises(ValueError) as excinfo:
        build_file_views([Path("/a/notes.txt")])
    for suffix in DATA_FILE_SUFFIXES:
        assert suffix in str(excinfo.value)


def test_discover_data_files_returns_sorted_data_files_only(tmp_path: Path):
    (tmp_path / "b.csv").write_text("id\n1\n")
    (tmp_path / "a.parquet").write_bytes(b"")
    (tmp_path / "notes.txt").write_text("skip me")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "nested.csv").write_text("id\n1\n")

    found = discover_data_files(tmp_path)

    assert [path.name for path in found] == ["a.parquet", "b.csv"]


def test_discover_data_files_on_missing_directory_is_empty(tmp_path: Path):
    assert discover_data_files(tmp_path / "does-not-exist") == []
    # A file (not a directory) also yields nothing rather than raising.
    a_file = tmp_path / "orders.csv"
    a_file.write_text("id\n1\n")
    assert discover_data_files(a_file) == []
