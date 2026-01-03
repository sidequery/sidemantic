"""Tests for MotherDuck adapter."""

import pytest

from sidemantic.db.motherduck import MotherDuckAdapter


def test_motherduck_from_url_matrix(monkeypatch):
    cases = [
        ("duckdb://md:db", {"database": "db", "token": None}),
        ("duckdb://md:warehouse", {"database": "warehouse", "token": None}),
        ("duckdb://md:", {"database": "my_db", "token": None}),
        ("duckdb://md:my_db", {"database": "my_db", "token": None}),
        ("duckdb://md:db_name", {"database": "db_name", "token": None}),
        ("duckdb://md:db-name", {"database": "db-name", "token": None}),
        ("duckdb://md:db123", {"database": "db123", "token": None}),
    ]

    for url, expected in cases:
        captured = {}

        def fake_init(self, database="my_db", token=None, **kwargs):
            captured["database"] = database
            captured["token"] = token

        monkeypatch.setattr(MotherDuckAdapter, "__init__", fake_init)
        adapter = MotherDuckAdapter.from_url(url)
        assert isinstance(adapter, MotherDuckAdapter)

        for key, value in expected.items():
            assert captured[key] == value


def test_motherduck_from_url_invalid():
    with pytest.raises(ValueError, match="Invalid MotherDuck URL"):
        MotherDuckAdapter.from_url("duckdb://local")

    with pytest.raises(ValueError, match="Invalid MotherDuck URL"):
        MotherDuckAdapter.from_url("duckdb://md")

    with pytest.raises(ValueError, match="Invalid MotherDuck URL"):
        MotherDuckAdapter.from_url("duckdb://mdx:db")
