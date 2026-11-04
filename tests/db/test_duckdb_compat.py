"""DuckDB Arrow compatibility across old and current result APIs."""

import pytest

from sidemantic.db.duckdb import DuckDBAdapter


@pytest.mark.parametrize("reader_method", ["fetch_record_batch", "to_arrow_reader"])
@pytest.mark.parametrize("adapter_kind", ["duckdb", "motherduck"])
def test_duckdb_arrow_reader_api_compatibility(reader_method, adapter_kind):
    """Both DuckDB adapters accept legacy and current driver result interfaces."""
    pa = pytest.importorskip("pyarrow")
    from sidemantic.db.motherduck import MotherDuckAdapter

    table = pa.table({"value": [1, None, 3]})
    result_type = type("DriverResult", (), {reader_method: lambda self: table.to_reader()})
    adapter_type = DuckDBAdapter if adapter_kind == "duckdb" else MotherDuckAdapter
    # Arrow conversion needs only the result, so no MotherDuck cloud connection.
    adapter = object.__new__(adapter_type)
    assert adapter.fetch_record_batch(result_type()).read_all() == table


def test_duckdb_independent_cursor_arrow_results():
    """Server cursor results use the same Arrow conversion as ordinary queries."""
    pytest.importorskip("pyarrow")
    adapter = DuckDBAdapter()
    try:
        with adapter.cursor() as cursor:
            result = cursor.execute("SELECT 1 AS value UNION ALL SELECT NULL")
            assert adapter.fetch_record_batch(result).read_all().to_pydict() == {"value": [1, None]}
    finally:
        adapter.close()
