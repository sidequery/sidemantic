import os

import sidemantic_rs

execute_with_adbc = getattr(sidemantic_rs, "execute_with_adbc", None)
if not callable(execute_with_adbc):
    raise AssertionError("python-adbc wheel should expose execute_with_adbc")

try:
    execute_with_adbc("adbc_driver_missing_for_sidemantic_smoke", "select 1")
except RuntimeError as exc:
    message = str(exc)
    if "rust ADBC execution failed" not in message:
        raise AssertionError(f"unexpected ADBC error: {message}") from exc
else:
    raise AssertionError("missing test driver should fail through the Rust ADBC execution path")

duckdb_driver = os.environ.get("SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER")
if duckdb_driver:
    result = execute_with_adbc(
        duckdb_driver,
        "select 42 as answer",
        entrypoint="duckdb_adbc_init",
        db_kwargs={"path": ":memory:"},
    )
    if result.get("columns") != ["answer"]:
        raise AssertionError(f"unexpected DuckDB columns: {result!r}")
    if result.get("rows") != [(42,)]:
        raise AssertionError(f"unexpected DuckDB rows: {result!r}")

    uri_result = execute_with_adbc(
        duckdb_driver,
        "select 7 as answer",
        uri=":memory:",
        entrypoint="duckdb_adbc_init",
    )
    if uri_result.get("columns") != ["answer"]:
        raise AssertionError(f"unexpected DuckDB URI columns: {uri_result!r}")
    if uri_result.get("rows") != [(7,)]:
        raise AssertionError(f"unexpected DuckDB URI rows: {uri_result!r}")
