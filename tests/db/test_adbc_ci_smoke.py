"""Best-effort ADBC integration smoke tests for CI.

These tests are intended to run in CI against whatever ADBC drivers are available.
If an ADBC driver (or a usable endpoint) is not available for a given DB, the
tests will skip rather than fail.
"""

from __future__ import annotations

import importlib
import os

import pytest

from sidemantic import Metric, Model, SemanticLayer

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("ADBC_TEST") != "1", reason="Set ADBC_TEST=1 to run ADBC CI smoke tests"),
]


def _adbc_db() -> str:
    db = os.getenv("ADBC_DB")
    if not db:
        pytest.skip("ADBC_DB is not set")
    return db.lower()


def _expected_dialect(db: str) -> str:
    expected = {
        "postgres": "postgres",
        "bigquery": "bigquery",
        "snowflake": "snowflake",
        "clickhouse": "clickhouse",
    }.get(db)
    if not expected:
        pytest.skip(f"Unsupported ADBC_DB={db!r}")
    return expected


def _target_uri(db: str) -> str:
    if db == "postgres":
        uri = os.getenv("POSTGRES_URL")
        if not uri:
            pytest.skip("POSTGRES_URL is not set")
        return uri

    if db == "bigquery":
        emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST")
        if emulator_host:
            os.environ["BIGQUERY_EMULATOR_HOST"] = emulator_host
        project = os.getenv("BIGQUERY_PROJECT", "test-project")
        dataset = os.getenv("BIGQUERY_DATASET", "test_dataset")
        return f"bigquery://{project}/{dataset}"

    if db == "snowflake":
        return "snowflake://test:test@test/testdb/public?warehouse=test_warehouse"

    if db == "clickhouse":
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = os.getenv("CLICKHOUSE_PORT", "8123")
        password = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
        return f"clickhouse://default:{password}@{host}:{port}/default"

    pytest.skip(f"Unsupported ADBC_DB={db!r}")


def _driver_suffix(db: str) -> str:
    if db == "postgres":
        return "postgresql"
    return db


def _candidate_adbc_drivers(db: str) -> list[str]:
    suffix = _driver_suffix(db)
    candidates: list[str] = []

    pkg_name = f"adbc_driver_{suffix}"
    try:
        importlib.import_module(pkg_name)
    except Exception:
        pass
    else:
        candidates.append(pkg_name)

    candidates.append(suffix)
    return candidates


@pytest.fixture(scope="module")
def adbc_layer() -> SemanticLayer:
    db = _adbc_db()
    uri = _target_uri(db)
    candidates = _candidate_adbc_drivers(db)

    from sidemantic.db.adbc import ADBCAdapter

    last_exc: Exception | None = None
    for driver in candidates:
        try:
            adapter = ADBCAdapter(driver=driver, uri=uri)
        except Exception as exc:
            last_exc = exc
            continue
        layer = SemanticLayer(connection=adapter)
        yield layer
        try:
            adapter.close()
        except Exception:
            pass
        return

    details = f"Tried drivers={candidates!r}. URI={uri!r}."
    if last_exc is not None:
        details += f" Last error={last_exc!r}"
    pytest.skip(f"No working ADBC driver for {db!r}. {details}")


def test_adbc_smoke_basic_execute(adbc_layer: SemanticLayer) -> None:
    result = adbc_layer.adapter.execute("SELECT 1 as x, 2 as y")
    assert result.fetchone() == (1, 2)


def test_adbc_smoke_semantic_layer_query_sum(adbc_layer: SemanticLayer) -> None:
    orders = Model(
        name="orders",
        table="(SELECT 1 as id, 10 as amount UNION ALL SELECT 2, 20)",
        primary_key="id",
        metrics=[Metric(name="total_amount", agg="sum", sql="amount")],
    )
    adbc_layer.add_model(orders)

    result = adbc_layer.query(metrics=["orders.total_amount"])
    row = result.fetchone()
    assert row is not None
    assert float(row[0]) == 30.0


def test_adbc_smoke_dialect(adbc_layer: SemanticLayer) -> None:
    expected = _expected_dialect(_adbc_db())
    assert adbc_layer.dialect == expected
