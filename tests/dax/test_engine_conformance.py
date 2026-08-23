"""Opt-in conformance probes against Microsoft's Power BI DAX engine.

This is test-only integration. It requires a dedicated Premium/Fabric semantic
model plus a short-lived token; no Power BI client is included in the package.

Run with::

    SIDEMANTIC_DAX_ENGINE_CONFORMANCE=1 \
    POWERBI_WORKSPACE_ID=... POWERBI_DATASET_ID=... POWERBI_ACCESS_TOKEN=... \
    uv run pytest -m integration tests/dax/test_engine_conformance.py -q

The endpoint returns DAX failures inside Arrow error streams with HTTP 200, so
transport success must not be mistaken for engine acceptance.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import pytest

dax_ast = pytest.importorskip("sidemantic_dax.ast")
pa = pytest.importorskip("pyarrow")
ipc = pytest.importorskip("pyarrow.ipc")


@dataclass(frozen=True)
class ConformanceCase:
    name: str
    dax: str
    local_accepts: bool
    engine_accepts: bool


@dataclass(frozen=True)
class EngineResult:
    accepts: bool
    errors: tuple[dict[str, Any], ...]


class EngineHarnessError(RuntimeError):
    """Authentication, transport, or malformed-response failure."""


CASES = (
    ConformanceCase(
        "row",
        'EVALUATE ROW("x", 1)',
        local_accepts=True,
        engine_accepts=True,
    ),
    ConformanceCase(
        "datetime_literal",
        'EVALUATE { dt"2020-12-15T12:30:59" }',
        local_accepts=True,
        engine_accepts=True,
    ),
    ConformanceCase(
        "function_default",
        """
        DEFINE
          FUNCTION AddOne = (x: SCALAR NUMERIC = 1) => x + 1
        EVALUATE { AddOne() }
        """,
        local_accepts=True,
        engine_accepts=True,
    ),
    ConformanceCase(
        "visual_shape",
        """
        DEFINE
          TABLE data = ROW("Year", 2000, "IsTotal", FALSE())
          WITH VISUAL SHAPE
            AXIS ROWS GROUP [Year] TOTAL [IsTotal] ORDER BY [Year]
            DENSIFY "IsDensified"
        EVALUATE data
        """,
        local_accepts=True,
        engine_accepts=True,
    ),
    ConformanceCase(
        "start_at_constant",
        'EVALUATE DATATABLE("A", INTEGER, {{1}}) ORDER BY [A] START AT 1',
        local_accepts=True,
        engine_accepts=True,
    ),
    ConformanceCase(
        "evaluate_scalar",
        "EVALUATE 1",
        local_accepts=False,
        engine_accepts=False,
    ),
    ConformanceCase(
        "unknown_function",
        'EVALUATE ROW("x", TOTALLY_NOT_DAX(1, 2))',
        # The local parser cannot reject an unknown call because it may name a
        # query UDF. The engine resolves the name against the complete query.
        local_accepts=True,
        engine_accepts=False,
    ),
    ConformanceCase(
        "invalid_arity",
        'EVALUATE ROW("x", SUM())',
        local_accepts=False,
        engine_accepts=False,
    ),
    ConformanceCase(
        "sumx_ignores_text",
        'EVALUATE ROW("x", SUMX(DATATABLE("Text", STRING, {{"x"}}), [Text]))',
        # Microsoft documents that SUMX ignores non-numeric expression
        # results rather than rejecting the expression.
        local_accepts=True,
        engine_accepts=True,
    ),
    ConformanceCase(
        "datatable_row_width",
        'EVALUATE DATATABLE("A", INTEGER, {{1, 2}})',
        local_accepts=False,
        engine_accepts=False,
    ),
    ConformanceCase(
        "start_at_expression",
        'EVALUATE DATATABLE("A", INTEGER, {{1}}) ORDER BY [A] START AT 1 + 2',
        local_accepts=False,
        engine_accepts=False,
    ),
    ConformanceCase(
        "malformed_parenthesis",
        'EVALUATE ROW("x", (1 + 2)',
        local_accepts=False,
        engine_accepts=False,
    ),
)


def _text(value: bytes | str) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value


def _parse_arrow_response(payload: bytes) -> EngineResult:
    if not payload:
        raise EngineHarnessError("Microsoft engine returned an empty Arrow response")

    source = pa.BufferReader(payload)
    errors: list[dict[str, Any]] = []
    stream_count = 0
    while source.tell() < len(payload):
        position = source.tell()
        try:
            reader = ipc.open_stream(source)
            table = reader.read_all()
        except (pa.ArrowException, OSError) as exc:
            raise EngineHarnessError(f"invalid Arrow stream at byte offset {position}: {exc}") from exc
        stream_count += 1
        metadata = {_text(key): _text(value) for key, value in (reader.schema.metadata or {}).items()}
        if metadata.get("IsError", "").lower() == "true":
            rows = table.to_pylist() or [{}]
            errors.extend({"metadata": metadata, "details": row} for row in rows)
        if source.tell() <= position:
            raise EngineHarnessError("Arrow reader did not consume response data")

    if stream_count == 0:
        raise EngineHarnessError("Microsoft engine returned no Arrow streams")
    return EngineResult(accepts=not errors, errors=tuple(errors))


def _execute_query(
    dax: str,
    *,
    workspace_id: str,
    dataset_id: str,
    access_token: str,
) -> EngineResult:
    query_timeout = int(os.environ.get("POWERBI_QUERY_TIMEOUT_SECONDS", "30"))
    url = (
        "https://api.powerbi.com/v1.0/myorg/groups/"
        f"{quote(workspace_id, safe='')}/datasets/{quote(dataset_id, safe='')}"
        "/executeDaxQueries"
    )
    body = json.dumps(
        {
            "query": dax,
            "culture": "en-US",
            "queryTimeout": query_timeout,
            "schemaOnly": True,
            "resultSetRowCountLimit": 1,
            "applicationContext": json.dumps({"application": "sidemantic-dax-conformance"}),
        }
    ).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.apache.arrow.stream",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=query_timeout + 15) as response:
            content_type = response.headers.get_content_type()
            payload = response.read()
            status = response.status
    except HTTPError as exc:
        raise EngineHarnessError(f"Microsoft engine returned HTTP {exc.code}") from exc
    except URLError as exc:
        raise EngineHarnessError(f"Microsoft engine request failed: {exc.reason}") from exc

    if status != 200 or content_type != "application/vnd.apache.arrow.stream":
        raise EngineHarnessError(f"unexpected Microsoft engine response: HTTP {status}, {content_type}")
    return _parse_arrow_response(payload)


def _local_accepts(dax: str) -> bool:
    try:
        dax_ast.parse_query(dax)
        return not dax_ast.validate_query(dax)
    except ValueError:
        return False


def _engine_configuration() -> tuple[str, str, str]:
    if os.environ.get("SIDEMANTIC_DAX_ENGINE_CONFORMANCE") != "1":
        pytest.skip("set SIDEMANTIC_DAX_ENGINE_CONFORMANCE=1 to probe Power BI")
    names = ("POWERBI_WORKSPACE_ID", "POWERBI_DATASET_ID", "POWERBI_ACCESS_TOKEN")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        pytest.fail(f"missing engine conformance variables: {', '.join(missing)}")
    return tuple(os.environ[name] for name in names)  # type: ignore[return-value]


def test_arrow_error_stream_is_a_rejection():
    schema = pa.schema(
        [("ErrorCode", pa.string()), ("ErrorMessage", pa.string())],
        metadata={"IsError": "true", "FaultCode": "0xdeadbeef"},
    )
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_table(
            pa.Table.from_pylist(
                [{"ErrorCode": "QueryError", "ErrorMessage": "invalid DAX"}],
                schema=schema,
            )
        )

    result = _parse_arrow_response(sink.getvalue().to_pybytes())

    assert result.accepts is False
    assert result.errors[0]["metadata"]["FaultCode"] == "0xdeadbeef"
    assert result.errors[0]["details"]["ErrorCode"] == "QueryError"


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_local_conformance_expectations_are_current(case: ConformanceCase):
    assert _local_accepts(case.dax) is case.local_accepts


@pytest.mark.integration
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_local_and_microsoft_engine_conformance(case: ConformanceCase):
    workspace_id, dataset_id, access_token = _engine_configuration()

    assert _local_accepts(case.dax) is case.local_accepts
    result = _execute_query(
        case.dax,
        workspace_id=workspace_id,
        dataset_id=dataset_id,
        access_token=access_token,
    )
    assert result.accepts is case.engine_accepts, result.errors
