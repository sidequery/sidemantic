# Query serving operations

Sidemantic's HTTP and PostgreSQL-compatible servers apply conservative,
process-local limits. They are safeguards for a library server, not a
multi-tenant persistence, scheduling, alerting, or dashboard system.

## Safe defaults

The HTTP API defaults are:

| Setting | Default | HTTP CLI flag |
| --- | ---: | --- |
| Maximum result rows | 10,000 | `--max-rows` |
| Maximum JSON or Arrow response | 16 MiB | `--max-response-bytes` |
| Execution deadline | 30 seconds | `--execution-timeout-seconds` |
| Concurrent queries | 4 | `--max-concurrent-queries` |
| Queued queries | 16 | `--max-queued-queries` |
| Maximum queue wait | 5 seconds | `--queue-timeout-seconds` |
| Retained local query events | 1,000 | `--query-history-size` |

The PostgreSQL wire server uses the same defaults. Configure them in
`pg_server`; HTTP settings live in `api_server`:

```yaml
connection:
  type: duckdb
  path: data/warehouse.duckdb

api_server:
  max_rows: 10000
  max_response_bytes: 16777216
  execution_timeout_seconds: 30
  max_concurrent_queries: 4
  max_queued_queries: 16
  queue_timeout_seconds: 5
  query_history_size: 1000

pg_server:
  max_rows: 10000
  max_response_bytes: 16777216
  execution_timeout_seconds: 30
  max_concurrent_queries: 4
  max_queued_queries: 16
  queue_timeout_seconds: 5
```

HTTP query responses include `X-Sidemantic-Query-ID`,
`X-Sidemantic-Request-ID`, `X-Sidemantic-Row-Count`, and
`X-Sidemantic-Response-Bytes`. An incoming `X-Request-ID` becomes the local
request ID; otherwise Sidemantic generates one.

The server applies an outer `LIMIT` and stops consuming Arrow batches once a
limit is crossed. JSON and Arrow serialization are both checked against the
exact response-byte ceiling before a response is sent. The result cache only
sees already bounded tables.

HTTP policy failures are stable:

- `413`: serialized response is too large
- `422`: result exceeds the row ceiling
- `429`: the bounded queue is full
- `503`: queue wait exceeded its deadline
- `504`: query execution exceeded its deadline

## Timeouts, cancellation, and disconnects

The execution deadline is local. Sidemantic also requests a warehouse-side
statement timeout for PostgreSQL, Snowflake, and ClickHouse. Other adapters
record a clear diagnostic that no portable statement-timeout setting exists.

On an HTTP disconnect, deadline, PostgreSQL client disconnect, or result-limit
overflow, Sidemantic requests cancellation on the exact execution handle. The
base adapter recognizes common driver methods such as `cancel_safe()`,
`cancel()`, `interrupt()`, and `adbc_cancel()`. Cancellation is best effort:
some drivers or deployments do not expose it, and their warehouse work may
continue. The query event records whether cancellation was supported and the
diagnostic returned by the adapter.

An uncancellable timed-out worker retains its concurrency slot until it really
finishes. This prevents timed-out warehouse work from silently exceeding the
configured concurrency ceiling.

## DuckDB read-only serving

File-backed DuckDB connections used by `sidemantic query`, `sidemantic server
api`, `sidemantic server postgres`, and `sidemantic dashboard serve` default to
read-only. Each query uses an independently opened connection, allowing
concurrent readers and avoiding a needless writer-mode lock across processes.
In-memory DuckDB stays writable and serialized because it cannot be reopened
read-only onto the same data.

Set the mode explicitly in project configuration when needed:

```yaml
connection:
  type: duckdb
  path: data/warehouse.duckdb
  read_only: false
```

Or opt into writer mode for a serving/query command with `--read-write`.
`sidemantic preagg refresh` remains an explicit write workflow and always opens
DuckDB writable. DuckDB does not allow an in-place writer alongside existing
read-only processes using a different access mode; stop readers during refresh
or publish refreshed data through an operational file-swap workflow.

Use read-only warehouse credentials for remote databases. DuckDB read-only mode
does not add permissions to PostgreSQL, BigQuery, Snowflake, or other systems.

## Process-local query telemetry

Every `SemanticLayer` owns a lightweight `query_telemetry` hook. HTTP, PostgreSQL
wire, and direct `SemanticLayer.query()` / `.sql()` executions record a local
query ID, duration, row/byte counts when available, cache and pre-aggregation
use, timeout/cancellation/error state, and sanitized SQL metadata.

```python
layer.query_telemetry.add_listener(lambda event: logger.info("query", extra=event.to_dict()))

for event in layer.query_telemetry.history(limit=20):
    print(event.query_id, event.duration_ms, event.row_count, event.error)
```

SQL literals and comments are removed before retention. If SQL cannot be parsed,
only its SHA-256 fingerprint is retained. Listener failures are logged and never
change query success. History is bounded and process-local; export events through
the listener if an operator needs external observability.
