# Security (row-level filters, access gates, field visibility)

Sidemantic can enforce **model-level security policies** — per-user row filters and
access gates — plus **field-level visibility**. Enforcement happens at query-compile
time: the policy is baked into the generated SQL before it runs, so a scoped query can
never return rows the user isn't allowed to see.

Security is **opt-in per model**. A model without a `security` block is unrestricted.

## Defining a policy

Attach a `SecurityPolicy` to a model via `security:`:

```yaml
models:
  - name: orders
    table: orders
    primary_key: id
    security:
      access: "user.role in ['analyst', 'admin']"   # bool gate over the `user` namespace
      row_filters:
        - "tenant_id = {{ user.tenant_id }}"          # AND-ed into the model's own CTE
    dimensions:
      - name: region
        type: categorical
      - name: margin
        type: numeric
        public: false                                  # hidden when enforce_visibility is on
    metrics:
      - name: revenue
        agg: sum
        sql: amount
```

- **`access`** — a literal `bool`, or a Jinja boolean expression over the `user`
  namespace. A falsy result denies any query touching that model.
- **`row_filters`** — SQL fragment templates rendered per request over `user` and
  AND-ed into the model's own CTE, so rows are scoped **before** joins and aggregation
  (this is fan-out-safe across one-to-many joins).

The only template namespace is `user`, a dict of the requesting user's attributes.

### Row-filter value safety

Attribute values are always rendered as **type-correct SQL literals** — strings are
single-quoted and escaped, numbers/booleans render bare, `None` becomes `NULL`. This
holds whether or not the template author wraps the placeholder in quotes, so a value
like `"1 OR 1=1"` cannot break out of its predicate. Both of these are safe and
equivalent for a string attribute:

```yaml
row_filters: ["region = {{ user.region }}"]     # unquoted (recommended)
row_filters: ["region = '{{ user.region }}'"]   # quoted (author quotes are stripped)
```

## Deny-by-default

When a model declares a `security` block, a query that supplies **no** `user_attributes`
is denied (`SecurityError`) rather than run unscoped. Pass `user_attributes={}` to
represent an authenticated user with no special attributes (the access gate and row
filters then evaluate against an empty set; a filter referencing a missing attribute
still raises via `StrictUndefined`).

```python
layer = SemanticLayer()
# ... add a model with a security policy ...
layer.query(metrics=["orders.revenue"])                                   # SecurityError (deny-by-default)
layer.query(metrics=["orders.revenue"], user_attributes={"tenant_id": 1}) # scoped to tenant 1
```

## Field visibility

Build the layer with `enforce_visibility=True` to reject any query that references a
`public: false` dimension or metric — whether it is projected, **filtered on, or ordered
by** (so a hidden field cannot be used as an information-disclosure oracle):

```python
layer = SemanticLayer(enforce_visibility=True)
layer.compile(dimensions=["orders.margin"])                 # SecurityError
layer.compile(metrics=["orders.revenue"], filters=["orders.margin > 100"])  # SecurityError
```

## Server enforcement

### Structured query expressions

Caller-supplied filters support semantic fields and an allowlist of scalar SQL
functions. They cannot introduce physical data sources, arbitrary UDFs, or extra
SQL clauses. Put custom functions in trusted model dimensions and filter on those
dimensions. Model, segment, and security predicates retain their trusted SQL
capabilities. Specialized metric ordering accepts selected output fields with
optional direction and null ordering.

Parameter values are emitted as SQL literals using the query dialect, including
inside Jinja conditionals and loops. Control-flow comparisons still use the raw
typed parameter values.

### HTTP (`sidemantic server api`)

Client-supplied identity headers are rejected by default. Enable
`--trust-user-header` only behind a header-sanitizing proxy that authenticates users,
sets the JSON `X-Sidemantic-User` header, and owns the API bearer credential. Do not
give that credential to clients who can reach the trusted-header endpoint directly.
Custom applications can instead supply `create_app(user_attributes_resolver=...)`
to derive attributes from a verified principal. The shared bearer by itself does
not verify caller-supplied roles or tenants. Resolved attributes scope structured
queries, semantic SQL, mounted MCP tools, and result-cache keys.

| Flag | Effect |
|------|--------|
| `--trust-user-header` | Explicitly trust identity headers from an authenticated proxy; requires bearer authentication. |
| `--user-header NAME` | Header carrying the JSON user-attributes object (default `X-Sidemantic-User`). |
| `--require-user-attrs` | Reject data requests that lack the header (HTTP 400). |
| `--enforce-visibility` | Apply field-visibility enforcement. |

A `SecurityError` (denied access gate, deny-by-default, undefined attribute) maps to
**HTTP 403**.

The embedded web UI never reads bearer tokens from a URL, persists them in
`localStorage`/`sessionStorage`, or includes them in shareable links. When the API is
bearer-protected, the UI prompts once and exchanges the bearer through
`POST /auth/session` for a short-lived credential. Same-origin deployments receive an
HttpOnly, `SameSite=Strict` cookie. Cross-origin deployments receive a short-lived
credential kept only in memory and sent with the `Sidemantic-Session` authorization
scheme. The server stores only a SHA-256 digest of either session credential and expires
it after ten minutes. API clients may continue to send the configured bearer directly.

The `/sql` endpoint rewrites semantic queries through row and access policies.
When security is active, `/raw` and SQL referencing unproven physical sources are
denied with HTTP 403.

### PostgreSQL wire server

The connecting Postgres username is mapped to user attributes via a startup
`--user-attrs-file` (JSON mapping usernames to attribute dictionaries). Semantic
SQL is rewritten through access gates and row filters. Physical-source SQL is
rejected when security is active; catalog discovery omits inaccessible models.

### MCP server

Stdio MCP may use `--user-attrs-file` as a local process identity. HTTP MCP
(`--http` or `--apps`) requires `--auth-token-file` and rejects static identity
files. Install both optional extras for this transport: `uv add 'sidemantic[mcp,api]'`.
It uses the same authenticated `/mcp/` mount as the API. All mounted transport
methods require the API's bearer or browser session when authentication is configured.
Tools receive request-local identity and layer context; they never inherit a stdio
process identity. Mounted sessions are stateless to prevent identity retention
across requests. Both `run_query` and `run_sql` enforce row filters.

### HTTP and MCP resource limits

Defaults can be adjusted with `--max-result-rows`, `--max-response-bytes`,
`--query-timeout` (seconds), and `--max-concurrent-queries` on the API/MCP CLI, or
`create_app(server_limits=ServerLimits(...))` in a custom application.

By default, query transports reject results above 10,000 rows or 16 MiB, admit at most four
concurrent executions per server instance, and bound chart dimensions to 1–2,000 pixels
on each axis. DuckDB execution and result draining have a 30-second deadline
that interrupts the request's independent cursor. Other adapters still require
backend statement timeouts; their drivers may materialize a batch before the
transport can check its byte size. These controls do not impose a hard process
memory bound. CLI and direct SemanticLayer execution defaults are unchanged.

## Importing policies from Cube / Rill

- **Cube** `access_policy` `row_level.filters` are imported into an enforced
  `SecurityPolicy` (see [Cube compatibility](compatibility/cube.md#access-control-access_policy--accesspolicy)).
- **Rill** metrics-view `security:` blocks (`access`, `row_filter`) are imported, with
  Go-template `.user.*` references translated to Sidemantic's `user.*` namespace.

## Limitations

- Row-level filters are enforced on the structured/compile path (Python engine). Queries
  touching a secured model are forced onto the Python generator even under `engine="rust"`.
- Pre-aggregation routing is disabled for a query while row filters are active (a rollup
  is materialized without per-user filtering).
