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

All query transports use the same Python security-aware SQL generator. Given the
same `user_attributes`, structured HTTP, PostgreSQL semantic SQL, MCP structured
tools, MCP `run_sql`, and `SemanticLayer.sql(..., user_attributes=...)` evaluate
the same access gates and inject the same row filters.

SQL transports also verify that a source-reading statement was actually rewritten
through the semantic layer whenever a security or enforced visibility control is
active. A statement that would pass through to an underlying table is rejected
with an actionable `SecurityError`; `SELECT 1`-style source-free statements remain
available. True raw database execution is never considered policy-aware.
Predicate subqueries are rejected while controls are active because nested reads
cannot yet be proven to receive the same policy rewrite; use structured filters or a
modeled semantic join instead.

### HTTP (`sidemantic server api`)

User attributes come from a trusted header (default `X-Sidemantic-User`, a JSON
object). The value is passed into `/query`, `/compile`, `/sql`, and `/sql/compile`;
the result cache is keyed per user so cached rows never leak across users.

| Flag | Effect |
|------|--------|
| `--user-header NAME` | Header carrying the JSON user-attributes object (default `X-Sidemantic-User`). |
| `--require-user-attrs` | Reject data requests that lack the header (HTTP 400). |
| `--enforce-visibility` | Apply field-visibility enforcement. |

A `SecurityError` (denied access gate, deny-by-default, undefined attribute) maps to
**HTTP 403**.

`/sql` and `/sql/compile` accept semantic SQL and apply the same access gates, row
filters, and field visibility as structured requests. When controls are active,
SQL that does not resolve to semantic models is rejected instead of passed through.

`/raw` bypasses semantic compilation, so it returns HTTP 403 whenever any model
declares a security policy or enforced `public: false` fields exist. Use `/query`
or `/sql` in that configuration.

#### Browser bearer handling

The bundled web UI never accepts `?token=`, never writes an API bearer to
`localStorage`/`sessionStorage`, and never puts it in a shareable URL. On a 401 it
prompts for the bearer once and exchanges it through `POST /auth/session` for an
opaque 10-minute session. Same-origin deployments use an `HttpOnly`,
`SameSite=Strict` cookie scoped to `/` and marked `Secure` over HTTPS. For an
explicit cross-origin backend, where that cookie cannot be relied upon, the exchange
returns a short-lived credential that the adapter keeps only in memory and sends with
the `Sidemantic-Session` authorization scheme. The server stores only a SHA-256 digest
of either credential. Session responses are `Cache-Control: no-store`; the UI keeps
the long-lived bearer only in the password input until the exchange completes.

Cross-origin deployments must list the UI origin in `--cors-origin`; credentialed
requests use CORS rather than placing either credential in a URL or browser storage.

API clients may continue to send the bearer in the `Authorization` header. The
session exchange is a library-local browser safety mechanism, not an identity or
SSO system.

### PostgreSQL wire server

The connecting Postgres username is mapped to user attributes via
`--user-attrs-file` (a JSON mapping of usernames to attribute objects). A map is
accepted only when password authentication is configured, so a client cannot select
another mapping by spoofing the startup username.

PostgreSQL semantic SQL is rewritten with that session's attributes and therefore
applies access gates and row filters before execution. With
`--enforce-visibility`, `public: false` fields are omitted from the registered
semantic catalog and rejected if referenced. When controls are active, SQL that
reads an unrecognized/non-semantic source fails closed instead of passing through.
The compatibility surface is read-only: mutations, DDL, commands, and
multi-statement SQL are rejected. PostgreSQL session and catalog compatibility queries remain available;
when controls are active, catalog responses expose semantic models without
enumerating physical source tables. `information_schema.columns` is synthesized from
that same semantic catalog, preserves ordinary projection/filter/order probes, and
omits non-public fields when visibility enforcement is enabled. Catalog queries mixed
with other table sources are not treated as compatibility probes and pass through the
normal fail-closed transport gate. `pg_catalog.pg_class` likewise lists semantic
tables only while controls are active.

### MCP server

The MCP server has no per-session identity. Supply one process-wide attribute object
with `--user-attrs-file`; it is applied to `run_query`, `create_chart`, and
`run_sql`. `run_sql` uses the same policy-aware semantic rewrite as other transports
and rejects unproven passthrough SQL. `--enforce-visibility` hides restricted fields
from MCP model/graph/catalog discovery and rejects them in structured and SQL tools.

For example:

```bash
sidemantic server mcp models/ \
  --user-attrs-file .secrets/mcp-user.json \
  --enforce-visibility
```

The file contains the attribute object itself, for example
`{"role":"analyst","tenant_id":1}`. It is not a user database.

## Importing policies from Cube / Rill

- **Cube** `access_policy` `row_level.filters` are imported into an enforced
  `SecurityPolicy` (see [Cube compatibility](compatibility/cube.md#access-control-access_policy--accesspolicy)).
- **Rill** metrics-view `security:` blocks (`access`, `row_filter`) are imported, with
  Go-template `.user.*` references translated to Sidemantic's `user.*` namespace.

## Limitations

- Queries touching a secured model are forced onto the Python generator even under
  `engine="rust"`; the Rust generator is not a security enforcement path.
- Pre-aggregation routing is disabled for a query while row filters are active (a rollup
  is materialized without per-user filtering).
- `public: false` is enforced only when visibility enforcement is enabled on the layer
  or with the corresponding server `--enforce-visibility` flag.
- HTTP trusts the configured user-attributes header, PostgreSQL trusts its authenticated
  username-to-attributes mapping, and MCP uses one static process identity. Deployments
  must ensure untrusted clients cannot forge those inputs.
- SSO, identity lifecycle, RBAC administration, content permissions, and embedding are
  intentionally out of scope for this library.
