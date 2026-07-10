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

### HTTP (`sidemantic api-serve`)

User attributes come from a trusted header (default `X-Sidemantic-User`, a JSON object).
The value is passed into every structured `/query` and `/compile` request, and the
result cache is keyed per user so cached rows never leak across users.

| Flag | Effect |
|------|--------|
| `--user-header NAME` | Header carrying the JSON user-attributes object (default `X-Sidemantic-User`). |
| `--require-user-attrs` | Reject data requests that lack the header (HTTP 400). |
| `--enforce-visibility` | Apply field-visibility enforcement. |

A `SecurityError` (denied access gate, deny-by-default, undefined attribute) maps to
**HTTP 403**.

**The `/sql` and `/raw` endpoints are disabled (HTTP 403) whenever any model declares a
security policy.** They rewrite/execute free-form SQL and cannot apply per-user row
filters, so they refuse rather than return unscoped rows — use the structured `/query`
endpoint, which enforces.

### PostgreSQL wire server

The connecting Postgres username is mapped to user attributes via a startup
`--user-attrs-file` (JSON mapping usernames → attribute dicts). Because the PG path is
SQL-first (it uses the query rewriter), it enforces the **access gate** for secured
models but does **not** apply row-level filters. Treat the PG server as coarse-grained
access control, not row security.

### MCP server

The MCP server applies static, process-wide user attributes supplied at startup. Its
`run_sql` tool (free-form SQL) does not apply row filters, mirroring the HTTP `/sql`
caveat.

## Importing policies from Cube / Rill

- **Cube** `access_policy` `row_level.filters` are imported into an enforced
  `SecurityPolicy` (see [Cube compatibility](compatibility/cube.md#access-control-access_policy--accesspolicy)).
- **Rill** metrics-view `security:` blocks (`access`, `row_filter`) are imported, with
  Go-template `.user.*` references translated to Sidemantic's `user.*` namespace.

## Limitations

- Row-level filters are enforced on the structured/compile path (Python engine). Queries
  touching a secured model are forced onto the Python generator even under `engine="rust"`.
- The PG wire server and MCP `run_sql` enforce the access gate but not row filters (above).
- Pre-aggregation routing is disabled for a query while row filters are active (a rollup
  is materialized without per-user filtering).
