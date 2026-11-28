# DuckDB Extension

The sqlctx DuckDB extension embeds the Rust native runtime in DuckDB. It reads native sqlctx definitions and Ossie logical model files directly.

## Status

The extension is experimental and targets **DuckDB 2.0.1 and 1.5.6**. It is **not** in the
DuckDB community extension registry, so `INSTALL sqlctx FROM community` fails.
Community publication is planned; once it lands, installation will be:

```sql
INSTALL sqlctx FROM community;
LOAD sqlctx;
```

Build sqlctx from source until a release artifact is available. Start the
DuckDB CLI with unsigned-extension loading enabled:

```bash
duckdb -unsigned
```

[The pre-rename release](https://github.com/sidequery/sqlctx/releases/tag/sidemantic-duckdb-v0.1.0)
contains builds for DuckDB 1.5.6 under the original extension name.

## Build From Source

The extension build needs Rust, DuckDB extension build tooling, and Ninja.

```bash
cd sqlctx-duckdb
make deps DUCKDB_VERSION=v2.0.1
make
SQLCTX_NATIVE_PEG=1 make test
```

For the DuckDB 1.5.6 compatibility frontend, use `make deps DUCKDB_VERSION=v1.5.6`
(the default), then `make` and `make test`. Build each host version in a separate
checkout or clean build directory. The repository vendors the extension build tools.

The local loadable extension is produced at:

```text
sqlctx-duckdb/build/release/extension/sqlctx/sqlctx.duckdb_extension
```

Load it in the DuckDB shell built by the extension workflow:

```bash
./build/release/duckdb -unsigned -cmd "LOAD '$PWD/build/release/extension/sqlctx/sqlctx.duckdb_extension';"
```

The official macOS CLI requires an absolute path when loading the extension.

For embedded clients, set DuckDB's `allow_unsigned_extensions` database configuration before opening the connection.

The extension reads existing `__sidemantic_catalog` snapshots and
`.sidemantic.sql` sidecars. A catalog mutation stores the updated snapshot in
`__sqlctx_catalog`.

## Runtime API

Load native YAML:

```sql
SELECT * FROM sqlctx_load('
version: 1
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
');
```

Load native YAML from disk:

```sql
SELECT * FROM sqlctx_load_file('/path/to/models.yml');
```

### Ossie files

The same loader accepts the released wrapped `0.1.1` format and the flat
`0.3.0.dev0` logical draft pinned at Apache Ossie commit `d27a6be`.
In a database containing the TPC-DS physical tables named in the model, run:

```sql
select * from sqlctx_load_file('/path/to/tpcds_semantic_model.yaml');
select total_sales, total_profit from metrics;
select item.i_brand, sales_by_brand from metrics order by item.i_brand;
```

The draft also declares a dataset-scoped measure:

```sql
select agg(s.total_quantity) as total_quantity from store_sales s;
```

The imported definitions persist in the database. A physical table, view, or
CTE named `metrics` keeps its ordinary DuckDB meaning. The wrapped file uses
the OSI reader. Flat files use the Rust Ossie importer and its offline schema.
Draft dataset metrics that require source measure-relation metadata
(`AGG(source.measure)`) are currently unsupported.

#### Windowed metrics in plain SQL

Selecting any of the TPC-DS draft's three windowed metrics by name from
`metrics` returns `E_DEFERRED_WINDOWED_METRIC`, as required by the
[pinned Ossie wide-table specification](https://github.com/apache/ossie/blob/d27a6beab519c8d3bf08ad5fc901c293205134eb/core-spec/wide_table_semantics.md#52-metric-components).
To calculate them in plain SQL, select `total_sales` with the grouping
dimensions in a CTE, then apply the DuckDB window function to those rows.

**Running sales by day (`cumulative_sales`).** Each row adds that day's sales
to the total through the previous day.

```sql
with daily as (
    select date_dim.d_date as sale_date, total_sales
    from metrics
)
select sale_date,
       sum(total_sales) over (
           order by sale_date rows between unbounded preceding and current row
       ) as cumulative_sales
from daily
order by sale_date;
```

**Brand rank within each store (`brand_rank_in_store`).** Rank brands by
descending sales within each store. Brands with equal sales share a rank.

```sql
with brand_sales as (
    select store.s_store_sk as store_id, item.i_brand as brand, total_sales
    from metrics
)
select store_id, brand,
       rank() over (
           partition by store_id order by total_sales desc
       ) as brand_rank_in_store
from brand_sales
order by store_id, brand_rank_in_store, brand;
```

**Change in monthly sales (`monthly_sales_change`).** Subtract sales for the
previous month present in the result. The first month has a `NULL` change.

```sql
with monthly as (
    select date_dim.d_year as sale_year, date_dim.d_moy as sale_month, total_sales
    from metrics
)
select sale_year, sale_month,
       total_sales - lag(total_sales) over (
           order by sale_year, sale_month
       ) as monthly_sales_change
from monthly
order by sale_year, sale_month;
```

### Native SQL definitions

Load native SQL definitions from disk:

```sql
SELECT * FROM sqlctx_load_file('/path/to/models.sql');
```

Native SQL definitions can also be entered directly through the parser extension:

```sql
MODEL (name orders, table orders, primary_key order_id);
DIMENSION (name status, type categorical);
METRIC revenue AS SUM(amount);
```

Compact native SQL model blocks are also accepted directly:

```sql
model orders from orders (
  primary key (order_id)
  status
  sum(amount) as revenue
  count(*) as order_count
);
```

Inspect loaded models:

```sql
SELECT * FROM sqlctx_models();
```

Rewrite semantic SQL without executing it:

```sql
SELECT sqlctx_rewrite_sql('SELECT orders.revenue FROM orders');
```

Run a semantic query through the parser override:

```sql
SELECT orders.revenue FROM orders;
```

The legacy `SEMANTIC SELECT ...` form remains supported.

### Save a reusable selection of dimensions and measures

With the `orders` model above, publish a selection and query it at two grains:

```sql
create semantic view order_report as
select status, revenue as gross_revenue from orders;

select status, gross_revenue from order_report order by status;
select gross_revenue from order_report;
```

A semantic view projects fields from one model or another semantic view. The consumer
chooses the grouping dimensions. Aliasing a ratio or filtered measure retains its
source definition. Replacing a source measure changes subsequent queries of the
semantic view. `CREATE OR REPLACE SEMANTIC VIEW`, `DROP SEMANTIC VIEW` and changes
to source definitions participate in the caller's transaction. An ordinary DuckDB
`CREATE VIEW ... AS SELECT ...` over a semantic query stores its compiled SQL.

Use `sqlctx_explain('select orders.revenue from orders')` to inspect the semantic
compiler's decisions. DuckDB's own `EXPLAIN` shows the resulting database plan.
For applications with a SemanticInput v1 document, the
`sqlctx_query_semantic_input(input_json, query_json)` table function returns the
requested columns and rows directly in the caller's transaction.

After attaching a database as `finance`, a query can explicitly select its model
with `from finance.semantic.orders`. Unqualified physical sources of a model in
another catalog resolve in that catalog's default schema. Explicitly qualified
sources retain their catalog.

## Release Path

Use `.github/workflows/duckdb-extension-release.yml`.

The workflow:

- fetches each selected DuckDB release tag with `make deps` and verifies the vendored `extension-ci-tools`,
- builds the Rust-backed extension for `linux_amd64`, `linux_arm64`, `osx_amd64`, and `osx_arm64`,
- runs the DuckDB sqllogictests, including native YAML load, native SQL definition file load, relationship rewrite, semantic select, persistence, and invalid-version coverage,
- uploads a separate extension artifact for each DuckDB version and platform with its checksum,
- on a `sqlctx-duckdb-v*` tag push (or a manual run with `create_github_release`), creates the GitHub release with the artifacts, `SHA256SUMS`, and both license notices. It never overwrites an existing release.

It does not publish to the DuckDB community extension registry.

## Compatibility

| Component | Current target |
|---|---:|
| Native format | `1` |
| Rust runtime crate | `0.1.0` |
| DuckDB extension release | `0.1.0` |
| DuckDB version | `2.0.1` or `1.5.6` |

DuckDB extension artifacts are ABI-sensitive. Rebuild the extension when changing the DuckDB target version or the Rust native runtime version.

For a database file written in DuckDB 2.0 storage format, open it with DuckDB
2.0.1 and load the matching 2.0.1 extension.

The extension should accept the same native YAML and native SQL definition contract as the Rust runtime. If extension-only syntax is added, it should either be aligned with the Rust SQL loader or documented as DuckDB-only.
