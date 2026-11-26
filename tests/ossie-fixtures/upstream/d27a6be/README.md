# Ossie 0.3.0.dev0 real-file fixture

`examples/tpcds_semantic_model.yaml` is an unchanged copy from Apache Ossie
commit [`d27a6beab519c8d3bf08ad5fc901c293205134eb`](https://github.com/apache/ossie/blob/d27a6beab519c8d3bf08ad5fc901c293205134eb/examples/tpcds_semantic_model.yaml).
The file declares `version: "0.3.0.dev0"`. SHA-256:
`72a98b17eda7daa6bcb777125ea44b1cbbb7788bdd6167430e0f58ea7e06a1ce`.

`LICENSE` and `NOTICE` are unchanged files from the same commit. Its Apache
License header is preserved. Retrieved November 24, 2026. This pins a specific
draft, not a promise of compatibility with future upstream revisions.

`sqlctx-duckdb/test/test_catalog_host.py` loads this exact file through
`sqlctx_load_file`, creates matching `tpcds.public` physical sources, and asserts
root-level total sales, total profit, joined brand sales, filtered sales, lifetime value, store productivity, and
the dataset-scoped `store_sales.total_quantity` metric. It repeats the queries
after reopening the persisted database read-only. CI uses only these local
bytes; no network is needed. The draft wide-table specification defers execution of windowed model metrics.
