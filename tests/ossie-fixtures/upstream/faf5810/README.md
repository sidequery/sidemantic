# Released Ossie 0.1.1 real-file fixture

`examples/tpcds_semantic_model.yaml` is an unchanged copy from Apache Ossie
commit [`faf581054dcf7964d5fe0ceae7d6f415c8ce32a5`](https://github.com/apache/ossie/blob/faf581054dcf7964d5fe0ceae7d6f415c8ce32a5/examples/tpcds_semantic_model.yaml).
The file declares `version: "0.1.1"`. SHA-256:
`1c8c8e1a41d6b1b4d957ae5934b883b46ca07ad3901927ec6ce0ecff55d2cb6b`.

`LICENSE`, `LICENSE-Docs`, and `NOTICE` are unchanged files from the same commit.
Retrieved November 24, 2026. The older `tests/fixtures/osi/tpcds_osi_official.yaml`
has different bytes and a different declared version; it is not this fixture.

`sqlctx-duckdb/test/test_catalog_host.py` loads this exact file through
`sqlctx_load_file`, creates matching `tpcds.public` physical sources, and asserts
total sales, total profit, joined brand sales, filtered sales, customer lifetime
value, and store productivity. It repeats
the queries after reopening the persisted database read-only. CI uses only
these local bytes; no network is needed.
