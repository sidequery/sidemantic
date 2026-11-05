# Apache Ossie conformance assessment — 2026-11-05

## Baseline and scope

This assessment targets Apache Ossie's development draft `0.2.0.dev0` at
[`172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`](https://github.com/apache/ossie/tree/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1),
the latest upstream commit fetched for this work. It is an immutable test baseline,
not a stable release or a promise to track subsequent upstream changes.

“Public Rust” uses the shared Python interchange importer and the Rust query engine
with fallback disabled. “Native Rust” uses the separate Rust forward importer.
The upstream [layer overview](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/spec_layers.md)
permits independent layer adoption; interchange support alone does not establish
query-interface conformance.

## Original gaps and implemented results

| Original gap | Implemented behavior |
|---|---|
| Latest logical syntax rejected | Immutable latest profiles support authoritative `expression.sql`, dataset-scoped metrics, open dialect labels, and current namespace/reference rules. Unsupported dialect variants remain opaque. |
| Portable NULL ordering lost | Both runtimes retain explicit and Ossie-default placement through final SQL generation, including changed DuckDB session defaults and bounded RANGE frames. |
| Shared aggregate FILTER rejected | FILTER binds logical fields and `source.column` separately, preserves empty groups, and avoids evaluating calculated inputs twice. Native COUNT predicates retain their bindings. |
| Standalone mappings rejected | Shared interchange validates and preserves mapping documents without execution. Supplied reference catalogs support cross-checks; unresolved references warn and are not fetched implicitly. |
| Latest synthesis violated metric rules | Graph synthesis expands derived references, declares required physical inputs, and preserves dataset metric scope. Latest validation is separate from historical contracts. |
| Official A01 blocked | The unchanged SCOTT model, data and AGG query return **29025.00 / 14** through all three routes and both public CLI engines. |
| Layer 2 limited to one relation | Measures retain their own row identities through authored joins, filters, CTEs, subqueries and views. Independent deduplication preserves ordinary aggregate multiplicity; grouping sets, derived measures, HAVING, ordering and scalar recursive CTEs are covered. |
| Missing query type/discovery contract | Analysis rejects scalar misuse of measures and invalid DISTINCT/set inputs. DESCRIBE, information-schema queries and zero-row metadata expose measures; view registration follows successful DDL. |
| Missing Layer 3 semantics | Strict dimensions/metrics/filter binding implements directed-path validation, independent component populations, LEFT joins, NULL-equal key unions, empty-input values and ordered diagnostics. |
| Source/output dialect confusion | Physical-source and expression dialects normalize independently and once. Cross-dialect checks cover array positions inside fields, source queries, nested aggregates and authored queries. |

`CURRENT_OSSIE_SCHEMA_COMMIT` retains historical `b6c702e`;
`LATEST_OSSIE_SCHEMA_COMMIT` identifies `172a3e5`. New constructs infer the latest
profile; ambiguous older shapes retain their historical interpretation unless
explicitly pinned. CLI input and output revision selectors are separate.
See the maintained [supported interfaces](../compatibility/ossie.md).

## Review follow-up

All three review findings are fixed in the replacement stack. The verified
follow-up scope is:

- Preserve DISTINCT around constant aggregate arguments while placing row anchors
  inside it. Regressions execute COUNT(DISTINCT 1), COUNT(DISTINCT NULL), and an
  empty aggregate filter over nonempty and empty relations across both interfaces
  and all three routes.
- Preserve unaggregated inline-window QUALIFY predicates before DISTINCT, ordering
  and pagination, including aliases and CTE measure lineage. Unsupported grouped
  or named-window QUALIFY shapes reject explicitly.
- Bind view identities to catalog, schema and name; respect session search paths
  and physical shadowing; keep discovery and cache invalidation consistent.

The implementation is split into four dependent PRs: pinned assets, import/export
compatibility, Layer 2 SQL, and Layer 3 queries. Assessment scratch files remain
in the original PR #460 history instead of the maintained stack.

| Stack verification | Result |
|---|---|
| Pinned schema profiles and integrity | 29 passed |
| Import/export, CLI, filtering, IR and DISTINCT | 294 passed; 3 warnings |
| Native importer at the compatibility stage | 15 passed |
| Layer 2 public-engine checks | 346 passed |
| Layer 2 native Rust checks | 136 passed |
| Layer 3, APIs, discovery, routing and transport security | 488 passed; 4 warnings |
| Final review regressions and discovery with rebuilt Rust extension | 109 passed |
| Final Rust library and importer | 674 + 15 passed |
| Changed Python lint/format, Rust format, whitespace | Passed |

Counts overlap across focused runs and must not be summed as unique coverage.
The final Rust extension includes the discovery precedence follow-up. Each stage
was checked before publication; native compilation also ran from the intermediate
compatibility and Layer 2 sources. Full unrelated repository CI and live warehouse
integrations were not rerun. The broader pre-review results below remain historical.

## Recorded pre-review verification

Implementation commit `6351374` was checked against independent expected results.
The maintained corpus contains **160 cases across three routes (480 executions)**.
Expected values come from explicit row populations or ordinary physical SQL,
never from choosing one backend as the oracle for another. Additional tests cover
CLI, discovery, views, security, dialect boundaries and engine routing.
See the [requirement matrix](../../tests/ossie-fixtures/layer23-172a3e5/README.md).

| Pre-review check | Result |
|---|---|
| Complete affected/regression matrix, Python default | 2,685 passed; 1 skipped; 9 warnings |
| Same matrix, Rust default | 2,685 passed; 1 skipped; 9 warnings |
| Rust library and native importer tests | 674 + 15 passed |
| Changed Python lint/format, Rust format, whitespace checks | Passed |

The two matrix counts overlap and are not a unique-test total. The single skip is
the Python parameter of an explicitly Rust-only subquery regression; its Rust
parameter passed in both runs. Warnings were existing deprecations and intentional
loader-shadowing fixtures. The extension was rebuilt from the tested functional
sources. Public runtime tests required no elevated stack; native library tests
used the previously reproduced baseline requirement `RUST_MIN_STACK=16777216`.

Reproduce the maintained query corpus with a current source-built Rust extension:

```sh
.venv/bin/python -m pytest --no-cov -q \
  tests/interchange/ossie/test_layer2_conformance.py \
  tests/interchange/ossie/test_layer3_conformance.py
RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 \
  cargo test -p sidemantic --lib --test ossie_forward -- --test-threads=2
```

## Coverage limits

Execution checks use DuckDB, including target-dialect SQL converted back for
execution. Live warehouse integration and the full unrelated repository suite
were not exercised. Native Rust remains a forward-import structural subset;
ontology and mapping preservation do not imply ontology reasoning or execution.
Unsupported security contexts fail closed.

The pinned specification's deferred features remain deferred, including measure
evaluation controls/windowing and unsupported Layer 3 path/filter shapes.
Passing the official validator does not prove all normative rules: it reports
some model-reference issues as warnings. The implementation combines pinned
upstream assets, official fixtures and independently authored runtime tests.
This assessment is not an official Apache certification.
