# Independent Ossie query regression corpus

Baseline: Apache Ossie draft `0.2.0.dev0`, commit
[`172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`](https://github.com/apache/ossie/tree/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1).
This repository authored these cases against the normative requirements in
[Layer 2](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/relational_semantics.md)
and [Layer 3](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/wide_table_semantics.md).
They are additional regression cases, not an official upstream certification suite.

`model.json` and `setup.sql` define a small commerce model. Orders deliberately lack
a declared primary key and contain two physically distinct but identical rows. The
data also includes unmatched and NULL foreign keys, NULL aggregate arguments, and
multiple fact tables with overlapping and disjoint dimension domains.

Expected rows are calculated directly from these populations. Cases with
`reference_sql` use ordinary SQL over physical tables without Sidemantic, measure
columns, or `AGG`. No backend supplies expected results for another backend. Row
comparison preserves multiplicity and treats unordered results as multisets;
explicit ordering cases also check sequence.

| Contract | Cases / test coverage |
|---|---|
| Layer 2 C1, §4: type discipline; §5: declaration | Scalar/predicate/group/join misuse, invalid AGG inputs, base and derived `TO_MEASURE`, declaration after joins |
| Layer 2 C2, §7: operator semantics | WHERE/HAVING/order/limit/offset, aggregates only in ORDER BY, GROUP BY ALL, grouping sets/rollup/cube, scalar windows, USING merged keys/star projection, outer aggregate FILTER on base/derived measures |
| Layer 2 C2, §7.8: DISTINCT and sets | Reject unevaluated measures; accept evaluated scalars and projections dropping measures; set ordering/pagination |
| Layer 2 C3, §8.2: slice-and-dice responsiveness | Predicates before evaluation, hidden inputs through CTEs and renamed/star projections, correlated EXISTS/NOT EXISTS, aggregate FILTER subsets |
| Layer 2 C4, §8.3: no duplication | Identical source rows, fan-out/chasm, self joins, inner/left/right/full/cross/non-equi joins, raw aggregate multiplicity, null extension, empty inputs, holistic aggregates |
| Layer 2 C5, §8.4: closure and stable field domains | Scalar/correlated subqueries, scalar recursive CTEs, top-N join, multistage aggregates, field-driven group domains with empty measure populations |
| Layer 2 C6, §11.1: measure discovery | Separate `test_latest_discovery.py` suite |
| Layer 3 §§3,8: shape, names, diagnostics | Strict shape and unknown-option rejection, aliases and ordered columns, category errors, filter scope and error precedence |
| Layer 3 §4: directed paths | Reverse/no-path rejection, partial reachability, multiple paths including unequal lengths/parallel edges, ignored self edges, dimensions-only anchors and ambiguous cycles |
| Layer 3 §5: populations and stitching | Independent branch identities, nullable keys, full domain union, wrong cardinality duplicates, absent branch versus present NULL, no-dimension and empty group cases |
| Layer 3 §§3.4,5.3–5.4: filter semantics | Constant/NULL predicates, null-rejecting/null-safe filters, branch-wide slicers versus aggregate FILTER, dimension-only anchor stability |
| Layer 3 §5.2: components | Dataset aggregate formulas, independent model aggregate atoms, holistic/ordered-set aggregates, mandated errors for finer-grain/windowed/ambiguous component scope |

The Python tests add small model variations for paths, scope diagnostics, aggregate
filters, and empty-value distinctions. All cases run through public Python, public
Rust with fallback disabled, and native Rust import/compile with physical DuckDB
execution. Native and shared import routes consume the same source document.

Run from the repository root with the configured Rust toolchain on PATH:

```sh
.venv/bin/pytest tests/interchange/ossie/test_layer2_conformance.py tests/interchange/ossie/test_layer3_conformance.py --no-cov
```

Measure declaration uses supported `TO_MEASURE(expression)` and the draft's
illustrative `expression TO MEASURE` form. Layer 3 uses the strict JSON binding.
Deferred features outside the pinned specification are not implied by this corpus.
