# Seeded compiler differential tests

`test_generated_differential.py` compiles every valid case through production
`SemanticLayer(engine="python", fallback=False)` and
`SemanticLayer(engine="rust", fallback=False)`, executes both SQL statements in
the same one-thread DuckDB database, and checks their public columns and rows.
The Rust layer also has a Python compiler tripwire, and both engine selections
must report the requested engine without a fallback reason. These tests require
the installed Rust extension built from the checkout being evaluated.

The default campaign has 52 cases. Thirteen rotating families generate base,
derived, nested derived, ratio, cumulative, filtered and graph-level metrics;
single-source, many-to-one, composite-key, one-to-many, double-fanout and
cross-source graphs; time grains; query and metric filters; segments; source
invariants; null defaults; empty populations; nulls (including time keys),
unmatched join keys, and ordered limits/offsets. Relational families also select
filtered, derived, ratio, nested and cumulative metrics, so joins/fanout interact
with metric semantics instead of being isolated feature buckets.
The default-time family deliberately omits explicit time grouping to exercise
implicit default groups in cross-source calculations; it has no ordering or
pagination, so hidden group keys cannot invalidate deterministic tie breakers.
Definitions vary aggregation, arithmetic, SQL dimension expressions, source
types, window sizes, grouping, predicates and selections as well as data.
Each `(seed, index)` is independently reproducible.

Run the actual pytest campaign at higher volume:

```sh
SIDEMANTIC_DIFFERENTIAL_REQUIRE_RUST=1 \
SIDEMANTIC_DIFFERENTIAL_SEED=20261005 \
SIDEMANTIC_DIFFERENTIAL_CASES=100000 \
SIDEMANTIC_DIFFERENTIAL_OUTPUT=/tmp/sidemantic-differential \
uv run --no-sync pytest --no-cov -q tests/semantic_conformance/test_generated_differential.py
```

`SIDEMANTIC_DIFFERENTIAL_START` selects the first index for a shard or a resumed
range. Use disjoint output directories for concurrent shards. A missing Rust
extension normally skips this module; `REQUIRE_RUST=1` makes it an error for
readiness gates. Compiler errors from either or both engines always fail the
campaign. Invalid generated definitions/data are separately counted failures,
never counted as successful parity cases.

For many data populations per compiler input, opt into compilation reuse:

```sh
SIDEMANTIC_DIFFERENTIAL_REQUIRE_RUST=1 \
SIDEMANTIC_DIFFERENTIAL_CASES=5000 \
SIDEMANTIC_DIFFERENTIAL_DATA_VARIANTS=40 \
SIDEMANTIC_DIFFERENTIAL_OUTPUT=/tmp/sidemantic-differential-populations \
uv run --no-sync pytest --no-cov -q tests/semantic_conformance/test_generated_differential.py
```

This requests **5,000 generated model/query inputs and 200,000 executed data
populations**, not 200,000 distinct compilations. The first population retains
the original generator data. Others independently seed rows using the model
seed, case index, and stored `data_variant`; exact duplicate populations within
an input are discarded and redrawn. Schemas and semantic definitions stay
unchanged while keys, fanout, nulls, empty sources and values vary. Every fixture
contains the complete actual population and replays without caching.

The cache reuses only successful compilation with the exact same canonical
model definitions, graph metrics, query, and typed table schemas. Every actual
compile checks strict engine selection; reused SQL executes again against the
new population. Successful immutable definition validation is also reused for
that exact key; every population still checks row shape, non-null unique primary
keys, and actual DuckDB insertion/type validity. Only the latest key per engine
and latest validated definition are retained. Default
`DATA_VARIANTS=1`, replay, and minimization always compile fresh. Reports expose
`unique_compiler_inputs`, per-engine `actual_compile_attempts`,
`actual_successful_compilations`, `compilation_cache_hits`, and
`checked_populations`; engine-selection counts refer to actual compilations,
while successful-execution counts include all populations. Cache reuse measures
data-dependent correctness at lower compile cost; it does not expand compiler
input coverage.

The campaign continues after failures and records counts by failure class,
fingerprint, family and feature. It records compile/execution wall time per
engine, successful execution and actual engine-selection counts, reduction time,
total elapsed time and checked/passed counts in
`report.json`, checkpointed every 100 cases and at exit. Counts describe generated
features, not proof that every defined feature was selected. The report path is
also a pytest/JUnit property. Fingerprints distinguish failure phase and exception
type/message, or result mismatches by family and public columns; they are
deduplication heuristics, not proof of distinct root causes.

Up to `SIDEMANTIC_DIFFERENTIAL_EXAMPLES` (default 12) distinct fingerprints get an
original JSON case, minimized `.case.json`, and `.evidence.json` containing SQL,
results/errors and reducer accounting. Further failures remain counted. Set
`SIDEMANTIC_DIFFERENTIAL_REDUCTIONS` (default 100) to limit attempts per example,
or zero to retain original cases without reducing them. A reducer accepts only
schema/reference-valid candidates with the original failure class and normalized
exception diagnostic (retaining referenced owner/column names); it removes
rows, query clauses, unused models/definitions, unused physical columns and their
cells, and simplifies non-key values.
Result mismatches additionally retain their kind (cardinality, ordering only,
or values), both public column lists, and a concrete differing-row witness.
Cardinality failures retain which engine has more rows. These conservative
constraints prevent swapping the defective metric or a hidden-group discrepancy
for another failure, at the cost of sometimes retaining otherwise removable
columns or data. Reduction is deterministic and budgeted, not a claim of a globally smallest
reproducer. Replay uses the saved definitions/data, not the current generator:

```sh
SIDEMANTIC_DIFFERENTIAL_REQUIRE_RUST=1 \
SIDEMANTIC_DIFFERENTIAL_REPLAY=/tmp/sidemantic-differential \
uv run --no-sync pytest --no-cov -q tests/semantic_conformance/test_generated_differential.py -k replay
```

`REPLAY` accepts a directory of `.case.json` files or one exact file. Without it,
replay discovers checked-in `differential/fixtures/*.case.json` regression cases.
Replay asserts parity, so a previously failing fixture becomes a permanent
regression contract after the production defect is fixed.

Unordered output uses duplicate-preserving bipartite multiset matching. Numeric
tolerance is `rel_tol=abs_tol=1e-9`; integer-to-integer comparisons stay exact.
Nulls, booleans, NaNs and infinities retain their own semantics. DATE and midnight
TIMESTAMP values are equivalent time buckets. Ordered output compares the actual
row sequence. Every grouping key is an explicit final tie breaker whenever order
is requested, and pagination is never generated without that deterministic order.

This is differential evidence, not an independent semantic oracle: identical
compiler bugs can pass. It complements the hand-authored conformance corpus with
independent expected results. It currently executes DuckDB only, uses a bounded
grammar rather than arbitrary SQL fuzzing, and does not cover security, remote
databases, adapter imports, calendars/timezones, or every advanced metric family.
Hundreds of thousands of cases are supported without keeping all cases/results
in memory; runtime is workload-dependent and no such volume is implied by the
52-case default.
