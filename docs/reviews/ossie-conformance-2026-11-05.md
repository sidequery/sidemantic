# Apache Ossie conformance assessment and remediation — 2026-11-05

## Layer 2/3 follow-up — final verification

The single-relation `AGG()` restriction has been removed in both Python and Rust
against the same pinned draft, `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`.
The implementation now includes:

- Layer 2 measure identity through joins, subqueries, CTEs and views; independent
  deduplication per measure; normal multiplicity for ordinary aggregates;
  grouping sets, filters, HAVING, ordering, scalar recursive CTEs, and correlated
  subqueries. Base and derived measure declarations preserve their input scope.
- Analysis-time measure type rules and DISTINCT/set boundaries, with ordinary
  scalar set ordering and pagination preserved after evaluation.
- Measure discovery through `DESCRIBE`, information-schema queries and zero-row
  driver metadata. Physical catalog entries remain available. View replacement
  validates cycles before DDL and changes the recipe registry only after success.
- Strict Layer 3 dimensions/metrics/filter binding through API and CLI
  `--ossie-wide`, directed path checks, independent metric components, LEFT-join
  populations, NULL-equal group stitching, empty-input values and ordered error
  precedence. Active security controls fail closed where unsupported.
- Source/output dialect handling, including array-index conversion in fields,
  source queries, nested aggregates and authored queries. Public Rust normalizes
  physical-source and expression dialects independently and exactly once.

The independent corpus contains **160 cases across three import/runtime routes
(480 executions)**: shared import with Python, shared import with Rust and
fallback disabled, and native Rust import/compilation. Additional tests cover
views, discovery, CLI, dialect boundaries, security and engine routing. Expected
results come from explicit row populations or ordinary physical SQL, never from
another backend. See the [requirement matrix](../../tests/ossie-fixtures/layer23-172a3e5/README.md)
and [supported interfaces](../compatibility/ossie.md).

| Final check | Result |
|---|---|
| Complete affected/regression matrix, Python default | **2,685 passed**, 1 skipped, 9 warnings |
| Same matrix, Rust default | **2,685 passed**, 1 skipped, 9 warnings |
| Rust library tests | **674 passed** |
| Native Ossie importer tests | **15 passed** |
| Changed Python lint/format; Rust format; whitespace checks | Passed |

Each matrix skips only the Python parameter of an explicitly Rust-only subquery
regression; its Rust parameter passes in both runs. Matrix counts overlap across
engine configurations and must not be added as a count of unique tests. Warnings concern existing parameter/API
deprecations and intentional loader-shadowing fixtures. The Rust extension was
rebuilt from the final functional sources at **2026-11-05T11:43:21-08:00**
(America/Los_Angeles). Public runtime tests use no elevated stack setting.
The native library command retains the previously reproduced baseline test-stack
requirement, `RUST_MIN_STACK=16777216`.

Final matrix commands, from the repository root:

```bash
export PATH=/workspace/.onboarding/cargo/bin:$PATH
export CARGO_HOME=/workspace/.onboarding/cargo
export RUSTUP_HOME=/workspace/.onboarding/rustup
export CARGO_BUILD_JOBS=2
for engine in python rust; do
  .venv/bin/python -m pytest -q -ra --no-cov --test-engine "$engine" \
    tests/interchange/ossie tests/adapters/osi tests/core/test_ossie_ir_fields.py \
    tests/semantic_conformance/test_ossie_aggregate_inputs.py tests/test_cli_contract.py \
    tests/queries/test_sql_rewriter.py tests/queries/test_rust_query_rewriter_route.py \
    tests/queries/test_rust_sql_rewriter_parity.py tests/core/test_semantic_handoff.py \
    tests/core/test_rust_rewrite_diagnostics.py tests/test_loaders.py tests/test_validation.py \
    tests/server/test_transport_security_parity.py \
    tests/semantic_conformance/test_rewriter_ci_regressions.py
done
RUST_MIN_STACK=16777216 cargo test -p sidemantic --lib --test ossie_forward -- --test-threads=2
```

Execution uses DuckDB, including converted target-dialect SQL round trips; live
warehouse integrations and the full unrelated repository suite remain outside
this validation. This is a tested implementation against the pinned draft, not
an official Apache certification. No release or deployment was made.
The earlier repair and original assessment below are retained as history.

## Remediation completed — 2026-11-05

The confirmed gaps below have been fixed in the working tree for both Python and Rust, against immutable upstream commit `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`. Historical profiles and schema assets remain available. The initial assessment is retained below as the before-state; its failures and counts are not the final implementation results.

| Confirmed gap | Implemented and verified behavior |
|---|---|
| Portable window NULL ordering | Final Python and Rust SQL generation preserves explicit and Ossie-default NULL placement across changed DuckDB session settings, including bounded RANGE frames. |
| Latest model syntax and scoped metrics | New checksum-verified latest profiles accept authoritative `expression.sql`, dataset metrics, and opaque dialect variants. Logical fields and `source.column` resolve separately; namespace/reference checks and reserved model-declaration `AGG` rules are enforced. Opaque metadata does not change profile inference. |
| Portable FILTER | Shared import accepts aggregate FILTER while rejecting standalone WHERE/scalar misuse and invalid cross-dataset predicates. Runtime tests also caught and fixed double evaluation of calculated logical fields inside filtered aggregates and a native COUNT(*) predicate-binding omission. |
| Standalone mapping documents | Parse, validate, preserve, and serialize mappings without generating executable scopes. An explicit supplied-reference catalog validates document names/schemas, concepts, inherited relationships, tuple arity, and model fields; unresolved references warn and are not fetched implicitly. |
| Latest validation and synthesis | Latest schema tightening applies to the new pin; derived metrics expand safely instead of emitting forbidden metric references. Synthesis defaults to the latest revision, declares physical aggregate inputs, and retains dataset metric scopes; historical exports can select their old pin explicitly. |
| Official A01 query and CLI paths | The unchanged official `AGG` query executes against the unchanged SCOTT model and data through Python, public Rust, and native Rust, returning **29025.00 / 14**. CLI query and rewrite paths now preserve metric ownership and recognize validated Ossie time fields without inventing native granularity requirements. |

`CURRENT_OSSIE_SCHEMA_COMMIT` continues to identify `b6c702e`; `LATEST_OSSIE_SCHEMA_COMMIT` identifies `172a3e5`. New constructs select the latest profile automatically; ambiguous older shapes retain their historical interpretation unless an explicit revision is supplied. The CLI now provides `--ossie-input-schema-revision` separately from the output revision selector. See `docs/compatibility/ossie.md` for the updated supported contract.

The initial repair provided the official A01 query through a bounded interface. The Layer 2/3 follow-up replaces that restriction with relational measure propagation and the strict wide-table binding described in `docs/compatibility/ossie.md`. Native Rust remains a forward-import structural subset, and ontology/mapping documents remain non-executable. Live warehouse behavior beyond DuckDB was not certified.

### Earlier repair verification

Before the Layer 2/3 follow-up, both affected-suite runs completed without skipped tests:

| Verification | Final result |
|---|---|
| Affected Ossie interchange, adapter, IR, aggregate-input, and CLI tests with Python as default engine | **1,617 passed**, 3 deprecation warnings, no skips |
| Same suite with Rust as default engine | **1,617 passed**, 3 deprecation warnings, no skips |
| Native Rust integration regressions | **15 passed** |
| Rust library suite | **672 passed** with `RUST_MIN_STACK=16777216` |
| Additional loader/validation/IR tests | **102 passed**; overlaps the integrated suite's IR tests |
| Existing query-rewriter regressions | **94 passed** |
| Changed/new Python lint and formatting; workspace Rust formatting; whitespace checks | Passed |

The integrated suites include the new offline official-validator gates, upstream flights source preservation, unchanged A01 execution/CLI cases, and 24 filtered-aggregate runtime cases across all three import/engine routes. Do not add the overlapping focused-suite counts to obtain a unique-test total.

Earlier integrated commands, from `/workspace/sidemantic`:

```bash
export PATH=/workspace/.onboarding/cargo/bin:$PATH
export CARGO_HOME=/workspace/.onboarding/cargo
export RUSTUP_HOME=/workspace/.onboarding/rustup
.venv/bin/python -m pytest -q --no-cov --test-engine python tests/interchange/ossie tests/adapters/osi tests/core/test_ossie_ir_fields.py tests/semantic_conformance/test_ossie_aggregate_inputs.py tests/test_cli_contract.py
.venv/bin/python -m pytest -q --no-cov --test-engine rust tests/interchange/ossie tests/adapters/osi tests/core/test_ossie_ir_fields.py tests/semantic_conformance/test_ossie_aggregate_inputs.py tests/test_cli_contract.py
```

The final native functional changes were built into `/workspace/sidemantic/.venv/lib/python3.12/site-packages/sidemantic_rs/sidemantic_rs.abi3.so` on November 5 at 08:03:51 America/Los_Angeles before integrated verification. The default-stack Rust library run overflowed in a dependency test; the same isolated failure was reproduced on a clean checkout of original commit `641d443`, confirming it predates these changes. The library suite passes with the stated test-thread stack setting; the final Python/Rust integration suites do not require that override.

No commits or pull requests were created. The full unrelated repository suite was not run. The diagnostic scripts and evidence in the sibling review directory record the initial assessment; maintained regression tests and the final results above establish the repaired behavior.

## Initial assessment (before implementation)

At original commit `641d443`, neither backend conformed to the latest Apache Ossie draft across the assessed surfaces. Both retained substantial passing support for their documented older snapshots. Incorrect portable window NULL ordering and unsupported newer model syntax were confirmed during the initial assessment below.

## Baseline and scope

- Sidemantic: `641d443896b3174aa647c09f86ce301e251013d5`, version 0.13.1; initially clean checkout.
- Upstream fetched before delegation: [Apache Ossie main at `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`](https://github.com/apache/ossie/tree/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1), committed **2026-11-04 23:12:56 America/Los_Angeles**. Checked out separately at `/workspace/ossie-latest`.
- This is the latest observed **development draft**, still `0.2.0.dev0`, not a new stable release. Remote tags exposed only `osi-0.1.1-rc1` (peeled commit `faf581054dcf7964d5fe0ceae7d6f415c8ce32a5`).
- At the initial assessment baseline, Sidemantic's newest supported snapshot was `b6c702ed1c07e91382a69e870c875cbd19570828` (October 1), 143 upstream commits behind the fetched baseline. Older profiles are intentional compatibility contracts. `docs/compatibility/ossie.md` explicitly disclaims monitoring future upstream revisions.
- A leader fetched and compared upstream, then delegated Python and Rust assessments separately. The parent independently reviewed the oracle, interface scope, CI coverage, and window-order reproductions.
- The [upstream layer overview](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/spec_layers.md#L48) permits independent adoption. Missing Layer 2/3 features are not themselves failures of Layer 1 model support.

Latest core-schema SHA-256: `ea513794167c8d67e29fb7a7c537c9e2c33e718fb1e50becbe1ac389884ae662`.
Latest validator SHA-256: `4dea25c854e1b9d4da74cd568304133cdc4b679dd59594c426cf4370ddadcab7`.

## Surface matrix

“Public Rust” means the Python interchange importer followed by the Rust query engine. “Native Rust” means the separate Rust forward importer. They have different acceptance boundaries.

| Surface | Python engine / shared interchange | Public Rust engine | Native Rust importer + engine |
|---|---|---|---|
| Documented pinned-profile tests | Passing focused suite | Passing focused suite | Passing focused suite; explicitly `closed_structural_subset` |
| Latest `expression.sql`, dataset metrics, open dialect strings | Rejected | Rejected by shared importer | Rejected by structural gate |
| Latest M0–M3 fixture imports and metric execution | 8/8 successful | 8/8 successful | 8/8 successful |
| Latest M0–M3 preserved-source roundtrip grading | 33/33 elements PORTABLE | Shared interchange capability | No independent source-roundtrip API |
| Latest M0–M3 graph synthesis grading, explicitly `ANSI_SQL` | 23 PORTABLE, 8 REWRITTEN, 2 RELABELED | Shared interchange capability | Forward-only; not graded |
| Portable aggregate `FILTER (WHERE ...)` | Rejected during lowering | Rejected by shared importer | Validates and imports; separate execution not measured |
| Portable window NULL order | Wrong under changed DuckDB session default | Same shared-import defect | Also wrong for default DESC in ordinary DuckDB session |
| Latest A01 relational query corpus | Blocked importing SCOTT dataset metrics | Same | Same |
| Latest standalone ontology mapping document | Rejected | Same shared importer | Ontology/mapping execution outside native subset |
| Layer 3 complete wide-table semantics | Not exhaustively audited | Not exhaustively audited | Not exhaustively audited |

The 8 REWRITTEN elements are cosmetic field qualification changes. The 2 RELABELED elements originate in the single `OSSIE_SQL_2026` fixture and reflect the explicitly requested graph-synthesis dialect `ANSI_SQL`; they are not a silent relabeling defect. Source preservation retains their original tags. These small fixtures do not cover the whole expression language, relationships, or multi-dataset semantics. Execution probes query fixture metrics; they do not independently evaluate every dimension (including M1's calculated dimension).

## Actionable findings

### 1. High priority: preserve portable window NULL placement through both SQL generators

For a portable field `ROW_NUMBER() OVER (ORDER BY amount ASC NULLS LAST)`, both Python and public Rust drop the explicit NULL placement. On data `10, NULL, 5`, changing DuckDB `default_null_order` to `NULLS_FIRST` produces `(NULL,1), (5,2), (10,3)`, instead of `(5,1), (10,2), (NULL,3)`.

Native Rust additionally emits bare `DESC` for `ROW_NUMBER() OVER (ORDER BY amount DESC)`. With DuckDB's normal NULLS_LAST default it produces `(10,1), (5,2), (NULL,3)` instead of the latest Ossie default `(NULL,1), (10,2), (5,3)`. Explicit NULLS LAST can also disappear in native output.

The [latest expression specification](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/expression_language.md#L854) requires explicit placement where session defaults can change and specifically names DuckDB and Snowflake. Losing an explicitly authored NULLS LAST is a semantic defect beyond merely lagging the newest default rule.

Implementation anchors: `sidemantic/interchange/ossie/portable.py:299` (SQLGlot generation), `sidemantic-rs/src/adapters/ossie_sql.rs:409` and `:446` (portable transformation and generation). Reproduced end to end through catalog import, compilation, and DuckDB execution: 12 native Rust scenarios and 16 Python/public-Rust scenarios. See retained `public_windows.py.txt`, `native_rust_windows.py.txt`, and `evidence.json`.

Recommended change: encode Ossie defaults and explicit clauses in the AST and preserve them through final generation, including subsequent compiler passes; regression-test both session settings. Warehouse behavior beyond DuckDB remains unverified.

### 2. High compatibility priority: add a new pinned profile for current model syntax

All three routes reject documents that the latest official validator accepts:

- `expression: {sql: amount}`: unknown `sql`, missing `dialects`. Latest `sql` is the authoritative portable expression when an OSSIE_SQL_2026 dialect variant is also present.
- `datasets[].metrics`: unknown property. This blocks the unchanged official SCOTT model and therefore the A01 query case before query evaluation.
- A `POSTGRES` dialect variant alongside a usable `ANSI_SQL` fallback: enum rejection. Latest dialect names are arbitrary nonempty strings, with unsupported variants opaque to consumers.

Python anchors: `sidemantic/interchange/ossie/profiles.py:8`, `schemas/logical/0.2.0.dev0-b6c702e/schema.json:40`, `:114`, `:229`; `lowering.py:147` and `:625`. These latter relative paths are under `sidemantic/interchange/ossie/`.
Rust anchors: `sidemantic-rs/src/adapters/ossie.rs:73`, `:1003`, `:1243`, `:1255`.

Upstream requirements: [Expression schema](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/ossie-schema.json#L113), [metric scoping](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/spec.md#L474).

Recommended change: add a revision-specific profile without mutating the historical contracts; implement `sql` precedence, dataset field/metric namespace checks, and dataset metric source/field resolution in both importers. Refreshing JSON Schema alone cannot implement these semantics.

### 3. Required portable FILTER aggregates fail at the shared import boundary

`SUM(orders.amount) FILTER (WHERE orders.amount > 10)` passes latest official validation and local schema validation, but shared lowering returns `ossie.lowering.expression_invalid`: `OSSIE_SQL_2026 expressions cannot contain Where`. No executable scopes remain. This affects Python and public Rust; native Rust accepts the same probe.

Cause: `sidemantic/interchange/ossie/portable.py:118` rejects every `exp.Where`, including the AST node inside an aggregate filter. The [latest requirement](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/expression_language.md#L258) makes FILTER mandatory for aggregates and requires preserving output groups. M3's CASE-based fixture does not exercise this syntax.

Recommended change: distinguish aggregate filter predicates from standalone query WHERE clauses, preserve the input-level filter, and test groups with no matching rows. Do not promote the predicate into query WHERE.

### 4. Standalone ontology mappings are not recognized

Latest `examples/flights.ontology.yaml` parses and validates in Python with zero executable scopes, as intended for ontology preservation. The companion `examples/flights.mapping.yaml` is rejected: `ontology_ref`, `semantic_model_ref`, and `concept_mappings` are unexpected, and `datasets` is required. Parser family detection supports only logical and ontology documents (`sidemantic/interchange/ossie/parser.py:279`).

Upstream introduced a separate [mapping schema](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/ontology/mapping.json) and reference cross-checking. This is a Layer 4 interchange gap, not a demand for ontology execution in either backend.

Recommended change: add a preserved mapping document family, pinned schema assets, and explicitly bounded reference resolution/validation; retain the non-executable contract.

### 5. Current validation/export semantics need a revision-specific tightening

The shared importer accepts `vendor_name: ''`, although the latest schema rejects it. It also accepts model metric references such as `double_total = total * 2`, unqualified model metric fields, and undeclared `orders.not_declared` without diagnostics. Latest normative model rules prohibit metric-to-metric references and require model metric `dataset.field` references; the current upstream validator reports these model-reference issues as **warnings**, not rejection. Therefore official-validator success alone cannot establish normative conformance.

Native Python graph synthesis with `portable_only=True` can emit a derived `total * 2` reference unchanged and report success; the latest validator warns on the exported document. This finding concerns native graph synthesis: import followed by synthesis can expand the reference correctly.

Anchors: `sidemantic/interchange/ossie/schemas/logical/0.2.0.dev0-b6c702e/schema.json:43`, `sidemantic/interchange/ossie/synthesis.py:635` and `:680`; [latest metric expression contract](https://github.com/apache/ossie/blob/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/core-spec/spec.md#L415). Retained `native_export.py.txt` reproduces the export case. These are latest-profile gaps; historical profiles should preserve their advertised compatibility behavior.

## Validation performed

Commands ran from `/workspace/sidemantic` unless noted. No production implementation was edited.

| Check | Result |
|---|---|
| Focused Python Ossie suite listed below | 820 passed, 322 deselected, 3 warnings |
| Shared/native Rust forward parity, engine conformance, aggregate inputs | 528 passed, no skips |
| `cargo test -p sidemantic --test ossie_forward` | 4 passed |
| `cargo test -p sidemantic --lib ossie` | 27 passed, 645 unrelated filtered |
| Latest official validator on 8 M0–M3 inputs + SCOTT | 9/9 passed; bare model references warn |
| Latest official ontology/mapping validators on flights examples | 2/2 passed; two mapping expressions warn that field-reference checking is skipped |
| Python source roundtrip through latest grader | 8 documents; 33 PORTABLE elements |
| Python graph synthesis through latest grader | 8 documents; 23 PORTABLE, 8 REWRITTEN, 2 RELABELED |
| Fresh M0–M3 metric executions | 8 Python, 8 public Rust, 8 native Rust successful |
| A01 physical DuckDB reference query | `(29025.00, 14)`, matching `expected.sql`; oracle check only |

The A01 Ossie query itself was not passed: its model cannot import. Passing an equivalent physical query is not an Ossie interface conformance result. Existing CI has broad shared engine coverage; the central gap is that its upstream schemas and corpus remain pinned. The full unrelated project suite, live warehouse matrix, exhaustive Layer 2/3 semantics, and ontology reasoning were not run.

Python suite:

```bash
.venv/bin/python -m pytest -q tests/interchange/ossie tests/adapters/osi/test_ossie_adapter.py tests/core/test_ossie_ir_fields.py -k 'not public_rust and not native_rust and not match_both_importers'
```

Rust/shared suite and toolchain setup:

```bash
export PATH=/workspace/.onboarding/cargo/bin:$PATH
export CARGO_HOME=/workspace/.onboarding/cargo
export RUSTUP_HOME=/workspace/.onboarding/rustup
cargo test -p sidemantic --test ossie_forward
cargo test -p sidemantic --lib ossie
.venv/bin/python -m pytest -q tests/adapters/osi/test_rust_ossie_forward_parity.py tests/interchange/ossie/test_ossie_engine_conformance.py tests/semantic_conformance/test_ossie_aggregate_inputs.py
```

Official validator, from `/workspace/ossie-latest` (isolated `uv` script environment honors its declared dependencies):

```bash
uv run validation/validate.py compliance/portability/m0/*.yaml compliance/portability/m1/*.yaml compliance/portability/m2/*.yaml compliance/portability/m3/*.yaml compliance/query/scott/scott_model.ossie.yaml
uv run validation/validate.py --schema ontology/mapping.json examples/flights.mapping.yaml
uv run validation/validate.py --schema ontology/ontology.json examples/flights.ontology.yaml
```

The Sidemantic environment used SQLGlot 30.23.0, jsonschema 4.25.1 and PyYAML 6.0.3. Assessment scripts importing the validator ran in that environment; the independent official command above installed the validator's declared dependencies, including jsonschema >=4.26.0. An initial Rust test invocation lacked Cargo on PATH; the corrected complete run passed and is the reported result.

## Retained reproductions

The sibling directory `ossie-conformance-2026-11-05/` contains assessment scripts, compact `evidence.json`, and exact `probe-inputs.json`. Scripts use the fetched `/workspace/ossie-latest` checkout and repository test harness; run from the repository root with the Rust environment above where applicable:

```bash
.venv/bin/python docs/reviews/ossie-conformance-2026-11-05/python_assessment.py.txt
.venv/bin/python docs/reviews/ossie-conformance-2026-11-05/rust_assessment.py.txt
.venv/bin/python docs/reviews/ossie-conformance-2026-11-05/public_windows.py.txt
.venv/bin/python docs/reviews/ossie-conformance-2026-11-05/native_rust_windows.py.txt
.venv/bin/python docs/reviews/ossie-conformance-2026-11-05/native_export.py.txt
```

Scripts are assessment artifacts, not installed product code or a new CI gate. Outputs go under `/tmp`. Python and Rust fixture execution harnesses intentionally use different controlled row values, so compare each to its own data setup rather than comparing their raw totals across scripts.

Suggested implementation order: fix NULL ordering, add the new immutable model profile and its lowering rules, unblock FILTER, then add mapping preservation and tightened semantics. Keep current pinned-profile tests and add a separately pinned latest-draft corpus so future upstream movement is visible.
