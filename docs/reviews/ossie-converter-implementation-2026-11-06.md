# Ossie converter research implementation

This follows the [pinned research comparison](ossie-converter-comparison-2026-11-06.md).
That comparison describes the baseline; the changes below implement its recommendations.

## Delivered

- Hardened Cube, MetricFlow, ThoughtSpot, TMDL, GoodData, Hex, Holistics, Omni,
  Snowflake and LookML imports/exports. The changes preserve supported native
  state, full join predicates, compound key identities and datatypes, and reject
  the unsupported semantic cases identified by the research.
- Added export fidelity reporting and staged shared conversion that protects
  existing destinations from rejected or empty output.
- Added Ossie source-dialect selection, validated document preservation and an
  explicit official-compatible export profile. The latter safely hoists dataset
  metrics and reports unspecified datatypes instead of implying type preservation.
- Added optional Lightdash and Sigma bridges to separately installed official
  CLIs, with explicit input subsets, output checks and no new core dependencies.
- Enforced forward-only relationship traversal in the Python planner, including
  target-side aggregate deduplication. Rust refuses this unsupported capability.

See [conversion guidance](../compatibility/conversion.md), the individual
[compatibility guides](../compatibility/index.md), [BI adapter details](../compatibility/bi-adapters.md)
and [Hex/Holistics/Omni details](../adapter-fidelity-yaml.md).

## Pre-review full-suite validation

Six implementation agents worked in separate scopes and cross-reviewed shared
conversion and planner changes. Final local checks used Python 3.12 with dev,
DAX, Rust and app extras, plus the SQLite ADBC driver.

- Repository Ruff, formatting, configured mypy checks, consumer typing checks,
  generated-document checks and `git diff --check` passed.
- The full Python-engine suite selected 14,060 tests: 13,820 passed, 148 skipped,
  80 failed because the native parity harness could not find `cargo`, and 12
  browser tests could not launch the downloaded Playwright browser. The default
  marker selection deselected another 167 integration tests.
- After exposing the existing Rust toolchain, all 80 affected tests passed.
  This accounts for **13,900 passing tests** from that full-suite selection.
- The 12 browser checks were retried with installed system Chromium. Every check
  was blocked on local `file://` navigation by `ERR_BLOCKED_BY_ADMINISTRATOR`.
  They remain unverified here; the browser policy was not changed.
- Full-run coverage was **80.74%**, above the configured 76% gate.
- A final 62-test interoperability run passed after the datatype diagnostic
  follow-up, with both actual pinned Lightdash and Sigma CLIs enabled. This
  overlaps the full suite and is not added to its passing-test count.
- Independent public Rust compilation confirmed rejection of
  `relationship.forward_only`. DuckDB result regressions cover directed joins,
  duplicate target rows, compound keys and metric populations.

The installed official CLIs came from converter commit
`011ea9f9a1e2c941f351e18e071622339ccae881` with Ossie submodule
`01a5488d840deda309aac68cc0b312c195369b8d`. Real probes also confirmed that the
official Cube exporter maps an unspecified datatype to string, while explicit
Decimal maps to number; compatibility diagnostics distinguish these cases.

## Review follow-up

The [independent review and resolution record](ossie-converter-pr-review-2026-11-06.md)
documents 13 subsequently fixed findings and the targeted validation run after
those corrections. It includes executable regressions for directed aggregate
populations and nested roles, default CLI output validation, and edits overriding
retained source metadata. Lightdash calculated dimension dependencies and Sigma
calculated columns/non-simple source paths are now rejected explicitly rather
than accepted with changed execution semantics. The full-suite numbers above
describe the pre-review run; the follow-up suites are recorded separately.

## Remaining boundaries

The pinned Sigma exporter can produce invalid relationship target IDs; the bridge
rejects those exports. Source preservation does not imply executable parity in
another vendor. Optional bridges require separately installed tools, and the
experimental Rust engine does not execute forward-only relationships. Live vendor
deployment, native vendor compiler certification and the full CI runtime/version
matrix were not run. No production services or catalogs were changed.
