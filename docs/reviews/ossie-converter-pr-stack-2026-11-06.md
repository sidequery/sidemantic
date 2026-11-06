# Converter PR map — 2026-11-06

The combined implementation in [#469](https://github.com/sidequery/sidemantic/pull/469)
is superseded by focused PRs. Each code PR includes its own regression tests and
compatibility documentation. The research, initial implementation validation and
review-resolution records remain together in this documentation change.

| PR | Scope | Review base / prerequisite |
|---|---|---|
| [#470](https://github.com/sidequery/sidemantic/pull/470) | Shared export fidelity, staged publication and Ossie modes | `main` |
| [#471](https://github.com/sidequery/sidemantic/pull/471) | Directed joins, aggregate populations and nested role keys | #470 |
| [#472](https://github.com/sidequery/sidemantic/pull/472) | Cube and MetricFlow source semantics | #471 |
| [#473](https://github.com/sidequery/sidemantic/pull/473) | Hex, Holistics and Omni fidelity | #472 |
| [#474](https://github.com/sidequery/sidemantic/pull/474) | Snowflake keys/types and LookML relationships | #473 |
| [#475](https://github.com/sidequery/sidemantic/pull/475) | ThoughtSpot, TMDL and GoodData fidelity | #474 |
| [#476](https://github.com/sidequery/sidemantic/pull/476) | Optional guarded Lightdash and Sigma bridges | #475 |
| This documentation PR | Research comparison, evidence and review resolutions | `main` |

## Merge order

Merge the code PRs in sequence: #470 → #471 → #472 → #473 → #474 → #475 → #476.
Each PR targets its immediate predecessor to keep its review diff focused.
After a predecessor merges, retarget the next PR to `main` before merging it.
Research documentation is independent of code execution; references
to newly introduced compatibility guides resolve once their companion PRs merge.

The initial split used sibling adapter branches. The final sequence brings
upstream changes into each successor so shared compatibility-table and test-fixture
conflicts can be resolved before merging, while CI checks the combined branches
concurrently. Changes to `docs/compatibility/index.md` can conflict;
regenerate it from the combined tree with `scripts/generate_compatibility_docs.py`
rather than choosing either stale version. The local recombination check found
no conflicting product behavior; overlapping target-model fixtures were deduplicated.

## Initial split verification

Before the PR-review fixes below, all product files under `sidemantic/` in the
recombined tree were byte-for-byte identical to reviewed implementation commit
`a6a6b47f5b594c58fbff080a329f863747ac8e93`. The only test-content change separated
the former combined Ossie/bridge CLI test into an Ossie test and a parametrized
bridge test. Directed-relationship documentation moved to a dedicated guide.

Isolated-branch checks passed: 257 foundation tests; 186 planner/conformance and
54 handoff tests; 510 Cube/MetricFlow tests; 275 Hex/Holistics/Omni tests; 722
Snowflake/LookML tests; 252 BI tests; and 301 bridge/CLI/contract tests with one
skip. Counts include overlapping shared contract checks and must not be summed.
Actual pinned CLIs were enabled for bridge validation. Each branch passed lint,
formatting and its generated-document checks. The earlier full-suite environment
limits remain recorded in the [implementation report](ossie-converter-implementation-2026-11-06.md).

## PR review resolutions

All six actionable PR-review findings were fixed and their review threads resolved:

| PR | Fix | Regression coverage |
|---|---|---|
| #470 | Preserve an existing destination directory's shape when staging an export, including names such as `datasets.v1`. | BSL, Superset and ThoughtSpot API/CLI exports; rejected exports preserve existing bytes. |
| #471 | Detect inverse fanout along the joins actually selected for a query, including directed cycles with an alternate return path. | Repeated orders do not duplicate customer credit; dimension and filter-only queries retain the expected aggregates. |
| #472 | Remove cleared filters, descriptions and labels when exporting latest-format MetricFlow conversion metrics. | Cleared fields stay absent while unrelated source modifiers survive. |
| #473 | Write stripped legacy Omni configuration even when empty, and clear obsolete relationship files during in-place export. | Reimport does not resurrect removed or edited predicates; unrelated configuration survives. |
| #474 | Reconcile runtime `bsl_how` edits with retained source join metadata when exporting LookML. | Edited left joins retain unmatched rows, and conflicting edits reject. |
| #475 | Recognize unchanged native ThoughtSpot column bindings containing spaces, punctuation or non-ASCII characters. | Native names round-trip exactly; unsupported edited expressions still reject. |

The Omni round-trip regression now selects its engine explicitly: Python checks
the resulting rows, while Rust checks the explicit unsupported-feature error for
forward-only relationships with fallback disabled. The full CI suite also exposed
two cross-format fixtures whose referenced customer model had been omitted from
the warehouse split. Those fixtures now supply the model and assert relationship
keys. LookML additionally maps the legacy Omni `always_left` join type to
`left_outer`, with an unmatched-row regression.

After these follow-ups, 728 warehouse adapter/document checks passed, and the
32 affected cross-format/relationship tests passed on each engine in the combined
tree. These are local validation results; the final GitHub CI runs and merge status
remain recorded on the individual PRs.
