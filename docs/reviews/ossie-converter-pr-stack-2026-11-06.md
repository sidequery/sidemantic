# Converter PR map — 2026-11-06

The combined implementation in [#469](https://github.com/sidequery/sidemantic/pull/469)
is superseded by focused PRs. Each code PR includes its own regression tests and
compatibility documentation. The research, initial implementation validation and
review-resolution records remain together in this documentation change.

| PR | Scope | Review base / prerequisite |
|---|---|---|
| [#470](https://github.com/sidequery/sidemantic/pull/470) | Shared export fidelity, staged publication and Ossie modes | `main` |
| [#471](https://github.com/sidequery/sidemantic/pull/471) | Directed joins, aggregate populations and nested role keys | #470 |
| [#472](https://github.com/sidequery/sidemantic/pull/472) | Cube and MetricFlow source semantics | #470 |
| [#473](https://github.com/sidequery/sidemantic/pull/473) | Hex, Holistics and Omni fidelity | #471, which includes #470 |
| [#474](https://github.com/sidequery/sidemantic/pull/474) | Snowflake keys/types and LookML relationships | #470 |
| [#475](https://github.com/sidequery/sidemantic/pull/475) | ThoughtSpot, TMDL and GoodData fidelity | #470 |
| [#476](https://github.com/sidequery/sidemantic/pull/476) | Optional guarded Lightdash and Sigma bridges | #470 |
| This documentation PR | Research comparison, evidence and review resolutions | `main` |

## Merge order

Merge #470 first. Its children target its branch to keep their review diffs
focused; retarget them to `main` after it merges. Merge #471 before retargeting
and merging #473. The remaining adapter and bridge PRs can proceed independently
after #470. Research documentation is independent of code execution; references
to newly introduced compatibility guides resolve once their companion PRs merge.

The adapter PRs each regenerate `docs/compatibility/index.md` for their own
isolated branch. Combining sibling PRs can conflict in this generated inventory;
regenerate it from the combined tree with `scripts/generate_compatibility_docs.py`
rather than choosing either stale version. The local recombination check had
no product-code conflicts.

## Split verification

All product files under `sidemantic/` in the recombined tree are byte-for-byte
identical to reviewed implementation commit
`a6a6b47f5b594c58fbff080a329f863747ac8e93`. The only test-content change separates
the former combined Ossie/bridge CLI test into an Ossie test and a parametrized
bridge test. Directed-relationship documentation moves to a dedicated guide.

Isolated-branch checks passed: 257 foundation tests; 186 planner/conformance and
54 handoff tests; 510 Cube/MetricFlow tests; 275 Hex/Holistics/Omni tests; 722
Snowflake/LookML tests; 252 BI tests; and 301 bridge/CLI/contract tests with one
skip. Counts include overlapping shared contract checks and must not be summed.
Actual pinned CLIs were enabled for bridge validation. Each branch passed lint,
formatting and its generated-document checks. The earlier full-suite environment
limits remain recorded in the [implementation report](ossie-converter-implementation-2026-11-06.md).
