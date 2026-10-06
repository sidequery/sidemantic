# Reduced generated regressions

These fixtures retain their original seed/index and actual reduced definitions,
query, and data. Feature labels describe the original generated case, not a claim
that every feature remains after reduction. Replay uses the stored case directly.

| Fixture | Adjudication | Independent result test |
| --- | --- | --- |
| `sibling-fanout.case.json` | One item worth 3 must contribute once when two refunds share a label. Rust incorrectly returned 6. | `test_sibling_fanout_parity.py` |
| `scalar-default-time.case.json` | A graph calculation with no time grouping combines independent source totals. Python incorrectly reapplied a model default inside its source subquery. | `test_default_time_differential.py` |
| `cumulative-null-date.case.json` | A NULL date remains NULL, and a cumulative window follows known periods before the NULL bucket. DuckDB 1.3.2 produced different results for equivalent projections; 1.4.0 exposed a date sentinel. Both independent cases pass on 1.4.4 and 1.5.0. | `test_cumulative_null_dates.py` |
| `cross-source-cumulative.case.json` | A cross-source scalar calculation beside a cumulative metric retains independent grouped source totals. Rust rejected the graph calculation as if it required one owner. | `test_cross_source_windows.py` |
| `cumulative-dimension-collision.case.json` | Window inputs must use the grouped aliases for both sibling labels. Both engines referenced missing `base.label` instead of `base.items_label` and `base.refunds_label`. | `test_cross_source_windows.py` |

The initial reductions were budgeted and preserve the original failure class;
they are not claims of globally minimal inputs. Their independent result tests
establish the specific semantic regression. The current reducer additionally
preserves a concrete result-mismatch witness.
