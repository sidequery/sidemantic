# Pre-PR converter review — 2026-11-06

Reviewed the uncommitted implementation against HEAD
`a7df3c9eae7c1a9a916ebe317ee378408d7d721b`.
Six fresh reviewers split planner, shared conversion, official bridges, warehouse,
BI and Cube/YAML adapters. The initial review reproduced four P1 findings and nine
P2 findings that existing tests did not cover. **All 13 findings below were fixed
before opening the PR and independently checked.** The descriptions and line
locations below preserve the original review snapshot, not outstanding defects.

## Resolution and follow-up validation

| Findings | Resolution |
|---|---|
| 1 | Shared and CLI output validation uses the target format; native text is not parsed as YAML because of its filename. Empty output still fails. |
| 2–3 | Multi-grain preaggregation recognizes both cardinality directions; join-key projection includes incoming role-instance edges. |
| 4–5 | Bridges reject unsupported calculated/sibling field references and Sigma non-simple source components before publishing output. Supported physical references and simple qualified paths remain tested. |
| 6 | Snowflake reconciles native/runtime join edits and omits an implicit join type only when unchanged. |
| 7–8 | Omni rejects stale topic references and honors edited or cleared time properties. |
| 9–10 | Hex and Cube replay source properties only while compatible with native edits; Cube reconciles time offsets and rejects ambiguous shift edits. |
| 11 | MetricFlow overlays edited conversion fields while preserving untouched native reference modifiers. |
| 12–13 | ThoughtSpot rejects unsupported native-table expressions; GoodData honors coherent unlabelled attribute rebinding and rejects conflicting edits while preserving separate native labels. |

Follow-up validation passed: 102 shared conversion/CLI/format tests; 151 planner
tests plus 65 directed/conformance checks; 621 Cube/MetricFlow/Hex tests plus a
final 11-case Cube regression run; 251 Snowflake/Omni tests; 97 ThoughtSpot/GoodData
tests; and 49 bridge tests with actual pinned executables. These suites overlap
and are not a combined test count. Independent reviewers also passed 52
planner/BI, 71 metadata and 16 real-CLI bridge checks, including the original
wrong-result reproductions. Repository lint/format, configured type checks and
the core lazy-import smoke passed. The full runtime/version matrix remains for CI.

## P1 findings

### 1. Default LookML and Malloy conversions fail validation

Location: `sidemantic/formats.py:383–384`.

`validate_conversion_output` infers serialization from the destination suffix.
The CLI defaults to `converted.{target}.yml`, including LookML and Malloy. With
`models: [{name: orders, table: orders}]`, either `convert source.yml --to lookml`
or `--to malloy` exits with a YAML parser error on valid target-language output.
Validate using the target format, or select a matching default extension.

### 2. Directed one-to-many joins double a derived metric

Location: `sidemantic/sql/generator.py:3249`.

The preaggregation decision recognizes only `many_to_one` paths. For forward-only
`customers → orders` declared `one_to_many`, customer credit 100, two orders worth
4 and 5, and `double_credit = credit * 2`, querying both metric populations by
customer region returns `(east, 400, 9)` instead of `(east, 200, 9)`. HEAD returns
the correct result on identical input. Account for fanout in the permitted
one-to-many direction as well.

### 3. Forward-only nested roles omit their join keys

Location: `sidemantic/core/semantic_graph.py:600`; interacts with
`SQLGenerator._model_needs_keyed_join_columns`.

For `orders → buyer(customers) → buyer$regions`, forward-only many-to-one edges
remove the reverse adjacency that previously identified the nested target as a
keyed join participant. Querying order amount by `buyer$regions.region` generates
a CTE containing only `region`, then joins on its nonexistent `id`. DuckDB raises
`BinderException`; HEAD executes successfully. Required key projection must also
consider incoming role-instance edges or the actual query join plan.

### 4. Both official bridges misbind calculated-column dependencies

Location: `sidemantic/adapters/official.py:376–386`.

Lightdash accepts `amount.sql = ${TABLE}.raw_amount`, `doubled.sql = ${amount} * 2`
and a sum metric on `doubled`. The official converter emits the logical reference
as physical SQL; the bridge's structural checks accept it. On a table containing
`raw_amount=10, amount=100`, querying the metric returns **200 instead of 20**.
Sigma reproduces the same result with `amount=[raw_amount]`, `doubled=[amount]*2`
and `revenue=Sum([doubled])`. Without the physical `amount` column, execution fails
instead. Resolve native calculated-field references, or reject them before
lowering. Both reproductions used the actual pinned official CLIs.

## P2 findings

### 5. Sigma changes physical source identity

Location: `sidemantic/adapters/official.py:184–187` (export checks also at 575–580).

Native `source.path: ["a.b"]` imports as SQL `a.b`, rather than the single table
identifier `"a.b"`. With both tables present, the bridge reads schema `a`/table
`b` and returns 100 instead of the intended table's 10. Exporting
`table='"a.b"'` produces `path: ['"a', 'b"']`; quoted identifiers retain literal
SQL quoting in other cases. Preserve component boundaries and decoded identifier
values, or reject unsupported paths in both directions.

### 6. Snowflake restores an implicit join type over an explicit edit

Location: `sidemantic/adapters/snowflake.py:1267–1268`.

Import a relationship with no `join_type`, then set both its `join_type` and
`bsl_how` metadata to `inner`. Export unconditionally removes `join_type` because
`implicit_join_type` was retained; reimport restores `left_outer`. Only omit the
property while its effective semantics remain the original default.

### 7. Omni topics recreate deleted relationships

Location: `sidemantic/adapters/omni.py:859–862`.

Import a topic joining orders to customers plus an explicit keyed relationship,
clear the orders relationships, and export to a fresh directory. The retained
topic still names the join. Reimport recreates the deleted relationship with both
keys unset. Reconcile retained topics with current graph relationships or reject
conflicting edits.

### 8. Omni ignores edited time granularities

Location: `sidemantic/adapters/omni.py:957`.

Import `timeframes: [date, month]`, then set `granularity="year"` and
`supported_granularities=["year"]`. Export still emits the original timeframes;
reimport resets the grain to day and restores month. Guard native replay with an
unchanged-projection check.

### 9. Hex ignores date/timestamp granularity edits

Location: `sidemantic/adapters/hex.py:619`.

Native type replay compares only `dim.type`, although dates and timestamps both
map to `time`. Changing an imported date's granularity to hour still exports a
date and reparses as day; changing a timestamp to day retains the timestamp.
Neither case reports export loss. HEAD honored these edits. Include the imported
granularity in the replay guard or diagnose an incompatible edit.

### 10. Cube source properties overwrite an edited dimension type

Location: `sidemantic/adapters/cube.py:1753`.

Import a switch dimension, change it to numeric with `sql="amount"`, then export.
Unconditional source-property replay restores `type: switch` and the original
values. HEAD exported `type: number`. Guard these properties against the current
projection, as the CASE branch already does. A related probe found retained
`time_shift.interval` overriding an edited native `time_offset` at line 1905.

### 11. MetricFlow legacy conversion export ignores edited summary fields

Location: `sidemantic/adapters/metricflow.py:1114–1115`.

Import a legacy conversion metric with a seven-day window, then change its
`graph.metadata["metricflow_conversion_metrics"][name]["window"]` to `"30 days"`.
Export returns the original source definition and still emits seven days. The
same branch bypasses other editable summary fields, unlike the latest-spec
branch. Overlay current values while preserving native reference modifiers, or
reject conflicting changes.

### 12. ThoughtSpot Table routing drops edited expressions

Location: `sidemantic/adapters/thoughtspot.py:1657–1663`.

Import `tests/fixtures/thoughtspot/table_joins.table.tml`, change the orders
metric SQL to `amount * 2`, export and reimport. SQL becomes `amount` because the
new unconditional Table route substitutes a simple column for the expression.
HEAD emitted a Worksheet formula and retained the expression. Validate field
expressions before retaining Table shape or export them faithfully.

### 13. GoodData retains an obsolete physical attribute binding

Location: `sidemantic/adapters/gooddata.py:788`.

Import an unlabelled attribute `{id: status, sourceColumn: old_status}`, change its
dimension SQL to `new_status`, and export. The attribute still points to
`old_status`; a newly generated label points to `new_status`. Changing the model's
primary key as well does not fix the retained attribute binding. A replacement
source containing only `new_status` therefore produces an LDM referencing a
missing column. HEAD rebound the attribute. Preserve separate native keys when
applicable, but guard or reject incompatible edits to an originally unlabelled
attribute.

## Verification boundaries

Reviewers ran targeted suites: 77 conversion/CLI tests, 149 formats/Ossie tests,
20 directed-relationship tests, 33 real-CLI bridge tests, 39 Cube/YAML regression
tests, 29 warehouse/Omni regression tests and 241 BI adapter tests. They passed;
the reproductions above expose gaps in those assertions. The lead independently
reproduced the default-output failure and both planner failures. Baseline module
comparisons verified the planner, Hex, Cube and ThoughtSpot regressions.

No new Holistics or TMDL issue was confirmed. This review did not repeat the full
suite or certify live vendor deployment. It did not change product code, create
commits or open a PR.
