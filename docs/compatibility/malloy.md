# Malloy compatibility

Sidemantic imports `.malloy` files with the bundled ANTLR grammar and maps the supported subset into native `Model`, `Dimension`, `Metric`, `Relationship`, `Explore`, and `SavedQuery` objects. It can also export semantic models back to Malloy.

The statuses below describe the Python adapter and query engine:

- **Supported** means the behavior has a native representation and regression coverage.
- **Partial** means only the stated subset is represented; unsupported shapes are omitted or rejected rather than approximated.
- **Unsupported** means no faithful native representation is currently emitted.

Lenient import records blocked or unsupported features and omits unsafe partial objects. `MalloyAdapter(strict=True)` raises at the same safety boundary. Syntax errors remain a separate `MalloySyntaxError` boundary.

## Sources and physical schema

| Feature | Status and boundary |
|---|---|
| `source: name is connection.table('path')` | **Supported.** The table and connection identifier are retained. Physical columns are exposed as intrinsic dimensions when the model is added to a `SemanticLayer`. |
| `source: name is connection.sql(...)` | **Supported without interpolation.** Static short and triple-quoted SQL is stored in `Model.sql`. SQL containing `%{ ... }` interpolation is rejected atomically because retaining it as raw SQL would not preserve Malloy substitution semantics. |
| Multiple sources and recursive directory discovery | **Supported.** Conflicting flat graph names are rejected in strict mode and omitted with diagnostics in lenient mode. |
| `source: child is base extend { ... }` | **Supported.** The raw child retains `Model.extends`; effective inheritance, including schema exposure controls, is resolved for consumption and export. Forward source references are rejected. |
| Explicit `primary_key:` | **Supported.** The declared physical key is retained and exported. |
| Source without `primary_key:` | **Supported.** The key remains unknown (`None`); the adapter does not manufacture an `id` key. A `with` join that requires an undeclared target key is rejected. |
| Intrinsic physical fields | **Supported for introspectable table/SQL sources.** `auto_dimensions` plus `SchemaExposure` expose physical columns without requiring passthrough `dimension: x is x` declarations. |
| Pipeline source (`base -> { ... } extend { ... }`) | **Unsupported/rejected.** A source-definition pipeline is omitted atomically in lenient mode and raises in strict mode; neither its base nor its descendants are retained as an approximation. |
| Old source refinement syntax (`base + { ... }`) | **Partial.** Supported source-level fields and properties in the refinement block are imported; query/view refinements are not. |
| `compose(...)` | **Unsupported/rejected.** Composition is omitted atomically rather than retaining only its first source. |
| Parameterized source declarations and invocations | **Unsupported/rejected.** `sourceParameters` and `sourceArguments` are not substituted; the affected source is omitted atomically. |
| Source-from-query (`from(...)`) | **Unsupported/rejected.** Query-backed source expressions are not degraded to their first or base source. This does not affect the bounded top-level `query:` mapping described below. |
| Virtual or otherwise non-introspectable sources | **Unsupported for intrinsic schema exposure.** Lenient mode keeps only safely declared structure and records a blocked feature; strict mode rejects the source shape. |

## Source governance and access

| Feature | Status and boundary |
|---|---|
| Source `where:` | **Supported as an invariant filter.** Ordered predicates populate `Model.invariant_filters`, are conjoined before joins and aggregation, execute on every query path, and round-trip as separate `where:` clauses. They are not reusable `Segment` objects. |
| Multiple source filters | **Supported.** All predicates are retained in declaration order and combined conjunctively. |
| Unsafe source filter expression | **Rejected atomically.** Lenient mode omits the affected source; strict mode raises. |
| `accept:` | **Supported.** The allowlist is enforced during physical schema introspection, before excluded columns can become public dimensions. |
| `except:` | **Supported.** Exclusions compose with inherited controls. When both `accept` and `except` are present, the effective allowlist is narrowed. |
| Primary-key exposure | **Supported.** `SchemaExposure.include_primary_key` keeps an explicitly declared physical key available even when schema controls are active. No key is inferred when absent. |
| `private` / `internal` dimensions, measures, and renames | **Supported with native visibility.** Fields are non-public; metric visibility and Malloy access metadata are retained and exported. Internal and private both enforce a non-public runtime boundary, although Sidemantic cannot reproduce every Malloy-internal visibility nuance. |
| Private renamed physical columns | **Supported.** Both the public alias and underlying physical name are excluded from intrinsic public exposure. |
| Inherited exposure controls | **Supported.** Child `accept`, `except`, and private fields narrow or compose with the parent; flattened export/reparse preserves the effective boundary. |
| `include { ... }` blocks | **Unsupported/rejected.** Include projection and access modifiers are not applied. The limitation is reported in lenient mode and rejected in strict mode rather than silently broadening or narrowing inherited fields. |
| Non-public joins | **Unsupported.** `Relationship` has no equivalent field-access boundary, so private/internal joins are omitted in lenient mode and rejected in strict mode. |
| Strict physical introspection failure | **Fail closed at model registration.** A strict, introspectable source may parse before a connection is available, but adding it to a layer raises if its physical schema cannot be read. |

## Dimensions and expressions

| Feature | Status and boundary |
|---|---|
| Direct field paths, literals, parentheses, unary operators | **Supported.** Qualified semantic paths are retained. |
| Arithmetic, comparison, logical, and null predicates | **Supported through typed lowering.** Precedence and non-associative tree shape are preserved; null equality becomes `IS NULL`/`IS NOT NULL`, while ordered null comparisons are rejected. |
| Casts | **Supported for the bounded neutral type set.** Malloy `number`, `string`, boolean, date/time, and timestamp-family casts render for DuckDB, PostgreSQL, BigQuery, or Snowflake. Unsupported target types are rejected. |
| Time truncation and fixed duration arithmetic | **Supported for the validated dialect/type matrix.** For example, BigQuery date plus a sub-day duration is rejected rather than approximated. |
| Regex comparison | **Supported.** Rendering is dialect-aware (`REGEXP_MATCHES`, PostgreSQL operators, `REGEXP_CONTAINS`, or `REGEXP_LIKE`). Established DuckDB dimension spellings retain their exact legacy-compatible form. |
| Standard scalar functions | **Partial.** A bounded, arity-checked set such as `abs`, `concat`, `coalesce`, `length`, `lower`, `replace`, `round`, `substring`, and `upper` is supported. Unknown/vendor functions are rejected. |
| `pick`, SQL-style `case`, apply-pick, coalesce, date literals, and supported partial match trees | **Supported on the established exact compatibility path.** Every descendant is recursively validated before legacy transformation. |
| Backtick identifiers | **Supported.** Quoting is rendered for the target dialect, including BigQuery escaping rules. |
| Unknown functions, parameters/given references, safe-cast forms, filter strings, ranges, record literals, locality, `all`, and `exclude` | **Unsupported in typed fields.** A rejected descendant blocks the complete dimension, metric, aggregate argument, or filter; raw Malloy text is not passed through as SQL. |
| Cross-source/relationship field paths | **Partial.** Paths are retained only when reachable through the source's relationship roles; arbitrary qualifiers and ambiguous unqualified leaves are rejected in query mapping. |

Dimension types and granularities are inferred from the lowered expression. Time-like casts, truncations, literals, durations, and time-oriented names produce time dimensions; comparisons produce booleans; arithmetic produces numeric dimensions; categorical is the fallback.

Dialect-sensitive expressions use the source connection's resolved SQL dialect. Built-in DuckDB, PostgreSQL, BigQuery, and Snowflake connection identifiers are known. Custom connection identifiers require an explicit `connection_dialects={connection_id: dialect}` mapping. Without one, dialect-sensitive fields, aggregate arguments, and filters are omitted with a diagnostic in lenient mode and rejected in strict mode.

## Measures

| Feature | Status and boundary |
|---|---|
| `count()`, `count(field)`, `count_distinct(field)` | **Supported.** Malloy `count(field)` maps to distinct count. |
| `sum`, `avg`, `min`, `max` | **Supported**, including expression arguments and established dot-method forms such as `cost.sum()`. |
| Derived measures and aggregate arithmetic | **Supported** when every referenced expression is safely lowerable. Aggregate functions in a compound expression are retained as executable derived SQL. |
| Filtered measures and chained filter refinements | **Supported.** Filters are lowered separately, kept in source order, and conjoined. |
| Dialect-sensitive aggregate arguments and filters | **Supported with a resolved dialect.** The aggregate root remains a native metric while casts, date truncation, and regex predicates render for the target dialect. |
| Rejected aggregate descendant | **Rejected atomically.** No partial metric or unsafe raw fallback is produced. |
| Measure references inside derived SQL | **Partial.** Native derived metric references are supported by the Sidemantic compiler, but Malloy symmetric/ungroup locality semantics are not inferred. |
| `all`, `exclude`, `source.count()` locality/symmetric semantics | **Unsupported.** These require Malloy query semantics not represented by a native metric. |

## Joins and relationship roles

| Feature | Status and boundary |
|---|---|
| `join_one: target with source_column` | **Supported** when `source_column` is one physical column and the target has exactly one declared/inherited primary key. The local foreign key and target primary key are both retained. |
| `join_many: target on ...` | **Supported** as `one_to_many`, with source and related key orientation retained. |
| Equality `on` predicates | **Supported exactly.** Differently named keys, either operand order, alternate keys, and ordered composite key pairs populate both relationship key sides. Explicit `on` keys never fall back to the model primary key. |
| Additional SQL-compatible predicates | **Supported.** Equality key metadata is retained and the complete predicate is stored as executable `Relationship.sql` with `{from}` / `{to}` placeholders. Range predicates and predicates referencing both sides execute in either traversal direction. |
| Arbitrary or Malloy-only predicate | **Unsupported.** Conditions that cannot be conservatively lowered to a validated SQL predicate are omitted/rejected. |
| Relationship alias (`role is source`) | **Supported.** The role name and canonical related model remain distinct, so multiple roles to one model compile, execute, export, and reparse independently. Module dependency emission follows the canonical related model. |
| Inline table/SQL source in a join | **Supported when the inline source is valid.** An invalid inline invariant omits only the inline model and relationship in lenient mode; it does not contaminate the outer source. Strict mode raises. |
| Bare `join_cross` | **Supported**, including a role alias. Conditional cross joins are rejected. |
| Join direction | **Partial.** Default/explicit `left` is supported. `inner`, `right`, and `full` are reported and omitted/rejected until the core relationship engine represents those semantics. |
| Non-column `with` expression or missing/composite target key | **Unsupported.** It is rejected rather than guessed. |

Native custom join SQL export is placeholder-aware. `{from}` and `{to}` are rewritten only in executable SQL regions; placeholders inside literals, quoted identifiers, or comments cause export to fail closed. The rendered predicate is validated before it is written.

## Modules, imports, and exports

| Feature | Status and boundary |
|---|---|
| Import-all and selective imports | **Supported.** Only exported symbols are visible to importing modules. |
| `import { local_name is exported_name }` | **Supported.** Malloy's local binding comes first; aliases retain defining-file provenance. |
| Explicit `export { ... }` | **Supported as an allowlist.** Multiple exports compose, and imported bindings can be re-exported. Exporting a symbol before it is defined/imported is rejected. |
| Transitive imports | **Supported.** Source dependencies required by inheritance, relationships, and imported queries are emitted under the correct local binding. |
| Query-only import | **Supported for a supported top-level query.** Its source remains bound in the query's defining module; it is not rebound to a same-named source in the entry file. |
| Project-relative paths | **Supported with containment checks.** Absolute paths, `file:`/network URLs, and paths escaping `import_root` are rejected. |
| Missing import | **Fail closed.** Strict mode raises; lenient mode retains independent declarations and records a blocked diagnostic. |
| Import cycle | **Detected and rejected** with the complete path chain. Relationship dependency cycles terminate without repeatedly emitting the same symbol. |
| Duplicate or ambiguous bindings/flat graph names | **Rejected.** Lenient mode removes the ambiguous binding and dependent models instead of selecting a winner. |
| Module-preserving Malloy export | **Unsupported.** Export writes a flattened semantic graph; it does not reconstruct the original file/import/export topology. |

## Top-level queries and consumption objects

Direct-source, single-stage top-level `query:` definitions are **supported** for the following bounded mapping:

| Malloy query operation | Native representation |
|---|---|
| Named source | Generated `Explore.model`; the source binding is preserved across module aliases/imports. |
| `group_by:` | `SavedQuery.dimensions` |
| `aggregate:` | `SavedQuery.metrics` |
| Dimension-only `where:` | `SavedQuery.filters`, compiled as `WHERE` |
| Metric-bearing `having:` | `SavedQuery.filters`, classified by the native compiler as `HAVING` |
| `order_by:` | `SavedQuery.order_by`; ordered fields must be selected and uniquely resolvable. |
| `limit:` | `SavedQuery.limit` |

The adapter inserts the generated `Explore` and `SavedQuery` only after both validate against a staging graph. Field catalogs are restricted to the query source and bounded reachable relationship-role paths; unrelated models, wrong roles, wrong qualifiers, ambiguous leaves, duplicate selections, and invalid WHERE/HAVING classification produce diagnostics and no partial consumption object. Self and two-model relationship cycles terminate while sibling roles remain independently addressable.

Query field validation uses declared semantic dimensions, metrics, and keys. Intrinsic physical columns discovered later during `SemanticLayer` registration are not available to top-level query mapping unless they are also declared in the Malloy source.

The following remain **unsupported** in adapter integration:

- source-local `view:` definitions (the standalone mapper exists, but source-local views are not retained and inserted by `MalloyAdapter`);
- multi-stage pipelines;
- query/view refinements;
- nesting;
- calculations, query-local joins/extensions, sampling, indexing, timezones, wildcards, output renames, and source arguments;
- `run:` as a retained named consumption contract.

## Rename, annotations, and source metadata

| Feature | Status and boundary |
|---|---|
| `rename: new is old` | **Supported**, including backtick identifiers and access modifiers. It becomes a dimension whose SQL points at the old physical field. |
| `##` descriptions and `# desc:` / `# description:` | **Supported** on models and fields and emitted on export. |
| Other `#` tags | **Supported as metadata** on models and fields. |
| `timezone:` | **Supported as model metadata.** It does not enable unsupported query-local timezone semantics. |
| Persist annotations | **Supported as model metadata**, not as a materialization executor. |
| Experimental pragmas, styles, sampling | **Parsed but not represented.** |

## Export and round-trip boundaries

| Feature | Status and boundary |
|---|---|
| Table/SQL sources, descriptions, keys, dimensions, measures, invariant filters | **Supported.** |
| Arbitrary model/field tags | **Import metadata only.** Non-description tags may be retained in native metadata during import, but export does not emit them, so they do not round-trip. |
| `accept`, `except`, private/internal fields and renames | **Supported.** Effective inherited schema exposure is preserved when export flattens a child. |
| Keyed, custom-predicate, role-aliased, and cross relationships | **Supported** within the join boundaries above. Composite joins require an exact retained predicate for export. |
| Passthrough physical dimensions | Intentionally omitted because Malloy exposes physical table columns intrinsically; re-import reconstructs them through schema introspection. |
| Unknown primary key | Preserved as unknown. An explicit `primary_key: id` is exported; `id` is not treated as an implicit default. |
| Top-level queries / saved queries | **Import-only.** Generated native consumption contracts are not exported back into Malloy query syntax. |
| Source-local views and module topology | **Unsupported**, because they are not retained in the semantic graph. |

## Differential coverage

The opt-in harness under `tests/malloy_differential/` compares the official Malloy DuckDB runtime with the Python Sidemantic path for manifest entries marked `compatible`. Executable fixtures cover core measures, empty results, a typed arithmetic dimension, conjunctive source filters, exact joins with additional predicates, ordered composite joins with fanout-safe aggregation, and intrinsic physical fields on a source without a declared primary key. Unsupported families remain declarative in the manifest and are reported without claiming runtime equivalence.
