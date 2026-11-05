# Apache Ossie compatibility

Sidemantic's Python integration handles [Apache Ossie](https://github.com/apache/ossie)
logical semantic-model, ontology, and standalone mapping source documents. Executable import and
graph synthesis apply only to logical documents. The canonical format name is
`ossie`; `osi`, `apache-ossie`, and `open-semantic-interchange` remain accepted
aliases.

The historical Python import spelling
`sidemantic.adapters.osi.OSIAdapter` now routes to this canonical contract.
The pre-profile implementation is isolated as `LegacyOSIAdapter` for explicit
migration work; format discovery, CLI aliases, and ordinary direct imports do
not bypass validation or scoped lowering.

Install the optional validator dependencies with `uv add 'sidemantic[ossie]'`,
or use `uv sync --extra ossie` in a Sidemantic checkout. Validation is offline:
Sidemantic loads only the vendored, checksum-verified schemas listed below and
does not retrieve schema resources from the network.

## Pinned profiles

Schema version, consumer profile, and YAML-versus-JSON serialization are
independent choices. Sidemantic supports these explicit contracts:

| Consumer profile | Declared version | Document family | Validation schema | Pinned upstream commit |
|---|---|---|---|---|
| `ossie-core` | `0.1.1` | Logical | `logical-0.1.1` | `faf581054dcf7964d5fe0ceae7d6f415c8ce32a5` |
| `ossie-core` | `0.2.0.dev0` | Logical | `logical-0.2.0.dev0` | `831f48e582731cf1ee2e65380ca5abf8157869c7` |
| `ossie-core` | `0.2.0.dev0` | Ontology | `ontology-0.2.0.dev0` | `831f48e582731cf1ee2e65380ca5abf8157869c7` |
| `ossie-core` | `0.2.0.dev0` | October logical | `logical-0.2.0.dev0-b6c702e` | `b6c702ed1c07e91382a69e870c875cbd19570828` |
| `ossie-core` | `0.2.0.dev0` | October ontology | `ontology-0.2.0.dev0-b6c702e` | `b6c702ed1c07e91382a69e870c875cbd19570828` |
| `ossie-core` | `0.2.0.dev0` | Latest pinned logical | `logical-0.2.0.dev0-172a3e5` | `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1` |
| `ossie-core` | `0.2.0.dev0` | Latest pinned ontology | `ontology-0.2.0.dev0-172a3e5` | `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1` |
| `ossie-core` | `0.2.0.dev0` | Standalone mapping | `mapping-0.2.0.dev0-172a3e5` | `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1` |
| `dbt-1.12` | `0.1.0` | Logical compatibility alias | Pinned `logical-0.1.1`; the retained document still declares `0.1.0` | `faf581054dcf7964d5fe0ceae7d6f415c8ce32a5` |
| `dbt-1.12` | `0.1.1` | Logical | `logical-0.1.1` | `faf581054dcf7964d5fe0ceae7d6f415c8ce32a5` |

`0.1.0` is not treated as an upstream Ossie schema version. It is accepted only
with the explicit `dbt-1.12` consumer profile. Validation, file-adapter and CLI
imports, lossless retained-source output, canonical serialization, and explicit
graph synthesis all preserve that consumer context while validating a temporary
copy against the pinned `logical-0.1.1` schema. The retained or emitted document
continues to declare `0.1.0`.

The vendored ontology runtime schema has two recorded deterministic rewrites: a
unique local `$id` and local references to the pinned logical schema. The
untouched upstream ontology schema is retained alongside it. Exact paths,
source URLs, transformations, and SHA-256 values are in
[`sidemantic/interchange/ossie/schemas/manifest.json`](../../sidemantic/interchange/ossie/schemas/manifest.json).

The September 12 snapshot retains the `semantic_model` array envelope, root
`dialects` and `vendors`, and its original expression labels. The October 1
snapshot uses a flat root containing `version`, `name`, and `datasets`, adds
`DAX` and `OSSIE_SQL_2026`, and supports ontology `prefixes` and `iri` metadata.
The November 4 snapshot adds portable `expression.sql`, dataset-scoped metrics,
open dialect names, and standalone ontology mappings. Historical snapshots
retain their original schema assets and checksums.

Parsing infers the logical snapshot from its shape. Current ontology markers
also select the newer snapshot; ambiguous ontology sources retain the older
default unless `OssieParseOptions(schema_revision=...)` selects a pinned commit.
The profile and preserved document carry that selection through serialization.
The mutable development version alone does not identify a schema revision.

New logical markers (`expression.sql`, dataset metrics, or an additional dialect
name) select the November snapshot. An ambiguous flat document keeps its
October interpretation. Use `OssieParseOptions(schema_revision=...)` or
`sidemantic convert --ossie-input-schema-revision 172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`
to validate such a source explicitly against the newer contract. The separate
`--ossie-schema-revision` option selects the synthesized output revision.
`LATEST_OSSIE_SCHEMA_COMMIT` identifies the November pin; the existing
`CURRENT_OSSIE_SCHEMA_COMMIT` constant retains its October value for compatibility.

## Pinned upstream validator gate

The conformance suite also runs the official Apache Ossie validator from a
pinned local fixture, using only the vendored upstream schemas and validator
code. It verifies that canonical Sidemantic exports pass for core `0.1.1`
(JSON), both pinned core `0.2.0.dev0` shapes, and current ontology. Deliberately
invalid documents are rejected. The gates are implemented by
[`tests/interchange/ossie/test_upstream_validator_gate.py`](../../tests/interchange/ossie/test_upstream_validator_gate.py)
and [`tests/interchange/ossie/test_current_schema.py`](../../tests/interchange/ossie/test_current_schema.py)
and do not require network access. These gates check the pinned
revision; it does not monitor future upstream changes. The `dbt-1.12`
compatibility alias is validated separately against its declared compatibility
contract; it is not presented as an upstream Ossie schema version.

The November gate in
[`tests/interchange/ossie/test_latest_upstream_gate.py`](../../tests/interchange/ossie/test_latest_upstream_gate.py)
runs the unmodified `172a3e5` validator and its local loader against the pinned
logical, ontology, and mapping schemas. It checks native graph exports as well
as upstream fixtures. The unchanged upstream A01 model, data, query, and expected
result are retained separately and exercised through both query engines and the
CLI with fallback disabled.

## Source documents and runtime graphs are different contracts

An Ossie source document is an immutable parsed YAML or JSON value. It retains
the declared document family and version, canonical data, source identity, and,
when requested, the original bytes. Unknown fields that pass the selected
schema remain in that source document.

A runtime `SemanticGraph` is a target-specific executable projection of one
logical model scope, either at the root or inside the older envelope. It contains only constructs Sidemantic can lower
safely for the selected runtime dialect. It is not an archival copy of the
source document and cannot reproduce alternate expression dialects, lexical
YAML details, ontology content, or every Ossie field.

Use the preserved-document APIs for validation, migration, and source round
trips. Use a compiled scope or its graph for execution. Converting through a
graph is synthesis, not source-document round trip.

## Validation and import policy

Import has separate parsing, pinned JSON Schema validation, semantic validation,
and lowering stages. Diagnostics have stable codes and JSON pointers; schema
diagnostics also identify the profile, schema commit, and checksum.

- `strict` is the default. Any parser, profile, schema, or semantic error blocks
  executable lowering. A lowering error also removes all executable scopes
  rather than returning a partly trusted strict result.
- `permissive` preserves the invalid source and projects only independently safe
  constructs. Unsafe fields, metrics, relationships, or datasets are excluded
  with diagnostics; they are not silently accepted. Resulting scopes remain
  marked `valid=False`.
- An invalid permissive scope is not executable by default.
  `SemanticLayer.from_catalog(...)` requires the explicit
  `allow_invalid=True` escape hatch. The graph-returning compatibility adapter
  also refuses an invalid project, so use the document/catalog APIs when the
  goal is to inspect a partial migration result.

The CLI exposes permissive parsing as `--ossie-permissive` and profile selection
as `--ossie-consumer-profile`, including automatically detected Ossie conversion
inputs. These options do not weaken the runtime binding
safeguards described above.

## Scoped `SemanticCatalog` behavior

Each logical `semantic_model` becomes an isolated `CompiledSemanticScope` in a
`SemanticCatalog`. Namespaces are never flattened or merged, so two scopes may
use the same dataset or metric names without colliding.

- A catalog with one scope may infer that scope.
- A catalog with multiple scopes requires an explicit scope ID, supplied by the
  CLI as `--ossie-scope`.
- Duplicate semantic-model names are diagnosed; deterministic permissive IDs
  use `name@index`.
- Directory imports preserve document boundaries and qualify IDs as
  `relative/path.yaml::scope`. Generated `target` and `dbt_packages`
  directories are skipped.
- Catalog membership and compilation metadata are immutable snapshots. Runtime
  layers receive isolated graph clones.
- A compiled scope records document/content/compilation identity, validation
  state, lowering policy, schema provenance, and target dialect. Cache identity
  changes when relevant source, profile, dialect, or policy inputs change.
- `SemanticLayer.from_catalog` rejects a runtime connection whose dialect does
  not match the scope's compiled target dialect.

## Target-aware expression selection

Executable import requires a target dialect. For each field or metric,
Sidemantic selects the exact matching Ossie expression when Ossie defines a
label for that target: `BIGQUERY`, `SNOWFLAKE`, or `DATABRICKS`. Otherwise it
lowers an `OSSIE_SQL_2026` variant when present, then falls back to an explicit
`ANSI_SQL` variant. If none exists, the construct is
diagnosed and excluded; Sidemantic does not select an arbitrary first variant,
relabel SQL, or claim that unchanged text was transpiled.

For the November profile, `expression.sql` supplies the authoritative portable
expression, including when an `OSSIE_SQL_2026` dialect variant is also supplied.
Unknown nonempty dialect labels can be preserved as opaque variants; a usable
target or portable variant is still required for execution.

The portable dialect is parsed as source syntax before translating required
functions to the execution dialect. It preserves logarithm argument order,
truncation precision, date arithmetic, ANSI window frames, and exact statistical
aggregates. BigQuery exact `MEDIAN` and ordered-set percentiles need query-level
lowering and produce explicit unsupported-target diagnostics. They are never
silently changed to approximate aggregates.

Portable aggregate `FILTER (WHERE ...)` predicates stay attached to the
aggregate; they do not remove output groups through a query-level `WHERE`.
Portable window ordering treats NULL as the largest value by default: last
for ascending order and first for descending order. Explicit NULL placement
and these defaults must survive compilation independently of mutable database
session defaults.

Selected SQL must parse as exactly one expression. Following the structural
boundary in the pinned
[Apache expression-language proposal](https://github.com/apache/ossie/blob/b6c702ed1c07e91382a69e870c875cbd19570828/core-spec/expression_language.md),
queries and nested subqueries, CTEs, set operations, query clauses, DDL, DML,
commands, projection aliases, malformed text, and multiple statements are
rejected. Dataset fields also reject aggregates outside a window. The gate does
not impose an ANSI function allowlist on vendor-dialect variants. Current target
parsers are BigQuery, Databricks, DuckDB, Postgres, Snowflake, and Spark.
`BIGQUERY` is supported both for target-aware import and explicit graph
synthesis.

`source_dialect` is separate and is used only when classifying a dataset
`source` as a table reference or SQL query. It is part of compilation identity.

Semantic identifiers follow the pinned proposal's comparison rules. Regular
identifiers are resolved case-insensitively after uppercasing. Double-quoted
identifiers are resolved exactly after stripping the outer quotes and unescaping
doubled quotes, so `orders` and `Orders` match while `orders` and `"orders"` do
not. Duplicate detection, relationship references, and key references use that
same normalization. Retained source text remains unchanged; runtime projections
decode quoted names and use deterministic aliases where native naming rules
would otherwise collide. Source-name metadata retains the declaration spelling.
Identifiers longer than 128 decoded characters are diagnosed and excluded from
executable lowering.

## Executable logical-model projection

| Ossie construct | Runtime behavior |
|---|---|
| `semantic_model` | One isolated catalog scope per uniquely identified model; no cross-scope merge |
| Dataset `source` | Preserved as a table reference or SQL query only when it can be classified safely |
| `primary_key` and `unique_keys` | Imported only when explicitly declared and structurally usable; no default `id` or other fabricated key |
| Dataset fields | Become dimensions only with a selected, valid target expression; datatype and explicit time-role information are retained where representable |
| Metrics | Become graph-level scalar SQL metrics only with a selected, valid target expression |
| Dataset-scoped metrics (November profile) | Retain the owning dataset and are queried by `dataset.metric`; local fields and `source.column` retain their distinct meanings |
| Relationships | Become many-to-one runtime edges only when identity, endpoints, key arrays, fields, arity, and target uniqueness are all verified |

Relationship `name` is the edge identity. It is retained as `edge_id` so
multiple edges between the same datasets remain distinguishable. Import never
invents relationship names or missing key columns. The target columns must
match the normalized column set of an explicitly declared primary or unique
key, irrespective of declaration order. The original ordered source/target pairs
are preserved when constructing the join; an unsafe relationship is
preserved in the source document but excluded from executable topology.

Both query engines evaluate declared logical fields before using them as keys or
aggregate inputs. Independent dataset aggregates keep their own entity grain;
an aggregate over a joined row expression keeps the joined population. Adding
an independent metric does not duplicate the inputs of another aggregate.
Metric references are bound before entering the runtime graph. Native model
metrics continue to use their existing physical-column input conventions.

## Relational query interface

Both Python and Rust implement the pinned Layer 2 measure semantics. Dataset
metrics are measure columns, evaluated explicitly with `AGG()`; model-scoped
metrics belong to the separate wide-table interface. Authored joins determine
the row population. Each measure deduplicates its own source-row identities
within each group, while ordinary SQL aggregates retain join multiplicity.
Distinct source rows with identical values remain distinct, including datasets
without declared primary keys.

```bash
sidemantic query --models scott_model.ossie.yaml --db scott.duckdb --engine rust --no-fallback \
  'SELECT d.dname, AGG(e.sum_sal) AS salary FROM scott_model.emp e
   LEFT JOIN scott_model.dept d ON e.deptno = d.deptno GROUP BY d.dname'
```

Measures propagate through projections, aliases, wildcards, filters, joins,
subqueries, CTEs, and registered SQL views. Grouped aggregation, grouping sets,
CUBE, ROLLUP, HAVING, ORDER BY, and derived measures preserve their consuming
population. The provisional measure-definition spellings are `TO_MEASURE(expr)`
and `expr TO MEASURE`. Set operations and DISTINCT reject unevaluated measures;
ordinary scalar results after `AGG()` retain normal SQL behavior. Using a measure
as a scalar, filter predicate, join key, or grouping key fails during binding.
Unaggregated inline-window `QUALIFY` preserves aliases and clause ordering;
combined aggregation or named-window `QUALIFY` shapes reject explicitly.
Wildcard `EXCLUDE`, `REPLACE`, and `RENAME` retain their projection semantics.
Native Rust rejects quoted wildcard `RENAME` operands because its pinned parser
loses their quoted-name identity; ordinary aliases remain supported.

`DESCRIBE` and `information_schema.columns` identify measure columns using a
`MEASURE` type marker. The metadata overlay includes semantic relations while
retaining physical catalog entries. DuckDB retains its full catalog column set;
other SQL targets expose the common standard information-schema columns.
`SemanticLayer.sql("CREATE VIEW ...")` registers the defining query in graph
metadata after successful database DDL. Querying the view preserves measure
recipes; replacing or dropping it invalidates cached rewrites. These recipes
travel with the semantic graph, so consumers should retain that graph alongside
the database view. Public view lookup resolves catalog/schema identities using
the current session and respects physical shadowing. Stored recipes retain their
creation-time relation bindings; missing database views remove stale recipes.

## Wide-table query interface

The strict Layer 3 binding accepts only `dimensions`, `metrics`, and one `filter`.
Use `--ossie-wide` with a JSON argument or `-` for stdin:

```bash
sidemantic query --models scott_model.ossie.yaml --db scott.duckdb --engine python --ossie-wide \
  '{"dimensions":["dept.dname"],"metrics":["emp.sum_sal","emp.emp_count"]}'
sidemantic rewrite --models scott_model.ossie.yaml --engine rust --no-fallback --ossie-wide \
  '{"metrics":["emp.sum_sal","emp.emp_count"]}'
```

`--ossie-scope` selects a model when needed. Python callers can use
`compile_ossie_wide(query)` and `query_ossie_wide(query)`. Native Rust exposes the
same query contract through its SQL generator and semantic-input bridge.

Each dataset metric retains one home population; model-scoped metrics are split
into aggregate components by home dataset. Directed relationship paths are
checked for reachability and ambiguity, including competing paths of different
lengths. Branches use LEFT joins, deduplicate home rows, and combine dimension
keys with NULL-equal matching. Missing branches contribute their aggregate's
empty-input value. Dimension-only queries select the required anchor. Invalid
queries report the pinned specification's error codes in its required precedence.

The existing structured API applies the same population semantics to latest
Ossie graphs, with explicit native ordering, aliases, and pagination extensions.
The strict JSON binding rejects those extra properties. Semantic security
controls that this binding cannot enforce fail closed. Historical Ossie profiles
keep their existing behavior.

The independent Layer 2/3 corpus and its requirement matrix are in
[`tests/ossie-fixtures/layer23-172a3e5/README.md`](../../tests/ossie-fixtures/layer23-172a3e5/README.md).
It exercises Python, public Rust with fallback disabled, and native Rust import
and compilation. Runtime verification uses DuckDB; target-dialect SQL rendering
does not constitute live warehouse certification.

## Ontology documents

Ontology documents use the pinned ontology schema and receive static checks
for built-in and declared concepts, duplicate identities, supertypes, role
references, identifying and mapped relationships, tuple arity, required value
ancestry, and embedded logical-model structure. Their complete
validated canonical data can be preserved and serialized as an opaque source
document.

Sidemantic does not perform ontology reasoning, does not turn ontology concepts
or mappings into runtime models, and does not execute ontology documents.
Ontology lowering therefore produces no executable catalog scopes.

The November profile also preserves standalone mapping documents with
`ontology_ref`, `semantic_model_ref`, and `concept_mappings`. They have their own
pinned schema and no executable catalog scopes. Parsing does not fetch referenced
documents over the network. `validate_ossie_mapping(..., references=...)` checks
explicitly supplied ontology/model documents, including names, concepts,
relationship roles, tuple arity, and mapped fields. Missing references produce
warnings rather than an implied successful cross-document check.

## Canonical and exact-byte source round trips

Canonical serialization validates the retained document and emits deterministic,
Unicode-safe JSON or YAML. It preserves canonical data, not lexical form: YAML
comments, anchors, aliases, quoting, scalar styles, key order, and whitespace
may change. Cross-serialization between YAML and JSON is always canonical.

Exact-byte reuse is an explicit source-document option. It succeeds only when
original bytes were retained, output serialization matches the input, and
reparsing those bytes produces the current canonical data. Otherwise Sidemantic
emits canonical output and reports an `ossie.serialization.exact_source_mismatch`
warning. This guarantee does not apply to an Ossie -> runtime graph -> Ossie
conversion.

## Graph synthesis and runtime preservation

Exporting a runtime graph creates a new logical document. It requires both:

- `scope_name`: the containing Ossie semantic-model name.
- `expression_dialect`: the exact label for emitted SQL. Supported values are
  `ANSI_SQL`, `BIGQUERY`, `DATABRICKS`, and `SNOWFLAKE`.

`schema_version` may also be selected explicitly; the current default is
`0.2.0.dev0` at revision `172a3e5`, producing the flat root. Pass
`schema_revision="b6c702ed1c07e91382a69e870c875cbd19570828"` to target the October
snapshot, or
`schema_revision="831f48e582731cf1ee2e65380ca5abf8157869c7"` or CLI
`--ossie-schema-revision 831f48e582731cf1ee2e65380ca5abf8157869c7` to synthesize
the older envelope. Preserved-document export retains its source shape.
Sidemantic emits one expression variant with the supplied label
and validates the completed document against the pinned schema. It validates
that expression text in the named dialect but does not infer its origin,
transpile it, or relabel it as another dialect.

The November profile checks its stricter model metric namespace: model-scoped
metrics reference declared `dataset.field` values and cannot reference other
metrics. Synthesis expands native derived metric references before emitting this
profile. Historical profiles retain their own validation contract.

Simple aggregate filters are emitted as conditional SQL and null filling as
`COALESCE`. Model-owned columnless aggregates such as `COUNT(*)` and `SUM(1)`
reference a constant dataset field so they retain their row source.

For native behaviors that Ossie core cannot express, synthesis preserves the
graph in a versioned `SIDEMANTIC` custom extension. This includes non-additive,
time and window metrics, inheritance, security restrictions, private fields,
custom or inactive joins, and relationship role aliases. Python Sidemantic restores
these definitions on import; security templates are evaluated for each query's
user context. The extension records its SQL dialect and a fingerprint of the
core projection. Unsupported versions, malformed payloads, incompatible dialects,
and edits that make the projection stale are rejected.

Extension-dependent documents contain only an empty placeholder dataset in
their core projection. Real sources and metrics live in the extension, so
consumers that ignore it cannot execute unrestricted or simplified definitions.
Export emits a warning that Sidemantic extension support is required. Use
`sidemantic convert ... --ossie-portable-only` (or `portable_only=True` in the
export API) to require portable core output instead. Invalid SQL, unresolved
inheritance references, and other invalid declarations still fail export.

The experimental Rust implementation does not restore this extension; it sees
the empty core placeholder.

Model-owned SQL is parsed and its column nodes are qualified in the owning
dataset's context. Function names, literals, quoted identifiers, existing column
qualifiers, and derived metric references retain their meaning.

Graph synthesis cannot create ontology documents or recover source-only fields.

Directory loading recognizes explicit `*.ossie.json`, `*.ossie.yaml`, and
`*.ossie.yml` files anywhere in the source tree, plus both logical YAML shapes.
Arbitrarily named JSON documents discovered in a directory follow the dbt
`OSI/` convention; an explicitly selected JSON file is inspected directly.
Generated `target/` and `dbt_packages/` artifacts are excluded. Malformed
explicitly named files report an error.

## CLI examples

Import one explicit scope for a BigQuery runtime projection:

```bash
sidemantic convert commerce.ossie.yaml \
  --from ossie \
  --to sidemantic \
  --output models.yml \
  --ossie-scope commerce \
  --ossie-target-dialect bigquery
```

Synthesize a new Ossie document while labeling only SQL already intended for
BigQuery:

```bash
sidemantic convert models/orders.yml \
  --from sidemantic \
  --to ossie \
  --output commerce.ossie.yaml \
  --ossie-scope commerce \
  --ossie-expression-dialect BIGQUERY \
  --ossie-schema-version 0.2.0.dev0
```

Use `--force` only when intentionally replacing an existing output file.

## Rust forward-import status

The experimental Rust runtime has a strict forward Ossie logical importer in
its dedicated `ossie` adapter. It supports explicit consumer profiles,
including `ossie-core` (`0.1.1` and `0.2.0.dev0`) and `dbt-1.12` compatibility
profiles. It preserves separate semantic-model scopes and uses
exact-target, portable, then `ANSI_SQL` expression selection for its supported runtime
targets: `ANSI_SQL`, `DUCKDB`, `POSTGRES`, `SNOWFLAKE`, `DATABRICKS`, and
`BIGQUERY`, `SPARK`, and `CLICKHOUSE`. Its import gate also checks scalar SQL structure, identifiers,
declared primary and unique keys, relationship identity and endpoints, key
arity, and target-key uniqueness. Invalid or unsupported input fails closed;
the legacy `osi` adapter remains a separate compatibility surface.
Spark retains its existing exact-dialect or ANSI fallback behavior; portable
`OSSIE_SQL_2026` lowering is not enabled for Spark.

The Python CLI/API uses the shared Python source importer before selecting the
Python or Rust query engine. Preserved source documents, complete pinned JSON
Schema validation, ontology validation, permissive import, and export remain
owned by that shared interchange API for both engine choices.

The separate native Rust forward importer validates logical documents against
the same four immutable JSON Schemas as Python: `0.1.1` and all three pinned
development revisions. These schemas are bundled for offline native and WASM
builds. The evaluator covers every assertion used by those schemas and fails
closed if an unsupported assertion is introduced. Logical validation reports
`pinned_json_schema`; semantic checks also validate recognized SQL variants even
when another variant is selected for execution.

The native importer does not duplicate the shared preserved-document, exact-byte,
permissive-import, ontology/mapping, or synthesis APIs. Those are interchange
operations available to both public engine choices through the shared importer;
they are separate from compiling logical models in native Rust.
Use `OssieCompiledScope::into_graph()` to retain graph-level metric
scope and logical expression semantics when passing a native import to the
compiler.

Native callers may select a pinned revision explicitly through the
`*_with_revision` adapter methods or the Python extension's optional
`schema_revision` argument. Newer logical syntax also selects the latest pin
automatically. Latest portable model expressions reject the reserved `AGG()`
operator; it belongs to the separate relational query interface.
