# Apache Ossie compatibility

Sidemantic's Python integration handles [Apache Ossie](https://github.com/apache/ossie)
logical semantic-model and ontology source documents. Executable import and
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
| `ossie-core` | `0.2.0.dev0` | Current logical | `logical-0.2.0.dev0-b6c702e` | `b6c702ed1c07e91382a69e870c875cbd19570828` |
| `ossie-core` | `0.2.0.dev0` | Current ontology | `ontology-0.2.0.dev0-b6c702e` | `b6c702ed1c07e91382a69e870c875cbd19570828` |
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
Both snapshots retain their original schema assets and checksums.

Parsing infers the logical snapshot from its shape. Current ontology markers
also select the newer snapshot; ambiguous ontology sources retain the older
default unless `OssieParseOptions(schema_revision=...)` selects a pinned commit.
The profile and preserved document carry that selection through serialization.
The mutable development version alone does not identify a schema revision.

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

The portable dialect is parsed as source syntax before translating required
functions to the execution dialect. It preserves logarithm argument order,
truncation precision, date arithmetic, ANSI window frames, and exact statistical
aggregates. BigQuery exact `MEDIAN` and ordered-set percentiles need query-level
lowering and produce explicit unsupported-target diagnostics. They are never
silently changed to approximate aggregates.

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
`0.2.0.dev0` at revision `b6c702e`, producing the flat root. Pass
`schema_revision="831f48e582731cf1ee2e65380ca5abf8157869c7"` or CLI
`--ossie-schema-revision 831f48e582731cf1ee2e65380ca5abf8157869c7` to synthesize
the older envelope. Preserved-document export retains its source shape.
Sidemantic emits one expression variant with the supplied label
and validates the completed document against the pinned schema. It validates
that expression text in the named dialect but does not infer its origin,
transpile it, or relabel it as another dialect.

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

The experimental Rust runtime now has a strict forward Ossie import subset in
its dedicated `ossie` adapter. It supports explicit consumer profiles,
including `ossie-core` (`0.1.1` and `0.2.0.dev0`) and `dbt-1.12` compatibility
profiles. It preserves separate semantic-model scopes and uses
exact-target, portable, then `ANSI_SQL` expression selection for its supported runtime
targets: `ANSI_SQL`, `DUCKDB`, `POSTGRES`, `SNOWFLAKE`, `DATABRICKS`, and
`BIGQUERY`. Its import gate also checks scalar SQL structure, identifiers,
declared primary and unique keys, relationship identity and endpoints, key
arity, and target-key uniqueness. Invalid or unsupported input fails closed;
the legacy `osi` adapter remains a separate compatibility surface.

The Python CLI/API uses the shared Python source importer before selecting the
Python or Rust query engine. Preserved source documents, complete pinned JSON
Schema validation, ontology validation, permissive import, and export remain
owned by that shared interchange API for both engine choices.

The separate native Rust forward importer has strict structural validation,
current and legacy logical shapes, and native expression lowering. It does not
duplicate the shared preserved-document, exact-byte, or synthesis APIs. Native
structural status is labelled `closed_structural_subset`, not full JSON Schema
validation. Use `OssieCompiledScope::into_graph()` to retain graph-level metric
scope and logical expression semantics when passing a native import to the
compiler. The conformance tests execute one corpus through Python import and
both runtimes, plus native Rust import/compilation, against DuckDB. They do not
certify live warehouse behavior for every target database.
