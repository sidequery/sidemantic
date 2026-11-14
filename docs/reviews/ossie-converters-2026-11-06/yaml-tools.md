# Hex, Holistics, Omni, Lightdash, Honeydew comparison

Inspected Sidemantic working tree (clean), commit `a7df3c9eae7c1a9a916ebe317ee378408d7d721b`; Apache `ossie-converters`, commit `011ea9f9a1e2c941f351e18e071622339ccae881`. These five upstream directories contain actual source and tests, not merely links to vendor tools. All are offline converters; none inspected here is a hosted conversion service. Findings are from source, tests, and the probes below, not README claims alone.

Important scope correction: Sidemantic's public `osi` alias resolves to the modern `OssieAdapter`, not the historical implementation retained earlier in `adapters/osi.py`. The modern flat-schema path is current; do not report wrapper incompatibility based on legacy code. See `/workspace/sidemantic/sidemantic/formats.py:159` and `/workspace/sidemantic/sidemantic/adapters/osi.py:1044`.

## Direction and real input/output

| Tool | Sidemantic | Official Apache converter |
|---|---|---|
| Hex | Reads model/view YAML, including multi-document files; writes one YAML per model/view in a directory. A YAML output filename writes only the first model. | **Only Ossie → Hex is implemented**, despite “bidirectional” README/package wording. Takes one flat Ossie file; writes Hex project resources. CLI exposes only `export`. |
| Holistics | Reads raw `.aml` files/directories using its ANTLR parser; resolves constants, imports, partial models, dataset definitions, and standalone metrics. Exports model AML plus relationships; a single `.aml` output takes the first model. | Compiled Holistics dataset JSON → Ossie; Ossie → AML source directory, including dataset file and model files. Requires `holistics aml compile` before import; not a raw AML parser. One AML Dataset becomes one Ossie model. |
| Omni | YAML file/directory → graph, graph → `views/<name>.yaml` and old-style relationships nested in `model.yaml`. | Bidirectional Ossie ↔ Omni directory: model.yaml, relationships.yaml, views, topics; accepts real API/IDE layouts, bare `.view`/`.topic`, schema header references, preserves original paths. |
| Lightdash | No native Lightdash adapter/registration found in Python source. Generic dbt support is not Lightdash-meta support. | Bidirectional. Imports both native Lightdash `type: model` files and dbt schema YAML with Lightdash meta. Default export is deployable native model directory/config; optional dbt-meta schema output, including dbt 1.10 `config.meta`. |
| Honeydew | No native Honeydew adapter/registration found. | Bidirectional Ossie file ↔ Honeydew workspace directory (`workspace.yml`, `schema/<entity>/...`, datasets, calculated attributes, metrics). |

Evidence: Sidemantic `/workspace/sidemantic/sidemantic/adapters/hex.py:28`, `:466`; `/workspace/sidemantic/sidemantic/adapters/holistics.py:242`, `:353`; `/workspace/sidemantic/sidemantic/adapters/omni.py:28`, `:717`. Upstream [Hex CLI](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/hex/src/ossie_hex/cli/main.py#L47), [Holistics usage](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/holistics/README.md#L14), [Omni layout](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/omni/README.md#L24), [Lightdash modes](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/lightdash/README.md#L20), [Honeydew implementation](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/honeydew/src/ossie_honeydew/converter.py#L79).

## Hex

Sidemantic has the broader direction coverage: real import, views preserved on `meta` (`base`, `contents`, visibility), display labels, filters, and full semi-additive object preservation. This is useful for native Hex roundtrips and querying supported SQL measures. Official Hex has no implemented import; statements about a HEX custom extension on import in its README are future/descriptive, not executable functionality.

Official export is substantially more deliberate about validation, SQL references, target ID allocation, dialect choice and losses. It loads/validates Ossie into typed objects, parses SQL with SQLGlot, analyzes references/relationship paths, and rewrites `dataset.field` to `${field}` or `${relation.field}`. Problems carry severity, phase and source cause path; CLI errors/fatals produce exit 1. Unsupported AI context, custom extensions, composite keys and temporal-role metadata are reported. Metric analysis reports zero referenced datasets and more than two referenced datasets as errors: this is not unrestricted cross-model export.

Sidemantic fidelity defects verified with executable probe:

* Hex `boolean` dimension imports as categorical, then exports `string`; `timestamp_naive` also collapses to timestamp_tz through its mapping.
* `func: median` imports as median, but export only recognizes count/count_distinct/sum/avg/min/max and falls back to **count**. The same mismatch affects imported stddev/variance variants.
* A compound `join_sql` is not a single equality AST, so no keys are recovered; export omits `join_sql`. Arbitrary predicates are not preserved.
* `expr_calc`/`func_calc` are treated as SQL-shaped expressions and exported as `expr_sql`/`func_sql`; no original language marker is retained.
* Explicitly requesting a single output YAML silently exports the first model only.

Priority: fix aggregate fallback and predicate loss, retain native type/expression-kind metadata, add a structured loss result, and define single-file multi-resource output explicitly. Existing view/semi-additive preservation provides a useful precedent.

Local: `/workspace/sidemantic/sidemantic/adapters/hex.py:210` (type mapping), `:285` (aggregate import), `:435` (join parse), `:479` (first model output), `:539` (type export), `:601` (aggregate export). Upstream [SQL rewrite](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/hex/src/ossie_hex/ossie_to_hex/convert_ossie_dialect_expression.py#L72), [metric restrictions](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/hex/src/ossie_hex/ossie_to_hex/convert_ossie_metric.py#L34), [loss codes](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/hex/src/ossie_hex/ossie_to_hex/problem_code.py#L20).

## Holistics

Sidemantic's direct AML parsing is a distinct advantage: no Holistics CLI compilation prerequisite. It supports composition, dataset-level metrics and selected AQL-to-SQL lowering. However its export emits Models and Relationships, not a reconstructed Dataset/import/module topology, and does not stash connection, owner, persistence, hidden state or arbitrary AML constructs. It maps selected label/description/format properties, not the entire original document.

Most consequential finding: AQL lowering intentionally discards table-shaping and metric-scope modifiers without a warning. Verified examples:

```text
sum(orders.amount) | of_all()                          → SUM(orders.amount)
sum(orders.amount) | running_total()                   → SUM(orders.amount)
orders | filter(orders.status == "paid") | sum(orders.amount)
                                                     → SUM(orders.amount)
```

The last expression loses the filter, and the others lose grouping/window semantics. This is executable semantic corruption, not just metadata loss. Export subsequently emits SQL and cannot recover the AQL.

Official import keeps AQL explicitly under `HOLISTICS_AQL`, emits an informational issue that it was not translated, and preserves vendor-specific details in versioned JSON-string `HOLISTICS` custom extensions. Its reverse converter restores dataset scope, module paths, data source, labels, hidden state, formats, params, persistence and relationship attributes. It can translate supported **Ossie SQL → AQL** for cross-dataset metrics (different direction from Sidemantic's AQL → SQL), reporting unsupported constructs, reference problems and dialect mismatches. It warns on relationship filters/inactive relationships because those change semantics for core-only consumers; range/many-to-many joins are stash-only. File provenance/comments/order are not retained from compiled JSON, so “same AML models” does not mean byte-identical source files.

Priority: preserve original AQL, reject or explicitly report unsupported scope/filter modifiers, and use a loss-aware compiled-JSON import/export path if migration fidelity matters. Keep raw AML import for convenience; do not replace it with a compilation requirement.

Local: `/workspace/sidemantic/sidemantic/adapters/holistics.py:353`, `:1128`, `:1852`, `:2072`. Upstream [compiled input contract](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/holistics/README.md#L14), [limitations and issue codes](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/holistics/docs/limitations.md#L14), [versioned payload](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/holistics/src/ossie_holistics/stash.py#L47), [reverse conversion](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/holistics/src/ossie_holistics/ossie_to_aml.py#L1254).

## Omni

Sidemantic imports dimensions/measures/filters, time-comparison constructs, relationships and topic records. It preserves selected metadata in memory, but export doesn't restore much of that state: it never writes topics, drops original paths/model configuration, conflates view description with label, forces `join_type: always_left`, and discards `reversible`. The directory scanner discovers only `.yaml`/`.yml`, not bare `.view`/`.topic`; when a `views` directory exists it scans only that directory's immediate YAML files. Names derive from schema/table or filename, not the canonical header.

Verified public API roundtrip of `${orders.amount} > ${users.threshold}`, `join_type: inner`, plus a topic:

```text
Sidemantic output files: model.yaml, views/orders.yaml, views/users.yaml
Sidemantic join: ${orders.amount} = ${users.threshold}, join_type: always_left
Official warning: non-equi join has no Ossie relationship form; preserved in custom_extensions only
Official restored join: original > predicate, join_type: inner
Official restored topic: {base_view: orders, label: Sales}
```

The key parser uses regex to extract the first reference from each view without inspecting the operator, so non-equi and multi-clause joins can change meaning. This deserves higher priority than presentation parity.

Official import/export preserves extensive Omni-only state (model/topics, view/dimension/measure extras, templated constructs, filters, non-equi joins, query/extends views, original paths) in `OMNI` extensions. Roundtrip tests compare parsed complete file maps, not just selected graph properties. This preserves native restoration but does **not** make stashed query/extends/filter features portable Ossie semantics. A project with only query/extends views is rejected. Official SQL handling is still mostly vendor-reference rewriting, with ANSI_SQL labeling because no OMNI dialect exists; it is not universal SQL transpilation. Export warns about dropped foreign vendor extensions/unique keys/unsupported metadata and rejects ambiguous base views, invalid source qualification or collisions.

Priority: retain arbitrary join predicates and join metadata, reject unsupported lowering, preserve canonical names/paths, support API suffixes, and restore topics/configuration. Build native-file roundtrip assertions in addition to graph-equivalence tests.

Local: `/workspace/sidemantic/sidemantic/adapters/omni.py:64`, `:188`, `:623`, `:717`, `:940`; `/workspace/sidemantic/tests/adapters/omni/test_roundtrip.py:18`. Upstream [real-world import](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/omni/src/ossie_omni/omni_to_ossie.py#L74), [non-equi handling](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/omni/src/ossie_omni/omni_to_ossie.py#L444), [roundtrip assertions](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/omni/tests/test_roundtrip.py#L47).

## Lightdash and Honeydew: concrete coverage additions, with caveats

Lightdash is a substantial actual implementation. It handles native/dbt-meta layouts, seeds, catalog type enrichment, metric placement/namespacing, SQL references and aliases, composite keys and joins. It uses typed Ossie models plus structured `ConverterResult(output, issues)`. Metadata is stored in `LIGHTDASH` JSON extensions and restored. SQLGlot qualification respects literals, comments and nested query scope; expression dialect fallback is explicit. Its “exact roundtrip” README claim should be qualified: unsupported query-time templating/parameters cause affected fields/metrics to be skipped; number/time types normalize; metric and row filters are stash-only, so core Ossie sees unfiltered expressions/unrestricted rows; alias joins flatten identity with an issue. No native Sidemantic adapter means this is a strong interoperability target, not proven support via Sidemantic's generic dbt adapter.

Honeydew likewise has real two-way files, metadata/extensions, calculated attributes, metrics, composite keys and relationship direction handling. It uses Python warnings plus errors rather than Lightdash's structured result. Limitations: one primary source dataset per entity; datatypes are approximated on export and not emitted as Ossie `datatype` on reimport; all Honeydew metrics export as `number`; perspectives/domains aren't converted. It emits Honeydew SQL as ANSI_SQL while retaining entity.attribute syntax, so labeling is not a portability guarantee. `ANSI_SQL` is preferred on export, falling back to the first dialect with a warning. Foreign extension/AI metadata is preserved in Honeydew metadata. Original connection expressions roundtrip in HONEYDEW extensions.

Upstream [Lightdash issues](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/lightdash/src/ossie_lightdash/converter_issues.py#L108), [Lightdash SQL rewrite](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/lightdash/src/ossie_lightdash/lightdash_to_ossie.py#L218), [Lightdash limits](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/lightdash/README.md#L214), [Honeydew types](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/honeydew/src/ossie_honeydew/converter.py#L50), [Honeydew dialect fallback](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/honeydew/src/ossie_honeydew/converter.py#L427), [Honeydew limitations](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/honeydew/README.md#L106).

## Validation performed

Using the existing Sidemantic virtual environment, with converter sources added to PYTHONPATH where required:

* Official Omni: **206 passed** (includes fixture/property roundtrips and core-schema checks).
* Official Honeydew: **151 passed**, two expected warnings in malformed-expression cases.
* Official Holistics: **92 passed, 6 skipped**; skips require external Holistics CLI, so generated AML was not vendor-compiled in this session. Schema/issue/snapshot tests did run.
* Official Lightdash: **189 passed, 1 skipped**; skipped test requires external Lightdash CLI. No deployed Lightdash/warehouse validation performed.
* Sidemantic Hex/Omni/Holistics focused suite: **228 passed in 133.21s**, via `uv run --no-sync pytest -q tests/adapters/hex tests/adapters/omni tests/adapters/holistics` from `/workspace/sidemantic`.
* Custom small executable probes reproduced Hex type/aggregate/join loss, AQL filter/scope loss and public Omni roundtrip behavior above.
* Official Hex reviewed from implementation/tests but not executed; requires `hex-sl-utils` and its pinned current dependencies.

Official suites used `/workspace/sidemantic/.venv/bin/python -m pytest -q tests` from each converter directory. Honeydew added `PYTHONPATH=src`; Lightdash added `PYTHONPATH=src:../ossie/python/src`. These passing suites coexist with the reproduced Sidemantic defects because the existing tests do not assert all the lost native semantics.

No product source was changed. This comparison distinguishes native restoration, portable semantics and runnable queries: preserving unsupported features in vendor extensions is valuable for roundtrip restoration, but it is not equivalent to implementing those semantics in another engine.
