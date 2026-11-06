# Interoperability and format coverage research

Research date 2026-11-06. No product changes. Sidemantic checkout `a7df3c9eae7c1a9a916ebe317ee378408d7d721b`; official apache/ossie-converters `011ea9f9a1e2c941f351e18e071622339ccae881`; official Ossie submodule `01a5488d840deda309aac68cc0b312c195369b8d`.

## Conclusions

1. Sidemantic has **20 registered import formats, 17 export formats**. These count Ossie and Sidemantic itself. Three formats are import-only: Graphene, Tableau XML/archive formats, Yardstick. A registered exporter is a graph serializer, not proof that all native semantics survive a pairwise conversion. Registry: `/workspace/sidemantic/sidemantic/formats.py:72-224`, [pinned source](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/formats.py#L72).
2. **LookML is present upstream**, under `bigquery`, export-only. Do not count it among wholly absent formats. Official `native_format: lookml` and `import: null`: `/workspace/ossie-converters-research/interop/manifest.yaml:23-32`, [pinned](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/interop/manifest.yaml#L23). Our LookML import/export is an additional direction.
3. Current default Sidemantic synthesis interoperates for a simple native SUM model: official schema+semantic validator passes, BigQuery/LookML, Omni, and Cube exporters retain the revenue measure. **Do not claim the different Ossie pins make all output incompatible.**
4. Imported **dataset-scoped metrics** are a real silent-loss boundary: Sidemantic preserves their scope on graph re-export; the official validator accepts it; official Cube and Omni drop the measure. BigQuery retains it. This is narrower than “Sidemantic always emits dataset metrics”: synthesis emits them only when metadata `ossie_dataset_metric` is true (usually from Ossie import), not for ordinary native model metrics. `/workspace/sidemantic/sidemantic/interchange/ossie/synthesis.py:815-849`, [pinned](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/interchange/ossie/synthesis.py#L815). Official Cube reads only `model.get("metrics")` in `_build_measures` (`cube/src/ossie_cube/ossie_to_cube.py:1054,1079`), Omni likewise (`omni/src/ossie_omni/ossie_to_omni.py:192`).
5. **Preserved-document roundtrip and generic conversion are different contracts.** `OssieAdapter.parse_document` + `export_document(exact_source=True)` can preserve source bytes, opaque extensions, and AI context. Generic `convert` loads a runtime `SemanticGraph`, then synthesizes; probe shows foreign `custom_extensions` and root `ai_context` disappear even on Ossie→Ossie. `/workspace/sidemantic/sidemantic/formats.py:302-321`, [pinned](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/formats.py#L302); document API `/workspace/sidemantic/sidemantic/adapters/ossie.py:189-218`, [pinned](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/ossie.py#L189).
6. **Portable output is opt-in enforcement.** An invariant filter causes default synthesis to store the executable graph in `SIDEMANTIC` extension and expose an empty sentinel dataset. Official BigQuery/Omni export succeeds, but exports only that empty dataset and warns that extension is dropped. This is a deliberate fail-safe, not successful functional conversion. `--ossie-portable-only` rejects the export. `/workspace/sidemantic/sidemantic/interchange/ossie/synthesis.py:933-969`, [pinned](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/interchange/ossie/synthesis.py#L933).

## Added coverage beyond official projects

| Sidemantic format | Directions | Concrete boundary | Official equivalent |
|---|---|---|---|
| AtScale SML | import/export | YAML files/project → graph; directory export | No registered spoke |
| BSL | import/export | Boring Semantic Layer YAML/Ibis expressions | No registered spoke |
| Malloy | import/export | `.malloy` source models; source-level where becomes invariant filter; adapter says views/queries skipped | No registered spoke |
| Rill | import/export | YAML definitions; directory export | No registered spoke |
| Superset | import/export | Dataset YAML, file/directory export | No registered spoke |
| Graphene | import only | `.gsql` table/extend/join/model declarations | No registered spoke |
| Tableau | import only | `.tds/.twb/.tdsx/.twbx` data sources/workbooks | Official Salesforce semantic model JSON is a different boundary; do not equate it with Tableau XML |
| Yardstick | import only | SQL CREATE VIEW with `AGG(expr) AS MEASURE name` | No registered spoke |
| LookML | import/export | `.lkml` model/view language | Official `bigquery` exports LookML; has no importer |
| Sidemantic | import/export | native YAML/SQL | Sidemantic-only runtime interchange |

Evidence: central registry above; BSL `/workspace/sidemantic/sidemantic/adapters/bsl.py:1-10,38-64`; Graphene `adapters/graphene.py:128-139`; Yardstick `adapters/yardstick.py:77-81`; Tableau `adapters/tableau.py:1026-1038`; Malloy `adapters/malloy.py:3308-3327`. All under [pinned adapters tree](https://github.com/sidequery/sidemantic/tree/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters). Absence evidence is complete official 23-project manifest, not a text search alone.

**Runtime support is separate:** Sidemantic's BigQuery/Databricks/Snowflake/Postgres/etc database adapters establish where Sidemantic SQL executes. They do not establish a BigQuery LookML or Databricks Metric View converter. `/workspace/sidemantic/sidemantic/db/__init__.py:8-41`, [pinned](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/db/__init__.py#L8). The semantic format registry has `lookml` but no `bigquery` format, and no `databricks` Metric View format.

## Schema versions and real compatibility limits

Ours supports upstream `0.1.1`, legacy wrapped `0.2.0.dev0` at `831f48e`, flat draft at `b6c702e`, and latest flat draft at `172a3e5`; dbt consumer profile also permits `0.1.0` as an explicit compatibility alias, not upstream schema version. `/workspace/sidemantic/sidemantic/interchange/ossie/profiles.py:8-10,89-176`, [pinned](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/interchange/ossie/profiles.py#L89). Shape autodetection chooses pins; flat alone does not distinguish all revisions (`validation.py:282-340`). Export defaults to latest `172a3e5` (`adapters/ossie.py:246-254`).

Official converters document flat `0.2.0.dev0`, one model per document; reject old wrappers; official harness uses submodule `01a5488` schema and semantic validator (`interop/checks.py:32-58`). README [pinned](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/README.md#L49), validator [pinned](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/interop/checks.py#L32).

JSON-normalized diff between ours latest vendored core schema and official submodule schema:

- Ours accepts `expression.sql` (portable OSSIE_SQL_2026) or `expression.dialects`; official requires `dialects` and rejects `sql`.
- Ours permits any nonempty dialect label; official has enum ANSI_SQL/SNOWFLAKE/MDX/TABLEAU/DATABRICKS/MAQL/BIGQUERY/SIGMA/THOUGHTSPOT/DAX/HOLISTICS_AQL/OSSIE_SQL_2026.
- Ours describes dataset metrics as able to use `source.column`; official prohibits references to undeclared raw source fields and qualified dataset-metric references in semantic validation.
- Other differences include minimum string lengths and extension description wording. Both already have flat root, datatypes, dataset metrics.

Schema evidence: [ours pinned latest](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/interchange/ossie/schemas/logical/0.2.0.dev0-172a3e5/schema.json), [official pinned core](https://github.com/apache/ossie/blob/01a5488d840deda309aac68cc0b312c195369b8d/core-spec/ossie-schema.json), [official scoped metric validation](https://github.com/apache/ossie/blob/01a5488d840deda309aac68cc0b312c195369b8d/validation/validate.py#L481).

Crucial caveat: ours graph synthesis emits `dialects`, not `sql`, and supports four explicit synthesis SQL labels (`ANSI_SQL`, `BIGQUERY`, `DATABRICKS`, `SNOWFLAKE`), so ordinary synthesized output avoids the first two mismatch classes (`synthesis.py:43-44,93-94`). Preserved latest documents can still expose those mismatches. Probe `expression-sql` gets six official schema errors and BigQuery “Missing expression for Field 'orders.id'”.

## Inbound interoperability probes

All 10 official `interop/fixtures/contracts/*/input.yaml` plus official TPC-DS and Flights examples were run through `OssieAdapter().parse_document()`:

- 7/10 contracts lower without diagnostics under default DuckDB (single/composite/unique/joined keys, two-/three-part sources, OSSIE_SQL_2026-only dialect).
- Snowflake-only and Databricks-only inputs report expression unavailable under default DuckDB, but lower cleanly with `target_dialect='snowflake'` or `'databricks'` respectively. This is explicit dialect selection, not a generic incompatibility. CLI exposes `--ossie-target-dialect`.
- Quoted source `` `analytics.prod`.`sales.area`.`Orders` `` fails source detection for all tested target dialects alone. Supplying API `source_dialect='bigquery'` or `'databricks'` fixes it. The convert CLI exposes target/expression dialect flags but **no source-dialect flag**, so this is a CLI usability/interop gap. `adapters/ossie.py:79-80,100-105`; `cli.py:1536-1579`; lowering selects source independently.
- TPC-DS lowers without diagnostics.
- Official Flights example fails strict ours validation at `/name`: `Flights semantic model` is not an ANSI identifier (spaces require quotes). Source `/workspace/ossie-converters-research/ossie/examples/flights.semantic_model.yaml:19`; ours identifier check `semantic_validation.py:219-227`. Do not overgeneralize to all official fixtures.

## Reproducible outbound probes and commands

Research script: `/workspace/interop-probes.py`. Full output: `/workspace/interop-probe-output/results.txt`. Inputs, serialized outputs and native exports saved alongside. Uses actual official converter functions loaded from their source directories in the Sidemantic environment; no live vendor compilation/warehouse execution claimed. Installed environment supplies PyYAML/sqlglot/jsonschema dependencies. Research run does not run each spoke's locked unit suite.

```bash
cd /workspace/sidemantic
uv run --no-sync python /workspace/interop-probes.py
uv run --no-sync sidemantic convert /workspace/ossie-converters-research/interop/fixtures/contracts/keys-single/input.yaml --from ossie --to ossie --ossie-scope orders_model --ossie-expression-dialect ANSI_SQL --ossie-portable-only --output /workspace/interop-probe-output/cli-keys-roundtrip.yaml
uv run --no-sync sidemantic convert /workspace/interop-probe-output/dataset-metric.yaml --from ossie --to ossie --ossie-scope sales --ossie-expression-dialect ANSI_SQL --ossie-portable-only --output /workspace/interop-probe-output/cli-dataset-metric.yaml
```

Both CLI conversions exit 0, report one converted model and all recognized import constructs translated. Existing output requires fresh path or `--force` when rerunning. Probe script exits 0.

| Probe | Actual result |
|---|---|
| Native orders(id,amount), SUM(amount) → Sidemantic default latest core | Schema-valid under official validator; root metric retained by official BigQuery, Omni, Cube |
| That core → official LookML → Sidemantic LookML import | orders table `analytics.orders`, metric revenue agg=sum SQL `({model}.amount)` retained |
| Dataset metric → Sidemantic import/export → official validator/Cube | Official validator accepts; Sidemantic preserves one dataset metric; Cube emits no measures and no dropped-metric diagnostic (only untyped-field approximations) |
| Dataset metric → official Omni | No measures; no warnings |
| Dataset metric → official BigQuery | `measure: revenue` retained with SUM |
| expression.sql-only core → official validator/BigQuery | Rejected schema, exporter missing expression |
| Model invariant filter → default Sidemantic Ossie | Explicit runtime-extension warning; empty core sentinel; BigQuery and Omni convert sentinel and drop foreign extension |
| Same invariant filter with portable_only=True | OssieSynthesisError; no portable output |
| Annotated Ossie → preserve document/exact source | Byte-identical output, including foreign extension and AI context |
| Annotated Ossie → generic graph conversion → Ossie | Foreign extension and root AI context absent |

Additional observation: a native Dimension(type='numeric') without `logical_data_type` synthesizes no Ossie datatype, so official exporters default it to string (Cube emits approximation diagnostics; BigQuery simple case no warning). This is a demonstrated typing-loss boundary, not a statement about fields that explicitly carry logical_data_type.

## Official interoperability infrastructure: strengths and limits

The official repo registers all 23 projects, with capability skips rather than pretending all are bidirectional. Tier A exports canonical/imported models; Tier B native→Ossie→other native→Ossie compares named structures; optional Tier C runs native gates (Cube compiler). Ten document regressions and two metric-binding regressions add deeper targeted checks. `/workspace/ossie-converters-research/interop/README.md:108-146`, [pinned](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/interop/README.md#L108).

Do not interpret green Actions as full interoperability: default mode returns zero despite converter failures, empty results, skips, and lossy outcomes; `--strict` blocks FAIL/EMPTY/unexpected SKIP but LOSSY still not blocking (`README:162-179`). CI intentionally invokes without `--strict`; general SQL equivalence, data types, AI context and metadata are not checked (`README:176-179,350-362`). [Pinned CI policy](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/interop/README.md#L350).

Actionable ideas for Sidemantic: adopt shared small fixture corpus/structural grades; add an Ossie consumer-target profile to reject unsupported modern constructs before downstream export; gate portable exports explicitly; provide preserved-document CLI route separately from graph conversion; expose source dialect; add typed-field roundtrips; contribute/extend upstream dataset-metric tests. Report runtime-extension output as requiring Sidemantic, not as vendor-portable success.
