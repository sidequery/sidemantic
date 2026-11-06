# Cube + dbt/MetricFlow comparison, 2026-11-06

## Pins and method

- Sidemantic: `a7df3c9eae7c1a9a916ebe317ee378408d7d721b` (`https://github.com/sidequery/sidemantic`). Main Python adapters; not Rust.
- Apache converter: `011ea9f9a1e2c941f351e18e071622339ccae881` (`https://github.com/apache/ossie-converters`).
- Its initialized Ossie dependency: `01a5488d840deda309aac68cc0b312c195369b8d`.
- Read implementations, tests, fixture assertions, issue handling; ran suites and cross-implementation fixture probes. No product changes. Probe artifacts: `/workspace/probe-cube-dbt.py`, `/workspace/probe-cube-dbt-results.json`, `/workspace/probe-official-dbt.py`, `/workspace/probe-official-dbt-results.json`.

## Central finding

Neither is uniformly more complete. Official Cube is a much stronger **reversible format bridge**, with named extension stashes and stricter refusal/parking of unrepresentable runtime behavior. Sidemantic supports **execution of more modeled concepts** (inheritance, some rolling metrics, row policies, native nonadditive metrics), but its exporters are less faithful than importer comments imply. Official dbt is a **compiled-manifest bridge**, not a source-YAML competitor; it has more principled entity resolution and aggregation expression conversion, but substantial loss and an important current dataset-local metric blind spot.

### Artifact boundaries

| Direction | Sidemantic | Apache Ossie |
|---|---|---|
| Cube import | YAML file/directory → SemanticGraph. YAML only; JS files warned/skipped. Models include expanded Cube views. | Mapping of filenames to YAML text / CLI file or directory → root Ossie YAML document. Normalizes list or keyed collections and camelCase keys. JS/TS/Python/Jinja and nonmodel YAML can be parked verbatim alongside static cubes. |
| Cube export | Graph → one YAML file with `cubes:` only. Model-local metrics emitted. Views explicitly skipped. | Root Ossie YAML → map of files, normally `model/cubes/*.yml` and `model/views/*.yml`; prior paths/views/extras restored. |
| dbt import | Source YAML: legacy `semantic_models` + measures + `metrics.type_params`, and latest `models[].semantic_model`, column entity/dimension declarations and folded simple metrics. | Actual `semantic_manifest.json`, parsed through MetricFlow semantic interfaces and transformed before conversion. |
| dbt export | Graph → **legacy source YAML only**, lowering latest folded measures to model measures. | Ossie document → JSON `PydanticSemanticManifest`, with metric aggregation params on metrics and `measures=[]`. Not a deployable dbt source project. |

Source links: [ours Cube parse/export](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/cube.py#L283), [ours MetricFlow parsing](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/metricflow.py#L97), [ours MetricFlow export](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/metricflow.py#L932), [official Cube input](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/src/ossie_cube/cube_to_ossie.py#L132), [official Cube output](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/src/ossie_cube/ossie_to_cube.py#L101), [official dbt CLI](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/cli.py#L85).

Local anchors: `/workspace/sidemantic/sidemantic/adapters/cube.py:283`, `:1504`; `/workspace/sidemantic/sidemantic/adapters/metricflow.py:97`, `:932`; `/workspace/ossie-converters-research/dbt/src/ossie_dbt/cli.py:85`.

## Cube: meaningful differences

### Official strengths, confirmed by cross-fixture probes

1. **Preservation is systematic.** Unmapped cube/dimension/measure properties go into `custom_extensions` vendor `CUBE` JSON stashes, including views and original files. Foreign Ossie extensions, AI context, unique keys, and original expressions park under Cube `meta.ossie` for the reverse direction. This is much broader than ours' hand-selected `meta.cube_internal` fields. The feature fixture roundtrip comparison normalizes only equivalent `{CUBE}.column` / bare-column SQL spelling, not arbitrary semantics. All ten official feature fixtures roundtripped in the probe.
2. **Views survive the format roundtrip.** Ordinary import preserves view curation as extensions; `--project --view` instead exposes a selected public surface and reports dropped projection properties. Our importer expands views to graph models, but exporter explicitly skips them. The official `view_curation.yml` imported as ours `orders, users, sales`, and exported only `orders, users`, with no warning.
3. **Unrepresentable execution is kept out of native metrics.** Official rolling windows, multi_stage/group_by/time_shift/rank, measure references nested in aggregates, and nonstatic dependent measures are parked, with structured issues. `sub_query` and switch dimensions similarly stay out of ordinary fields. Our rank is an explicitly warned COUNT fallback, and multi-stage measures remain queryable using simplified SQL, with warnings.
4. **Fanout visibility.** Official reports `FANOUT_UNSAFE_METRIC` and optionally refuses with `--strict-fanout`; it explicitly distinguishes irreversible loss, inert parked values and approximations. This is not a full SQL execution solution: static Ossie SQL cannot recreate Cube query-time deduplication. Sidemantic's execution engine may handle fanout itself; compare query planning separately from the importer's field mapping.
5. **Dynamic CASE labels work.** Official `conditional_dimensions.yml` includes a CASE label mapping containing SQL. Official converts and roundtrips it. Ours crashes at `.replace()` on a dict (`cube.py:743` / `:747`). This is a concrete supported input shape missing in ours, not merely a metadata gap.

Evidence: [official collection/normalization](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/src/ossie_cube/cube_to_ossie.py#L270), [native keys vs stash](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/src/ossie_cube/cube_to_ossie.py#L98), [parking nonstatic measures](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/src/ossie_cube/cube_to_ossie.py#L1444), [issue taxonomy](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/src/ossie_cube/converter_issues.py#L35), [feature contract](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/tests/test_feature_matrix.py#L18), [ours dynamic label bug](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/cube.py#L735).

Local anchors: `/workspace/ossie-converters-research/cube/src/ossie_cube/cube_to_ossie.py:98`, `:270`, `:1444`; `/workspace/ossie-converters-research/cube/tests/test_feature_matrix.py:18`; `/workspace/sidemantic/sidemantic/adapters/cube.py:735`.

### Sidemantic strengths and official limits

- Ours resolves cube inheritance and inherited hierarchies plus view inheritance (`cube.py:325-383`). Official **rejects cube `extends` outright**, requiring flattening (`cube_to_ossie.py:304`). Official view curation may be preserved losslessly without being executed/projected.
- Ours maps supported Cube row-level `access_policy` filters into `SecurityPolicy` and native segments/preaggregations/hierarchy parents. Official preserves these mostly as extension data, so another Ossie consumer does not automatically enforce a Cube access policy or execute preaggregations. Neither metadata preservation nor a clean roundtrip means runtime enforcement.
- Ours models trailing/to-date rolling windows and some period comparisons natively; official preserves them but excludes them from static metrics. Ours leading/offset still inert; many complex time_shift cases do not translate.
- Official refuses duplicate member names, bad joins and unknown aggregate types; ours accepts some unknown dimensions as categorical and unknown measures as COUNT with fidelity notes. Stricter rejection can block a whole conversion; it also prevents silently invented meaning.

Links: [ours inheritance](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/cube.py#L325), [ours policies](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/cube.py#L220), [official extends rejection](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/src/ossie_cube/cube_to_ossie.py#L304).

### Ours export fidelity defects beyond views

Confirmed against official feature fixtures:

- Custom `granularities` are exported as `meta.custom_granularities`, not real Cube granularities. Definitions survive as opaque data but are inactive in Cube.
- `revenue_by_region` loses `multi_stage: true`; `group_by` is demoted to meta. `revenue_prior_year` loses its entire `time_shift`, becoming ordinary SUM(amount). Warnings occur on import, but no accurate full roundtrip results.
- Measure `currency: USD` becomes `meta.currency`, affecting display behavior.
- Cube hierarchies become dimension parents on import; no `hierarchies` key is emitted on export.
- Export only loops model-local measures; graph-level metrics (important for dbt→Cube) have no general placement/lowering path. Role aliases distinct from target model are expressly rejected; many-to-many/cross joins warn and are omitted.

These are more consequential than key order or SQL normalization. Ours does correctly restore selected `cube_internal` fields such as geo latitude/longitude, sub_query, rank properties, governance flags, and rolling leading/offset. Do not characterize all its meta handling as absent.

Evidence: [ours granularities import](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/cube.py#L801), [ours multi-stage meta](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/cube.py#L1078), [ours export](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/cube.py#L1504).

## dbt/MetricFlow: meaningful differences

### Official strengths

- Compiled manifest input uses real resolved relation names (`node_relation.relation_name`), entities, measures, and MetricFlow's transformer. Ours can accidentally parse a JSON manifest as YAML, but ignores node_relation: the official interop fixture produced `orders.table=None, sql=None` in ours. It is **not manifest compatibility**.
- Entity relationships use a shared-entity index and actual PRIMARY/UNIQUE versus FOREIGN types. Ours resolves a foreign entity primarily by singular/plural matching model names. Probe: foreign entity `customer` could not resolve to semantic model `people_rollup` even though its primary entity is named customer. Ours also does not map UNIQUE/NATURAL entities in the same way as primary/foreign.
- Aggregates include percentile/discrete percentile and sum_boolean with explicit CASE conversion. Ours drops percentile (with a fidelity note for legacy measures; latest folded metric drops have no note in `_parse_metric`), and maps sum_boolean directly to SUM.
- Official produces structured issues for dropped conversion/private metrics, natural entities, nonportable time-spine/fill/offset dependencies, ambiguous references, and unresolved row-count ownership. Cumulative metrics are retained as base aggregate with `CUMULATIVE_SEMANTICS_LOSS`; this is explicitly lossy.
- Reverse direction parses SQL ASTs to recover simple aggregations and ratios, with generated submetric collision handling. It can handle compound scalar operands and percentile parameters, and refuses complex aggregate combinations it cannot express safely.

Evidence: [official manifest conversion](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/msi_to_ossie.py#L146), [official entity index](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/msi_to_ossie.py#L534), [official aggregation](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/msi_to_ossie.py#L616), [reverse AST conversion](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/ossie_to_msi.py#L338), [ours entity heuristic](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/metricflow.py#L1230).

### Ours strengths and official limits

- Ours directly serves source-YAML users, including latest dbt model-embedded declarations; no dbt compilation/manifest prerequisite. Latest-spec disabled models and folded aggregate ownership have explicit handling.
- Ours retains and executes native nonadditive measure window choice/groupings, cumulative window/grain-to-date, dimensions/grains, labels, and segment metadata. Official loses granularity (reverse time dimensions always DAY), aggregation time dimension/defaults, and semi-additive semantics.
- **Confirmed silent official loss:** added `non_additive_dimension={name:created_at, window_choice:max, window_groupings:[order_id]}` plus saved_queries to a manifest; manifest→Ossie→manifest returned no saved queries and non_additive_dimension=None, with `issues=[]` in both directions. Existing datatype support does not make this a general metadata-preserving adapter.
- Official does not preserve general `meta`, custom_extensions, saved queries, project configuration, labels on metrics, or most AI context via an extension protocol. Ours only preserves selected metadata too, but at least conversion specs and saved queries remain available on its graph immediately after import.
- Official filter Jinja support is best-effort: e.g. `Dimension('order__status')` becomes the token `order__status`, **not an actually resolved warehouse column**; source comments explicitly leave this resolution to consumers. Thus valid-looking generated SQL can still need downstream semantic resolution.
- Official reverse drops expressions such as `SUM(a)+SUM(b)` instead of producing general derived metrics; it rejects composite keys. Its result is not guaranteed a fully usable dbt project with model files, calendar/time spine and validation defaults.

Links: [ours nonadditive](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/metricflow.py#L606), [official default DAY / discarded params](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/ossie_to_msi.py#L106), [official filter resolution limitation](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/filter_utils.py#L133).

### Ours export gaps verified from source and probes

- `graph.metadata.saved_queries` and `metricflow_conversion_metrics` are never emitted by MetricFlow export, despite comments describing roundtripping. A probe with both lost both on export. Retained on import is not roundtrip support.
- Cumulative export only writes window; it omits base/input measure, grain_to_date and period_agg. Probe `revenue_7d` emitted only `type:cumulative, type_params.cumulative_type_params.window:7 days`.
- Derived export recomputes dependencies and omits stored input aliases, input filters and offsets; preservation in `metric.metadata.input_metrics` is not enough. Offset/filtered aliases remain registered in a graph metric despite lacking a proper executable equivalent.
- Model table export discards catalog/schema and writes `ref(last_component)`. Model SQL is emitted as a Sidemantic-supported `sql` extension rather than native dbt semantic-model source `model`. Dimension exporter writes internal dimension types directly, so numeric/boolean graph dimensions from another adapter are not converted to MetricFlow categorical types. Latest folded aggregate lowering writes internal `avg`, unlike regular measure export's `average` mapping.
- Relationship resolution/export and field-level opaque metadata need dedicated fidelity coverage, beyond existing roundtrip tests that explicitly disable relationship/table-schema comparison.

Links: [ours export body](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/metricflow.py#L932), [ours metric export](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/sidemantic/adapters/metricflow.py#L1154), [current roundtrip-test limits](https://github.com/sidequery/sidemantic/blob/a7df3c9eae7c1a9a916ebe317ee378408d7d721b/tests/adapters/metricflow/test_roundtrip.py#L57).

## Current schema interoperability: critical shared official defect

Both official exporters still read **only root/model `metrics`**, although the pinned core accepts `datasets[].metrics`. Probe passed a valid document with `orders.metrics=[SUM(amount)]`:

- official dbt: zero output metrics, **zero issues**;
- official Cube: no output measures, no metric-loss issue (only an unrelated missing-datatype approximation on the field).

This makes newer Sidemantic Ossie output with dataset-local metrics incompatible with these official spokes unless hoisted or fixed. It is an observed current bug, not stale issue wording. Cube's `version` check is exact `0.2.0.dev0`; official dbt's Pydantic model validates against its pinned schema, so version equality alone does not prove feature compatibility.

Evidence: [official Cube root-metric loops](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/cube/src/ossie_cube/ossie_to_cube.py#L1038), [official dbt root-metric loop](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/ossie_to_msi.py#L300). Local `/workspace/ossie-converters-research/cube/src/ossie_cube/ossie_to_cube.py:1054`, `:1079`; `/workspace/ossie-converters-research/dbt/src/ossie_dbt/ossie_to_msi.py:304`.

Dialect caveat: upstream issue wording about selecting the first expression is no longer generally accurate for dbt. Current reverse preference is configured dialect (default ANSI_SQL), then OSSIE_SQL_2026 regardless of array position, then first readable SQL fallback. Non-SQL metric dialects are excluded. Array order only still matters for fallback and duplicate dialect entries. [Current implementation](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/dbt/src/ossie_dbt/ossie_to_msi.py#L528).

## Priorities

1. **P0 interoperability:** support/fix dataset-local metrics in both official spokes (or add an explicit Sidemantic compatibility lowering option). Add an end-to-end fixture based on current Sidemantic Ossie export, not only handcrafted root metrics.
2. **P1 ours correctness:** dynamic CASE SQL-label input; export cumulative base/grain/period parameters; use an entity-name index; ensure unsupported rank/multi-stage concepts cannot accidentally look equivalent to runnable metrics.
3. **P1 ours roundtrip:** emit saved queries/conversion metadata and derived modifiers; restore Cube view definitions, hierarchies, granularities and multi-stage properties at correct schema locations. Consider a generic versioned adapter stash rather than an expanding list of special cases. Distinguish executable mappings, inert preservation and loss in machine-readable reports in both directions.
4. **P1 official correctness:** report/preserve semiadditive semantics rather than flatten with zero issue; preserve/issue saved query and metadata loss; fix dataset metrics before adding more connectors.
5. **P2 boundary coverage:** add manifest adapter intentionally if needed, never silently accept manifest as source YAML with no relation; validate emitted dbt source YAML/manifest using actual MetricFlow parsing/semantic validation. Adopt canonical structural roundtrip + schema validation + gated Cube compilation. Add direct cross-spoke fixtures for graph metrics and relationship role aliases.

## Validation performed

- `uv run pytest -q tests/adapters/cube tests/adapters/metricflow` in Sidemantic: **482 passed, 322 warnings**, 17.73s.
- `uv run pytest -q` in official dbt: **317 passed**, five snapshots passed, 4.72s.
- `uv run pytest -q` in official Cube: **1191 passed, 52 skipped**, 123.39s. Cube compiler is not installed/configured; no claim of compiler validation in this environment. Focused `uv run pytest -q -rs tests/test_feature_matrix.py` after core initialization: **43 passed, 10 skipped**; all ten skips explicitly require a built Cube checkout via OSSIE_CUBE_REPO. Schema validation passed in that focused run.
- Cross-fixture script: six official Cube fixtures tested through both import/export paths; direct canonical check of **all ten** official feature fixtures passed official roundtrip. Initial raw-equality booleans in JSON are false on two official fixtures only due to documented `{CUBE}.column` normalization; do not count these as losses.
- Direct probes confirmed ours metadata/cumulative/entity and source-relation gaps, official saved-query/nonadditive losses, and both official dataset-local metric omissions.
- No product code or committed tests changed. Full Sidemantic suite, live dbt execution and actual Cube compiler checks were not run.
