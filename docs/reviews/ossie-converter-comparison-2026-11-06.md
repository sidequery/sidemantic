# Sidemantic versus official Apache Ossie converters

Research date: November 6, 2026. Six parallel investigations covered all **23 projects** in the official converter registry, Sidemantic's complete format registry, source implementations, fixtures, selected unit suites, and targeted conversion probes. This is a conversion comparison, not a query-engine benchmark or vendor certification.

**Recommendation:** retain Sidemantic's broad source-format importers and CLI, prioritize semantic fidelity in existing exports, and selectively integrate official converters for missing formats. Replacing our adapters wholesale would lose useful input directions and source-language support. Treating every official converter as a reliable drop-in would also be incorrect: several silently omit newer dataset-scoped metrics.

## Reproducible baseline

| Component | Inspected revision |
|---|---|
| Sidemantic | [`a7df3c9eae7c1a9a916ebe317ee378408d7d721b`](https://github.com/sidequery/sidemantic/tree/a7df3c9eae7c1a9a916ebe317ee378408d7d721b) |
| Official converters | [`011ea9f9a1e2c941f351e18e071622339ccae881`](https://github.com/apache/ossie-converters/tree/011ea9f9a1e2c941f351e18e071622339ccae881) |
| Official converters' Ossie submodule | [`01a5488d840deda309aac68cc0b312c195369b8d`](https://github.com/apache/ossie/tree/01a5488d840deda309aac68cc0b312c195369b8d) |
| Our latest supported Ossie draft pin | [`172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`](https://github.com/apache/ossie/tree/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1) |

“Official” here means checked-in implementation in `apache/ossie-converters`, rather than an ecosystem member, issue proposal, or README illustration. The repository was located through current web/GitHub research and checked out with its pinned core submodule. Open issues were used as leads and status context, not as proof of current defects.

Our [registry](../../sidemantic/formats.py) has **20 formats: 20 import-capable, 17 export-capable**, including native Sidemantic and Ossie. This is **18 external format families, 15 with export**. These counts are declarations of entry points, not fidelity scores. Apache's 23 projects also include ontology infrastructure, a catalog service integration, and a deliberately fixture-scoped pipeline; the counts are not directly comparable.

## Complete official inventory

Directions are relative to Ossie: **import = native → Ossie**, **export = Ossie → native**. Our corresponding path normally passes through `SemanticGraph`. A bidirectional entry does not imply a lossless round trip.

| Official project | Implemented directions / boundary | Our corresponding surface | Practical comparison |
|---|---|---|---|
| **Cube** | Both; Cube YAML projects | Both | Official preserves substantially more vendor state/views; ours offers executable graph semantics but loses export features. Both need care with nonportable expressions. |
| **dbt** | Both; compiled `semantic_manifest.json` / MetricFlow interfaces | Both; source semantic YAML, legacy and newer input | Different boundaries. Ours exports legacy source YAML; official manifest conversion is not a replacement source parser. |
| **GoodData** | Both; declarative LDM JSON | Both; additional legacy/SDK forms | Ours supports more input forms and selected metric expressions; official handles composite reference identity better. Official does not implement general MAQL metric migration. |
| **Hex** | **Export only**, despite bidirectional wording in its README | Both | Ours adds import and view/semi-additive handling. Verified type, aggregate and join losses limit our export fidelity. |
| **Holistics** | Compiled dataset JSON import; AML source export | Raw AML import and AML export | Ours avoids requiring a Holistics compiler for source import. Official's two directions are not an offline source round trip. Our AQL handling needs stricter semantic checks. |
| **Omni** | Both; model/view/topic project | Both; narrower YAML layout | Official recognizes native `.view`/`.topic` layouts and preserves broader state; our discovery, topic handling and join export lag. |
| **ThoughtSpot** | Both; Model + Table + SQL View TML | Imports Worksheet/Model/Table; exports Worksheet/Table | Different native shapes. Official covers SQL Views and more reversible metadata; our round trip drops security metadata. |
| **Microsoft** | Both; BIM/TMSL, optional TOM-based TMDL/PBIP loading | Native TMDL import/export | Official has BIM support and conservative SQL→DAX conversion. Ours has substantial direct TMDL/DAX import support but unsafe export cases. Do not count official loading support as full split-PBIP export. |
| **Snowflake** | **Export only**; Cortex Analyst YAML | Both; Cortex YAML | Ours imports and retains useful Cortex enrichments; official preserves compound keys and precise types better. |
| **BigQuery** | **Export only, to LookML**, using BigQuery expressions | General LookML import/export | Not a BigQuery SQL connection feature. Official emits explores/joins missing from our exporter; ours has much broader LookML import. |
| **Databricks** | Both; Unity Catalog Metric View YAML 1.1 | No Metric View adapter | Genuine missing format. Our Databricks database connector does not provide this conversion. |
| **Lightdash** | Both; Lightdash files / dbt metadata | None | Genuine additional format; cannot infer support from our dbt adapter. |
| **Honeydew** | Both; workspace/schema YAML | None | Genuine additional format. |
| **NVIDIA Auto Ontology** | Both core-model directions; additionally native → ontology + mapping | None | Real semantic-model and ontology-generation coverage; formerly GSF, not two separate formats. |
| **OrionBelt / OBML** | Both core-model directions; additionally OBML → ontology | None | Official-hosted implementation; README identifies an external canonical development/publishing repo. Preserved unsupported metrics are not executable OBML. |
| **Sigma** | Both; Data Model code-representation JSON | None | Real missing format; native restoration via extensions is stronger than portable expression coverage. |
| **Wisdom** | Both; domain-export JSON 1.0 | None | Real missing format, with documented context, key and extension losses. |
| **Salesforce** | Both; Semantic Model JSON / Data Cloud / Tableau Next | None | Our Tableau XML importer is a different surface. Java tooling and separately obtained Salesforce schema are prerequisites. |
| **Polaris** | Both; live Iceberg REST catalog table schemas | None | Catalog bootstrap, including table creation on export; not a semantic-model API or metric-preserving file converter. |
| **Palantir** | **Import only**; extracted ontology JSON/ZIP → Ossie ontology | None | Ontology conversion, not ordinary SQL-model lowering. |
| **RelationalAI** | **Export only**; Ossie ontology → PyRel | None | Optional SDK/config needed; default provider reads warehouse metadata, declared-table path supports offline generation. |
| **ontology** | Spec document ↔ rich in-memory ontology model/AST | Static validation and source preservation only | Shared ontology infrastructure, not a separate vendor format. Ours does not provide the same reasoning/model conversion. |
| **wisdom-semantic-view** | Four legs joining semantic-view YAML, Ossie, Wisdom JSON | No equivalent pipeline | Explicit finance-fixture implementation with hard-coded expression limits; do not score as general Snowflake↔Wisdom support. |

The implementation evidence and nuances for every row are in the six supporting investigations linked below. The official [interop capability registry](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/interop/manifest.yaml) independently distinguishes unsupported directions and prerequisites.

## Where our coverage is distinct

Within this pinned official repository, there are no corresponding implementations for our **AtScale SML, BSL, Malloy, Rill, Superset, Tableau XML, Graphene, or Yardstick** formats. The first five are bidirectional in our registry; Tableau, Graphene and Yardstick are import-only. LookML must not be included in that absence list because the official `bigquery` project exports LookML.

We also provide import directions absent from the official **Snowflake, Hex and BigQuery/LookML** projects. Direct Holistics AML parsing, source dbt YAML handling and direct TMDL processing are useful capabilities distinct from the official input boundaries. These are coverage advantages, not evidence that every supported source feature survives conversion.

Current official proposals such as [LookML import #71](https://github.com/apache/ossie-converters/issues/71), [Superset #44](https://github.com/apache/ossie-converters/issues/44), [WrenAI #47](https://github.com/apache/ossie-converters/issues/47), and [Gravitino #45](https://github.com/apache/ossie-converters/issues/45) are not counted as implemented support.

## Fidelity findings that should drive the work

These are bounded source/probe findings at the pinned revisions, not assertions about every possible model. Detailed reproductions and source locations are in the supporting notes.

| Priority | Finding | Why it matters / suggested response |
|---|---|---|
| **P0** | ThoughtSpot table round trip drops `rls_rules` and connection metadata; SQL View input can produce an empty graph. | Block or explicitly diagnose unsupported governance/empty imports before presenting a conversion as ready. Preserve metadata where faithful restoration is possible. |
| **P0** | Holistics AQL `filter()`, `of_all()` and `running_total()` modifiers can collapse to the same plain aggregate; Hex median export becomes count; TMDL export copies derived SQL as DAX and has COUNT/DISTINCTCOUNT blank-semantics mismatches. | Reject unsupported semantics or translate with independent value-based tests. A successful file write is not acceptable evidence. |
| **P0** | Snowflake compound PKs/relationships are truncated and unique keys lost; Omni non-equi joins can be reconstructed as equality. | Preserve full predicates/key tuples or refuse conversion. These change join multiplicity and can change metric values. |
| **P1** | Official Snowflake emits `NUMBER(38,0)`, which our importer re-exports as TEXT. | Normalize parameterized native types and test official-output→our-input contracts. Also preserve source-query definitions. |
| **P1** | Our Cube exporter drops views/hierarchies and demotes or loses execution-relevant features; an official conditional-label fixture crashes our importer. | Use the official feature corpus as differential inputs and add explicit source-state restoration where supported. |
| **P1** | Our LookML exporter writes views without explores/relationships; Omni exporter omits topics and native relationship metadata. | Add complete project export contracts, using official export behavior as a reference. |
| **P1** | Our MetricFlow export loses saved queries, conversion specifications and parts of cumulative/derived semantics; official dbt also drops some metadata. | Keep source-YAML and manifest support as separate declared capabilities; preserve or report unsupported features in each direction. |
| **P1** | GoodData composite reference mapping loses the target's full key identity locally. | Test ordered source→target column pairs, not just relationship counts. |

The existing CLI already stages output and checks an import-fidelity report. The missing step is complete instrumentation and an **export/conversion report**: several adapters do not emit structured fidelity diagnostics, and valid graph construction says nothing about losses in the destination exporter. See [CLI conversion](../../sidemantic/cli.py), [fidelity reporting](../../sidemantic/fidelity.py), and [generic graph conversion](../../sidemantic/formats.py).

## Ossie interoperability is a separate axis

Our active `osi` alias resolves to the newer `OssieAdapter`; the older `adapters/osi.py` is not the public registry implementation. Do not infer current flat-document incompatibility from that legacy file.

The official converters pin an older core snapshot despite sharing the `0.2.0.dev0` version string with newer drafts. Our newer parser supports constructs such as `expression.sql` and open dialect labels. **Pin age alone is not a compatibility failure**: our default native synthesis emits `expression.dialects`, and a simple native SUM model passed the official validator and selected exporters.

There is nevertheless a real feature mismatch: **dataset-scoped metrics are omitted by official Cube, dbt, Snowflake and Omni in the probes**, whereas official BigQuery/LookML retains them. Ordinary native Sidemantic model metrics synthesize at the root; imported Ossie dataset metrics retain their scope through metadata and can trigger the mismatch on re-export. This requires a consumer-specific capability check or a proven normalization, not simply a schema-version switch.

Inbound tests also found narrower incompatibilities: the official Flights example fails our strict model-name identifier check, and quoted source names require an API source-dialect option that our conversion CLI does not expose. Seven of ten official contract fixtures lower cleanly with default DuckDB settings; two more need their matching target dialect, and the quoted-source case works with the explicit API source dialect. The official TPC-DS example lowers cleanly. A separate outbound probe found that a native `numeric` dimension without `logical_data_type` emits no Ossie datatype and can become a string downstream.

Likewise, these operations have different guarantees:

- **Preserved document → document:** our Ossie API can retain canonical source data, with optional exact-source-byte reuse under explicit conditions.
- **Document → runtime graph → synthesized document:** reconstructs a logical model and can lose foreign extensions/AI metadata.
- **Vendor A → graph/Ossie → vendor B:** additionally depends on both adapters, expression translation, key/predicate representation, and target capabilities.
- **Native state stored in a vendor extension:** may restore a same-vendor round trip while remaining unusable to other consumers.

Our `SIDEMANTIC` runtime extension preserves otherwise inexpressible semantics and uses an empty portable core placeholder. That is deliberately not useful executable content for official tools that do not understand the extension. Use portable-only synthesis when the destination requires portable core, and report refusal clearly; do not advertise extension restoration as universal interchange.

## Recommended sequence

1. **Fix or reject silent semantic changes in current formats.** Start with governance, metric modifiers, join predicates, compound keys and SQL↔DAX semantics. This improves trust more than another nominal format entry.
2. **Build a pinned differential conversion corpus.** Reuse official fixtures and add independent expectations for NULLs, aggregate population, source identity, compound keys, filters, access controls, unknown expressions, and metadata placement. Compare destination meaning, not only round-trip structure.
3. **Add target capability checks and conversion diagnostics.** Separate exact translation, preserved-but-not-portable state, deliberate loss, and rejection. Include schema revision, dialect, input/output native shapes and dataset-metric support.
4. **Improve existing exporters using official implementations as references.** High-value areas are Cube state restoration, LookML explores, Omni project structure, Snowflake types/keys and conservative Microsoft SQL→DAX handling.
5. **Evaluate official converters as optional external integrations for missing formats.** Databricks Metric Views, Lightdash, Honeydew, Sigma and OBML are concrete candidates. Keep Java/SDK/service requirements separate from our lightweight core. No integration or end-to-end guarantee is implied by this research.
6. **Treat ontology and live catalog integrations as explicit product decisions.** Palantir/RelationalAI require ontology semantics; Polaris mutates a catalog; the Wisdom fixture pipeline is not ready as a general format adapter.

## Supporting investigations and validation

- [Cube and dbt](ossie-converters-2026-11-06/cube-dbt.md)
- [Microsoft, ThoughtSpot and GoodData](ossie-converters-2026-11-06/bi-models.md)
- [Hex, Holistics, Omni, Lightdash and Honeydew](ossie-converters-2026-11-06/yaml-tools.md)
- [Snowflake, Databricks, BigQuery/LookML and Polaris](ossie-converters-2026-11-06/warehouse.md)
- [Ontology and remaining projects](ossie-converters-2026-11-06/ontology-rest.md)
- [Cross-cutting interoperability and our additional formats](ossie-converters-2026-11-06/interop.md)

Each note distinguishes executed tests/probes from source/test inspection. Passing existing unit suites does not contradict the demonstrated gaps: those suites do not assert all cross-format semantics. No production adapter code was changed. No live vendor deployment, full pairwise matrix, or query-equivalence certification was performed.

The disjoint selected Sidemantic adapter suites recorded **1,029 passed, 29 skipped**. Selected official suites recorded **4,610 passed, 60 skipped**; a further focused Cube rerun recorded 43 passed/10 skipped and is excluded from those totals. Skips/exclusions include optional DAX, vendor CLIs, built Cube compiler, packaging and Microsoft TOM integration. Databricks/Polaris Java and the nine ontology/remaining projects were source/test-inspected, not executed; official Hex was also not executed. The notes record exact commands and boundaries. These counts measure checks run, not comparative quality.

The official interop harness is a useful starting point, but its default mode returns success even when individual conversions fail, and strict mode still permits `LOSSY` outcomes. Its broad structural checks do not prove general SQL equivalence, type preservation or access-policy preservation. See the pinned [interop grading and exit rules](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/interop/README.md#L150). This research did not run the full harness.
