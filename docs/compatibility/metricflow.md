# dbt MetricFlow compatibility

The MetricFlow adapter reads dbt **source YAML**: legacy `semantic_models` and `metrics`, or model-embedded semantic declarations with folded simple metrics. It exports legacy source YAML. A compiled `semantic_manifest.json` is rejected explicitly; its resolved `node_relation` is not a source-model `ref()`.

## Source preservation and execution

| Construct | Import / execution | Export |
|---|---|---|
| Simple aggregates | Native measures; folded metrics receive model-qualified expressions | Folded metrics become separate measures and simple references; `avg` exports as `average` |
| Primary and unique entities | Foreign references resolve by declared entity name and backing column, even when the target model has a different name | Entity role names and unique entities are retained; ambiguous owners produce an unsupported diagnostic |
| Cumulative metrics | Base metric, window and grain-to-date are mapped | Retains input references, window, grain-to-date and period aggregation |
| Cumulative input filters/offsets and `period_agg` | Retained with an unsupported diagnostic; not executed | Source parameters retained |
| Derived aliases | Unmodified input aliases resolve to their native metric for execution | Original source expression and declared input descriptors retained |
| Derived input offsets / filters | Retained with an unsupported diagnostic; not executed | Aliases, offsets and filters retained; editing the compiled expression requires reconciling its stored descriptors |
| Conversion metrics | Source definitions retained as graph metadata; no queryable funnel metric is invented | Legacy definitions restored; latest simple inputs lower to generated legacy measures; unrepresentable inputs are rejected |
| Saved queries | Retained as graph metadata with an unsupported execution diagnostic | Query parameters, exports and source configuration restored |
| Semi-additive measures | Window choice and groupings are mapped natively | Native non-additive parameters retained |
| Natural entities / SCD joins | Source entity retained; SCD behavior is unsupported | Source entity retained with an unsupported diagnostic |

The adapter stores source-only definitions separately from executable graph fields. Preserving a property for a source roundtrip does not mean it is used by Sidemantic's query engine. Fidelity reports distinguish retained but unsupported execution from native mappings.

Export is not a complete dbt project generator. It reports unsupported SQL-only models, relationship kinds without a foreign-entity mapping, and metrics that cannot become legacy measures. Warehouse qualification is lost when a table is lowered to `ref('model_name')`, and is reported as an approximation. Percentile aggregation remains unsupported. Exported source should be validated in the destination dbt project, including its model refs, time spine and project configuration.
