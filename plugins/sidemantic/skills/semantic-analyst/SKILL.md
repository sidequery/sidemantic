---
name: semantic-analyst
description: Answer analytical, KPI, metric, trend, cohort, and business-performance questions through a Sidemantic semantic layer. Use when asked to analyze data, explain why a metric changed, compare segments or periods, calculate a business measure, or explore warehouse data when Sidemantic MCP tools or semantic model files are available. Discover and reuse trusted definitions, distinguish durable semantic-model gaps from one-off query logic, and prefer semantic queries over duplicated raw SQL.
---

# Semantic Analyst

Use the semantic layer as accumulated business knowledge, not merely as a convenient query interface.

## Workflow

1. Search the semantic catalog for the user's concepts. Search by business terms and likely synonyms; do not dump the full graph unless search is unavailable or broader topology is necessary.
2. Explain candidate metrics and inspect the relevant models. Check descriptions, dependencies, filters, aggregation or formula, grain, relationships, source file, and provenance before deciding that a metric matches the request.
3. Translate the question into existing metrics, dimensions, segments, time grains, and one-off filters. Validate uncertain field combinations.
4. Prefer a structured semantic query. Use semantic SQL only when the structured query cannot express a needed operation. Use raw warehouse SQL only for genuinely exploratory work outside the modeled surface, and label that result as outside the semantic layer.
5. Check the result for the requested grain, units, time range, exclusions, null behavior, and denominator. State material assumptions.
6. Answer the business question directly. Include the metric definition or query provenance when it affects interpretation.

## Decide What Belongs in the Model

Promote durable business semantics into the model; keep ephemeral analytical operations in the query.

- Reuse an existing metric for requests such as revenue last week by state.
- Apply query filters for narrow cohorts such as California users who signed up Tuesday.
- Model definitions with durable policy, such as net revenue excluding refunds or repayment rate with an eligibility denominator.
- Model ratios or derived measures that recur across analyses.
- Keep unusual exploratory cohorts or temporary calculations out of the model until they become reusable.

Do not create combinatorial metrics that merely bake a dimension value, date range, or one-off filter into an otherwise reusable metric.

## Handle Semantic Gaps

When no existing definition faithfully represents the request, do not silently invent equivalent SQL. Return a concise structured gap:

```yaml
semantic_gap:
  requested_concept: net revenue
  why_missing: Existing revenue does not define refund treatment.
  reusable: true
  recommended_action: model_metric
  proposed_definition:
    name: net_revenue
    policy: gross revenue minus refunded amount
    open_questions:
      - Which refund statuses count?
```

Use `recommended_action: query_only` for ephemeral operations and `clarify` when business policy is ambiguous.

If working as a coding agent with repository access, inspect the model's reported `source_file`, update the semantic definition through normal files and Git, run `sidemantic validate path/to/models/ --verbose`, run relevant project tests, and retry the original semantic query. Use the separate `modeler` skill for substantial authoring or migration work. If files cannot be edited, report the gap and proposed definition without claiming it was persisted.

## Guardrails

- Never substitute a similarly named metric without checking its definition.
- Never hide missing business policy inside ad hoc SQL.
- Never persist a new metric merely because a query is complex.
- Never claim causality from descriptive results alone.
- Preserve access controls and field visibility; do not work around unavailable semantic fields.
