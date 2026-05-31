# Graphene GSQL Compatibility

Sidemantic can import Graphene `.gsql` semantic model files through `load_from_directory()` and CLI commands that use the normal model loader.

The importer is a Sidemantic-owned compatibility parser for Graphene's documented model declarations. It does not bundle Graphene, call the Graphene runtime, or vendor Graphene's parser/grammar/analyzer.

## Supported

- `table name (...)` physical table definitions
- `table name as (...)` derived table definitions, with the view query preserved as model SQL
- `extend name (...)` blocks
- Base columns as Sidemantic dimensions
- Computed scalar expressions as dimensions
- Aggregate expressions and measure-composed expressions as metrics
- `join one` as `many_to_one`
- `join many` as `one_to_many`
- Graphene metadata annotations such as `#ratio`, `#pct`, `#currency=USD`, `#unit=minutes`, `#timeGrain=day`, `#timeOrdinal=month_of_year`, `#description=...`, and `#pii`

## Not Supported

- Graphene Markdown pages are not imported as semantic models.
- Full GSQL query compilation is not reimplemented; `select` statements are not imported as standalone semantic models, and `table name as (...)` query text is preserved.
- Graphene-specific query-time behavior such as implicit modeled joins, multi-hop dot traversal, `group by all`, query DAGs, and page input interpolation is outside the importer.
- Relationship aliases are represented by generated alias models that point at the same physical table as the target model.
