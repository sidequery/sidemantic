# Malloy Compatibility

Sidemantic's Malloy adapter parses `.malloy` files using an ANTLR4-generated parser built from the official Malloy grammar (lexer and parser `.g4` files from the malloydata/malloy repository). It maps Malloy sources to Sidemantic's semantic model (Model, Dimension, Metric, Segment, Relationship) and supports exporting back to Malloy for roundtrip workflows.

Features are marked **supported**, **partial support**, or **unsupported**. Partial support entries include notes explaining the limitation. Properties that parse without error but have no Sidemantic equivalent are grouped together per section rather than listed individually.

---

## Sources

| Feature | Status |
|---------|--------|
| `source: name is connection.table('path') extend { ... }` | Supported |
| `source: name is connection.sql("""...""") extend { ... }` | Supported (SQL stored in `Model.sql`, `Model.table = None`) |
| `source: name is connection.sql('...')` (short string) | Supported |
| Multiple sources per file | Supported |
| Comma-separated source definitions in one `source:` statement | Supported |
| Directory parsing (recursive `.malloy` discovery) | Supported |
| Empty/minimal sources (no dimensions or measures) | Supported |
| Connection identifier (`duckdb`, `bigquery`, etc.) | Supported (stored in `Model.metadata["connection"]`; export uses the original connection name) |
| `source: name is other_source extend { ... }` (ID reference) | Supported (sets `Model.extends` to the base source name; inheritance resolved via `resolve_model_inheritance()`) |
| `source: name is base -> { ... } extend { ... }` (pipeline source) | Supported (base source's table/extends preserved; pipeline query not evaluated; extend block processed) |
| `source: name is compose(...)` (composite sources) | Partial support: parses without error; first composed source processed for table/extends. Composition logic not evaluated. |
| `source()` (parameterized sources) | Partial support: parameters are parsed by the grammar but parameter values are not stored or substituted. |
| Old `+` syntax for extending (`base + { ... }`) | Supported (base source processed; refinement block processed best-effort for dimension:, measure:, join:, where:, primary_key: statements) |
| `from()` (source-from-query) | Unsupported (grammar-level construct not handled by the visitor). |

Not mapped: `connection:` statement-level declarations (source-level connection identifiers are captured).

---

## Dimensions

| Feature | Status |
|---------|--------|
| `dimension: name is expression` | Supported |
| Comma-separated dimension lists | Supported |
| Column references (`column_name`) | Supported |
| Arithmetic expressions (`(revenue - cost) / revenue * 100`) | Supported (type inferred as `numeric`) |
| String concatenation (`concat(a, '-', b)`) | Supported |
| Comparison expressions (`value > 0`, `status = 'active'`) | Supported (type inferred as `boolean`) |
| `::date`, `::timestamp`, `::timestamptz` casts | Supported (type inferred as `time`) |
| `DATE_TRUNC('granularity', field)` | Supported (type inferred as `time`, granularity extracted) |
| `field.granularity` (Malloy time truncation: `.day`, `.month`, `.year`, etc.) | Supported (granularity extracted from trailing `.timeframe` pattern) |
| `pick ... when ... else ...` (conditional bucketing) | Supported (transformed to SQL `CASE WHEN ... THEN ... ELSE ... END`) |
| `field ? pick ... when ...` (apply-pick) | Supported (the `?` apply operator is detected; partial comparisons like `when < 5` are expanded to `WHEN field < 5`, and value matches like `when 'ASW'` become `WHEN field = 'ASW'`) |
| `case ... when ... then ... end` (SQL-style CASE) | Supported (grammar parses it; expression preserved as-is) |
| `floor()`, `substr()`, `regexp_extract()` and other functions | Supported (expression preserved verbatim) |
| `??` (null coalescing) | Supported (transformed to `COALESCE(a, b, ...)`) |
| Cross-source field references (`joined_source.field`) | Supported (preserved as-is in SQL) |
| Struct navigation (`event_params.value.int_value`) | Partial support: preserved as-is in the expression text. Works if the database supports dot notation for structs. |

### Type Inference

The adapter infers dimension types heuristically from the SQL expression and field name:

| Inferred Type | Detection Rule |
|---------------|----------------|
| `time` | Expression contains `date_trunc`, `::date`, `::timestamp`, `extract`, `strftime`, `to_date`, `to_timestamp`, or name contains `date`, `time`, `timestamp`, `_at`, `created`, `updated` |
| `boolean` | Expression contains comparison operators (`=`, `!=`, `>`, `<`, `>=`, `<=`) unless inside a `pick`/`case` block |
| `numeric` | Expression contains arithmetic operators (`+`, `-`, `*`, `/`) but not string concatenation (`\|\|`) |
| `categorical` | Default fallback |

### Granularity Extraction

| Pattern | Extracted Granularity |
|---------|----------------------|
| `DATE_TRUNC('minute', ...)` | `minute` |
| `DATE_TRUNC('hour', ...)` | `hour` |
| `DATE_TRUNC('day', ...)` | `day` |
| `DATE_TRUNC('week', ...)` | `week` |
| `DATE_TRUNC('month', ...)` | `month` |
| `DATE_TRUNC('quarter', ...)` | `quarter` |
| `DATE_TRUNC('year', ...)` | `year` |
| `field.second` through `field.year` | Corresponding granularity |
| `::date` cast | `day` |

Not mapped: `access` modifiers (`public`, `private`, `internal`).

---

## Measures

| Feature | Status |
|---------|--------|
| `count()` | Supported |
| `count(field)` | Supported (mapped to `count_distinct` per Malloy semantics) |
| `count_distinct(field)` | Supported |
| `sum(field)` | Supported |
| `avg(field)` | Supported |
| `min(field)` | Supported |
| `max(field)` | Supported |
| `sum(expression)` (e.g., `sum(quantity * price)`) | Supported (expression preserved as the `sql` of the metric) |
| Derived/computed measures (no aggregation function) | Supported (mapped to `type="derived"`) |
| Filtered measures: `count() { where: condition }` | Supported (filter expressions extracted and stored) |
| Filtered measures: `sum(x) { where: condition }` | Supported |
| Comma-separated measure lists | Supported |
| `field.sum()`, `field.avg()`, `field.count()` (dot-method aggregation) | Supported (e.g., `cost.sum()` -> `agg="sum", sql="cost"`; handles dotted paths like `event_params.value.double_value.sum()`) |
| Backtick-quoted field with dot-method (`` `number`.sum() ``) | Supported (backtick-quoted fields handled correctly in dot-method pattern) |
| `all(measure)` (ungrouped aggregate) | Partial support: parses without error, expression preserved as-is, but `all()` is not recognized as an aggregation wrapper. Measures using `all()` become derived. |
| `exclude(measure, dimension)` (symmetric aggregate) | Partial support: expression preserved as-is but not interpreted. |
| Measure references in derived measures | Partial support: referenced by name in the SQL expression but not resolved to their definitions. |
| `source.count()` (cross-source symmetric aggregation) | Partial support: expression preserved verbatim but not recognized as a count aggregation. |

Not mapped: `access` modifiers (`public`, `private`, `internal`), `order_by:` within field properties, `partition_by:`, `grouped_by:`.

---

## Annotations and Descriptions

| Feature | Status |
|---------|--------|
| `## Description text` (doc annotation) | Supported (extracted as `description` on source, dimension, or measure) |
| `# desc: value` tag annotation | Supported (extracted as `description`) |
| `# description: value` tag annotation | Supported (extracted as `description`) |
| Multiple `##` lines on one entity | Supported (joined with spaces) |
| Statement-level `#` tags (before `source:`) | Supported (applied as source description if the source itself has none) |
| `# tag_name` (non-description tags) | Partial support: parsed without error but only `desc:` and `description:` prefixed tags are extracted. Other tags are discarded. |
| `#@ persist` and `#@ persist name=...` | Unsupported (parsed by the grammar but not recognized by the visitor). |

Not mapped: visualization hint tags (`# line_chart`, `# bar_chart`, `# list_detail`, `# shape_map`, `# percent`, `# currency`, `# number`), `--! styles` directives, `##! experimental` pragmas.

---

## Joins

| Feature | Status |
|---------|--------|
| `join_one: target with foreign_key` | Supported (maps to `Relationship(type="many_to_one")`) |
| `join_many: target on condition` | Supported (maps to `Relationship(type="one_to_many")`) |
| `join_cross: target` | Supported (maps to `Relationship(type="one_to_one")`) |
| `join_one: alias is source with fk` (aliased join) | Supported (relationship name is the alias) |
| `join_one: alias is source on condition` | Supported (FK extracted from first identifier before `=` in the on-expression) |
| Multiple joins in comma-separated list | Supported |
| Inline source definition in join (`join_one: name is connection.table(...) extend { ... } with fk`) | Supported (inline source extracted as a separate model; relationship created with correct FK) |
| Matrix operations (`left`, `right`, `full`, `inner`) | Partial support: parsed by the grammar but the join direction is not stored. All joins use the default mapping based on `join_one`/`join_many`/`join_cross`. |
| Multi-condition `on` clause (`a = b.a and c = b.c`) | Supported (first equality used as FK; all equality FKs stored in `metadata["composite_keys"]`; full condition stored in `metadata["on_condition"]`) |
| Cross-source join conditions (e.g., `gender = cohort.gender and state = cohort.state`) | Supported (all FKs extracted; full condition preserved in metadata) |

Not mapped: join `type` (`left`, `right`, `full`, `inner`).

---

## Imports

| Feature | Status |
|---------|--------|
| `import 'path/to/file.malloy'` (import all sources) | Supported |
| `import { source1, source2 } from 'file.malloy'` (named imports) | Supported (only listed sources are added to the graph) |
| `import { source is alias } from 'file.malloy'` (aliased imports) | Supported (model is renamed to the alias) |
| Relative path resolution | Supported (import paths resolved relative to the importing file) |
| Transitive imports (A imports B which imports C) | Supported (depth-first resolution) |
| Circular import detection | Supported (each file parsed at most once per resolution chain) |
| Missing import file handling | Supported (silently skipped, remaining sources still parsed) |
| Directory-level deduplication | Supported (first model with a given name wins; duplicates skipped) |

---

## Source-Level Where (Segments)

| Feature | Status |
|---------|--------|
| `where: condition` in source extend block | Supported (mapped to `Segment`) |
| Multiple filter conditions (comma-separated) | Supported (each becomes a separate segment) |
| Filter expressions with comparisons, `and`, `or` | Supported (expression preserved as-is) |
| Malloy partial application (`field ? pick ... when ...`) | Partial support: expression text is captured but the `?` operator is not evaluated. |
| Malloy value matching (`field ? 'a' \| 'b'`) | Partial support: expression preserved as-is, not converted to SQL `IN`. |

Segment naming: first filter is named `default_filter`, subsequent filters are named `default_filter_1`, `default_filter_2`, etc.

---

## Rename

Supported. `rename:` statements (e.g., `rename: new_name is old_name`, `rename: year_born is \`year\``) are mapped to `Dimension(name=new_name, sql=old_name)`. The dimension type is inferred from the old field name. Comma-separated rename lists are supported. Downstream dimension and measure expressions that reference the new name work correctly since the old name is preserved in the dimension's SQL.

---

## Views (Named Queries Within Sources)

Views defined inside a source with `view: name is { ... }` are parsed by the grammar but not extracted by the visitor. They do not produce separate models or any stored query definitions. This is intentional: views represent query definitions, not semantic model structure.

Content within view blocks, including `group_by:`, `aggregate:`, `nest:`, `order_by:`, `limit:`, `top:`, `where:`, `having:`, `sample:`, `select:`, and `index:`, is parsed without error by the grammar but entirely ignored by the semantic extraction.

---

## Top-Level Queries

`query:` and `run:` statements at the top level of a Malloy file are parsed by the grammar but not extracted by the visitor. They do not produce models. Named queries (`query: name is source -> { ... }`) and anonymous run statements (`run: source -> { ... }`) are both silently skipped.

---

## Query Pipelines

The arrow operator (`->`) for chaining query stages is parsed by the grammar. When used in a source definition (e.g., `source: cohort is names -> { ... } extend { ... }`), the base source's table or extends reference is preserved. The pipeline query body is not evaluated (its aggregate/group_by fields are not extracted), but the extend block is fully processed. This means pipeline-derived sources retain their connection to the base table.

---

## Refinements

The `+` operator for query/view refinement is parsed by the grammar as `SQRefinedQuery` or `SegRefine`. In the context of source definitions, when `+` is used instead of `extend` (old Malloy syntax), the base source is processed and the refinement block is processed best-effort for dimension:, measure:, join:, where:, and primary_key: statements. In the context of views and queries (which are not extracted), refinements are naturally skipped.

---

## Nesting

`nest:` statements within queries/views allow embedding sub-queries as nested result sets. The grammar parses these correctly, including named nests (`nest: name is { ... }`), reference nests (`nest: view_name`), and refined nests (`nest: view_name + { ... }`). Since views and queries are not extracted, nesting has no effect on the semantic model.

---

## Grouping and Aggregation (Query-Level)

`group_by:`, `aggregate:`, `calculate:`, `project:`/`select:`, `index:`, and `declare:` statements within query blocks are parsed by the grammar but not extracted. These are query-time operations, not semantic model definitions.

---

## Accept/Except (Field Visibility)

Partial support. `accept:` and `except:` statements within source extend blocks are recognized by the visitor. The field names are parsed and stored internally, though field filtering is best-effort since the adapter doesn't have knowledge of all underlying table columns. The `except:` clauses in composite source patterns (e.g., `flights_cubed extend { where: ... except: \`field1\`, \`field2\` }`) are parsed.

---

## Include Blocks

`source extend { ... } include { ... }` blocks are parsed by the grammar. The `SQInclude` context in the visitor processes the base source expression but does not handle the include block contents. Field visibility restrictions from include blocks are not applied.

---

## Type System

Malloy's type system (`string`, `number`, `boolean`, `date`, `timestamp`, `timestamptz`) appears in the grammar for casts (`::type`, `:::type`), function type parameters, and source parameter declarations. The adapter does not use Malloy's declared types for dimension type assignment. Instead, types are inferred heuristically from expression content and field names (see Type Inference table above).

---

## Expressions

### Supported Expression Patterns

| Pattern | Status |
|---------|--------|
| Arithmetic (`+`, `-`, `*`, `/`, `%`) | Supported (preserved in expression text) |
| Comparison (`=`, `!=`, `>`, `<`, `>=`, `<=`) | Supported |
| Logical (`and`, `or`, `not`) | Supported |
| `is null`, `is not null` | Supported |
| String literals (`'value'`, `"value"`) | Supported |
| Numeric literals | Supported |
| `true`, `false` | Supported |
| `null` | Supported |
| `pick ... when ... else ...` | Supported (transformed to CASE) |
| `case ... when ... then ... else ... end` | Supported |
| Parenthesized expressions | Supported |
| Function calls (`floor()`, `concat()`, `regexp_extract()`, etc.) | Supported |
| Backtick-quoted identifiers (`` `year` ``) | Supported |
| Type casts (`::date`, `::number`, `::string`) | Supported (preserved in text) |

### Partially Supported Expression Patterns

| Pattern | Status |
|---------|--------|
| `??` (null coalescing) | Supported: transformed to `COALESCE(a, b, ...)` |
| `?` (apply/partial comparison) in dimensions | Supported: `field ? pick ... when ...` is expanded to proper CASE with base field prepended to partial conditions |
| `?` (apply/partial comparison) in filters | Partial support: preserved as-is in segment/filter expressions |
| `~` and `!~` (regex match) | Supported: `expr ~ r'pattern'` transformed to `REGEXP_MATCHES(expr, 'pattern')` |
| `\|` (alternative/or-tree) | Supported: `field ? 'a' \| 'b'` transformed to `field IN ('a', 'b')` |
| `&` (and-tree/partial filter) | Supported: `field < X & > Y` transformed to `field < X AND field > Y`; `field != 'A' & 'B'` transformed to `field != 'A' AND field != 'B'` |
| `!` (type assertion, e.g., `timestamp_seconds!timestamp(x)`) | Supported: `func!type(args)` stripped to `func(args)` |
| `field ? pick ... when ...` (apply-pick) | Supported in dimensions: base field prepended to partial comparisons, transformed to CASE |
| Date literals (`@2024-01-01`, `@2024-Q1`, `@2024`) | Supported: `@YYYY-MM-DD` -> `DATE 'YYYY-MM-DD'`, `@YYYY-MM` -> `DATE 'YYYY-MM-01'`, `@YYYY` -> `DATE 'YYYY-01-01'` |
| Range expressions (`x to y`, `x for y days`) | Partial support: parsed by grammar, preserved as-is |
| Array literals (`[1, 2, 3]`) | Partial support: parsed by grammar, preserved as-is |
| Record literals (`{key: value}`) | Partial support: parsed by grammar, preserved as-is |
| `now` | Supported: standalone `now` transformed to `CURRENT_TIMESTAMP` |
| Filter strings (`f'...'`, `f"..."`) | Partial support: parsed by grammar, preserved as-is |
| `ungroup()` / `all()` / `exclude()` | Partial support: parsed but not interpreted semantically |

---

## SQL Interpolation

SQL strings with `%{ expression }` interpolation (used in `connection.sql("""...""")` sources) are parsed by the grammar. The SQL content between `"""` delimiters is extracted, but `%{ }` interpolation blocks are not evaluated. The raw SQL including any `%{ }` markers is stored as the model's SQL.

---

## Malloy Export (Roundtrip)

Sidemantic can export its semantic model back to Malloy format.

| Feature | Status |
|---------|--------|
| Sources with `connection.table('path')` | Supported (uses the original connection name from parsing, defaults to `duckdb`) |
| Sources with `connection.sql("""...""")` | Supported (SQL preserved in triple-quoted string) |
| Source descriptions as `# desc:` annotations | Supported |
| Dimension descriptions as `# desc:` annotations | Supported |
| Measure descriptions as `# desc:` annotations | Supported |
| Non-passthrough dimensions | Supported (passthrough dimensions where `sql == name` are skipped since Malloy auto-exposes table columns) |
| Time dimensions with granularity | Supported (Malloy `.granularity` suffix appended when not already present in SQL) |
| Standard aggregation measures | Supported (`count()`, `sum(x)`, `avg(x)`, `min(x)`, `max(x)`) |
| Filtered measures | Supported (exported as `agg(x) { where: filter }`) |
| Derived measures | Supported (expression exported as-is) |
| Ratio metrics | Supported (exported as `numerator / denominator`) |
| `primary_key:` | Supported (exported when not the default `id`) |
| `join_one:` / `join_many:` with `with` clause | Supported |
| Roundtrip fidelity (parse -> export -> re-parse) | Supported (semantically equivalent graphs; passthrough dimensions intentionally dropped) |
| `join_cross:` export | Supported (one_to_one relationships exported as `join_cross:`) |
| `rename:` export | Partial support: renames are captured as dimensions during parsing; exported as `dimension:` not `rename:` |
| `view:` export | Unsupported (views are not captured during parsing) |

---

## Experimental and Advanced Features

Unsupported. `##! experimental{...}` pragma annotations, `compose()` for composite sources, `timezone:` statements, `sample:` specifications, and `declare:` field declarations are all parsed by the grammar without error but not processed by the visitor.

---

## Liquid / Templating

Not applicable. Malloy does not use Liquid templating. SQL interpolation via `%{ }` is the closest equivalent and is handled as described above.

---

## Grammar Coverage

The adapter uses the full official Malloy grammar (MalloyLexer.g4 and MalloyParser.g4) with ANTLR4-generated Python parser classes. All valid Malloy syntax parses without error. The visitor selectively extracts only semantic model information (sources, dimensions, measures, joins, segments, imports). Grammar constructs that relate to query execution (views, queries, runs, pipelines, nesting, grouping, ordering, limiting) parse correctly but are intentionally not mapped to Sidemantic concepts.
