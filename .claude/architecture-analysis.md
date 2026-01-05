# Sidemantic Codebase Architecture Analysis

## Project Overview

Sidemantic is a **SQL-first semantic layer** that provides a universal interface for defining and querying metrics across different data stacks. It supports multiple input formats (Cube, MetricFlow/dbt, LookML, Hex, Rill, Superset, Omni, BSL) and multiple databases (DuckDB, MotherDuck, PostgreSQL, BigQuery, Snowflake, ClickHouse, Databricks, Spark).

The project is built in Python using:
- **SQLGlot** for SQL parsing and generation
- **Pydantic** for data validation and type safety
- **YAML** for configuration
- **Jinja2** for templating

## 1. Parsing Architecture

### 1.1 Custom SQL Dialect Extension

The project extends SQLGlot with custom syntax for defining semantic models using SQL-like statements:

**File:** `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/dialect.py`

**Custom Expression Types:**
- `ModelDef` - Defines a model/dataset
- `DimensionDef` - Defines a dimension (attribute for grouping/filtering)
- `RelationshipDef` - Defines relationships between models
- `MetricDef` - Defines metrics (aggregations)
- `SegmentDef` - Defines segments (named filters)
- `PropertyEQ` - Property assignments within definitions

**Parser:** `SidemanticParser` extends SQLGlot's parser to recognize these custom statements.

**Example Syntax:**
```sql
MODEL (
    name orders,
    table orders,
    primary_key order_id
);

DIMENSION (
    name status,
    type categorical,
    sql status
);

RELATIONSHIP (
    name customer,
    type many_to_one,
    foreign_key customer_id
);

METRIC (
    name revenue,
    agg sum,
    sql amount
);

SEGMENT (
    name completed,
    expression status = 'completed'
);
```

**Property Aliases:**
- `expression` → `sql`
- `aggregation` → `agg`
- `filter` → `filters`

### 1.2 SQL Definition Parsing

**File:** `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/sql_definitions.py`

**Key Functions:**
- `parse_sql_definitions(sql)` - Parses METRIC() and SEGMENT() statements
- `parse_sql_model(sql)` - Parses complete model with all child definitions
- `parse_sql_file_with_frontmatter(path)` - Parses .sql files with optional YAML frontmatter
- `_extract_properties()` - Extracts property assignments from definitions

**Conversion Process:**
1. Parse SQL using custom SQLGlot dialect
2. Extract property assignments from each definition
3. Resolve property aliases
4. Create Pydantic model instances (Model, Dimension, Metric, etc.)

### 1.3 Adapter System

**File:** `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/adapters/`

**Base Adapter:** `BaseAdapter` (abstract class)
- `parse(source)` - Parse external format into SemanticGraph
- `export(graph, output_path)` - Export SemanticGraph to external format
- `validate(graph)` - Validate imported graph

**Sidemantic Adapter:** `SidemanticAdapter`
- Supports three input formats:
  1. **Pure SQL** - Complete model in SQL (uses `parse_sql_model`)
  2. **YAML with frontmatter + SQL** - YAML model definition with SQL metrics/segments
  3. **Pure YAML** - YAML with optional embedded `sql_metrics`/`sql_segments` fields

**Parsing Flow:**
```
.sql file → Check for MODEL() → Parse as pure SQL
         ↓
         Check for --- frontmatter → Parse YAML + SQL metrics
         ↓
         Otherwise → Error

.yml/.yaml file → Parse YAML
                ↓
                Check for sql_metrics field → Parse embedded SQL
                ↓
                Check for sql_segments field → Parse embedded SQL
```

### 1.4 Multi-Format Loaders

**File:** `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/loaders.py`

**Function:** `load_from_directory(layer, directory)`

**Auto-detection logic:**
- `.lkml` → LookMLAdapter
- `.sql` → SidemanticAdapter
- `.yml/.yaml` → Detect format by content:
  - `models:` → SidemanticAdapter
  - `cubes:` or `views:` + `measures:` → CubeAdapter
  - `semantic_models:` or `metrics:` + `type:` → MetricFlowAdapter
  - `base_sql_table:` + `measures:` → HexAdapter
  - `tables:` + `base_table:` → SnowflakeAdapter
  - `_.` + dimensions/measures → BSLAdapter

**Post-processing:**
- Infers cross-model relationships based on foreign key naming conventions (`*_id` → parent model)
- Tracks source format and file for each model

## 2. Core Data Structures

### 2.1 Pydantic Models

**Model** (`/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/model.py`)
```python
class Model(BaseModel):
    name: str
    table: str | None
    sql: str | None  # For derived tables
    description: str | None
    extends: str | None  # For inheritance
    relationships: list[Relationship]
    primary_key: str = "id"
    dimensions: list[Dimension]
    metrics: list[Metric]
    segments: list[Segment]
    pre_aggregations: list[PreAggregation]
    default_time_dimension: str | None
    default_grain: Literal[...]
```

**Dimension** (`/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/dimension.py`)
```python
class Dimension(BaseModel):
    name: str
    type: Literal["categorical", "time", "boolean", "numeric"]
    sql: str | None  # Defaults to name
    granularity: Literal[...] | None  # For time dimensions
    supported_granularities: list[str] | None
    description: str | None
    label: str | None
    format: str | None
    value_format_name: str | None
    parent: str | None  # For hierarchies
```

**Metric** (`/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/metric.py`)
```python
class Metric(BaseModel):
    name: str
    extends: str | None

    # Simple aggregation
    agg: Literal["sum", "count", "count_distinct", "avg", "min", "max", "median"] | None
    sql: str | None

    # Complex metric types
    type: Literal["ratio", "derived", "cumulative", "time_comparison", "conversion"] | None

    # Type-specific fields
    numerator: str | None  # ratio
    denominator: str | None  # ratio
    window: str | None  # cumulative
    grain_to_date: Literal[...] | None  # cumulative
    base_metric: str | None  # time_comparison
    comparison_type: Literal[...] | None  # time_comparison
    entity: str | None  # conversion
    base_event: str | None  # conversion
    conversion_event: str | None  # conversion

    # Common
    filters: list[str] | None
    description: str | None
    label: str | None
    format: str | None
    drill_fields: list[str] | None
    non_additive_dimension: str | None
```

**Relationship** (`/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/relationship.py`)
```python
class Relationship(BaseModel):
    name: str  # Target model name
    type: Literal["many_to_one", "one_to_many", "one_to_one"]
    foreign_key: str | None
    primary_key: str | None
```

**Segment** (`/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/segment.py`)
```python
class Segment(BaseModel):
    name: str
    sql: str  # Filter expression
    description: str | None
    public: bool = True
```

### 2.2 Semantic Graph

**File:** `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/semantic_graph.py`

**SemanticGraph:**
```python
class SemanticGraph:
    models: dict[str, Model]
    metrics: dict[str, Metric]  # Graph-level metrics
    table_calculations: dict[str, TableCalculation]
    parameters: dict[str, Parameter]
    _adjacency: dict[str, list[tuple[str, str]]]  # model → [(join_key, target_model)]
```

**Key Methods:**
- `add_model(model)` - Add model to graph, rebuild adjacency
- `add_metric(metric)` - Add graph-level metric
- `build_adjacency()` - Build join graph from relationships
- `find_relationship_path(from_model, to_model)` - BFS to find join path
- `find_all_models_for_query(dimensions, measures)` - Determine required models

**Join Path Discovery:**
- Uses BFS to find shortest join path between models
- Adjacency list built from relationship definitions
- Handles many_to_one, one_to_many, one_to_one relationships

## 3. Module System & Imports

**Current State:** No import/export system exists. All models must be defined in:
1. Single file (YAML or SQL)
2. Multiple files loaded via `load_from_directory()` (no cross-file references)

**Module Resolution:**
- Models are referenced by name (string) within the same graph
- Relationships use model names: `Relationship(name="customers", ...)`
- Cross-model metrics use qualified names: `orders.revenue`
- Auto-registration via registry pattern (context-based)

**Registry System** (`/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/registry.py`):
- Thread-local storage for current SemanticLayer
- Auto-registers models/metrics when created if layer is set
- Used for Python API (not YAML/SQL definitions)

## 4. SQL Query Processing

### 4.1 Query Rewriter

**File:** `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/sql/query_rewriter.py`

**QueryRewriter:**
- Parses user SQL (using SQLGlot)
- Identifies semantic layer references (model.metric, model.dimension)
- Rewrites to proper SQL using SQLGenerator
- Handles CTEs, subqueries, JOINs

**Example:**
```sql
-- User SQL
SELECT orders.revenue, customers.region
FROM orders
WHERE customers.tier = 'enterprise'

-- Rewritten SQL (generated)
SELECT
    SUM(orders.amount) AS revenue,
    customers.region
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.customer_id
WHERE customers.tier = 'enterprise'
GROUP BY customers.region
```

### 4.2 SQL Generator

**File:** `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/sql/generator.py`

**SQLGenerator:**
- Takes semantic query (metrics, dimensions, filters)
- Generates optimized SQL
- Handles:
  - Metric aggregation
  - Automatic joins via graph traversal
  - Time dimension granularity
  - Segment filters
  - Pre-aggregation routing
  - Symmetric aggregates
  - Window functions

## 5. Extension Points for Imports

### 5.1 Where Imports Would Fit

**Potential Locations:**

1. **Dialect Extension** (Best fit)
   - Add `IMPORT` statement to `SidemanticParser`
   - Parse: `IMPORT 'path/to/module.sql';`
   - Similar to how MODEL/DIMENSION/METRIC are parsed

2. **Adapter Level**
   - Modify `SidemanticAdapter.parse()` to handle imports
   - Recursively parse imported files
   - Build combined SemanticGraph

3. **Loader Level**
   - Extend `load_from_directory()` to handle explicit imports
   - Resolve import paths relative to current file

### 5.2 Considerations

**Namespace Management:**
- Current: Flat namespace (model names must be unique)
- With imports: Need qualified names or namespace collision handling
- Options:
  1. Module prefixes: `my_module.orders`
  2. Explicit aliasing: `IMPORT 'models.sql' AS base_models;`
  3. Keep flat namespace, error on collisions

**Circular Dependencies:**
- Need cycle detection in import graph
- Track visited files during parsing
- Error if circular import detected

**Path Resolution:**
- Absolute vs relative paths
- Search paths (like Python's sys.path)
- Standard library location

**Caching:**
- Parse each file only once
- Cache SemanticGraph fragments
- Merge graphs on import

**Export System:**
- Need to mark which models are public/private
- Explicit exports vs implicit (all public)
- Export lists in model files

### 5.3 Proposed Syntax

**Option 1: Simple Import**
```sql
-- Import entire file
IMPORT 'models/customers.sql';
IMPORT 'models/orders.sql';

-- Then reference in relationships
RELATIONSHIP (
    name customers,
    type many_to_one,
    foreign_key customer_id
);
```

**Option 2: Named Import**
```sql
-- Import specific models
IMPORT customers FROM 'models/base.sql';
IMPORT orders, products FROM 'models/ecommerce.sql';

-- Then use as normal
RELATIONSHIP (
    name customers,
    type many_to_one,
    foreign_key customer_id
);
```

**Option 3: Namespaced Import**
```sql
-- Import with namespace
IMPORT 'models/base.sql' AS base;

-- Reference with prefix
RELATIONSHIP (
    name base.customers,
    type many_to_one,
    foreign_key customer_id
);
```

## 6. Current File Organization Examples

**Example: ecommerce_sql_yml**
```
ecommerce_sql_yml/
├── models/
│   ├── order_items.sql  (separate MODEL file)
│   ├── products.sql     (separate MODEL file)
│   └── ...
└── data/
    └── ...
```

Each file is independent, loaded via `load_from_directory()`. No cross-file references.

**Example: multi_format_demo**
```
multi_format_demo/
├── cube/
│   └── orders.js
├── hex/
│   └── orders.yml
└── lookml/
    └── orders.lkml
```

Same model in different formats for comparison.

## 7. Key Insights

### 7.1 Strengths of Current Architecture

1. **Clean separation** between parsing (adapters) and execution (SQL generator)
2. **Extensible adapter system** - easy to add new formats
3. **Strong typing** with Pydantic - validation at parse time
4. **SQLGlot-based** - powerful SQL parsing/generation
5. **Graph-based joins** - automatic join path discovery

### 7.2 Limitations for Import System

1. **No cross-file references** - each file is parsed independently
2. **Flat namespace** - all models in same scope
3. **No module system** - no way to organize/share definitions
4. **No dependency tracking** - can't detect missing imports
5. **Single parse pass** - imports would require multi-pass parsing

### 7.3 Implementation Complexity

**Low Complexity (1-2 days):**
- Simple IMPORT statement in dialect
- Path resolution (relative to current file)
- Recursive parsing in adapter
- Cycle detection

**Medium Complexity (3-5 days):**
- Named imports (IMPORT x FROM y)
- Namespace management
- Export declarations
- Better error messages

**High Complexity (1-2 weeks):**
- Module system with qualified names
- Search paths and package management
- Lazy loading
- Import graph optimization

## 8. Recommended Approach

### Phase 1: Basic Import Support (MVP)

1. **Add IMPORT statement to dialect**
   ```sql
   IMPORT 'path/to/file.sql';
   ```

2. **Modify SidemanticAdapter.parse()**
   - Track imports during parsing
   - Recursively parse imported files
   - Merge SemanticGraphs

3. **Path resolution**
   - Relative to current file
   - Support both .sql and .yml/.yaml

4. **Cycle detection**
   - Track parse stack
   - Error on circular imports

5. **Tests**
   - Basic import
   - Multi-level imports
   - Circular import detection
   - Cross-format imports

### Phase 2: Enhanced Features

1. **Named imports**
   ```sql
   IMPORT customers, orders FROM 'base.sql';
   ```

2. **Export declarations**
   ```sql
   EXPORT customers, products;
   ```

3. **Better error messages**
   - Show import chain on error
   - Suggest similar names
   - Validate imports resolve

4. **Search paths**
   - Environment variable: SIDEMANTIC_PATH
   - Config file support
   - Standard library location

### Phase 3: Advanced Features

1. **Namespaces**
   ```sql
   IMPORT 'base.sql' AS base;
   RELATIONSHIP (name base.customers, ...);
   ```

2. **Wildcard imports**
   ```sql
   IMPORT * FROM 'base.sql';
   ```

3. **Package management**
   - Remote imports (URLs)
   - Version pinning
   - Lock files

## 9. Files to Modify

### Core Implementation
- `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/dialect.py` - Add IMPORT statement
- `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/adapters/sidemantic.py` - Handle imports in parse()
- `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/sql_definitions.py` - Parse IMPORT statements

### Supporting Files
- `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/core/semantic_graph.py` - Merge graphs from imports
- `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/sidemantic/loaders.py` - Handle imports in directory loading

### Tests
- Create `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/tests/core/test_imports.py`
- Create `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/tests/adapters/sidemantic_adapter/test_imports.py`

### Documentation
- Update `/Users/nico/conductor/workspaces/sidemantic/tyler-v3/README.md` with import examples
- Create examples showing import usage

## 10. Alternative Approaches

### A. YAML-Based Imports
Instead of SQL syntax, use YAML frontmatter:
```yaml
---
imports:
  - models/base.sql
  - models/shared.yml
---

MODEL (...);
```

**Pros:** Simpler parsing, no dialect changes
**Cons:** Not pure SQL, YAML-only

### B. Python-Only Imports
Keep SQL files separate, import only in Python:
```python
from sidemantic import SemanticLayer, load_from_directory

layer = SemanticLayer()
layer.load('models/base.sql')
layer.load('models/orders.sql')
```

**Pros:** No parsing changes needed
**Cons:** Breaks pure SQL workflow

### C. Include Mechanism
Simple file inclusion (like C preprocessor):
```sql
#include "base.sql"

MODEL (...);
```

**Pros:** Very simple, no namespace issues
**Cons:** Not SQL-like, name collisions

## Summary

Sidemantic has a well-architected parsing system built on SQLGlot with custom dialect extensions. Adding import support would require:

1. **Dialect extension** - Add IMPORT statement type
2. **Parser enhancement** - Track and resolve imports
3. **Graph merging** - Combine SemanticGraphs from multiple files
4. **Path resolution** - Handle relative/absolute paths
5. **Cycle detection** - Prevent circular imports

The cleanest approach is to extend the existing SQL dialect with an IMPORT statement that works similarly to MODEL/DIMENSION/METRIC statements, keeping the pure SQL workflow intact.
