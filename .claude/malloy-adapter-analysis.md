# Malloy Adapter Implementation Analysis

## Executive Summary

The Malloy adapter at `sidemantic/adapters/malloy.py` is a **well-implemented** ANTLR4-based parser with solid core functionality. It successfully handles single-file and directory parsing, supports export/round-trip, and covers most essential Malloy constructs. However, **import statements are not yet implemented**, which is a significant gap for multi-file Malloy projects.

---

## Current Implementation Status

### ✅ What's Implemented

#### 1. **Parsing Infrastructure** (Lines 23-534)
- **ANTLR4-based parser** using official Malloy grammar
- **MalloyModelVisitor** class that walks the AST
- Supports both single files and directory parsing
- Preserves whitespace in expressions using token stream

#### 2. **Core Malloy Constructs**
- **Sources** → Models (lines 206-227)
  - `source: name is duckdb.table('...')`
  - `source: name is duckdb.sql("""...""")`
  - `source: name is base extend { ... }` (inheritance)
- **Primary keys** (lines 311-315)
- **Dimensions** (lines 318-322, 345-385)
  - Type inference (categorical, numeric, time, boolean)
  - Time granularity detection (day, month, year, etc.)
  - `pick/when/else` → CASE expression transformation (lines 170-204)
- **Measures** → Metrics (lines 325-329, 387-452)
  - Aggregations: count, sum, avg, min, max, count_distinct
  - Filtered measures: `count() { where: ... }` (lines 409-431)
  - Derived/ratio metrics
- **Joins** → Relationships (lines 332-336, 454-518)
  - `join_one` → many_to_one
  - `join_many` → one_to_many
  - `join_cross` → one_to_one
  - Foreign key extraction from `with` and `on` clauses
- **Source-level where** → Segments (lines 339-343, 520-533)

#### 3. **Export Functionality** (Lines 598-708)
- Generates valid Malloy from SemanticGraph
- Resolves model inheritance before export (uses `sidemantic/core/inheritance.py`)
- Round-trip tested (parse → export → parse)

#### 4. **Multi-file Support**
- Directory parsing via `rglob("*.malloy")` (lines 562-568)
- Each file parsed independently
- Works with `load_from_directory` in `sidemantic/loaders.py`

---

### ❌ What's Missing

#### 1. **Import Statements** (Critical Gap)
**Grammar exists** (MalloyParser.g4 lines 72-88):
```antlr
importStatement
  : IMPORT importSelect? importURL
  ;

importSelect
  : OCURLY
    importItem (COMMA importItem)*
    CCURLY FROM
  ;

importItem
  : id (IS id)?
  ;

importURL
  : string
  ;
```

**Malloy import syntax**:
```malloy
import "shared/base.malloy"                    // Import all sources
import { customers } from "models/crm.malloy"  // Import specific source
import { carriers is air_carriers } from "ref/carriers.malloy"  // Import with alias
```

**Current behavior**: Import statements are **ignored** (not processed by visitor)

**Impact**:
- Cannot parse real-world multi-file Malloy projects
- Cross-file model references (e.g., `source: orders extend customers.base_orders`) fail
- No visibility into imported source dependencies

#### 2. **Advanced Malloy Features** (Not Yet Needed)
- **Queries/Views** - Intentionally skipped (line 547: "Views and queries are not part of semantic model")
- **SQL blocks** - Partial support (only for sources)
- **Turducken queries** - Not applicable to semantic layer
- **Named queries** - Skipped (queries are runtime, not model definitions)
- **Annotations/tags** - Parsed but not extracted

---

## Test Coverage

### Test Files
- `tests/adapters/malloy/test_parsing.py` (210 lines)

### Test Fixtures
- `tests/fixtures/malloy/flights.malloy` (50 lines)
  - 3 sources: flights, carriers, airports
  - Dimensions with type detection, pick/when, time fields
  - Measures with aggregations and filters
  - join_one relationships
- `tests/fixtures/malloy/ecommerce.malloy` (69 lines)
  - 4 sources: orders, customers, order_items, products
  - Time dimensions with granularity
  - join_one and join_many relationships

### Test Functions
1. `test_malloy_adapter_flights()` - Validates flights.malloy parsing
2. `test_malloy_adapter_ecommerce()` - Validates ecommerce.malloy parsing
3. `test_malloy_adapter_directory()` - Tests directory-level parsing
4. `test_malloy_adapter_export()` - Tests export to .malloy
5. `test_malloy_adapter_roundtrip()` - Tests parse → export → parse equivalence

### Coverage Gaps
- **No tests for imports** (the main missing feature)
- **No tests for source extends** (inheritance works but not explicitly tested)
- **No tests for complex join conditions** (only simple `with` and `on`)
- **No tests for SQL-based sources** (only table-based)

---

## Architecture Deep Dive

### How Imports Could Be Added

#### 1. **Visitor Changes** (malloy.py)
Add a new visitor method to handle import statements:

```python
def visitImportStatement(self, ctx: MalloyParser.ImportStatementContext):
    """Visit import statement and track dependencies."""
    # Extract import URL
    import_url = ctx.importURL()
    if import_url:
        file_path = self._extract_string(self._get_text(import_url.string()))

        # Track what to import
        import_select = ctx.importSelect()
        if import_select:
            # Selective import: import { source1, source2 as alias } from "file"
            for item in import_select.importItem():
                ids = item.id()
                source_name = self._get_text(ids[0])
                alias = self._get_text(ids[1]) if len(ids) > 1 else source_name
                # Store: (file_path, source_name, alias)
                self.imports.append((file_path, source_name, alias))
        else:
            # Import all: import "file"
            self.imports.append((file_path, None, None))
```

#### 2. **Parser Changes** (malloy.py)
The `_parse_file` method needs to:
1. Resolve relative import paths
2. Parse imported files first (dependency order)
3. Make imported sources available to current file
4. Handle circular imports

**Proposed approach** (similar to LookML's two-pass strategy):
```python
def parse(self, source: str | Path) -> SemanticGraph:
    graph = SemanticGraph()
    source_path = Path(source)

    if source_path.is_dir():
        # Parse all files, tracking imports
        for malloy_file in source_path.rglob("*.malloy"):
            self._parse_file(malloy_file, graph, source_path)
    else:
        self._parse_file(source_path, graph, source_path.parent)

    return graph

def _parse_file(self, file_path: Path, graph: SemanticGraph, base_dir: Path,
                parsed_files: set[Path] | None = None) -> None:
    """Parse file with import resolution."""
    if parsed_files is None:
        parsed_files = set()

    if file_path in parsed_files:
        return  # Already parsed (circular import protection)

    parsed_files.add(file_path)

    # First pass: extract imports
    visitor = MalloyModelVisitor()
    # ... parse to get visitor.imports ...

    # Parse imported files first (depth-first)
    for import_path, source_name, alias in visitor.imports:
        import_file = (file_path.parent / import_path).resolve()
        if import_file.exists():
            self._parse_file(import_file, graph, base_dir, parsed_files)

    # Second pass: parse current file's sources
    # Now all imported sources are in graph.models
    for model in visitor.models:
        graph.add_model(model)
```

#### 3. **Source Reference Resolution**
When processing `source: orders extend customers.base_orders`:
- Check if "customers" is an imported source
- If imported with alias, resolve to original name
- Look up in graph.models and use inheritance system

This already works via the existing `extends` field and `sidemantic/core/inheritance.py:resolve_model_inheritance()`

---

## Comparison with Other Adapters

### LookML Adapter Pattern
- **Two-pass parsing** (lines 46-56 in lookml.py):
  1. Parse all views (models)
  2. Parse explores (relationships)
- **Directory support** via `rglob("*.lkml")`
- **No import resolution** (LookML uses `include:` but it's not parsed)

### Key Insight
LookML doesn't handle imports either! But Malloy imports are more critical because:
- Malloy encourages splitting sources across files
- Sources can extend imported sources
- Import aliases change name resolution

---

## Import Implementation Strategy

### Phase 1: Basic Import Support
1. Add `visitImportStatement` to visitor
2. Track imports in `MalloyModelVisitor.__init__`
3. Implement recursive file parsing with cycle detection
4. Resolve import paths relative to current file

### Phase 2: Source Reference Resolution
1. Handle `import { source as alias }` syntax
2. Build import alias mapping
3. Resolve `extends` to imported sources
4. Handle cross-file join references

### Phase 3: Export with Imports
1. Track which sources came from which files
2. Generate appropriate import statements on export
3. Decide when to inline vs. import (config option?)

---

## Recommendations

### Immediate Next Steps
1. **Add import tracking** to MalloyModelVisitor
2. **Create test fixtures** with multi-file imports
3. **Implement recursive parsing** with dependency resolution
4. **Test with real Malloy projects** (e.g., Malloy samples repo)

### Design Decisions Needed
1. **Import path resolution**:
   - Relative to current file? (Standard)
   - Support absolute paths?
   - Handle URLs? (Malloy spec allows this)

2. **Circular import handling**:
   - Error out? (Safest)
   - Skip already-parsed? (Current LookML approach)
   - Allow if no actual dependency cycle?

3. **Export behavior**:
   - Preserve original import structure?
   - Consolidate into single file?
   - Config flag to control?

4. **Alias scope**:
   - File-local only? (Cleanest)
   - Global across all files? (More complex)

### Testing Strategy
Create fixtures like:
```
tests/fixtures/malloy/multi-file/
  base.malloy          # Base sources
  customers.malloy     # Imports base, extends sources
  orders.malloy        # Imports customers with alias
```

---

## Code Quality Notes

### Strengths
- Clean separation of concerns (visitor pattern)
- Good type inference heuristics
- Comprehensive pick/when → CASE transformation
- Robust whitespace preservation
- Well-documented with docstrings

### Areas for Improvement
- No logging/debug output for troubleshooting
- Import path resolution will need careful testing
- Alias tracking adds complexity (consider using a class)
- Circular dependency detection needs explicit tests

---

## Summary

The Malloy adapter is **85% complete** for single-file use cases. Adding import support is the critical missing piece for production use. The architecture is solid and can accommodate imports with moderate refactoring (~200-300 lines). The existing inheritance system will handle cross-file extends with minimal changes.

**Estimated effort**: 4-6 hours for basic import support, 2-3 hours for comprehensive tests.
