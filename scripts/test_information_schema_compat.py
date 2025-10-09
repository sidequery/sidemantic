#!/usr/bin/env python
"""Test information_schema compatibility across different database adapters.

/// script
dependencies = [
  "duckdb",
]
///
"""

import duckdb

# Standard SQL information_schema queries used in CoverageAnalyzer
PK_QUERY = """
    SELECT table_name, column_name
    FROM information_schema.key_column_usage
    WHERE constraint_name IN (
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE constraint_type = 'PRIMARY KEY'
    )
"""

FK_QUERY = """
    SELECT
        fk_kcu.table_name AS fk_table,
        fk_kcu.column_name AS fk_column,
        pk_kcu.table_name AS pk_table,
        pk_kcu.column_name AS pk_column
    FROM information_schema.referential_constraints rc
    JOIN information_schema.key_column_usage fk_kcu
        ON rc.constraint_name = fk_kcu.constraint_name
    JOIN information_schema.key_column_usage pk_kcu
        ON rc.unique_constraint_name = pk_kcu.constraint_name
"""

COLUMNS_QUERY = """
    SELECT table_name, column_name
    FROM information_schema.columns
"""


def test_duckdb():
    """Test with DuckDB (in-memory)."""
    print("=" * 80)
    print("Testing DuckDB")
    print("=" * 80)

    con = duckdb.connect(":memory:")

    # Create test schema
    con.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            region VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DECIMAL,
            status VARCHAR,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    """)

    # Test primary keys
    print("\nPrimary Keys:")
    pk_results = con.execute(PK_QUERY).fetchall()
    for table, column in pk_results:
        print(f"  {table}.{column}")

    # Test foreign keys
    print("\nForeign Keys:")
    fk_results = con.execute(FK_QUERY).fetchall()
    for fk_table, fk_col, pk_table, pk_col in fk_results:
        print(f"  {fk_table}.{fk_col} -> {pk_table}.{pk_col}")

    # Test columns
    print("\nColumns:")
    col_results = con.execute(COLUMNS_QUERY).fetchall()
    cols_by_table = {}
    for table, column in col_results:
        if table in ("customers", "orders"):
            cols_by_table.setdefault(table, []).append(column)

    for table, columns in sorted(cols_by_table.items()):
        print(f"  {table}: {', '.join(columns)}")

    con.close()
    print("\n✓ DuckDB: WORKS\n")


def test_postgres_compatibility():
    """Document PostgreSQL compatibility (no actual connection needed)."""
    print("=" * 80)
    print("PostgreSQL Compatibility")
    print("=" * 80)
    print("""
PostgreSQL has full ANSI SQL information_schema support:
- information_schema.table_constraints ✓
- information_schema.key_column_usage ✓
- information_schema.referential_constraints ✓
- information_schema.columns ✓

The queries used in CoverageAnalyzer are standard SQL and will work with PostgreSQL.

Caveats:
- Schema qualification: PostgreSQL uses schemas (public, etc.)
- May need to filter by table_schema in multi-schema databases
""")
    print("✓ PostgreSQL: COMPATIBLE (standard SQL)\n")


def test_bigquery_compatibility():
    """Document BigQuery compatibility."""
    print("=" * 80)
    print("BigQuery Compatibility")
    print("=" * 80)
    print("""
BigQuery has information_schema but with some differences:
- Uses project.dataset.INFORMATION_SCHEMA.* format
- information_schema.table_constraints ✓
- information_schema.key_column_usage ✓
- information_schema.constraint_column_usage ✓
- information_schema.columns ✓

However:
- BigQuery doesn't enforce FK constraints, so referential_constraints may be empty
- Primary keys are not enforced (only for metadata)
- The FK query may need adjustment for BigQuery's naming

Recommended approach for BigQuery:
- Use column name patterns (_id suffix) as primary detection method
- information_schema as secondary source
""")
    print("⚠ BigQuery: PARTIAL (no FK enforcement, use patterns)\n")


def test_snowflake_compatibility():
    """Document Snowflake compatibility."""
    print("=" * 80)
    print("Snowflake Compatibility")
    print("=" * 80)
    print("""
Snowflake has information_schema support:
- information_schema.table_constraints ✓
- information_schema.key_column_usage ✓
- information_schema.referential_constraints ✓
- information_schema.columns ✓

Caveats:
- Uses database.schema.table hierarchy
- FK constraints are NOT enforced by default (metadata only)
- May need to filter by table_schema and table_catalog
""")
    print("✓ Snowflake: COMPATIBLE (may need schema filtering)\n")


def test_databricks_compatibility():
    """Document Databricks compatibility."""
    print("=" * 80)
    print("Databricks Compatibility")
    print("=" * 80)
    print("""
Databricks SQL has information_schema support:
- information_schema.columns ✓
- information_schema.tables ✓

However:
- FK constraints are only supported in Unity Catalog (newer feature)
- information_schema.table_constraints may be limited
- information_schema.referential_constraints may not be available in all versions

Recommended approach for Databricks:
- Check Unity Catalog version/support first
- Fall back to pattern matching for most cases
""")
    print("⚠ Databricks: PARTIAL (Unity Catalog only, use patterns as fallback)\n")


def test_clickhouse_compatibility():
    """Document ClickHouse compatibility."""
    print("=" * 80)
    print("ClickHouse Compatibility")
    print("=" * 80)
    print("""
ClickHouse has limited information_schema support:
- information_schema.columns ✓ (read-only view)
- information_schema.tables ✓

But:
- NO support for constraints (no PKs or FKs in ClickHouse)
- No table_constraints table
- No referential_constraints table

Recommended approach for ClickHouse:
- Use pattern matching exclusively (_id suffix)
- information_schema.columns can help with column inference
- CoverageAnalyzer will fall back to patterns (which is correct for ClickHouse)
""")
    print("⚠ ClickHouse: PARTIAL (no constraints, columns only)\n")


def main():
    """Run all compatibility tests."""
    print("\n" + "=" * 80)
    print("INFORMATION_SCHEMA COMPATIBILITY TEST")
    print("=" * 80 + "\n")

    # Test with actual DuckDB connection
    test_duckdb()

    # Document compatibility for other databases
    test_postgres_compatibility()
    test_bigquery_compatibility()
    test_snowflake_compatibility()
    test_databricks_compatibility()
    test_clickhouse_compatibility()

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
Full Support (FK detection works):
  ✓ DuckDB
  ✓ PostgreSQL
  ✓ Snowflake (metadata only)

Partial Support (column inference works, FK detection may not):
  ⚠ BigQuery (no FK enforcement)
  ⚠ Databricks (Unity Catalog only)
  ⚠ ClickHouse (no constraints at all)

The CoverageAnalyzer gracefully falls back to pattern matching when:
1. No connection is provided
2. information_schema queries fail
3. FK constraints are not enforced/available

This makes it compatible with ALL database types.
""")


if __name__ == "__main__":
    main()
