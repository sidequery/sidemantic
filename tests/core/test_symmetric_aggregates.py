"""Tests for the symmetric aggregate module.

These tests cover the core symmetric aggregate functionality that prevents
fan-out issues (double-counting) in joins.
"""

import duckdb
import pytest

from sidemantic.core.symmetric_aggregate import (
    build_symmetric_aggregate_sql,
    needs_symmetric_aggregate,
)


class TestBuildSymmetricAggregateSql:
    """Tests for build_symmetric_aggregate_sql function."""

    # ==========================================================================
    # SUM aggregation tests
    # ==========================================================================

    def test_sum_basic(self):
        """Test basic SUM symmetric aggregate."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        # Should contain symmetric aggregate pattern
        assert "SUM(DISTINCT" in sql
        assert "HASH(order_id)" in sql
        assert "amount" in sql
        # DuckDB uses HUGEINT for large number support
        assert "HUGEINT" in sql

    def test_sum_with_model_alias(self):
        """Test SUM with table alias prefix."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            model_alias="orders_cte",
        )

        assert "HASH(orders_cte.order_id)" in sql
        assert "orders_cte.amount" in sql

    def test_sum_with_complex_measure(self):
        """Test SUM with complex measure expression."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="price * quantity",
            primary_key="line_id",
            agg_type="sum",
        )

        assert "HASH(line_id)" in sql
        assert "price * quantity" in sql

    # ==========================================================================
    # AVG aggregation tests
    # ==========================================================================

    def test_avg_basic(self):
        """Test basic AVG symmetric aggregate."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="avg",
        )

        # AVG is implemented as SUM / COUNT DISTINCT pk
        assert "SUM(DISTINCT" in sql
        assert "COUNT(DISTINCT order_id)" in sql
        assert "NULLIF" in sql  # Prevents division by zero
        assert "/" in sql

    def test_avg_with_model_alias(self):
        """Test AVG with table alias prefix."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="price",
            primary_key="product_id",
            agg_type="avg",
            model_alias="products_cte",
        )

        assert "HASH(products_cte.product_id)" in sql
        assert "products_cte.price" in sql
        assert "COUNT(DISTINCT products_cte.product_id)" in sql

    # ==========================================================================
    # COUNT aggregation tests
    # ==========================================================================

    def test_count_basic(self):
        """Test COUNT symmetric aggregate (counts distinct PKs)."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="*",
            primary_key="order_id",
            agg_type="count",
        )

        # COUNT uses COUNT DISTINCT on primary key
        assert sql == "COUNT(DISTINCT order_id)"

    def test_count_with_model_alias(self):
        """Test COUNT with table alias."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="*",
            primary_key="id",
            agg_type="count",
            model_alias="events_cte",
        )

        assert sql == "COUNT(DISTINCT events_cte.id)"

    # ==========================================================================
    # COUNT DISTINCT aggregation tests
    # ==========================================================================

    def test_count_distinct_basic(self):
        """Test COUNT DISTINCT on measure column."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="customer_id",
            primary_key="order_id",
            agg_type="count_distinct",
        )

        # COUNT DISTINCT doesn't need symmetric aggregates
        # because DISTINCT already handles uniqueness
        assert sql == "COUNT(DISTINCT customer_id)"

    def test_count_distinct_with_model_alias(self):
        """Test COUNT DISTINCT with table alias."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="user_id",
            primary_key="event_id",
            agg_type="count_distinct",
            model_alias="events_cte",
        )

        assert sql == "COUNT(DISTINCT events_cte.user_id)"

    # ==========================================================================
    # Error handling tests
    # ==========================================================================

    def test_unsupported_agg_type(self):
        """Test that unsupported aggregation types raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported aggregation type"):
            build_symmetric_aggregate_sql(
                measure_expr="amount",
                primary_key="order_id",
                agg_type="median",
            )

    def test_unsupported_agg_type_min(self):
        """Test that MIN raises ValueError (not supported in symmetric aggregates)."""
        with pytest.raises(ValueError, match="Unsupported aggregation type"):
            build_symmetric_aggregate_sql(
                measure_expr="amount",
                primary_key="order_id",
                agg_type="min",
            )

    def test_unsupported_agg_type_max(self):
        """Test that MAX raises ValueError (not supported in symmetric aggregates)."""
        with pytest.raises(ValueError, match="Unsupported aggregation type"):
            build_symmetric_aggregate_sql(
                measure_expr="amount",
                primary_key="order_id",
                agg_type="max",
            )

    # ==========================================================================
    # Dialect-specific tests
    # ==========================================================================

    def test_duckdb_dialect(self):
        """Test DuckDB-specific SQL generation."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="duckdb",
        )

        assert "HASH(order_id)::HUGEINT" in sql
        assert "(1::HUGEINT << 20)" in sql

    def test_bigquery_dialect(self):
        """Test BigQuery-specific SQL generation."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="bigquery",
        )

        assert "FARM_FINGERPRINT" in sql
        assert "CAST(order_id AS STRING)" in sql
        assert "1048576" in sql  # 2^20 literal

    def test_postgres_dialect(self):
        """Test PostgreSQL-specific SQL generation."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="postgres",
        )

        assert "hashtext(order_id::text)::bigint" in sql
        assert "1024" in sql  # Smaller multiplier to avoid overflow

    def test_postgresql_dialect_alias(self):
        """Test 'postgresql' dialect alias works same as 'postgres'."""
        sql_postgres = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="postgres",
        )
        sql_postgresql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="postgresql",
        )

        assert sql_postgres == sql_postgresql

    def test_snowflake_dialect(self):
        """Test Snowflake-specific SQL generation."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="snowflake",
        )

        assert "HASH(order_id)" in sql
        assert "% 1000000000" in sql  # Modulo constraint
        assert "100" in sql  # Small multiplier

    def test_clickhouse_dialect(self):
        """Test ClickHouse-specific SQL generation."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="clickhouse",
        )

        assert "halfMD5" in sql
        assert "CAST(order_id AS String)" in sql
        assert "1048576" in sql

    def test_databricks_dialect(self):
        """Test Databricks-specific SQL generation."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="databricks",
        )

        assert "xxhash64" in sql
        assert "CAST(order_id AS STRING)" in sql
        assert "1048576" in sql

    def test_spark_dialect(self):
        """Test Spark SQL-specific SQL generation."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
            dialect="spark",
        )

        # Spark uses same pattern as Databricks
        assert "xxhash64" in sql
        assert "CAST(order_id AS STRING)" in sql

    # ==========================================================================
    # AVG dialect tests
    # ==========================================================================

    def test_avg_bigquery_dialect(self):
        """Test AVG with BigQuery dialect."""
        sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="avg",
            dialect="bigquery",
        )

        assert "FARM_FINGERPRINT" in sql
        assert "COUNT(DISTINCT order_id)" in sql
        assert "NULLIF" in sql


class TestNeedsSymmetricAggregate:
    """Tests for needs_symmetric_aggregate function."""

    def test_one_to_many_base_model_needs_symmetric(self):
        """Test that one-to-many relationship on base model needs symmetric aggs."""
        result = needs_symmetric_aggregate(
            relationship="one_to_many",
            is_base_model=True,
        )
        assert result is True

    def test_one_to_many_non_base_model(self):
        """Test that one-to-many on non-base model doesn't need symmetric aggs."""
        result = needs_symmetric_aggregate(
            relationship="one_to_many",
            is_base_model=False,
        )
        assert result is False

    def test_many_to_one_base_model(self):
        """Test that many-to-one on base model doesn't need symmetric aggs."""
        result = needs_symmetric_aggregate(
            relationship="many_to_one",
            is_base_model=True,
        )
        assert result is False

    def test_many_to_one_non_base_model(self):
        """Test that many-to-one on non-base model doesn't need symmetric aggs."""
        result = needs_symmetric_aggregate(
            relationship="many_to_one",
            is_base_model=False,
        )
        assert result is False

    def test_one_to_one_base_model(self):
        """Test that one-to-one on base model doesn't need symmetric aggs."""
        result = needs_symmetric_aggregate(
            relationship="one_to_one",
            is_base_model=True,
        )
        assert result is False

    def test_one_to_one_non_base_model(self):
        """Test that one-to-one on non-base model doesn't need symmetric aggs."""
        result = needs_symmetric_aggregate(
            relationship="one_to_one",
            is_base_model=False,
        )
        assert result is False


class TestSymmetricAggregateExecution:
    """Integration tests that execute symmetric aggregate SQL against DuckDB."""

    @pytest.fixture
    def conn(self):
        """Create in-memory DuckDB connection with test data."""
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_sum_prevents_double_counting_basic(self, conn):
        """Test that symmetric SUM prevents double-counting in fan-out join."""
        # Setup: Orders joined to order_items (1-to-many)
        # Order 1 has $100, 2 items -> naive join would count $200
        # Order 2 has $50, 3 items -> naive join would count $150
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 100),
                (2, 50)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 2),
                (4, 2),
                (5, 2)
            ) AS t(item_id, order_id)
        """)

        # Join creates fan-out
        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        # Naive SUM would give 100*2 + 50*3 = 350
        naive_result = conn.execute("SELECT SUM(amount) FROM joined_data").fetchone()[0]
        assert naive_result == 350  # Wrong due to fan-out

        # Symmetric aggregate gives correct result
        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )
        correct_result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert correct_result == 150  # Correct: 100 + 50

    def test_count_prevents_double_counting(self, conn):
        """Test that symmetric COUNT prevents double-counting in fan-out join."""
        conn.execute("""
            CREATE TABLE customers AS
            SELECT * FROM (VALUES
                (1, 'Alice'),
                (2, 'Bob')
            ) AS t(customer_id, name)
        """)

        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 2)
            ) AS t(order_id, customer_id)
        """)

        # Join creates fan-out (Alice has 3 orders, Bob has 1)
        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT c.customer_id, c.name, o.order_id
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
        """)

        # Naive COUNT would give 4 (number of rows)
        naive_result = conn.execute("SELECT COUNT(*) FROM joined_data").fetchone()[0]
        assert naive_result == 4

        # Symmetric COUNT gives correct customer count
        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="*",
            primary_key="customer_id",
            agg_type="count",
        )
        correct_result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert correct_result == 2  # Correct: 2 customers

    def test_avg_correct_in_fanout(self, conn):
        """Test that symmetric AVG calculates correctly in fan-out join."""
        conn.execute("""
            CREATE TABLE products AS
            SELECT * FROM (VALUES
                (1, 10.0),
                (2, 20.0),
                (3, 30.0)
            ) AS t(product_id, price)
        """)

        conn.execute("""
            CREATE TABLE reviews AS
            SELECT * FROM (VALUES
                (1, 1, 5),
                (2, 1, 4),
                (3, 1, 3),
                (4, 2, 5),
                (5, 3, 2)
            ) AS t(review_id, product_id, rating)
        """)

        # Product 1 has 3 reviews, Product 2 has 1, Product 3 has 1
        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT p.product_id, p.price, r.review_id
            FROM products p
            JOIN reviews r ON p.product_id = r.product_id
        """)

        # Naive AVG: (10+10+10+20+30)/5 = 16
        naive_result = conn.execute("SELECT AVG(price) FROM joined_data").fetchone()[0]
        assert naive_result == 16.0  # Wrong

        # Symmetric AVG: (10+20+30)/3 = 20
        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="price",
            primary_key="product_id",
            agg_type="avg",
        )
        correct_result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert correct_result == 20.0  # Correct

    def test_count_distinct_unaffected_by_fanout(self, conn):
        """Test that COUNT DISTINCT works correctly (naturally handles uniqueness)."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 'A'),
                (2, 'A'),
                (3, 'B')
            ) AS t(order_id, category)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 2),
                (4, 3)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.category, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        # COUNT DISTINCT category should be 2 regardless of fan-out
        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="category",
            primary_key="order_id",
            agg_type="count_distinct",
        )
        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result == 2

    def test_grouped_symmetric_sum(self, conn):
        """Test symmetric SUM with GROUP BY dimension."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 'Jan', 100),
                (2, 'Jan', 200),
                (3, 'Feb', 150)
            ) AS t(order_id, month, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 2),
                (4, 3),
                (5, 3),
                (6, 3)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.month, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"""
            SELECT month, {symmetric_sql} as total
            FROM joined_data
            GROUP BY month
            ORDER BY month
        """).fetchall()

        # Feb: 150 (order 3)
        # Jan: 100 + 200 = 300 (orders 1 and 2)
        assert result == [("Feb", 150), ("Jan", 300)]

    def test_null_values_handled_with_coalesce(self, conn):
        """Test that NULL values are handled by wrapping measure with COALESCE.

        Note: The raw symmetric aggregate formula doesn't handle NULLs well because
        HASH(pk) + NULL = NULL, which breaks the deduplication pattern.
        The recommended approach is to use COALESCE(measure, 0) in the measure expression.
        """
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 100),
                (2, NULL),
                (3, 50)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 2),
                (4, 3)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        # Wrap measure with COALESCE to handle NULLs
        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="COALESCE(amount, 0)",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        # NULL treated as 0: 100 + 0 + 50 = 150
        assert result == 150

    def test_empty_result_set(self, conn):
        """Test symmetric aggregate on empty result set."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 100)
            ) AS t(order_id, amount)
            WHERE 1=0
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1)
            ) AS t(item_id, order_id)
            WHERE 1=0
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result is None

    def test_single_row_no_fanout(self, conn):
        """Test symmetric aggregate when there's no actual fan-out."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 100)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result == 100

    def test_multiple_fanout_levels(self, conn):
        """Test symmetric aggregate with multi-hop fan-out (orders -> items -> variants)."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 1000)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE TABLE item_variants AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 2),
                (4, 2),
                (5, 2)
            ) AS t(variant_id, item_id)
        """)

        # Double fan-out: 1 order * 2 items * (2+3) variants = 5 rows
        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id, v.variant_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
            JOIN item_variants v ON i.item_id = v.item_id
        """)

        # Naive: 1000 * 5 = 5000
        naive_result = conn.execute("SELECT SUM(amount) FROM joined_data").fetchone()[0]
        assert naive_result == 5000

        # Symmetric: 1000
        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )
        correct_result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert correct_result == 1000


class TestSymmetricAggregateEdgeCases:
    """Edge case tests for symmetric aggregates."""

    @pytest.fixture
    def conn(self):
        """Create in-memory DuckDB connection."""
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_large_values_no_overflow(self, conn):
        """Test that large values don't cause overflow."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 1000000000000),
                (2, 2000000000000)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 2)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result == 3000000000000

    def test_negative_values(self, conn):
        """Test that negative values are handled correctly."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, -100),
                (2, 50)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 2)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result == -50  # -100 + 50

    def test_decimal_values(self, conn):
        """Test that decimal values are handled correctly."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 99.99),
                (2, 0.01)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 1),
                (3, 2)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        # Convert to float for comparison (DuckDB may return Decimal)
        assert abs(float(result) - 100.0) < 0.01  # 99.99 + 0.01

    def test_string_primary_key(self, conn):
        """Test symmetric aggregate with string primary key."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                ('ORD-001', 100),
                ('ORD-002', 200)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 'ORD-001'),
                (2, 'ORD-001'),
                (3, 'ORD-002')
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result == 300

    def test_composite_key_workaround(self, conn):
        """Test workaround for composite keys using concatenation."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, 'A', 100),
                (1, 'B', 200),
                (2, 'A', 150)
            ) AS t(order_id, region, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1, 'A'),
                (2, 1, 'A'),
                (3, 1, 'B'),
                (4, 2, 'A')
            ) AS t(item_id, order_id, region)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT
                o.order_id || '-' || o.region as composite_key,
                o.amount,
                i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id AND o.region = i.region
        """)

        # Use composite key for deduplication
        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="composite_key",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result == 450  # 100 + 200 + 150

    def test_all_null_measures(self, conn):
        """Test when all measure values are NULL."""
        conn.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES
                (1, NULL),
                (2, NULL)
            ) AS t(order_id, amount)
        """)

        conn.execute("""
            CREATE TABLE order_items AS
            SELECT * FROM (VALUES
                (1, 1),
                (2, 2)
            ) AS t(item_id, order_id)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="sum",
        )

        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result is None

    def test_avg_with_zero_count(self, conn):
        """Test AVG returns NULL when count is zero (empty set)."""
        conn.execute("""
            CREATE TABLE orders (order_id INT, amount FLOAT)
        """)
        conn.execute("""
            CREATE TABLE order_items (item_id INT, order_id INT)
        """)

        conn.execute("""
            CREATE VIEW joined_data AS
            SELECT o.order_id, o.amount, i.item_id
            FROM orders o
            JOIN order_items i ON o.order_id = i.order_id
        """)

        symmetric_sql = build_symmetric_aggregate_sql(
            measure_expr="amount",
            primary_key="order_id",
            agg_type="avg",
        )

        # NULLIF prevents division by zero, returns NULL
        result = conn.execute(f"SELECT {symmetric_sql} FROM joined_data").fetchone()[0]
        assert result is None
