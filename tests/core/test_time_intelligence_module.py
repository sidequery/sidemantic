"""Comprehensive tests for time intelligence module.

Tests cover:
- TimeComparison model validation and properties
- TrailingPeriod model validation and properties
- generate_time_comparison_sql function for all calculation types
- Edge cases and error handling
"""

import pytest
from pydantic import ValidationError

from sidemantic.core.time_intelligence import (
    TimeComparison,
    TrailingPeriod,
    generate_time_comparison_sql,
)


class TestTimeComparisonModel:
    """Tests for the TimeComparison Pydantic model."""

    # =========================================================================
    # Basic construction tests
    # =========================================================================

    def test_basic_yoy_comparison(self):
        """Test creating a basic year-over-year comparison."""
        comparison = TimeComparison(type="yoy", metric="revenue")
        assert comparison.type == "yoy"
        assert comparison.metric == "revenue"
        assert comparison.calculation == "percent_change"  # default
        assert comparison.offset is None
        assert comparison.offset_unit is None

    def test_all_comparison_types(self):
        """Test all supported comparison types can be created."""
        types = ["yoy", "mom", "wow", "dod", "qoq", "prior_period"]
        for comp_type in types:
            comparison = TimeComparison(type=comp_type, metric="test_metric")
            assert comparison.type == comp_type

    def test_all_calculation_types(self):
        """Test all supported calculation types."""
        calculations = ["difference", "percent_change", "ratio"]
        for calc in calculations:
            comparison = TimeComparison(type="yoy", metric="revenue", calculation=calc)
            assert comparison.calculation == calc

    # =========================================================================
    # Offset interval tests
    # =========================================================================

    def test_dod_offset_interval(self):
        """Test day-over-day offset interval."""
        comparison = TimeComparison(type="dod", metric="orders")
        assert comparison.offset_interval == (1, "day")

    def test_wow_offset_interval(self):
        """Test week-over-week offset interval."""
        comparison = TimeComparison(type="wow", metric="orders")
        assert comparison.offset_interval == (1, "week")

    def test_mom_offset_interval(self):
        """Test month-over-month offset interval."""
        comparison = TimeComparison(type="mom", metric="revenue")
        assert comparison.offset_interval == (1, "month")

    def test_qoq_offset_interval(self):
        """Test quarter-over-quarter offset interval."""
        comparison = TimeComparison(type="qoq", metric="revenue")
        assert comparison.offset_interval == (1, "quarter")

    def test_yoy_offset_interval(self):
        """Test year-over-year offset interval."""
        comparison = TimeComparison(type="yoy", metric="revenue")
        assert comparison.offset_interval == (1, "year")

    def test_prior_period_default_offset(self):
        """Test prior_period default offset is 1 day."""
        comparison = TimeComparison(type="prior_period", metric="revenue")
        assert comparison.offset_interval == (1, "day")

    # =========================================================================
    # Custom offset tests
    # =========================================================================

    def test_custom_offset_days(self):
        """Test custom offset in days."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=7, offset_unit="day")
        assert comparison.offset_interval == (7, "day")

    def test_custom_offset_weeks(self):
        """Test custom offset in weeks."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=4, offset_unit="week")
        assert comparison.offset_interval == (4, "week")

    def test_custom_offset_months(self):
        """Test custom offset in months."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=3, offset_unit="month")
        assert comparison.offset_interval == (3, "month")

    def test_custom_offset_quarters(self):
        """Test custom offset in quarters."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=2, offset_unit="quarter")
        assert comparison.offset_interval == (2, "quarter")

    def test_custom_offset_years(self):
        """Test custom offset in years."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=2, offset_unit="year")
        assert comparison.offset_interval == (2, "year")

    def test_custom_offset_overrides_type_default(self):
        """Test that custom offset overrides the default for any type."""
        # Using yoy type but with custom 2-year offset
        comparison = TimeComparison(type="yoy", metric="revenue", offset=2, offset_unit="year")
        assert comparison.offset_interval == (2, "year")

    def test_custom_offset_large_value(self):
        """Test large custom offset values."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=365, offset_unit="day")
        assert comparison.offset_interval == (365, "day")

    # =========================================================================
    # SQL offset generation tests
    # =========================================================================

    def test_get_sql_offset_yoy(self):
        """Test SQL INTERVAL for year-over-year."""
        comparison = TimeComparison(type="yoy", metric="revenue")
        assert comparison.get_sql_offset() == "INTERVAL '1 year'"

    def test_get_sql_offset_mom(self):
        """Test SQL INTERVAL for month-over-month."""
        comparison = TimeComparison(type="mom", metric="revenue")
        assert comparison.get_sql_offset() == "INTERVAL '1 month'"

    def test_get_sql_offset_wow(self):
        """Test SQL INTERVAL for week-over-week."""
        comparison = TimeComparison(type="wow", metric="orders")
        assert comparison.get_sql_offset() == "INTERVAL '1 week'"

    def test_get_sql_offset_dod(self):
        """Test SQL INTERVAL for day-over-day."""
        comparison = TimeComparison(type="dod", metric="orders")
        assert comparison.get_sql_offset() == "INTERVAL '1 day'"

    def test_get_sql_offset_qoq(self):
        """Test SQL INTERVAL for quarter-over-quarter."""
        comparison = TimeComparison(type="qoq", metric="revenue")
        assert comparison.get_sql_offset() == "INTERVAL '1 quarter'"

    def test_get_sql_offset_custom(self):
        """Test SQL INTERVAL for custom offset."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=7, offset_unit="day")
        assert comparison.get_sql_offset() == "INTERVAL '7 day'"

    def test_get_sql_offset_custom_weeks(self):
        """Test SQL INTERVAL for custom offset in weeks."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=4, offset_unit="week")
        assert comparison.get_sql_offset() == "INTERVAL '4 week'"

    # =========================================================================
    # Validation tests
    # =========================================================================

    def test_invalid_comparison_type(self):
        """Test that invalid comparison type raises ValidationError."""
        with pytest.raises(ValidationError):
            TimeComparison(type="invalid_type", metric="revenue")

    def test_invalid_calculation_type(self):
        """Test that invalid calculation type raises ValidationError."""
        with pytest.raises(ValidationError):
            TimeComparison(type="yoy", metric="revenue", calculation="invalid")

    def test_invalid_offset_unit(self):
        """Test that invalid offset unit raises ValidationError."""
        with pytest.raises(ValidationError):
            TimeComparison(type="prior_period", metric="revenue", offset=7, offset_unit="invalid")

    def test_missing_metric(self):
        """Test that missing metric field raises ValidationError."""
        with pytest.raises(ValidationError):
            TimeComparison(type="yoy")

    def test_missing_type(self):
        """Test that missing type field raises ValidationError."""
        with pytest.raises(ValidationError):
            TimeComparison(metric="revenue")

    # =========================================================================
    # Edge cases
    # =========================================================================

    def test_zero_offset_raises_validation_error(self):
        """Test that zero offset raises ValidationError.

        Zero offset would mean comparing a period to itself, which doesn't
        make practical sense for time comparisons. Users should get an
        explicit error rather than having their input silently changed.
        """
        with pytest.raises(ValidationError, match="offset cannot be 0"):
            TimeComparison(type="prior_period", metric="revenue", offset=0, offset_unit="day")

    def test_offset_one_works_correctly(self):
        """Test that offset=1 works correctly (the common case)."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=1, offset_unit="day")
        assert comparison.offset == 1
        assert comparison.offset_unit == "day"
        assert comparison.offset_interval == (1, "day")
        assert comparison.get_sql_offset() == "INTERVAL '1 day'"

    def test_negative_offset(self):
        """Test negative offset value (future comparison)."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=-1, offset_unit="month")
        assert comparison.offset_interval == (-1, "month")
        assert comparison.get_sql_offset() == "INTERVAL '-1 month'"


class TestTrailingPeriod:
    """Tests for the TrailingPeriod Pydantic model."""

    # =========================================================================
    # Basic construction tests
    # =========================================================================

    def test_trailing_7_days(self):
        """Test trailing 7 days configuration."""
        trailing = TrailingPeriod(amount=7, unit="day")
        assert trailing.amount == 7
        assert trailing.unit == "day"

    def test_trailing_30_days(self):
        """Test trailing 30 days configuration."""
        trailing = TrailingPeriod(amount=30, unit="day")
        assert trailing.amount == 30
        assert trailing.unit == "day"

    def test_trailing_90_days(self):
        """Test trailing 90 days configuration."""
        trailing = TrailingPeriod(amount=90, unit="day")
        assert trailing.amount == 90
        assert trailing.unit == "day"

    def test_trailing_weeks(self):
        """Test trailing weeks configuration."""
        trailing = TrailingPeriod(amount=4, unit="week")
        assert trailing.amount == 4
        assert trailing.unit == "week"

    def test_trailing_months(self):
        """Test trailing months configuration."""
        trailing = TrailingPeriod(amount=3, unit="month")
        assert trailing.amount == 3
        assert trailing.unit == "month"

    def test_trailing_quarters(self):
        """Test trailing quarters configuration."""
        trailing = TrailingPeriod(amount=4, unit="quarter")
        assert trailing.amount == 4
        assert trailing.unit == "quarter"

    def test_trailing_years(self):
        """Test trailing years configuration."""
        trailing = TrailingPeriod(amount=2, unit="year")
        assert trailing.amount == 2
        assert trailing.unit == "year"

    # =========================================================================
    # SQL interval generation tests
    # =========================================================================

    def test_get_sql_interval_days(self):
        """Test SQL INTERVAL generation for days."""
        trailing = TrailingPeriod(amount=7, unit="day")
        assert trailing.get_sql_interval() == "INTERVAL '7 day'"

    def test_get_sql_interval_weeks(self):
        """Test SQL INTERVAL generation for weeks."""
        trailing = TrailingPeriod(amount=2, unit="week")
        assert trailing.get_sql_interval() == "INTERVAL '2 week'"

    def test_get_sql_interval_months(self):
        """Test SQL INTERVAL generation for months."""
        trailing = TrailingPeriod(amount=3, unit="month")
        assert trailing.get_sql_interval() == "INTERVAL '3 month'"

    def test_get_sql_interval_quarters(self):
        """Test SQL INTERVAL generation for quarters."""
        trailing = TrailingPeriod(amount=1, unit="quarter")
        assert trailing.get_sql_interval() == "INTERVAL '1 quarter'"

    def test_get_sql_interval_years(self):
        """Test SQL INTERVAL generation for years."""
        trailing = TrailingPeriod(amount=1, unit="year")
        assert trailing.get_sql_interval() == "INTERVAL '1 year'"

    def test_get_sql_interval_large_value(self):
        """Test SQL INTERVAL generation for large values."""
        trailing = TrailingPeriod(amount=365, unit="day")
        assert trailing.get_sql_interval() == "INTERVAL '365 day'"

    # =========================================================================
    # Validation tests
    # =========================================================================

    def test_invalid_unit(self):
        """Test that invalid unit raises ValidationError."""
        with pytest.raises(ValidationError):
            TrailingPeriod(amount=7, unit="invalid")

    def test_missing_amount(self):
        """Test that missing amount raises ValidationError."""
        with pytest.raises(ValidationError):
            TrailingPeriod(unit="day")

    def test_missing_unit(self):
        """Test that missing unit raises ValidationError."""
        with pytest.raises(ValidationError):
            TrailingPeriod(amount=7)

    # =========================================================================
    # Edge cases
    # =========================================================================

    def test_zero_amount(self):
        """Test zero amount (current period only)."""
        trailing = TrailingPeriod(amount=0, unit="day")
        assert trailing.amount == 0
        assert trailing.get_sql_interval() == "INTERVAL '0 day'"

    def test_one_day_trailing(self):
        """Test single day trailing period."""
        trailing = TrailingPeriod(amount=1, unit="day")
        assert trailing.get_sql_interval() == "INTERVAL '1 day'"


class TestGenerateTimeComparisonSQL:
    """Tests for the generate_time_comparison_sql function."""

    # =========================================================================
    # Calculation type tests
    # =========================================================================

    def test_difference_calculation(self):
        """Test difference calculation generates correct SQL."""
        comparison = TimeComparison(type="mom", metric="revenue", calculation="difference")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_date")

        assert "LAG(" in sql
        assert "SUM(amount)" in sql
        assert "ORDER BY order_date" in sql
        # Difference: current - prior
        assert "-" in sql
        # Should NOT have percent change pattern
        assert "* 100" not in sql

    def test_percent_change_calculation(self):
        """Test percent change calculation generates correct SQL."""
        comparison = TimeComparison(type="mom", metric="revenue", calculation="percent_change")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_date")

        assert "LAG(" in sql
        assert "SUM(amount)" in sql
        # Percent change: (current - prior) / prior * 100
        assert "NULLIF" in sql  # Divide by zero protection
        assert "* 100" in sql

    def test_ratio_calculation(self):
        """Test ratio calculation generates correct SQL."""
        comparison = TimeComparison(type="mom", metric="revenue", calculation="ratio")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_date")

        assert "LAG(" in sql
        assert "SUM(amount)" in sql
        # Ratio: current / prior
        assert "NULLIF" in sql  # Divide by zero protection
        # Should NOT have percent change pattern
        assert "* 100" not in sql

    # =========================================================================
    # SQL structure tests
    # =========================================================================

    def test_lag_window_function_structure(self):
        """Test that LAG window function is properly structured."""
        comparison = TimeComparison(type="yoy", metric="revenue", calculation="difference")
        sql = generate_time_comparison_sql(comparison, "SUM(revenue)", "date_col")

        # Verify LAG is used with correct structure
        assert "LAG(SUM(revenue))" in sql
        assert "OVER" in sql
        assert "ORDER BY date_col" in sql

    def test_nullif_protection(self):
        """Test NULLIF is used to protect against division by zero."""
        comparison = TimeComparison(type="yoy", metric="revenue", calculation="percent_change")
        sql = generate_time_comparison_sql(comparison, "SUM(revenue)", "date_col")

        # NULLIF should wrap the divisor with 0 check
        assert "NULLIF(" in sql
        assert ", 0)" in sql

    # =========================================================================
    # Different comparison types
    # =========================================================================

    def test_yoy_comparison_sql(self):
        """Test year-over-year comparison SQL generation."""
        comparison = TimeComparison(type="yoy", metric="revenue")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "year")
        assert "LAG(" in sql
        assert "ORDER BY year" in sql

    def test_mom_comparison_sql(self):
        """Test month-over-month comparison SQL generation."""
        comparison = TimeComparison(type="mom", metric="revenue")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "month")
        assert "LAG(" in sql
        assert "ORDER BY month" in sql

    def test_wow_comparison_sql(self):
        """Test week-over-week comparison SQL generation."""
        comparison = TimeComparison(type="wow", metric="orders")
        sql = generate_time_comparison_sql(comparison, "COUNT(*)", "week")
        assert "LAG(" in sql
        assert "ORDER BY week" in sql

    def test_dod_comparison_sql(self):
        """Test day-over-day comparison SQL generation."""
        comparison = TimeComparison(type="dod", metric="orders")
        sql = generate_time_comparison_sql(comparison, "COUNT(*)", "date")
        assert "LAG(" in sql
        assert "ORDER BY date" in sql

    def test_qoq_comparison_sql(self):
        """Test quarter-over-quarter comparison SQL generation."""
        comparison = TimeComparison(type="qoq", metric="revenue")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "quarter")
        assert "LAG(" in sql
        assert "ORDER BY quarter" in sql

    # =========================================================================
    # Edge cases and error handling
    # =========================================================================

    def test_invalid_calculation_raises_error(self):
        """Test that invalid calculation type raises ValueError."""
        comparison = TimeComparison(type="mom", metric="revenue", calculation="difference")
        # Bypass pydantic validation by directly setting attribute
        comparison.calculation = "invalid_calc"  # type: ignore[assignment]

        with pytest.raises(ValueError, match="Unknown calculation type"):
            generate_time_comparison_sql(comparison, "SUM(amount)", "order_date")

    def test_complex_metric_sql(self):
        """Test with complex metric SQL expression."""
        comparison = TimeComparison(type="yoy", metric="aov", calculation="percent_change")
        complex_metric = "SUM(amount) / NULLIF(COUNT(DISTINCT order_id), 0)"
        sql = generate_time_comparison_sql(comparison, complex_metric, "date")

        # The complex metric should appear in the output
        assert complex_metric in sql
        assert "LAG(" + complex_metric in sql

    def test_qualified_time_dimension(self):
        """Test with fully qualified time dimension name."""
        comparison = TimeComparison(type="mom", metric="revenue")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "orders.order_date")
        assert "ORDER BY orders.order_date" in sql

    def test_with_custom_offset(self):
        """Test SQL generation with custom offset."""
        comparison = TimeComparison(
            type="prior_period",
            metric="revenue",
            offset=7,
            offset_unit="day",
            calculation="difference",
        )
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "date")

        # Should still use LAG window function
        assert "LAG(" in sql
        assert "SUM(amount)" in sql


class TestTimeOffsetUnits:
    """Tests for all valid TimeOffsetUnit values."""

    @pytest.mark.parametrize(
        "unit",
        ["day", "week", "month", "quarter", "year"],
    )
    def test_all_offset_units_valid(self, unit):
        """Test all valid offset units can be used."""
        comparison = TimeComparison(type="prior_period", metric="revenue", offset=1, offset_unit=unit)
        assert comparison.offset_unit == unit

    @pytest.mark.parametrize(
        "unit",
        ["day", "week", "month", "quarter", "year"],
    )
    def test_trailing_period_all_units(self, unit):
        """Test TrailingPeriod with all valid units."""
        trailing = TrailingPeriod(amount=1, unit=unit)
        assert trailing.unit == unit
        assert f"INTERVAL '1 {unit}'" == trailing.get_sql_interval()


class TestTimeComparisonTypes:
    """Tests for all valid TimeComparisonType values."""

    @pytest.mark.parametrize(
        "comparison_type,expected_offset",
        [
            ("yoy", (1, "year")),
            ("mom", (1, "month")),
            ("wow", (1, "week")),
            ("dod", (1, "day")),
            ("qoq", (1, "quarter")),
            ("prior_period", (1, "day")),
        ],
    )
    def test_all_comparison_types_with_expected_offsets(self, comparison_type, expected_offset):
        """Test all comparison types return expected default offsets."""
        comparison = TimeComparison(type=comparison_type, metric="revenue")
        assert comparison.offset_interval == expected_offset


class TestSQLDialectCompatibility:
    """Tests for SQL dialect compatibility considerations.

    Note: The time_intelligence module generates standard SQL that should work
    across most SQL dialects. These tests verify the SQL patterns are dialect-agnostic.
    """

    def test_interval_format_standard(self):
        """Test INTERVAL format uses standard SQL syntax."""
        comparison = TimeComparison(type="mom", metric="revenue")
        sql_offset = comparison.get_sql_offset()
        # Standard SQL INTERVAL format: INTERVAL 'N unit'
        assert sql_offset.startswith("INTERVAL '")
        assert sql_offset.endswith("'")

    def test_lag_window_function_standard(self):
        """Test LAG window function uses standard SQL syntax."""
        comparison = TimeComparison(type="yoy", metric="revenue", calculation="difference")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "date")
        # LAG is a standard SQL window function
        assert "LAG(" in sql
        assert "OVER (" in sql
        assert "ORDER BY" in sql

    def test_nullif_standard(self):
        """Test NULLIF uses standard SQL syntax."""
        comparison = TimeComparison(type="yoy", metric="revenue", calculation="percent_change")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "date")
        # NULLIF is a standard SQL function
        assert "NULLIF(" in sql


class TestEdgeCasesDateBoundaries:
    """Tests for date boundary edge cases.

    Note: Actual date arithmetic (leap years, month boundaries) is handled by
    the SQL engine. These tests verify the SQL expressions are correctly formed.
    """

    def test_february_month_boundary(self):
        """Test MoM comparison SQL is valid for February scenarios."""
        comparison = TimeComparison(type="mom", metric="revenue")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_month")
        # SQL is valid - actual date handling is done by database
        assert "LAG(" in sql

    def test_year_boundary(self):
        """Test YoY comparison SQL is valid for year boundary scenarios."""
        comparison = TimeComparison(type="yoy", metric="revenue")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_year")
        # SQL is valid - actual date handling is done by database
        assert "LAG(" in sql

    def test_quarter_boundary(self):
        """Test QoQ comparison SQL is valid for quarter boundary scenarios."""
        comparison = TimeComparison(type="qoq", metric="revenue")
        sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_quarter")
        # SQL is valid - actual date handling is done by database
        assert "LAG(" in sql


class TestTrailingPeriodCommonPatterns:
    """Tests for common trailing period patterns."""

    def test_trailing_7_days_pattern(self):
        """Test trailing 7 days (1 week) pattern."""
        trailing = TrailingPeriod(amount=7, unit="day")
        assert trailing.get_sql_interval() == "INTERVAL '7 day'"

    def test_trailing_30_days_pattern(self):
        """Test trailing 30 days (approx 1 month) pattern."""
        trailing = TrailingPeriod(amount=30, unit="day")
        assert trailing.get_sql_interval() == "INTERVAL '30 day'"

    def test_trailing_90_days_pattern(self):
        """Test trailing 90 days (approx 1 quarter) pattern."""
        trailing = TrailingPeriod(amount=90, unit="day")
        assert trailing.get_sql_interval() == "INTERVAL '90 day'"

    def test_trailing_365_days_pattern(self):
        """Test trailing 365 days (approx 1 year) pattern."""
        trailing = TrailingPeriod(amount=365, unit="day")
        assert trailing.get_sql_interval() == "INTERVAL '365 day'"

    def test_mtd_equivalent(self):
        """Test month-to-date equivalent (trailing 1 month)."""
        trailing = TrailingPeriod(amount=1, unit="month")
        assert trailing.get_sql_interval() == "INTERVAL '1 month'"

    def test_qtd_equivalent(self):
        """Test quarter-to-date equivalent (trailing 1 quarter)."""
        trailing = TrailingPeriod(amount=1, unit="quarter")
        assert trailing.get_sql_interval() == "INTERVAL '1 quarter'"

    def test_ytd_equivalent(self):
        """Test year-to-date equivalent (trailing 1 year)."""
        trailing = TrailingPeriod(amount=1, unit="year")
        assert trailing.get_sql_interval() == "INTERVAL '1 year'"


class TestTimeIntelligenceExecution:
    """Integration tests that execute time intelligence SQL against DuckDB."""

    @pytest.fixture
    def time_series_db(self):
        """Create DuckDB with time-series test data.

        Data includes:
        - 2023 and 2024 data for YoY testing
        - Multiple months for MoM testing
        - Multiple weeks for WoW testing
        - Daily granularity available
        """
        import duckdb

        conn = duckdb.connect(":memory:")

        # Create daily sales table with 2 years of data
        conn.execute("""
            CREATE TABLE daily_sales (
                sale_date DATE,
                revenue DECIMAL(10,2),
                orders INTEGER
            )
        """)

        # Insert specific data points for predictable test assertions
        # 2023 data (prior year)
        conn.execute("""
            INSERT INTO daily_sales VALUES
                -- January 2023
                ('2023-01-01', 1000.00, 10),
                ('2023-01-15', 1500.00, 15),
                ('2023-01-31', 1200.00, 12),
                -- February 2023
                ('2023-02-01', 800.00, 8),
                ('2023-02-15', 900.00, 9),
                ('2023-02-28', 850.00, 8),
                -- March 2023
                ('2023-03-01', 1100.00, 11),
                ('2023-03-15', 1300.00, 13),
                -- Q2 2023
                ('2023-04-01', 950.00, 9),
                ('2023-05-01', 1050.00, 10),
                ('2023-06-01', 1150.00, 11),
                -- Q3-Q4 2023 (for full year coverage)
                ('2023-07-01', 1200.00, 12),
                ('2023-08-01', 1250.00, 12),
                ('2023-09-01', 1100.00, 11),
                ('2023-10-01', 1300.00, 13),
                ('2023-11-01', 1400.00, 14),
                ('2023-12-01', 1600.00, 16),
                -- 2024 data (current year for YoY comparison)
                -- January 2024 (compare to January 2023)
                ('2024-01-01', 1200.00, 12),
                ('2024-01-15', 1800.00, 18),
                ('2024-01-31', 1400.00, 14),
                -- February 2024
                ('2024-02-01', 1000.00, 10),
                ('2024-02-15', 1100.00, 11),
                ('2024-02-29', 1050.00, 10),
                -- March 2024
                ('2024-03-01', 1400.00, 14),
                ('2024-03-15', 1600.00, 16)
        """)

        yield conn
        conn.close()

    @pytest.fixture
    def monthly_summary_db(self):
        """Create DuckDB with pre-aggregated monthly data for simpler YoY/MoM tests."""
        import duckdb

        conn = duckdb.connect(":memory:")

        conn.execute("""
            CREATE TABLE monthly_sales (
                month DATE,
                revenue DECIMAL(10,2),
                orders INTEGER
            )
        """)

        # Monthly data with specific values for predictable assertions
        conn.execute("""
            INSERT INTO monthly_sales VALUES
                -- 2023
                ('2023-01-01', 3700.00, 37),
                ('2023-02-01', 2550.00, 25),
                ('2023-03-01', 2400.00, 24),
                ('2023-04-01', 950.00, 9),
                ('2023-05-01', 1050.00, 10),
                ('2023-06-01', 1150.00, 11),
                ('2023-07-01', 1200.00, 12),
                ('2023-08-01', 1250.00, 12),
                ('2023-09-01', 1100.00, 11),
                ('2023-10-01', 1300.00, 13),
                ('2023-11-01', 1400.00, 14),
                ('2023-12-01', 1600.00, 16),
                -- 2024
                ('2024-01-01', 4400.00, 44),
                ('2024-02-01', 3150.00, 31),
                ('2024-03-01', 3000.00, 30)
        """)

        yield conn
        conn.close()

    @pytest.fixture
    def weekly_db(self):
        """Create DuckDB with weekly data for WoW tests."""
        import duckdb

        conn = duckdb.connect(":memory:")

        conn.execute("""
            CREATE TABLE weekly_sales (
                week_start DATE,
                revenue DECIMAL(10,2),
                orders INTEGER
            )
        """)

        # 8 weeks of data for WoW testing
        conn.execute("""
            INSERT INTO weekly_sales VALUES
                ('2024-01-01', 1000.00, 10),
                ('2024-01-08', 1100.00, 11),
                ('2024-01-15', 1050.00, 10),
                ('2024-01-22', 1200.00, 12),
                ('2024-01-29', 1150.00, 11),
                ('2024-02-05', 1300.00, 13),
                ('2024-02-12', 1250.00, 12),
                ('2024-02-19', 1400.00, 14)
        """)

        yield conn
        conn.close()

    # =========================================================================
    # Year-over-Year (YoY) Tests
    # =========================================================================

    def test_yoy_lag_returns_prior_year_value(self, monthly_summary_db):
        """Test YoY LAG returns the correct prior year value."""
        conn = monthly_summary_db

        # Build SQL manually using the LAG pattern from generate_time_comparison_sql
        # LAG offset of 12 for monthly data gives YoY
        sql = """
            SELECT
                month,
                revenue,
                LAG(revenue, 12) OVER (ORDER BY month) AS prior_year_revenue
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # 2024-01-01 should have prior year value from 2023-01-01
        jan_2024 = [r for r in result if str(r[0]) == "2024-01-01"][0]
        assert jan_2024[1] == 4400.00  # Current revenue
        assert jan_2024[2] == 3700.00  # Prior year revenue (2023-01)

        # 2024-02-01 should have prior year value from 2023-02-01
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]
        assert feb_2024[1] == 3150.00  # Current revenue
        assert feb_2024[2] == 2550.00  # Prior year revenue (2023-02)

    def test_yoy_percent_change_calculation(self, monthly_summary_db):
        """Test YoY percent change calculation returns correct values."""
        conn = monthly_summary_db

        # Build percent change SQL
        sql = """
            SELECT
                month,
                revenue,
                LAG(revenue, 12) OVER (ORDER BY month) AS prior_year_revenue,
                ((revenue - LAG(revenue, 12) OVER (ORDER BY month))
                    / NULLIF(LAG(revenue, 12) OVER (ORDER BY month), 0) * 100) AS yoy_pct_change
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # 2024-01-01: (4400 - 3700) / 3700 * 100 = 18.92%
        jan_2024 = [r for r in result if str(r[0]) == "2024-01-01"][0]
        expected_pct = (4400.00 - 3700.00) / 3700.00 * 100
        assert abs(float(jan_2024[3]) - expected_pct) < 0.01

        # 2024-02-01: (3150 - 2550) / 2550 * 100 = 23.53%
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]
        expected_pct = (3150.00 - 2550.00) / 2550.00 * 100
        assert abs(float(feb_2024[3]) - expected_pct) < 0.01

    def test_yoy_difference_calculation(self, monthly_summary_db):
        """Test YoY difference calculation returns correct absolute change."""
        conn = monthly_summary_db

        sql = """
            SELECT
                month,
                revenue,
                (revenue - LAG(revenue, 12) OVER (ORDER BY month)) AS yoy_diff
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # 2024-01-01: 4400 - 3700 = 700
        jan_2024 = [r for r in result if str(r[0]) == "2024-01-01"][0]
        assert float(jan_2024[2]) == 700.00

        # 2024-02-01: 3150 - 2550 = 600
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]
        assert float(feb_2024[2]) == 600.00

    def test_yoy_ratio_calculation(self, monthly_summary_db):
        """Test YoY ratio calculation returns correct ratio."""
        conn = monthly_summary_db

        sql = """
            SELECT
                month,
                revenue,
                (revenue / NULLIF(LAG(revenue, 12) OVER (ORDER BY month), 0)) AS yoy_ratio
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # 2024-01-01: 4400 / 3700 = 1.189...
        jan_2024 = [r for r in result if str(r[0]) == "2024-01-01"][0]
        expected_ratio = 4400.00 / 3700.00
        assert abs(float(jan_2024[2]) - expected_ratio) < 0.001

    # =========================================================================
    # Month-over-Month (MoM) Tests
    # =========================================================================

    def test_mom_returns_prior_month_value(self, monthly_summary_db):
        """Test MoM LAG returns the correct prior month value."""
        conn = monthly_summary_db

        sql = """
            SELECT
                month,
                revenue,
                LAG(revenue, 1) OVER (ORDER BY month) AS prior_month_revenue
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # 2024-02-01 should have prior month value from 2024-01-01
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]
        assert feb_2024[1] == 3150.00  # Current revenue
        assert feb_2024[2] == 4400.00  # Prior month revenue (2024-01)

        # 2024-03-01 should have prior month value from 2024-02-01
        mar_2024 = [r for r in result if str(r[0]) == "2024-03-01"][0]
        assert mar_2024[1] == 3000.00  # Current revenue
        assert mar_2024[2] == 3150.00  # Prior month revenue (2024-02)

    def test_mom_percent_change_calculation(self, monthly_summary_db):
        """Test MoM percent change calculation returns correct values."""
        conn = monthly_summary_db

        sql = """
            SELECT
                month,
                revenue,
                ((revenue - LAG(revenue, 1) OVER (ORDER BY month))
                    / NULLIF(LAG(revenue, 1) OVER (ORDER BY month), 0) * 100) AS mom_pct_change
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # 2024-02-01: (3150 - 4400) / 4400 * 100 = -28.41%
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]
        expected_pct = (3150.00 - 4400.00) / 4400.00 * 100
        assert abs(float(feb_2024[2]) - expected_pct) < 0.01

    # =========================================================================
    # Week-over-Week (WoW) Tests
    # =========================================================================

    def test_wow_returns_prior_week_value(self, weekly_db):
        """Test WoW LAG returns the correct prior week value."""
        conn = weekly_db

        sql = """
            SELECT
                week_start,
                revenue,
                LAG(revenue, 1) OVER (ORDER BY week_start) AS prior_week_revenue
            FROM weekly_sales
            ORDER BY week_start
        """

        result = conn.execute(sql).fetchall()

        # Week 2024-01-08 should have prior week from 2024-01-01
        week2 = result[1]
        assert week2[1] == 1100.00  # Current revenue
        assert week2[2] == 1000.00  # Prior week revenue

        # Week 2024-01-15 should have prior week from 2024-01-08
        week3 = result[2]
        assert week3[1] == 1050.00  # Current revenue
        assert week3[2] == 1100.00  # Prior week revenue

    def test_wow_percent_change_calculation(self, weekly_db):
        """Test WoW percent change calculation returns correct values."""
        conn = weekly_db

        sql = """
            SELECT
                week_start,
                revenue,
                ((revenue - LAG(revenue, 1) OVER (ORDER BY week_start))
                    / NULLIF(LAG(revenue, 1) OVER (ORDER BY week_start), 0) * 100) AS wow_pct_change
            FROM weekly_sales
            ORDER BY week_start
        """

        result = conn.execute(sql).fetchall()

        # Week 2024-01-08: (1100 - 1000) / 1000 * 100 = 10%
        week2 = result[1]
        assert abs(float(week2[2]) - 10.0) < 0.01

        # Week 2024-01-15: (1050 - 1100) / 1100 * 100 = -4.545%
        week3 = result[2]
        expected_pct = (1050.00 - 1100.00) / 1100.00 * 100
        assert abs(float(week3[2]) - expected_pct) < 0.01

    # =========================================================================
    # Edge Cases: First Period (No Prior Data)
    # =========================================================================

    def test_first_period_returns_null(self, monthly_summary_db):
        """Test that first period has NULL for prior period (no data to compare)."""
        conn = monthly_summary_db

        sql = """
            SELECT
                month,
                revenue,
                LAG(revenue, 1) OVER (ORDER BY month) AS prior_month_revenue,
                ((revenue - LAG(revenue, 1) OVER (ORDER BY month))
                    / NULLIF(LAG(revenue, 1) OVER (ORDER BY month), 0) * 100) AS mom_pct_change
            FROM monthly_sales
            ORDER BY month
            LIMIT 1
        """

        result = conn.execute(sql).fetchone()

        # First row should have NULL for prior period
        assert result[2] is None  # prior_month_revenue
        assert result[3] is None  # mom_pct_change (division by NULL)

    def test_first_year_yoy_returns_null(self, monthly_summary_db):
        """Test that first year periods return NULL for YoY (no prior year)."""
        conn = monthly_summary_db

        sql = """
            SELECT
                month,
                revenue,
                LAG(revenue, 12) OVER (ORDER BY month) AS prior_year_revenue
            FROM monthly_sales
            WHERE month < '2024-01-01'
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # All 2023 rows should have NULL for prior year (no 2022 data)
        for row in result:
            assert row[2] is None

    # =========================================================================
    # Edge Cases: NULL Values
    # =========================================================================

    def test_null_current_value_propagates(self):
        """Test that NULL current values propagate correctly in calculations."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test_data (period INT, value DECIMAL(10,2))
        """)
        conn.execute("""
            INSERT INTO test_data VALUES (1, 100.00), (2, NULL), (3, 150.00)
        """)

        sql = """
            SELECT
                period,
                value,
                LAG(value, 1) OVER (ORDER BY period) AS prior_value,
                (value - LAG(value, 1) OVER (ORDER BY period)) AS diff
            FROM test_data
            ORDER BY period
        """

        result = conn.execute(sql).fetchall()

        # Period 2 has NULL value, so diff should be NULL
        period2 = result[1]
        assert period2[1] is None  # value
        assert period2[2] == 100.00  # prior_value (from period 1)
        assert period2[3] is None  # diff (NULL - 100 = NULL)

        # Period 3 has valid value but prior is NULL
        period3 = result[2]
        assert period3[1] == 150.00  # value
        assert period3[2] is None  # prior_value (from NULL period 2)
        assert period3[3] is None  # diff (150 - NULL = NULL)

        conn.close()

    def test_null_prior_value_handled_by_nullif(self):
        """Test that NULL prior values are handled by NULLIF in division."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test_data (period INT, value DECIMAL(10,2))
        """)
        conn.execute("""
            INSERT INTO test_data VALUES (1, NULL), (2, 100.00)
        """)

        sql = """
            SELECT
                period,
                value,
                LAG(value, 1) OVER (ORDER BY period) AS prior_value,
                ((value - LAG(value, 1) OVER (ORDER BY period))
                    / NULLIF(LAG(value, 1) OVER (ORDER BY period), 0) * 100) AS pct_change
            FROM test_data
            ORDER BY period
        """

        result = conn.execute(sql).fetchall()

        # Period 2 has prior value of NULL, so percent change should be NULL
        period2 = result[1]
        assert period2[1] == 100.00  # value
        assert period2[2] is None  # prior_value
        assert period2[3] is None  # pct_change (division by NULL)

        conn.close()

    # =========================================================================
    # Edge Cases: Zero Values (Division by Zero Protection)
    # =========================================================================

    def test_zero_prior_value_returns_null_for_ratio(self):
        """Test that zero prior value returns NULL for ratio/percent (NULLIF protection)."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test_data (period INT, value DECIMAL(10,2))
        """)
        conn.execute("""
            INSERT INTO test_data VALUES (1, 0.00), (2, 100.00)
        """)

        sql = """
            SELECT
                period,
                value,
                LAG(value, 1) OVER (ORDER BY period) AS prior_value,
                ((value - LAG(value, 1) OVER (ORDER BY period))
                    / NULLIF(LAG(value, 1) OVER (ORDER BY period), 0) * 100) AS pct_change,
                (value / NULLIF(LAG(value, 1) OVER (ORDER BY period), 0)) AS ratio
            FROM test_data
            ORDER BY period
        """

        result = conn.execute(sql).fetchall()

        # Period 2 has prior value of 0, NULLIF should make division return NULL
        period2 = result[1]
        assert period2[1] == 100.00  # value
        assert period2[2] == 0.00  # prior_value
        assert period2[3] is None  # pct_change (NULLIF(0, 0) = NULL)
        assert period2[4] is None  # ratio (NULLIF(0, 0) = NULL)

        conn.close()

    def test_zero_difference_calculated_correctly(self):
        """Test that zero difference is calculated correctly (not null)."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test_data (period INT, value DECIMAL(10,2))
        """)
        conn.execute("""
            INSERT INTO test_data VALUES (1, 100.00), (2, 100.00)
        """)

        sql = """
            SELECT
                period,
                value,
                (value - LAG(value, 1) OVER (ORDER BY period)) AS diff
            FROM test_data
            ORDER BY period
        """

        result = conn.execute(sql).fetchall()

        # Period 2: 100 - 100 = 0 (not NULL)
        period2 = result[1]
        assert period2[2] == 0.00

        conn.close()

    # =========================================================================
    # Trailing Period Tests
    # =========================================================================

    def test_trailing_7_day_sum(self, time_series_db):
        """Test trailing 7-day sum calculation."""
        conn = time_series_db

        sql = """
            SELECT
                sale_date,
                revenue,
                SUM(revenue) OVER (
                    ORDER BY sale_date
                    RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
                ) AS trailing_7_day_revenue
            FROM daily_sales
            WHERE sale_date BETWEEN '2023-01-01' AND '2023-01-31'
            ORDER BY sale_date
        """

        result = conn.execute(sql).fetchall()

        # First day should only have itself
        first_day = result[0]
        assert first_day[2] == 1000.00

        # Jan 15 should sum Jan 9-15 (only Jan 15 in that range)
        jan_15 = [r for r in result if str(r[0]) == "2023-01-15"][0]
        assert jan_15[2] == 1500.00  # Only Jan 15 in the range

    def test_trailing_30_day_sum(self, monthly_summary_db):
        """Test trailing 30-day sum (approximately 1 month)."""
        conn = monthly_summary_db

        sql = """
            SELECT
                month,
                revenue,
                SUM(revenue) OVER (
                    ORDER BY month
                    RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW
                ) AS trailing_30_day_revenue
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # First month only has itself
        first_month = result[0]
        assert first_month[2] == 3700.00

        # February includes Jan and Feb (within 30 days)
        feb = result[1]
        # Jan 1 to Feb 1 is 31 days, so Feb only includes itself
        assert feb[2] == 2550.00  # Just February

    # =========================================================================
    # Generate Time Comparison SQL Integration Tests
    # =========================================================================

    def test_generate_time_comparison_sql_difference_executes(self, monthly_summary_db):
        """Test that generate_time_comparison_sql difference output executes correctly."""
        conn = monthly_summary_db

        comparison = TimeComparison(type="mom", metric="revenue", calculation="difference")
        sql_expr = generate_time_comparison_sql(comparison, "revenue", "month")

        # Execute the generated SQL
        full_sql = f"""
            SELECT
                month,
                revenue,
                {sql_expr} AS mom_diff
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(full_sql).fetchall()

        # Verify a specific calculation
        # 2024-02: (3150 - 4400) = -1250
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]
        assert float(feb_2024[2]) == -1250.00

    def test_generate_time_comparison_sql_percent_change_executes(self, monthly_summary_db):
        """Test that generate_time_comparison_sql percent_change output executes correctly."""
        conn = monthly_summary_db

        comparison = TimeComparison(type="mom", metric="revenue", calculation="percent_change")
        sql_expr = generate_time_comparison_sql(comparison, "revenue", "month")

        full_sql = f"""
            SELECT
                month,
                revenue,
                {sql_expr} AS mom_pct
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(full_sql).fetchall()

        # 2024-02: (3150 - 4400) / 4400 * 100 = -28.41%
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]
        expected_pct = (3150.00 - 4400.00) / 4400.00 * 100
        assert abs(float(feb_2024[2]) - expected_pct) < 0.01

    def test_generate_time_comparison_sql_ratio_executes(self, monthly_summary_db):
        """Test that generate_time_comparison_sql ratio output executes correctly."""
        conn = monthly_summary_db

        comparison = TimeComparison(type="mom", metric="revenue", calculation="ratio")
        sql_expr = generate_time_comparison_sql(comparison, "revenue", "month")

        full_sql = f"""
            SELECT
                month,
                revenue,
                {sql_expr} AS mom_ratio
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(full_sql).fetchall()

        # 2024-02: 3150 / 4400 = 0.7159...
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]
        expected_ratio = 3150.00 / 4400.00
        assert abs(float(feb_2024[2]) - expected_ratio) < 0.001

    # =========================================================================
    # Leap Year Edge Case
    # =========================================================================

    def test_leap_year_february_handling(self):
        """Test that leap year February 29th is handled correctly in YoY."""
        import duckdb

        conn = duckdb.connect(":memory:")

        # 2024 is a leap year, 2023 is not
        conn.execute("""
            CREATE TABLE leap_year_test (
                date DATE,
                value DECIMAL(10,2)
            )
        """)
        conn.execute("""
            INSERT INTO leap_year_test VALUES
                ('2023-02-28', 100.00),
                ('2024-02-28', 120.00),
                ('2024-02-29', 50.00)
        """)

        # YoY comparison for Feb 28
        sql = """
            SELECT
                date,
                value,
                LAG(value, 1) OVER (ORDER BY date) AS prior_value
            FROM leap_year_test
            ORDER BY date
        """

        result = conn.execute(sql).fetchall()

        # Feb 28, 2024 should see Feb 28, 2023 as prior (LAG 1 with daily data)
        # Note: This is a simplified test; real YoY with daily data needs
        # matching logic (same day in prior year), not just LAG(365)
        feb_28_2024 = result[1]
        assert feb_28_2024[1] == 120.00
        assert feb_28_2024[2] == 100.00  # Prior row is 2023-02-28

        # Feb 29, 2024 has no equivalent in 2023
        feb_29_2024 = result[2]
        assert feb_29_2024[1] == 50.00
        assert feb_29_2024[2] == 120.00  # Prior row is 2024-02-28

        conn.close()

    # =========================================================================
    # Multiple Comparison Types Together
    # =========================================================================

    def test_multiple_comparison_types_in_single_query(self, monthly_summary_db):
        """Test that multiple comparison types can be used in the same query."""
        conn = monthly_summary_db

        sql = """
            SELECT
                month,
                revenue,
                LAG(revenue, 1) OVER (ORDER BY month) AS prior_month,
                LAG(revenue, 12) OVER (ORDER BY month) AS prior_year,
                (revenue - LAG(revenue, 1) OVER (ORDER BY month)) AS mom_diff,
                (revenue - LAG(revenue, 12) OVER (ORDER BY month)) AS yoy_diff,
                ((revenue - LAG(revenue, 1) OVER (ORDER BY month))
                    / NULLIF(LAG(revenue, 1) OVER (ORDER BY month), 0) * 100) AS mom_pct,
                ((revenue - LAG(revenue, 12) OVER (ORDER BY month))
                    / NULLIF(LAG(revenue, 12) OVER (ORDER BY month), 0) * 100) AS yoy_pct
            FROM monthly_sales
            ORDER BY month
        """

        result = conn.execute(sql).fetchall()

        # Check 2024-02-01 which has both MoM and YoY comparisons
        feb_2024 = [r for r in result if str(r[0]) == "2024-02-01"][0]

        # MoM: comparing to 2024-01-01
        assert feb_2024[2] == 4400.00  # prior_month (2024-01)
        assert float(feb_2024[4]) == -1250.00  # mom_diff (3150 - 4400)

        # YoY: comparing to 2023-02-01
        assert feb_2024[3] == 2550.00  # prior_year (2023-02)
        assert float(feb_2024[5]) == 600.00  # yoy_diff (3150 - 2550)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
