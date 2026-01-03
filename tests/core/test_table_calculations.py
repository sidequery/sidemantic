"""Comprehensive tests for table calculation core functionality.

Tests cover:
- TableCalculation model (Pydantic)
- TableCalculationProcessor
- All calculation types
- Edge cases and error handling
"""

import pytest

from sidemantic.core.table_calculation import TableCalculation
from sidemantic.sql.table_calc_processor import TableCalculationProcessor

# =============================================================================
# TableCalculation Model Tests
# =============================================================================


class TestTableCalculationModel:
    """Test TableCalculation Pydantic model."""

    def test_minimal_formula_calculation(self):
        """Test creating minimal formula calculation."""
        calc = TableCalculation(
            name="test_formula",
            type="formula",
            expression="${a} + ${b}",
        )
        assert calc.name == "test_formula"
        assert calc.type == "formula"
        assert calc.expression == "${a} + ${b}"

    def test_full_calculation_with_all_fields(self):
        """Test creating calculation with all optional fields."""
        calc = TableCalculation(
            name="ma_7",
            type="moving_average",
            description="7-day moving average of revenue",
            field="revenue",
            partition_by=["region", "category"],
            order_by=["date"],
            window_size=7,
        )
        assert calc.name == "ma_7"
        assert calc.type == "moving_average"
        assert calc.description == "7-day moving average of revenue"
        assert calc.field == "revenue"
        assert calc.partition_by == ["region", "category"]
        assert calc.order_by == ["date"]
        assert calc.window_size == 7

    def test_hash_function(self):
        """Test that TableCalculation is hashable by name."""
        calc1 = TableCalculation(name="calc1", type="row_number")
        calc2 = TableCalculation(name="calc1", type="rank", field="value")
        calc3 = TableCalculation(name="calc3", type="row_number")

        # Same name means same hash
        assert hash(calc1) == hash(calc2)
        # Different names mean different hashes (usually)
        assert hash(calc1) != hash(calc3)

        # Can be used in sets
        calc_set = {calc1, calc3}
        assert len(calc_set) == 2

    def test_valid_calculation_types(self):
        """Test all valid calculation types can be created."""
        valid_types = [
            "formula",
            "percent_of_total",
            "percent_of_previous",
            "percent_of_column_total",
            "running_total",
            "rank",
            "row_number",
            "percentile",
            "moving_average",
        ]

        for calc_type in valid_types:
            calc = TableCalculation(name=f"test_{calc_type}", type=calc_type)
            assert calc.type == calc_type

    def test_invalid_calculation_type_raises(self):
        """Test that invalid calculation type raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TableCalculation(name="bad", type="invalid_type")


# =============================================================================
# Processor - Empty and Edge Cases
# =============================================================================


class TestProcessorEdgeCases:
    """Test processor edge cases."""

    def test_process_empty_calculations_list(self):
        """Test processor with empty calculations list."""
        processor = TableCalculationProcessor([])
        results = [(1, 2), (3, 4)]
        column_names = ["a", "b"]

        processed, columns = processor.process(results, column_names)

        # Should return unchanged
        assert processed == results
        assert columns == column_names

    def test_process_empty_results(self):
        """Test processing empty results."""
        calc = TableCalculation(name="running", type="running_total", field="value")
        processor = TableCalculationProcessor([calc])

        processed, columns = processor.process([], ["value"])

        assert processed == []
        assert columns == ["value"]

    def test_single_row_running_total(self):
        """Test running total with single row."""
        calc = TableCalculation(name="running", type="running_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 100

    def test_single_row_percent_of_total(self):
        """Test percent of total with single row equals 100%."""
        calc = TableCalculation(name="pct", type="percent_of_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(50,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 100.0

    def test_single_row_rank(self):
        """Test rank with single row."""
        calc = TableCalculation(name="rnk", type="rank", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 1

    def test_single_row_moving_average(self):
        """Test moving average with single row."""
        calc = TableCalculation(name="ma", type="moving_average", field="value", window_size=5)
        processor = TableCalculationProcessor([calc])

        results = [(100,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 100.0

    def test_single_row_percent_of_previous(self):
        """Test percent of previous with single row returns None."""
        calc = TableCalculation(name="pct_prev", type="percent_of_previous", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] is None


# =============================================================================
# NULL Value Handling Tests
# =============================================================================


class TestNullHandling:
    """Test NULL value handling in calculations."""

    def test_null_in_running_total(self):
        """Test running total treats NULL as 0."""
        calc = TableCalculation(name="running", type="running_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(10,), (None,), (20,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 10
        assert processed[1][1] == 10  # NULL treated as 0
        assert processed[2][1] == 30

    def test_null_in_percent_of_total(self):
        """Test percent of total with NULL values."""
        calc = TableCalculation(name="pct", type="percent_of_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,), (None,), (100,)]
        processed, columns = processor.process(results, ["value"])

        # Total = 200 (NULL as 0), so 100/200 = 50%
        assert abs(processed[0][1] - 50.0) < 0.01
        assert processed[1][1] == 0  # NULL -> 0%
        assert abs(processed[2][1] - 50.0) < 0.01

    def test_null_in_percent_of_previous(self):
        """Test percent of previous with NULL values."""
        calc = TableCalculation(name="pct_prev", type="percent_of_previous", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,), (None,), (200,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] is None  # First row
        assert processed[1][1] is None  # Current value is None
        # After None, should handle gracefully
        assert processed[2][1] is None  # Previous was None

    def test_null_in_rank(self):
        """Test rank with NULL values."""
        calc = TableCalculation(name="rnk", type="rank", field="value")
        processor = TableCalculationProcessor([calc])

        results = [("A", 100), ("B", None), ("C", 50)]
        processed, columns = processor.process(results, ["name", "value"])

        # NULL treated as 0, so ranking: A=100 > C=50 > B=0
        ranks = {row[0]: row[2] for row in processed}
        assert ranks["A"] == 1
        assert ranks["C"] == 2
        assert ranks["B"] == 3

    def test_null_in_moving_average(self):
        """Test moving average with NULL values."""
        calc = TableCalculation(name="ma", type="moving_average", field="value", window_size=3)
        processor = TableCalculationProcessor([calc])

        results = [(10,), (None,), (20,)]
        processed, columns = processor.process(results, ["value"])

        # NULL treated as 0
        assert processed[0][1] == 10.0
        assert processed[1][1] == 5.0  # (10 + 0) / 2
        assert processed[2][1] == 10.0  # (10 + 0 + 20) / 3

    def test_all_null_values(self):
        """Test calculations with all NULL values."""
        calc = TableCalculation(name="running", type="running_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(None,), (None,), (None,)]
        processed, columns = processor.process(results, ["value"])

        # All zeros
        assert processed[0][1] == 0
        assert processed[1][1] == 0
        assert processed[2][1] == 0


# =============================================================================
# Formula Expression Tests
# =============================================================================


class TestFormulaExpressions:
    """Test formula expression evaluation."""

    def test_simple_addition(self):
        """Test simple addition formula."""
        calc = TableCalculation(name="sum", type="formula", expression="${a} + ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(10, 20)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] == 30

    def test_subtraction(self):
        """Test subtraction formula."""
        calc = TableCalculation(name="diff", type="formula", expression="${a} - ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(100, 30)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] == 70

    def test_multiplication(self):
        """Test multiplication formula."""
        calc = TableCalculation(name="prod", type="formula", expression="${a} * ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(5, 6)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] == 30

    def test_division(self):
        """Test division formula."""
        calc = TableCalculation(name="ratio", type="formula", expression="${a} / ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(100, 4)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] == 25.0

    def test_floor_division(self):
        """Test floor division formula."""
        calc = TableCalculation(name="floor", type="formula", expression="${a} // ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(10, 3)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] == 3

    def test_modulo(self):
        """Test modulo formula."""
        calc = TableCalculation(name="mod", type="formula", expression="${a} % ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(10, 3)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] == 1

    def test_power(self):
        """Test power formula."""
        calc = TableCalculation(name="pow", type="formula", expression="${a} ** ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(2, 3)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] == 8

    def test_unary_negation(self):
        """Test unary negation in formula."""
        calc = TableCalculation(name="neg", type="formula", expression="-${a}")
        processor = TableCalculationProcessor([calc])

        results = [(100,)]
        processed, columns = processor.process(results, ["a"])

        assert processed[0][1] == -100

    def test_unary_positive(self):
        """Test unary positive in formula."""
        calc = TableCalculation(name="pos", type="formula", expression="+${a}")
        processor = TableCalculationProcessor([calc])

        results = [(100,)]
        processed, columns = processor.process(results, ["a"])

        assert processed[0][1] == 100

    def test_complex_expression(self):
        """Test complex multi-operator expression."""
        calc = TableCalculation(name="complex", type="formula", expression="(${revenue} - ${cost}) / ${revenue} * 100")
        processor = TableCalculationProcessor([calc])

        results = [(200, 150)]
        processed, columns = processor.process(results, ["revenue", "cost"])

        assert processed[0][2] == 25.0

    def test_nested_parentheses(self):
        """Test formula with nested parentheses."""
        calc = TableCalculation(name="nested", type="formula", expression="((${a} + ${b}) * ${c}) / 2")
        processor = TableCalculationProcessor([calc])

        results = [(10, 20, 3)]
        processed, columns = processor.process(results, ["a", "b", "c"])

        assert processed[0][3] == 45.0  # ((10 + 20) * 3) / 2 = 45

    def test_formula_with_null_becomes_zero(self):
        """Test that NULL values in formulas become 0."""
        calc = TableCalculation(name="sum", type="formula", expression="${a} + ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(10, None)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] == 10  # 10 + 0


# =============================================================================
# Safe Eval Security Tests
# =============================================================================


class TestSafeEvalSecurity:
    """Test that safe_eval blocks dangerous operations."""

    def test_function_call_blocked(self):
        """Test that function calls are blocked."""
        processor = TableCalculationProcessor([])

        with pytest.raises(ValueError, match="Invalid expression"):
            processor._safe_eval("print(1)")

    def test_attribute_access_blocked(self):
        """Test that attribute access is blocked."""
        processor = TableCalculationProcessor([])

        with pytest.raises(ValueError, match="Invalid expression"):
            processor._safe_eval("''.join(['a'])")

    def test_import_blocked(self):
        """Test that import is blocked."""
        processor = TableCalculationProcessor([])

        with pytest.raises(ValueError, match="Invalid expression"):
            processor._safe_eval("__import__('os')")

    def test_builtin_access_blocked(self):
        """Test that builtin access is blocked."""
        processor = TableCalculationProcessor([])

        with pytest.raises(ValueError, match="Invalid expression"):
            processor._safe_eval("eval('1+1')")

    def test_list_comprehension_blocked(self):
        """Test that list comprehension is blocked."""
        processor = TableCalculationProcessor([])

        with pytest.raises(ValueError, match="Invalid expression"):
            processor._safe_eval("[x for x in range(10)]")

    def test_valid_arithmetic_allowed(self):
        """Test that valid arithmetic is allowed."""
        processor = TableCalculationProcessor([])

        result = processor._safe_eval("1 + 2 * 3")
        assert result == 7

    def test_valid_with_floats(self):
        """Test arithmetic with floats."""
        processor = TableCalculationProcessor([])

        result = processor._safe_eval("3.14 * 2.0")
        assert abs(result - 6.28) < 0.01


# =============================================================================
# Rank Behavior Tests
# =============================================================================


class TestRankBehavior:
    """Test rank calculation behavior."""

    def test_rank_descending_by_default(self):
        """Test that rank orders descending (highest value = rank 1)."""
        calc = TableCalculation(name="rnk", type="rank", field="value")
        processor = TableCalculationProcessor([calc])

        results = [("A", 10), ("B", 30), ("C", 20)]
        processed, columns = processor.process(results, ["name", "value"])

        ranks = {row[0]: row[2] for row in processed}
        assert ranks["B"] == 1  # Highest
        assert ranks["C"] == 2
        assert ranks["A"] == 3  # Lowest

    def test_rank_ties_get_same_rank(self):
        """Test that tied values get the same rank."""
        calc = TableCalculation(name="rnk", type="rank", field="value")
        processor = TableCalculationProcessor([calc])

        results = [("A", 100), ("B", 200), ("C", 200), ("D", 50)]
        processed, columns = processor.process(results, ["name", "value"])

        ranks = {row[0]: row[2] for row in processed}
        assert ranks["B"] == 1  # Tied for first
        assert ranks["C"] == 1  # Tied for first
        assert ranks["A"] == 3  # Gap after ties
        assert ranks["D"] == 4

    def test_rank_all_ties(self):
        """Test rank when all values are the same."""
        calc = TableCalculation(name="rnk", type="rank", field="value")
        processor = TableCalculationProcessor([calc])

        results = [("A", 100), ("B", 100), ("C", 100)]
        processed, columns = processor.process(results, ["name", "value"])

        # All should be rank 1
        for row in processed:
            assert row[2] == 1


# =============================================================================
# Moving Average Tests
# =============================================================================


class TestMovingAverage:
    """Test moving average calculation."""

    def test_window_size_one(self):
        """Test moving average with window size 1 (equals current value)."""
        calc = TableCalculation(name="ma", type="moving_average", field="value", window_size=1)
        processor = TableCalculationProcessor([calc])

        results = [(10,), (20,), (30,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 10.0
        assert processed[1][1] == 20.0
        assert processed[2][1] == 30.0

    def test_window_exactly_fits_data(self):
        """Test moving average when window exactly fits all data."""
        calc = TableCalculation(name="ma", type="moving_average", field="value", window_size=3)
        processor = TableCalculationProcessor([calc])

        results = [(10,), (20,), (30,)]
        processed, columns = processor.process(results, ["value"])

        # Last row averages all 3: (10+20+30)/3 = 20
        assert processed[2][1] == 20.0

    def test_large_window_size(self):
        """Test moving average with window larger than data."""
        calc = TableCalculation(name="ma", type="moving_average", field="value", window_size=100)
        processor = TableCalculationProcessor([calc])

        results = [(10,), (20,), (30,), (40,)]
        processed, columns = processor.process(results, ["value"])

        # Each row averages all data up to that point
        assert processed[0][1] == 10.0
        assert processed[1][1] == 15.0  # (10+20)/2
        assert processed[2][1] == 20.0  # (10+20+30)/3
        assert processed[3][1] == 25.0  # (10+20+30+40)/4

    def test_moving_average_precision(self):
        """Test moving average maintains precision."""
        calc = TableCalculation(name="ma", type="moving_average", field="value", window_size=3)
        processor = TableCalculationProcessor([calc])

        results = [(1,), (2,), (3,)]
        processed, columns = processor.process(results, ["value"])

        # (1+2+3)/3 = 2.0
        assert processed[2][1] == 2.0


# =============================================================================
# Percent Calculations Tests
# =============================================================================


class TestPercentCalculations:
    """Test percent-based calculations."""

    def test_percent_of_total_basic(self):
        """Test basic percent of total."""
        calc = TableCalculation(name="pct", type="percent_of_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(25,), (25,), (50,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 25.0
        assert processed[1][1] == 25.0
        assert processed[2][1] == 50.0

    def test_percent_of_total_sums_to_100(self):
        """Test that percent of total sums to 100."""
        calc = TableCalculation(name="pct", type="percent_of_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(17,), (29,), (54,)]
        processed, columns = processor.process(results, ["value"])

        total_pct = sum(row[1] for row in processed)
        assert abs(total_pct - 100.0) < 0.01

    def test_percent_of_previous_increase(self):
        """Test percent change showing increase."""
        calc = TableCalculation(name="pct_prev", type="percent_of_previous", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,), (120,)]  # 20% increase
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] is None
        assert abs(processed[1][1] - 20.0) < 0.01

    def test_percent_of_previous_decrease(self):
        """Test percent change showing decrease."""
        calc = TableCalculation(name="pct_prev", type="percent_of_previous", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,), (80,)]  # 20% decrease
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] is None
        assert abs(processed[1][1] - (-20.0)) < 0.01

    def test_percent_of_previous_double(self):
        """Test percent change when value doubles."""
        calc = TableCalculation(name="pct_prev", type="percent_of_previous", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(50,), (100,)]  # 100% increase
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] is None
        assert abs(processed[1][1] - 100.0) < 0.01

    def test_percent_of_previous_zero_previous(self):
        """Test percent change when previous value is zero."""
        calc = TableCalculation(name="pct_prev", type="percent_of_previous", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(0,), (100,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] is None
        assert processed[1][1] is None  # Division by zero handled

    def test_percent_of_column_total_without_partition(self):
        """Test percent_of_column_total without partition equals percent_of_total."""
        calc = TableCalculation(name="pct", type="percent_of_column_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,), (200,), (700,)]
        processed, columns = processor.process(results, ["value"])

        assert abs(processed[0][1] - 10.0) < 0.01
        assert abs(processed[1][1] - 20.0) < 0.01
        assert abs(processed[2][1] - 70.0) < 0.01

    def test_percent_of_column_total_multi_partition(self):
        """Test percent_of_column_total with multiple partition columns."""
        calc = TableCalculation(
            name="pct",
            type="percent_of_column_total",
            field="value",
            partition_by=["region", "product"],
        )
        processor = TableCalculationProcessor([calc])

        results = [
            ("US", "A", 100),
            ("US", "A", 200),
            ("US", "B", 300),  # Different product
            ("EU", "A", 150),
            ("EU", "A", 350),
        ]
        processed, columns = processor.process(results, ["region", "product", "value"])

        # US-A total = 300, so 100/300 = 33.33%, 200/300 = 66.67%
        assert abs(processed[0][3] - 33.33) < 0.01
        assert abs(processed[1][3] - 66.67) < 0.01

        # US-B total = 300, so 300/300 = 100%
        assert abs(processed[2][3] - 100.0) < 0.01

        # EU-A total = 500
        assert abs(processed[3][3] - 30.0) < 0.01
        assert abs(processed[4][3] - 70.0) < 0.01


# =============================================================================
# Multiple Calculations Tests
# =============================================================================


class TestMultipleCalculations:
    """Test applying multiple calculations."""

    def test_calculations_applied_in_order(self):
        """Test that calculations are applied in specified order."""
        calcs = [
            TableCalculation(name="double", type="formula", expression="${value} * 2"),
            TableCalculation(name="running", type="running_total", field="value"),
        ]
        processor = TableCalculationProcessor(calcs)

        results = [(10,), (20,), (30,)]
        processed, columns = processor.process(results, ["value"])

        assert columns == ["value", "double", "running"]

        # double should be applied to original values
        assert processed[0][1] == 20
        assert processed[1][1] == 40
        assert processed[2][1] == 60

        # running_total on original value column
        assert processed[0][2] == 10
        assert processed[1][2] == 30
        assert processed[2][2] == 60

    def test_calculation_can_reference_previous_calculation(self):
        """Test that later calculation can reference earlier one."""
        calcs = [
            TableCalculation(name="double", type="formula", expression="${value} * 2"),
            TableCalculation(name="triple_double", type="formula", expression="${double} * 1.5"),
        ]
        processor = TableCalculationProcessor(calcs)

        results = [(10,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 20  # double
        assert processed[0][2] == 30  # triple_double = 20 * 1.5

    def test_many_calculations(self):
        """Test applying many calculations at once."""
        calcs = [
            TableCalculation(name="row_num", type="row_number"),
            TableCalculation(name="running", type="running_total", field="value"),
            TableCalculation(name="pct", type="percent_of_total", field="value"),
            TableCalculation(name="ma_2", type="moving_average", field="value", window_size=2),
            TableCalculation(name="rnk", type="rank", field="value"),
        ]
        processor = TableCalculationProcessor(calcs)

        results = [(30,), (10,), (20,)]
        processed, columns = processor.process(results, ["value"])

        assert len(columns) == 6
        assert "row_num" in columns
        assert "running" in columns
        assert "pct" in columns
        assert "ma_2" in columns
        assert "rnk" in columns


# =============================================================================
# Row Number Tests
# =============================================================================


class TestRowNumber:
    """Test row_number calculation."""

    def test_row_number_basic(self):
        """Test basic row numbering."""
        calc = TableCalculation(name="rn", type="row_number")
        processor = TableCalculationProcessor([calc])

        results = [("A",), ("B",), ("C",), ("D",)]
        processed, columns = processor.process(results, ["name"])

        assert processed[0][1] == 1
        assert processed[1][1] == 2
        assert processed[2][1] == 3
        assert processed[3][1] == 4

    def test_row_number_no_field_required(self):
        """Test row_number doesn't require field parameter."""
        calc = TableCalculation(name="rn", type="row_number")
        processor = TableCalculationProcessor([calc])

        results = [(100,)]
        processed, columns = processor.process(results, ["value"])

        # Should not raise, row_number doesn't need field
        assert processed[0][1] == 1


# =============================================================================
# Running Total Tests
# =============================================================================


class TestRunningTotal:
    """Test running_total calculation."""

    def test_running_total_basic(self):
        """Test basic running total."""
        calc = TableCalculation(name="running", type="running_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,), (50,), (150,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 100
        assert processed[1][1] == 150
        assert processed[2][1] == 300

    def test_running_total_with_negatives(self):
        """Test running total with negative values."""
        calc = TableCalculation(name="running", type="running_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,), (-30,), (50,), (-20,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 100
        assert processed[1][1] == 70
        assert processed[2][1] == 120
        assert processed[3][1] == 100

    def test_running_total_floats(self):
        """Test running total with floating point values."""
        calc = TableCalculation(name="running", type="running_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(1.1,), (2.2,), (3.3,)]
        processed, columns = processor.process(results, ["value"])

        assert abs(processed[0][1] - 1.1) < 0.01
        assert abs(processed[1][1] - 3.3) < 0.01
        assert abs(processed[2][1] - 6.6) < 0.01


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Test error handling in calculations."""

    def test_formula_missing_expression(self):
        """Test error when formula missing expression."""
        calc = TableCalculation(name="bad", type="formula")
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing expression"):
            processor.process([(1,)], ["value"])

    def test_percent_of_total_missing_field(self):
        """Test error when percent_of_total missing field."""
        calc = TableCalculation(name="bad", type="percent_of_total")
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing field"):
            processor.process([(1,)], ["value"])

    def test_percent_of_previous_missing_field(self):
        """Test error when percent_of_previous missing field."""
        calc = TableCalculation(name="bad", type="percent_of_previous")
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing field"):
            processor.process([(1,)], ["value"])

    def test_percent_of_column_total_missing_field(self):
        """Test error when percent_of_column_total missing field."""
        calc = TableCalculation(name="bad", type="percent_of_column_total")
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing field"):
            processor.process([(1,)], ["value"])

    def test_running_total_missing_field(self):
        """Test error when running_total missing field."""
        calc = TableCalculation(name="bad", type="running_total")
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing field"):
            processor.process([(1,)], ["value"])

    def test_rank_missing_field(self):
        """Test error when rank missing field."""
        calc = TableCalculation(name="bad", type="rank")
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing field"):
            processor.process([(1,)], ["value"])

    def test_moving_average_missing_field(self):
        """Test error when moving_average missing field."""
        calc = TableCalculation(name="bad", type="moving_average", window_size=3)
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing field"):
            processor.process([(1,)], ["value"])

    def test_moving_average_missing_window_size(self):
        """Test error when moving_average missing window_size."""
        calc = TableCalculation(name="bad", type="moving_average", field="value")
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing field or window_size"):
            processor.process([(1,)], ["value"])

    def test_division_by_zero_in_formula(self):
        """Test division by zero returns None instead of raising."""
        calc = TableCalculation(name="ratio", type="formula", expression="${a} / ${b}")
        processor = TableCalculationProcessor([calc])

        results = [(10, 0)]
        processed, columns = processor.process(results, ["a", "b"])

        assert processed[0][2] is None

    def test_invalid_expression_syntax(self):
        """Test invalid expression syntax raises ValueError at creation time."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="Invalid formula expression syntax"):
            TableCalculation(name="bad", type="formula", expression="invalid syntax ++")


# =============================================================================
# Field Reference Tests
# =============================================================================


class TestFieldReferences:
    """Test field reference handling."""

    def test_missing_field_reference_in_formula(self):
        """Test formula with reference to non-existent field."""
        calc = TableCalculation(name="bad", type="formula", expression="${missing} + 1")
        processor = TableCalculationProcessor([calc])

        results = [(10,)]
        processed, columns = processor.process(results, ["value"])

        # Field reference not replaced, should cause eval error -> None
        assert processed[0][1] is None

    def test_missing_field_in_running_total(self):
        """Test running_total with non-existent field uses 0."""
        calc = TableCalculation(name="running", type="running_total", field="missing")
        processor = TableCalculationProcessor([calc])

        results = [(10,), (20,)]
        processed, columns = processor.process(results, ["value"])

        # Missing field returns None, treated as 0
        assert processed[0][1] == 0
        assert processed[1][1] == 0

    def test_field_with_special_characters_in_name(self):
        """Test field with special characters in column name."""
        calc = TableCalculation(name="running", type="running_total", field="my_value_1")
        processor = TableCalculationProcessor([calc])

        results = [(10,), (20,)]
        processed, columns = processor.process(results, ["my_value_1"])

        assert processed[0][1] == 10
        assert processed[1][1] == 30


# =============================================================================
# Percentile Type Tests
# =============================================================================


class TestPercentileType:
    """Test percentile calculation type."""

    def test_percentile_type_accepted(self):
        """Test that percentile type is accepted in model."""
        calc = TableCalculation(name="p50", type="percentile", field="value", percentile=0.5)
        assert calc.type == "percentile"
        assert calc.percentile == 0.5

    def test_percentile_median(self):
        """Test percentile calculation for median (p50)."""
        calc = TableCalculation(name="p50", type="percentile", field="value", percentile=0.5)
        processor = TableCalculationProcessor([calc])

        # Odd number of values: 1, 2, 3, 4, 5 - median is 3
        results = [(1,), (2,), (3,), (4,), (5,)]
        processed, columns = processor.process(results, ["value"])

        assert columns == ["value", "p50"]
        # All rows get the same percentile value
        assert all(row[1] == 3.0 for row in processed)

    def test_percentile_p95(self):
        """Test percentile calculation for p95."""
        calc = TableCalculation(name="p95", type="percentile", field="value", percentile=0.95)
        processor = TableCalculationProcessor([calc])

        # Values 1-100
        results = [(i,) for i in range(1, 101)]
        processed, columns = processor.process(results, ["value"])

        # p95 of 1-100 using linear interpolation: 95.05
        assert columns == ["value", "p95"]
        assert processed[0][1] == 95.05

    def test_percentile_p0_and_p100(self):
        """Test percentile at boundaries (min and max)."""
        calc_min = TableCalculation(name="pmin", type="percentile", field="value", percentile=0.0)
        calc_max = TableCalculation(name="pmax", type="percentile", field="value", percentile=1.0)
        processor = TableCalculationProcessor([calc_min, calc_max])

        results = [(10,), (20,), (30,), (40,), (50,)]
        processed, columns = processor.process(results, ["value"])

        assert columns == ["value", "pmin", "pmax"]
        # p0 = min value, p100 = max value
        assert processed[0][1] == 10.0
        assert processed[0][2] == 50.0

    def test_percentile_single_value(self):
        """Test percentile with single value."""
        calc = TableCalculation(name="p50", type="percentile", field="value", percentile=0.5)
        processor = TableCalculationProcessor([calc])

        results = [(42,)]
        processed, columns = processor.process(results, ["value"])

        # Single value: any percentile is that value
        assert processed[0][1] == 42.0

    def test_percentile_with_none_values(self):
        """Test percentile ignores None values."""
        calc = TableCalculation(name="p50", type="percentile", field="value", percentile=0.5)
        processor = TableCalculationProcessor([calc])

        results = [(1,), (None,), (3,), (None,), (5,)]
        processed, columns = processor.process(results, ["value"])

        # Median of [1, 3, 5] is 3
        assert processed[0][1] == 3.0

    def test_percentile_all_none_values(self):
        """Test percentile with all None values returns None."""
        calc = TableCalculation(name="p50", type="percentile", field="value", percentile=0.5)
        processor = TableCalculationProcessor([calc])

        results = [(None,), (None,), (None,)]
        processed, columns = processor.process(results, ["value"])

        assert all(row[1] is None for row in processed)

    def test_percentile_missing_field_raises(self):
        """Test percentile without field raises error."""
        calc = TableCalculation(name="p50", type="percentile", percentile=0.5)
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing field"):
            processor.process([(1,), (2,)], ["value"])

    def test_percentile_missing_percentile_value_raises(self):
        """Test percentile without percentile value raises error."""
        calc = TableCalculation(name="p50", type="percentile", field="value")
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="missing percentile value"):
            processor.process([(1,), (2,)], ["value"])

    def test_percentile_invalid_range_raises(self):
        """Test percentile outside 0-1 range raises error."""
        calc = TableCalculation(name="p150", type="percentile", field="value", percentile=1.5)
        processor = TableCalculationProcessor([calc])

        with pytest.raises(ValueError, match="must be between 0 and 1"):
            processor.process([(1,), (2,)], ["value"])

    def test_percentile_interpolation(self):
        """Test percentile linear interpolation between values."""
        calc = TableCalculation(name="p25", type="percentile", field="value", percentile=0.25)
        processor = TableCalculationProcessor([calc])

        # Values: 0, 10, 20, 30, 40
        # p25 position = 0.25 * 4 = 1.0, so exactly at index 1 = 10
        results = [(0,), (10,), (20,), (30,), (40,)]
        processed, columns = processor.process(results, ["value"])

        assert processed[0][1] == 10.0


# =============================================================================
# Large Dataset Tests
# =============================================================================


class TestLargeDatasets:
    """Test calculations with larger datasets."""

    def test_running_total_large_dataset(self):
        """Test running total with many rows."""
        calc = TableCalculation(name="running", type="running_total", field="value")
        processor = TableCalculationProcessor([calc])

        # 1000 rows, each value is row index
        results = [(i,) for i in range(1, 1001)]
        processed, columns = processor.process(results, ["value"])

        # Running total of 1+2+...+1000 = 1000*1001/2 = 500500
        assert processed[999][1] == 500500

    def test_percent_of_total_large_dataset(self):
        """Test percent of total with many rows."""
        calc = TableCalculation(name="pct", type="percent_of_total", field="value")
        processor = TableCalculationProcessor([calc])

        results = [(100,) for _ in range(100)]
        processed, columns = processor.process(results, ["value"])

        # Each row should be 1%
        for row in processed:
            assert abs(row[1] - 1.0) < 0.01

    def test_rank_large_dataset(self):
        """Test ranking with many rows."""
        calc = TableCalculation(name="rnk", type="rank", field="value")
        processor = TableCalculationProcessor([calc])

        # Values from 1 to 100
        results = [(i, i) for i in range(1, 101)]
        processed, columns = processor.process(results, ["id", "value"])

        # Value 100 should be rank 1, value 1 should be rank 100
        ranks = {row[1]: row[2] for row in processed}
        assert ranks[100] == 1
        assert ranks[1] == 100

    def test_moving_average_large_window_large_data(self):
        """Test moving average with large window on large dataset."""
        calc = TableCalculation(name="ma", type="moving_average", field="value", window_size=50)
        processor = TableCalculationProcessor([calc])

        # 200 rows of value 10
        results = [(10,) for _ in range(200)]
        processed, columns = processor.process(results, ["value"])

        # After window fills, MA should be 10
        assert processed[100][1] == 10.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
