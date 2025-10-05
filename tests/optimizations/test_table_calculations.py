"""Test table calculations (post-query runtime calculations)."""

from sidemantic.core.table_calculation import TableCalculation
from sidemantic.sql.table_calc_processor import TableCalculationProcessor


def test_formula_calculation():
    """Test formula-based table calculation."""
    calc = TableCalculation(
        name="profit_margin", type="formula", expression="(${revenue} - ${cost}) / ${revenue} * 100"
    )

    processor = TableCalculationProcessor([calc])
    results = [
        (100, 60),  # revenue=100, cost=60
        (200, 150),  # revenue=200, cost=150
    ]
    column_names = ["revenue", "cost"]

    processed, columns = processor.process(results, column_names)

    assert "profit_margin" in columns
    assert len(processed) == 2
    assert processed[0][2] == 40.0  # (100-60)/100 * 100
    assert processed[1][2] == 25.0  # (200-150)/200 * 100


def test_percent_of_total():
    """Test percent of total calculation."""
    calc = TableCalculation(name="pct_of_total", type="percent_of_total", field="revenue")

    processor = TableCalculationProcessor([calc])
    results = [
        ("A", 100),
        ("B", 200),
        ("C", 700),
    ]
    column_names = ["category", "revenue"]

    processed, columns = processor.process(results, column_names)

    # Total = 1000, so A=10%, B=20%, C=70%
    assert abs(processed[0][2] - 10.0) < 0.01
    assert abs(processed[1][2] - 20.0) < 0.01
    assert abs(processed[2][2] - 70.0) < 0.01


def test_percent_of_previous():
    """Test percent change from previous row."""
    calc = TableCalculation(name="pct_change", type="percent_of_previous", field="value")

    processor = TableCalculationProcessor([calc])
    results = [
        (100,),
        (150,),  # +50%
        (120,),  # -20%
    ]
    column_names = ["value"]

    processed, columns = processor.process(results, column_names)

    assert processed[0][1] is None  # No previous
    assert abs(processed[1][1] - 50.0) < 0.01  # (150-100)/100 * 100
    assert abs(processed[2][1] - (-20.0)) < 0.01  # (120-150)/150 * 100


def test_running_total():
    """Test running total calculation."""
    calc = TableCalculation(name="running_sum", type="running_total", field="amount")

    processor = TableCalculationProcessor([calc])
    results = [
        (10,),
        (20,),
        (30,),
    ]
    column_names = ["amount"]

    processed, columns = processor.process(results, column_names)

    assert processed[0][1] == 10
    assert processed[1][1] == 30  # 10 + 20
    assert processed[2][1] == 60  # 10 + 20 + 30


def test_rank():
    """Test ranking calculation."""
    calc = TableCalculation(name="revenue_rank", type="rank", field="revenue")

    processor = TableCalculationProcessor([calc])
    results = [
        ("A", 100),
        ("B", 300),
        ("C", 200),
        ("D", 300),  # Tie with B
    ]
    column_names = ["name", "revenue"]

    processed, columns = processor.process(results, column_names)

    # Find ranks by name
    ranks = {row[0]: row[2] for row in processed}
    assert ranks["B"] == 1  # Highest (tied)
    assert ranks["D"] == 1  # Highest (tied)
    assert ranks["C"] == 3  # Next
    assert ranks["A"] == 4  # Lowest


def test_row_number():
    """Test row number calculation."""
    calc = TableCalculation(name="row_num", type="row_number")

    processor = TableCalculationProcessor([calc])
    results = [
        ("A",),
        ("B",),
        ("C",),
    ]
    column_names = ["name"]

    processed, columns = processor.process(results, column_names)

    assert processed[0][1] == 1
    assert processed[1][1] == 2
    assert processed[2][1] == 3


def test_moving_average():
    """Test moving average calculation."""
    calc = TableCalculation(name="ma_3", type="moving_average", field="value", window_size=3)

    processor = TableCalculationProcessor([calc])
    results = [
        (10,),
        (20,),
        (30,),
        (40,),
    ]
    column_names = ["value"]

    processed, columns = processor.process(results, column_names)

    assert processed[0][1] == 10.0  # Just 10
    assert processed[1][1] == 15.0  # (10+20)/2
    assert processed[2][1] == 20.0  # (10+20+30)/3
    assert abs(processed[3][1] - 30.0) < 0.01  # (20+30+40)/3


def test_multiple_calculations():
    """Test applying multiple table calculations."""
    calcs = [
        TableCalculation(name="pct_total", type="percent_of_total", field="revenue"),
        TableCalculation(name="running_sum", type="running_total", field="revenue"),
    ]

    processor = TableCalculationProcessor(calcs)
    results = [
        (100,),
        (200,),
        (700,),
    ]
    column_names = ["revenue"]

    processed, columns = processor.process(results, column_names)

    # Should have original + 2 new columns
    assert len(columns) == 3
    assert "pct_total" in columns
    assert "running_sum" in columns

    # Check values
    assert abs(processed[0][1] - 10.0) < 0.01  # pct_total
    assert processed[0][2] == 100  # running_sum
    assert abs(processed[1][1] - 20.0) < 0.01
    assert processed[1][2] == 300


def test_empty_results():
    """Test processing empty results."""
    calc = TableCalculation(name="pct_total", type="percent_of_total", field="revenue")

    processor = TableCalculationProcessor([calc])
    results = []
    column_names = ["revenue"]

    processed, columns = processor.process(results, column_names)

    assert len(processed) == 0
    # Columns may or may not include calc names when results are empty
    assert len(columns) >= 1


def test_missing_field():
    """Test error when calculation field doesn't exist."""
    calc = TableCalculation(name="pct_total", type="percent_of_total", field="missing_field")

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["revenue"]

    # Should handle missing field gracefully or error
    try:
        processed, columns = processor.process(results, column_names)
        # If it doesn't error, verify it handles it gracefully
        assert len(processed) > 0
    except (ValueError, KeyError, IndexError):
        # Expected if processor validates field exists
        pass


def test_division_by_zero_in_formula():
    """Test handling division by zero in formula."""
    calc = TableCalculation(name="ratio", type="formula", expression="${revenue} / ${count}")

    processor = TableCalculationProcessor([calc])
    results = [
        (100, 5),  # OK
        (200, 0),  # Division by zero
    ]
    column_names = ["revenue", "count"]

    processed, columns = processor.process(results, column_names)

    # First row should work
    assert processed[0][2] == 20.0

    # Second row: division by zero results in None (handled gracefully)
    # This is expected behavior
    assert processed[1][2] is None


def test_percent_of_total_all_zeros():
    """Test percent of total when all values are zero."""
    calc = TableCalculation(name="pct_total", type="percent_of_total", field="revenue")

    processor = TableCalculationProcessor([calc])
    results = [
        (0,),
        (0,),
        (0,),
    ]
    column_names = ["revenue"]

    processed, columns = processor.process(results, column_names)

    # Should handle gracefully (likely all 0% or None)
    for row in processed:
        assert row[1] == 0 or row[1] is None


def test_moving_average_window_larger_than_data():
    """Test moving average with window size larger than dataset."""
    calc = TableCalculation(name="ma", type="moving_average", field="value", window_size=10)

    processor = TableCalculationProcessor([calc])
    results = [
        (10,),
        (20,),
        (30,),
    ]
    column_names = ["value"]

    processed, columns = processor.process(results, column_names)

    # Should average all available data
    assert processed[0][1] == 10.0
    assert processed[1][1] == 15.0  # (10+20)/2
    assert processed[2][1] == 20.0  # (10+20+30)/3


def test_negative_values_in_calculations():
    """Test calculations with negative values."""
    calc = TableCalculation(name="running_sum", type="running_total", field="value")

    processor = TableCalculationProcessor([calc])
    results = [
        (100,),
        (-50,),
        (30,),
    ]
    column_names = ["value"]

    processed, columns = processor.process(results, column_names)

    assert processed[0][1] == 100
    assert processed[1][1] == 50  # 100 - 50
    assert processed[2][1] == 80  # 100 - 50 + 30


def test_percent_of_column_total_with_partition():
    """Test percent_of_column_total with partition_by."""
    calc = TableCalculation(
        name="pct_of_region",
        type="percent_of_column_total",
        field="revenue",
        partition_by=["region"],
    )

    processor = TableCalculationProcessor([calc])
    results = [
        ("US", 100),
        ("US", 200),
        ("US", 700),  # US total = 1000
        ("EU", 300),
        ("EU", 700),  # EU total = 1000
    ]
    column_names = ["region", "revenue"]

    processed, columns = processor.process(results, column_names)

    # US percentages (100/1000=10%, 200/1000=20%, 700/1000=70%)
    assert abs(processed[0][2] - 10.0) < 0.01
    assert abs(processed[1][2] - 20.0) < 0.01
    assert abs(processed[2][2] - 70.0) < 0.01

    # EU percentages (300/1000=30%, 700/1000=70%)
    assert abs(processed[3][2] - 30.0) < 0.01
    assert abs(processed[4][2] - 70.0) < 0.01


def test_formula_without_expression():
    """Test error when formula calculation missing expression."""
    import pytest

    calc = TableCalculation(name="bad_formula", type="formula")

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["revenue"]

    with pytest.raises(ValueError, match="missing expression"):
        processor.process(results, column_names)


def test_percent_of_total_without_field():
    """Test error when percent_of_total missing field."""
    import pytest

    calc = TableCalculation(name="pct", type="percent_of_total")

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["revenue"]

    with pytest.raises(ValueError, match="missing field"):
        processor.process(results, column_names)


def test_percent_of_previous_without_field():
    """Test error when percent_of_previous missing field."""
    import pytest

    calc = TableCalculation(name="pct_prev", type="percent_of_previous")

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["value"]

    with pytest.raises(ValueError, match="missing field"):
        processor.process(results, column_names)


def test_percent_of_column_total_without_field():
    """Test error when percent_of_column_total missing field."""
    import pytest

    calc = TableCalculation(name="pct_col", type="percent_of_column_total")

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["value"]

    with pytest.raises(ValueError, match="missing field"):
        processor.process(results, column_names)


def test_running_total_without_field():
    """Test error when running_total missing field."""
    import pytest

    calc = TableCalculation(name="running", type="running_total")

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["value"]

    with pytest.raises(ValueError, match="missing field"):
        processor.process(results, column_names)


def test_rank_without_field():
    """Test error when rank missing field."""
    import pytest

    calc = TableCalculation(name="my_rank", type="rank")

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["value"]

    with pytest.raises(ValueError, match="missing field"):
        processor.process(results, column_names)


def test_moving_average_without_field():
    """Test error when moving_average missing field."""
    import pytest

    calc = TableCalculation(name="ma", type="moving_average", window_size=3)

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["value"]

    with pytest.raises(ValueError, match="missing field"):
        processor.process(results, column_names)


def test_moving_average_without_window_size():
    """Test error when moving_average missing window_size."""
    import pytest

    calc = TableCalculation(name="ma", type="moving_average", field="value")

    processor = TableCalculationProcessor([calc])
    results = [(100,)]
    column_names = ["value"]

    with pytest.raises(ValueError, match="missing field or window_size"):
        processor.process(results, column_names)


def test_unknown_calculation_type():
    """Test error for unknown calculation type.

    Note: Pydantic validates type at TableCalculation creation,
    so invalid types raise ValidationError at model instantiation.
    """
    import pytest
    from pydantic import ValidationError

    # Invalid type should raise ValidationError at creation
    with pytest.raises(ValidationError):
        TableCalculation(name="bad", type="unknown_type", field="value")
