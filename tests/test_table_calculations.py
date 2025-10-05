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
