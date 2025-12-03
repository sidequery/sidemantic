"""Test relative date range helper."""

from sidemantic.core.relative_date import RelativeDateRange


def test_today_yesterday_tomorrow():
    """Test single day expressions."""
    assert RelativeDateRange.parse("today") == "CURRENT_DATE"
    assert RelativeDateRange.parse("yesterday") == "CURRENT_DATE - 1"
    assert RelativeDateRange.parse("tomorrow") == "CURRENT_DATE + 1"


def test_last_n_days():
    """Test last N days expressions."""
    assert RelativeDateRange.parse("last 7 days") == "CURRENT_DATE - 7"
    assert RelativeDateRange.parse("last 30 days") == "CURRENT_DATE - 30"
    assert RelativeDateRange.parse("last 1 day") == "CURRENT_DATE - 1"


def test_last_n_weeks():
    """Test last N weeks expressions."""
    assert RelativeDateRange.parse("last 1 week") == "CURRENT_DATE - 7"
    assert RelativeDateRange.parse("last 2 weeks") == "CURRENT_DATE - 14"


def test_this_last_next_week():
    """Test this/last/next week."""
    assert RelativeDateRange.parse("this week") == "DATE_TRUNC('week', CURRENT_DATE)"
    assert RelativeDateRange.parse("last week") == "DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'"
    assert RelativeDateRange.parse("next week") == "DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'"


def test_this_last_next_month():
    """Test this/last/next month."""
    assert RelativeDateRange.parse("this month") == "DATE_TRUNC('month', CURRENT_DATE)"
    assert RelativeDateRange.parse("last month") == "DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'"
    assert RelativeDateRange.parse("next month") == "DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"


def test_this_last_next_quarter():
    """Test this/last/next quarter."""
    assert RelativeDateRange.parse("this quarter") == "DATE_TRUNC('quarter', CURRENT_DATE)"
    assert RelativeDateRange.parse("last quarter") == "DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months'"
    assert RelativeDateRange.parse("next quarter") == "DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'"


def test_this_last_next_year():
    """Test this/last/next year."""
    assert RelativeDateRange.parse("this year") == "DATE_TRUNC('year', CURRENT_DATE)"
    assert RelativeDateRange.parse("last year") == "DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'"
    assert RelativeDateRange.parse("next year") == "DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'"


def test_to_range_last_n_days():
    """Test converting to range filter for last N days."""
    result = RelativeDateRange.to_range("last 7 days", "created_at")
    assert result == "created_at >= CURRENT_DATE - 7"


def test_to_range_this_month():
    """Test converting to range filter for this month."""
    result = RelativeDateRange.to_range("this month", "order_date")
    assert "order_date >= DATE_TRUNC('month', CURRENT_DATE)" in result
    assert "order_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'" in result


def test_to_range_last_quarter():
    """Test converting to range filter for last quarter."""
    result = RelativeDateRange.to_range("last quarter", "created_at")
    assert "created_at >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months'" in result
    assert "INTERVAL '3 months'" in result


def test_to_range_today():
    """Test converting single day to filter."""
    result = RelativeDateRange.to_range("today", "event_date")
    assert result == "event_date = CURRENT_DATE"


def test_is_relative_date():
    """Test checking if expression is a relative date."""
    assert RelativeDateRange.is_relative_date("last 7 days") is True
    assert RelativeDateRange.is_relative_date("this month") is True
    assert RelativeDateRange.is_relative_date("yesterday") is True

    # Not relative dates
    assert RelativeDateRange.is_relative_date("2024-01-01") is False
    assert RelativeDateRange.is_relative_date("some random string") is False


def test_case_insensitive():
    """Test that parsing is case insensitive."""
    assert RelativeDateRange.parse("LAST 7 DAYS") == "CURRENT_DATE - 7"
    assert RelativeDateRange.parse("This Month") == "DATE_TRUNC('month', CURRENT_DATE)"
    assert RelativeDateRange.parse("TODAY") == "CURRENT_DATE"


def test_whitespace_handling():
    """Test that extra whitespace is handled."""
    assert RelativeDateRange.parse("  last 7 days  ") == "CURRENT_DATE - 7"
    assert RelativeDateRange.parse("  this month  ") == "DATE_TRUNC('month', CURRENT_DATE)"


def test_bigquery_dialect():
    """Test BigQuery dialect generates correct DATE_TRUNC syntax."""
    # BigQuery uses DATE_TRUNC(column, MONTH) not DATE_TRUNC('month', column)
    assert RelativeDateRange.parse("this month", dialect="bigquery") == "DATE_TRUNC(CURRENT_DATE, MONTH)"
    assert RelativeDateRange.parse("this week", dialect="bigquery") == "DATE_TRUNC(CURRENT_DATE, WEEK)"
    assert RelativeDateRange.parse("this year", dialect="bigquery") == "DATE_TRUNC(CURRENT_DATE, YEAR)"
    assert (
        RelativeDateRange.parse("last month", dialect="bigquery")
        == "DATE_TRUNC(CURRENT_DATE, MONTH) - INTERVAL '1 month'"
    )

    # Simple patterns don't use DATE_TRUNC, should be unchanged
    assert RelativeDateRange.parse("last 7 days", dialect="bigquery") == "CURRENT_DATE - 7"
    assert RelativeDateRange.parse("today", dialect="bigquery") == "CURRENT_DATE"


def test_bigquery_to_range():
    """Test BigQuery dialect in to_range method."""
    result = RelativeDateRange.to_range("this month", "order_date", dialect="bigquery")
    assert "DATE_TRUNC(CURRENT_DATE, MONTH)" in result
    assert "DATE_TRUNC('month'" not in result
