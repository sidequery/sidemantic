from __future__ import annotations

import pytest

from sidemantic.interchange.ossie.expression_validation import scalar_sql_expression_error


@pytest.mark.parametrize(
    "expression",
    [
        "SELECT amount FROM orders",
        "amount IN (SELECT amount FROM refunds)",
        "WITH cte_values AS (SELECT 1) SELECT * FROM cte_values",
        "SELECT amount FROM orders UNION SELECT amount FROM refunds",
        "CREATE TABLE unsafe (id INT)",
        "DROP TABLE unsafe",
        "INSERT INTO unsafe VALUES (1)",
        "UPDATE unsafe SET id = 2",
        "DELETE FROM unsafe",
    ],
)
def test_prohibited_query_and_statement_corpus_is_rejected(expression: str) -> None:
    error = scalar_sql_expression_error(expression, sqlglot_dialect=None)

    assert error is not None
    assert "cannot contain" in error


@pytest.mark.parametrize(
    "expression",
    [
        "amount + tax",
        "status IN ('paid', 'refunded')",
        "CASE WHEN amount > 0 THEN amount ELSE 0 END",
        "COUNT(DISTINCT customer_id)",
        "SUM(amount) OVER (PARTITION BY region ORDER BY occurred_at)",
        "COALESCE(discount, 0)",
        "CAST(occurred_at AS DATE)",
    ],
)
def test_required_scalar_constructs_cross_the_structural_gate(expression: str) -> None:
    assert scalar_sql_expression_error(expression, sqlglot_dialect=None) is None


def test_multiple_statements_are_rejected() -> None:
    assert scalar_sql_expression_error("amount; DROP TABLE unsafe", sqlglot_dialect=None) == (
        "expression must contain exactly one SQL expression"
    )
