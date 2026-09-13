"""Execute the supported CLI rewrite subset against independent seed results."""

from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from sidemantic.cli import app

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize(
    "source,sql,columns,rows",
    [
        (
            source,
            "select customers.region, orders.revenue from orders order by customers.region",
            ["region", "revenue"],
            [("EU", 90), ("US", 150)],
        )
        for source in ["native/orders", "cube"]
    ]
    + [
        (
            "native/orders",
            "select revenue_per_customer from metrics",
            ["revenue_per_customer"],
            [(120.0,)],
        ),
        (
            "native/orders",
            "select orders.revenue as first_total, orders.revenue as second_total from metrics",
            ["first_total", "second_total"],
            [(240, 240)],
        ),
        (
            "native/monthly",
            "select monthly.sale_date__month as month, revenue_mom as change from metrics order by month",
            ["month", "change"],
            [("2024-01-01", None), ("2024-03-01", None)],
        ),
        (
            "native/monthly",
            "select monthly.sale_date__month, revenue_mom from metrics order by monthly.sale_date__month",
            ["sale_date__month", "revenue_mom"],
            [("2024-01-01", None), ("2024-03-01", None)],
        ),
        (
            "native/roles",
            "select origin.city as origin_city, destination.city as destination_city, "
            "flights.flight_count from metrics order by destination.city",
            ["origin_city", "destination_city", "flight_count"],
            [("SFO", "JFK", 1), ("SFO", "LAX", 1)],
        ),
    ],
)
@pytest.mark.parametrize("engine", ["python", "rust"])
def test_cli_rewrite_executes_imported_query(source, sql, columns, rows, engine, tmp_path):
    if engine == "rust":
        pytest.importorskip("sidemantic_rs", reason="The real Rust extension must be installed for acceptance")
    models = tmp_path / "models"
    models.mkdir()
    (models / "models.yml").write_text((FIXTURES / f"{source}.yml").read_text())
    result = CliRunner().invoke(
        app,
        ["rewrite", "-", "--models", str(models), "--engine", engine],
        input=sql,
    )
    assert result.exit_code == 0, result.output
    with duckdb.connect() as connection:
        connection.execute((FIXTURES / "seed.sql").read_text())
        relation = connection.execute(result.stdout)
        assert [column[0] for column in relation.description] == columns
        actual = [
            tuple(value.isoformat() if hasattr(value, "isoformat") else value for value in row)
            for row in relation.fetchall()
        ]
        assert actual == rows
