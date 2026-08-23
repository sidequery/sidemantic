from sidemantic import Metric, Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


def test_graph_metric_sql_reference_order_determines_one_to_one_join_anchor():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="shifts",
            table="shifts",
            primary_key="id",
            relationships=[
                Relationship(
                    name="shift_finances",
                    type="one_to_one",
                    foreign_key="shift_id",
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="shift_finances",
            table="shift_finances",
            primary_key="shift_id",
        )
    )
    graph.add_metric(
        Metric(
            name="total_amount",
            agg="sum",
            sql="COALESCE(shifts.amount, shift_finances.amount) + shifts.adjustment",
        )
    )

    generator = SQLGenerator(graph)

    assert generator._extract_models_from_sql(graph.get_metric("total_amount").sql) == [
        "shifts",
        "shift_finances",
    ]
    assert generator._find_required_models(["total_amount"], []) == ["shifts", "shift_finances"]

    sql = generator.generate(metrics=["total_amount"], dimensions=[])
    assert "FROM shifts_cte\nLEFT JOIN shift_finances_cte" in sql
