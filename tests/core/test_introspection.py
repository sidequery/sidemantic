from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_describe_preserves_explicit_and_named_formats():
    layer = SemanticLayer()
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            dimensions=[Dimension(name="amount", type="numeric", value_format_name="usd")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount", value_format_name="usd"),
                Metric(name="count", agg="count"),
                Metric(
                    name="aov",
                    type="ratio",
                    numerator="revenue",
                    denominator="count",
                    format="#,##0.00",
                    value_format_name="decimal_2",
                ),
            ],
        )
    )
    layer.add_metric(Metric(name="global_revenue", type="derived", sql="orders.revenue", value_format_name="usd"))

    described = layer.describe_models()
    orders = described["models"][0]
    assert orders["dimensions"][0]["value_format_name"] == "usd"
    revenue, count, aov = orders["metrics"]
    assert revenue["value_format_name"] == "usd"
    assert "format" not in revenue
    assert "value_format_name" not in count
    assert aov["format"] == "#,##0.00"
    assert aov["value_format_name"] == "decimal_2"
    global_revenue = next(metric for metric in described["metrics"] if metric["name"] == "global_revenue")
    assert global_revenue["value_format_name"] == "usd"
