from sidemantic import Metric, Model, SemanticLayer


def test_duckdb_strips_unattached_catalog():
    layer = SemanticLayer(connection="duckdb:///:memory:")

    layer.adapter.execute("CREATE SCHEMA METRICS")
    layer.adapter.execute(
        """
        CREATE TABLE METRICS.dim_location (
            sk_location_id INTEGER,
            amount INTEGER
        )
        """
    )
    layer.adapter.execute("INSERT INTO METRICS.dim_location VALUES (1, 10), (2, 20)")

    model = Model(
        name="location",
        table="LOCKERS_AGGREGATION__DEV.METRICS.dim_location",
        primary_key="sk_location_id",
        metrics=[Metric(name="count", agg="count", sql="sk_location_id")],
    )

    layer.add_model(model)

    assert layer.get_model("location").table == "METRICS.dim_location"

    result = layer.query(metrics=["location.count"])
    assert result.fetchone()[0] == 2
