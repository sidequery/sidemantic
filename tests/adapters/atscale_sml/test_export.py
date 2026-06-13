"""Tests for AtScale SML adapter export."""

import pytest
import yaml

from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


class TestAtScaleSMLExport:
    def test_export_simple_graph(self, tmp_path):
        sales = Model(
            name="sales",
            table="public.sales",
            primary_key="sale_id",
            dimensions=[
                Dimension(name="sale_id", type="numeric", sql="sale_id"),
                Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(name="total_sales", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count", sql="sale_id"),
            ],
            relationships=[
                Relationship(name="customers", type="many_to_one", foreign_key="customer_id", primary_key="id"),
            ],
        )

        customers = Model(
            name="customers",
            table="public.customers",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric", sql="id"),
                Dimension(name="name", type="categorical", sql="name"),
            ],
        )

        graph = SemanticGraph()
        graph.add_model(sales)
        graph.add_model(customers)

        adapter = AtScaleSMLAdapter()
        adapter.export(graph, tmp_path)

        datasets_dir = tmp_path / "datasets"
        dimensions_dir = tmp_path / "dimensions"
        metrics_dir = tmp_path / "metrics"
        models_dir = tmp_path / "models"

        assert datasets_dir.exists()
        assert dimensions_dir.exists()
        assert metrics_dir.exists()
        assert models_dir.exists()

        with open(datasets_dir / "sales.yml") as f:
            dataset = yaml.safe_load(f)

        assert dataset["object_type"] == "dataset"
        assert dataset["unique_name"] == "sales"
        assert dataset["table"] == "public.sales"
        assert "columns" in dataset

        with open(dimensions_dir / "sales.yml") as f:
            dimension = yaml.safe_load(f)

        assert dimension["object_type"] == "dimension"
        assert dimension["unique_name"] == "sales"
        assert len(dimension["level_attributes"]) == 3

        with open(metrics_dir / "total_sales.yml") as f:
            metric = yaml.safe_load(f)

        assert metric["object_type"] == "metric"
        assert metric["unique_name"] == "total_sales"
        assert metric["calculation_method"] == "sum"

        with open(models_dir / "sales.yml") as f:
            model_def = yaml.safe_load(f)

        assert model_def["object_type"] == "model"
        assert model_def["unique_name"] == "sales"
        assert "relationships" in model_def
        assert model_def["relationships"][0]["to"]["dimension"] == "customers"

    def test_export_percentile_metadata_roundtrips(self, tmp_path):
        """Percentile metric metadata (custom_quantiles, compression) exports."""
        orders = Model(
            name="orders",
            table="public.orders",
            primary_key="order_id",
            dimensions=[Dimension(name="order_id", type="numeric", sql="order_id")],
            metrics=[
                Metric(
                    name="amount_p75",
                    agg="median",
                    sql="amount",
                    metadata={"custom_quantiles": [0.75], "compression": 10000},
                ),
                Metric(name="amount_median", agg="median", sql="amount"),
            ],
        )

        graph = SemanticGraph()
        graph.add_model(orders)

        adapter = AtScaleSMLAdapter()
        adapter.export(graph, tmp_path)

        with open(tmp_path / "metrics" / "amount_p75.yml") as f:
            metric = yaml.safe_load(f)
        assert metric["calculation_method"] == "percentile"
        assert metric["custom_quantiles"] == [0.75]
        assert metric["compression"] == 10000

        with open(tmp_path / "metrics" / "amount_median.yml") as f:
            metric = yaml.safe_load(f)
        assert metric["calculation_method"] == "percentile"
        assert metric["named_quantiles"] == "median"

    def test_imported_custom_percentile_roundtrips(self, tmp_path):
        """An imported SML v1.5 custom_quantiles percentile re-exports as a percentile metric.

        Imported custom-quantile percentiles parse to derived metrics with agg=None and a
        PERCENTILE_CONT(...) expression. Export must preserve custom_quantiles/compression
        instead of degrading to a metric_calc that drops both fields.
        """
        src = tmp_path / "src"
        for sub in ("metrics", "models", "datasets"):
            (src / sub).mkdir(parents=True)
        (src / "atscale.yml").write_text(
            yaml.safe_dump({"object_type": "catalog", "unique_name": "cat", "label": "cat"})
        )
        (src / "models" / "orders_model.yml").write_text(
            yaml.safe_dump(
                {
                    "object_type": "model",
                    "unique_name": "orders_model",
                    "label": "Orders",
                    "metrics": [{"unique_name": "amount_p75"}],
                    "dimensions": ["orders"],
                }
            )
        )
        (src / "datasets" / "orders.yml").write_text(
            yaml.safe_dump(
                {
                    "object_type": "dataset",
                    "unique_name": "orders",
                    "label": "Orders",
                    "sql": "select * from public.orders",
                    "columns": [{"name": "order_id"}, {"name": "amount"}],
                }
            )
        )
        (src / "metrics" / "amount_p75.yml").write_text(
            yaml.safe_dump(
                {
                    "object_type": "metric",
                    "unique_name": "amount_p75",
                    "label": "Amount P75",
                    "calculation_method": "percentile",
                    "dataset": "orders",
                    "column": "amount",
                    "custom_quantiles": [0.75],
                    "compression": 10000,
                }
            )
        )

        adapter = AtScaleSMLAdapter()
        graph = adapter.parse(src)

        out = tmp_path / "out"
        adapter.export(graph, out)

        with open(out / "metrics" / "amount_p75.yml") as f:
            exported = yaml.safe_load(f)
        assert exported["object_type"] == "metric"
        assert exported["calculation_method"] == "percentile"
        assert exported["custom_quantiles"] == [0.75]
        assert exported["compression"] == 10000
        assert exported["column"] == "amount"

    def test_export_relationship_level_uses_dimension(self, tmp_path):
        orders = Model(
            name="orders",
            table="public.orders",
            primary_key="order_id",
            dimensions=[Dimension(name="order_id", type="numeric", sql="order_id")],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )

        customers = Model(
            name="customers",
            table="public.customers",
            primary_key="id",
            dimensions=[Dimension(name="customer_id", type="numeric", sql="customer_id")],
        )

        graph = SemanticGraph()
        graph.add_model(orders)
        graph.add_model(customers)

        adapter = AtScaleSMLAdapter()
        adapter.export(graph, tmp_path)

        with open(tmp_path / "models" / "orders.yml") as f:
            model_def = yaml.safe_load(f)

        level = model_def["relationships"][0]["to"]["level"]
        assert level == "customer_id"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
