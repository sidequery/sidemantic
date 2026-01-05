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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
