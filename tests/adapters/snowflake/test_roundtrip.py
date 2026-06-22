"""Tests for Snowflake adapter roundtrip (parse -> export -> parse)."""

from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.snowflake import SnowflakeAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph


@pytest.fixture
def adapter():
    return SnowflakeAdapter()


@pytest.fixture
def examples_dir():
    return Path(__file__).parent.parent.parent.parent / "examples" / "snowflake"


class TestSnowflakeRoundtrip:
    """Test roundtrip conversion: Snowflake -> Sidemantic -> Snowflake."""

    def test_roundtrip_simple_model(self, adapter, examples_dir, tmp_path):
        """Test roundtrip of a simple model."""
        # Parse original
        graph = adapter.parse(examples_dir / "simple.yaml")

        # Export to temp file
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)

        # Parse exported file
        graph2 = adapter.parse(output_file)

        # Compare models
        assert "sales" in graph2.models
        model = graph2.models["sales"]

        assert model.description == "Sales transactions"
        assert model.primary_key == "id"

    def test_roundtrip_preserves_dimensions(self, adapter, examples_dir, tmp_path):
        """Test that dimensions are preserved through roundtrip."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)
        graph2 = adapter.parse(output_file)

        model = graph2.models["sales"]
        dim_names = [d.name for d in model.dimensions]

        assert "region" in dim_names
        assert "sale_date" in dim_names

        region = model.get_dimension("region")
        assert region.type == "categorical"

        sale_date = model.get_dimension("sale_date")
        assert sale_date.type == "time"

    def test_roundtrip_preserves_facts(self, adapter, examples_dir, tmp_path):
        """Test that facts are preserved through roundtrip."""
        graph = adapter.parse(examples_dir / "simple.yaml")
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)
        graph2 = adapter.parse(output_file)

        model = graph2.models["sales"]

        amount = model.get_metric("amount")
        assert amount is not None
        assert amount.agg == "sum"

    def test_roundtrip_preserves_relationships(self, adapter, examples_dir, tmp_path):
        """Test that relationships are preserved through roundtrip."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)
        graph2 = adapter.parse(output_file)

        orders = graph2.models["orders"]
        rel_names = [r.name for r in orders.relationships]

        assert "customers" in rel_names
        assert "products" in rel_names

    def test_roundtrip_preserves_segments(self, adapter, examples_dir, tmp_path):
        """Test that segments/filters are preserved through roundtrip."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)
        graph2 = adapter.parse(output_file)

        orders = graph2.models["orders"]
        segment_names = [s.name for s in orders.segments]

        assert "completed_orders" in segment_names
        completed = orders.get_segment("completed_orders")
        # Segment SQL contains {model}.column format and gets re-qualified on re-parse
        assert "status" in completed.sql
        assert "'delivered'" in completed.sql


class TestSnowflakeExportFromSidemantic:
    """Test exporting Sidemantic models to Snowflake format."""

    def test_export_basic_model(self, adapter, tmp_path):
        """Test exporting a basic model to Snowflake format."""
        graph = SemanticGraph()
        model = Model(
            name="test_model",
            table="db.schema.table",
            description="Test model",
            primary_key="id",
            dimensions=[
                Dimension(name="category", type="categorical", sql="category"),
                Dimension(name="created_at", type="time", sql="created_at"),
            ],
            metrics=[
                Metric(name="total", agg="sum", sql="amount"),
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        # Verify output structure
        with open(output_file) as f:
            data = yaml.safe_load(f)

        assert data["name"] == "export"
        assert len(data["tables"]) == 1

        table = data["tables"][0]
        assert table["name"] == "test_model"
        assert table["description"] == "Test model"
        assert table["base_table"]["database"] == "db"
        assert table["base_table"]["schema"] == "schema"
        assert table["base_table"]["table"] == "table"

    def test_export_separates_time_dimensions(self, adapter, tmp_path):
        """Test that time dimensions are exported separately."""
        graph = SemanticGraph()
        model = Model(
            name="test",
            table="test",
            dimensions=[
                Dimension(name="cat", type="categorical"),
                Dimension(name="ts", type="time"),
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        table = data["tables"][0]
        assert "dimensions" in table
        assert "time_dimensions" in table
        assert len(table["dimensions"]) == 1
        assert len(table["time_dimensions"]) == 1
        assert table["dimensions"][0]["name"] == "cat"
        assert table["time_dimensions"][0]["name"] == "ts"

    def test_export_separates_facts_and_metrics(self, adapter, tmp_path):
        """Test that simple aggregations become facts, complex become metrics."""
        graph = SemanticGraph()
        model = Model(
            name="test",
            table="test",
            metrics=[
                Metric(name="sum_amount", agg="sum", sql="amount"),  # Simple -> fact
                Metric(
                    name="ratio", type="ratio", numerator="test.sum_amount", denominator="test.count"
                ),  # Complex -> metric
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        table = data["tables"][0]
        assert "facts" in table
        assert "metrics" in table
        assert len(table["facts"]) == 1
        assert len(table["metrics"]) == 1
        assert table["facts"][0]["name"] == "sum_amount"
        assert table["metrics"][0]["name"] == "ratio"

    def test_export_relationships(self, adapter, tmp_path):
        """Test exporting relationships."""
        graph = SemanticGraph()

        customers = Model(name="customers", table="customers", primary_key="customer_id")
        orders = Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    primary_key="customer_id",
                )
            ],
        )
        graph.add_model(customers)
        graph.add_model(orders)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        assert "relationships" in data
        assert len(data["relationships"]) == 1

        rel = data["relationships"][0]
        assert rel["left_table"] == "orders"
        assert rel["right_table"] == "customers"
        assert rel["relationship_type"] == "many_to_one"
        assert rel["relationship_columns"][0]["left_column"] == "customer_id"

    def test_export_segments_as_filters(self, adapter, tmp_path):
        """Test exporting segments as filters."""
        graph = SemanticGraph()
        model = Model(
            name="test",
            table="test",
            segments=[
                Segment(name="active", sql="status = 'active'", description="Active records"),
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        table = data["tables"][0]
        assert "filters" in table
        assert len(table["filters"]) == 1
        assert table["filters"][0]["name"] == "active"
        assert table["filters"][0]["expr"] == "status = 'active'"

    def test_export_dimension_data_types(self, adapter, tmp_path):
        """Test that dimension types map to Snowflake data types."""
        graph = SemanticGraph()
        model = Model(
            name="test",
            table="test",
            dimensions=[
                Dimension(name="cat", type="categorical"),
                Dimension(name="num", type="numeric"),
                Dimension(name="bool", type="boolean"),
                Dimension(name="ts", type="time"),
            ],
        )
        graph.add_model(model)

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        table = data["tables"][0]

        # Find dimensions by name
        dims = {d["name"]: d for d in table.get("dimensions", [])}
        time_dims = {d["name"]: d for d in table.get("time_dimensions", [])}

        assert dims["cat"]["data_type"] == "TEXT"
        assert dims["num"]["data_type"] == "NUMBER"
        assert dims["bool"]["data_type"] == "BOOLEAN"
        assert time_dims["ts"]["data_type"] == "TIMESTAMP"


class TestSnowflakeRoundtripYamlStructure:
    """Test that exported YAML has correct Snowflake structure."""

    def test_export_creates_valid_snowflake_yaml(self, adapter, examples_dir, tmp_path):
        """Test that exported YAML follows Snowflake spec."""
        graph = adapter.parse(examples_dir / "ecommerce.yaml")
        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        # Check top-level structure
        assert "name" in data
        assert "tables" in data

        # Check table structure
        for table in data["tables"]:
            assert "name" in table
            # base_table should have proper structure if present
            if "base_table" in table:
                base = table["base_table"]
                assert "table" in base

            # primary_key should have columns list
            if "primary_key" in table:
                assert "columns" in table["primary_key"]
                assert isinstance(table["primary_key"]["columns"], list)

            # facts should have default_aggregation
            for fact in table.get("facts", []):
                assert "name" in fact
                assert "default_aggregation" in fact

            # metrics should have expr
            for metric in table.get("metrics", []):
                assert "name" in metric


class TestSnowflakeTopLevelMetrics:
    """Test parsing/exporting of graph-level (top-level) metrics.

    Snowflake semantic-view metrics that omit ``table`` (or reference a table not
    present in the model) become graph-level Sidemantic metrics that reference
    other fields with ``model.field`` syntax.
    """

    @pytest.fixture
    def top_level_yaml(self, tmp_path):
        path = tmp_path / "top_level.yaml"
        path.write_text("""
name: shop
tables:
  - name: orders
    base_table:
      table: orders
    primary_key:
      columns:
        - id
    facts:
      - name: total_revenue
        expr: amount
        default_aggregation: sum
      - name: order_count
        expr: id
        default_aggregation: count
metrics:
  - name: revenue_per_order
    expr: orders.total_revenue / orders.order_count
""")
        return path

    def test_top_level_metric_is_not_overqualified(self, adapter, top_level_yaml):
        """Graph-level metric expressions must keep model.field references intact."""
        graph = adapter.parse(top_level_yaml)

        assert "revenue_per_order" in graph.metrics
        metric = graph.metrics["revenue_per_order"]
        assert metric.type == "derived"
        # Must NOT be corrupted with the {model} placeholder.
        assert "{model}" not in metric.sql
        assert metric.sql == "orders.total_revenue / orders.order_count"

    def test_top_level_metric_survives_roundtrip(self, adapter, top_level_yaml, tmp_path):
        """Graph-level metrics must be re-exported into the top-level metrics section."""
        graph = adapter.parse(top_level_yaml)

        output_file = tmp_path / "roundtrip.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        # Top-level metrics section must be present after export.
        assert "metrics" in data
        names = {m["name"]: m for m in data["metrics"]}
        assert "revenue_per_order" in names
        assert names["revenue_per_order"]["expr"] == "orders.total_revenue / orders.order_count"

        # And it must survive a full re-parse without being lost or corrupted.
        graph2 = adapter.parse(output_file)
        assert "revenue_per_order" in graph2.metrics
        assert graph2.metrics["revenue_per_order"].sql == "orders.total_revenue / orders.order_count"

    def test_table_scoped_metric_still_qualified(self, adapter, tmp_path):
        """Table-scoped derived metrics must still get the {model} placeholder."""
        path = tmp_path / "scoped.yaml"
        path.write_text("""
name: shop
tables:
  - name: orders
    base_table:
      table: orders
    primary_key:
      columns:
        - id
metrics:
  - name: weird_ratio
    table: orders
    expr: SUM(amount) / COUNT(id)
""")
        graph = adapter.parse(path)

        metric = graph.models["orders"].get_metric("weird_ratio")
        assert metric is not None
        assert metric.type == "derived"
        # Bare table-local columns must be qualified with {model}.
        assert "{model}.amount" in metric.sql
        assert "{model}.id" in metric.sql

    def test_export_skips_auto_registered_model_metrics(self, adapter, tmp_path):
        """Model-owned metrics auto-registered at graph level must not leak into top-level metrics.

        ``graph.add_model()`` registers ``time_comparison``/``conversion`` metrics in
        ``graph.metrics``. These are already serialized inside their owning table and
        have no valid Snowflake top-level representation, so export must skip them.
        """
        model = Model(
            name="orders",
            table="ORDERS",
            primary_key="id",
            metrics=[
                Metric(name="total_revenue", agg="sum", sql="amount"),
                Metric(name="revenue_yoy", type="time_comparison", base_metric="total_revenue", comparison_type="yoy"),
            ],
        )
        graph = SemanticGraph()
        graph.add_model(model)
        # Sanity check: the time_comparison metric is auto-registered at graph level.
        assert "revenue_yoy" in graph.metrics

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)

        with open(output_file) as f:
            data = yaml.safe_load(f)

        # No top-level metrics section should be emitted for model-owned metrics.
        assert "metrics" not in data
        # The export must still re-parse cleanly.
        adapter.parse(output_file)

    def test_export_preserves_top_level_metric_sharing_model_metric_name(self, adapter, tmp_path):
        """A distinct top-level metric must survive even if it shares a model-local name.

        The owned-metric skip in export must match by object identity, not name, so
        a genuine graph-level metric that merely shares a name with a table-local
        metric is not dropped on export.
        """
        model = Model(
            name="orders",
            table="ORDERS",
            primary_key="id",
            metrics=[Metric(name="summary", agg="sum", sql="amount")],
        )
        graph = SemanticGraph()
        graph.add_model(model)
        # Distinct graph-level derived metric that shares the name "summary".
        top_level = Metric(name="summary", type="derived", sql="orders.summary * 2")
        graph.metrics["summary"] = top_level
        assert graph.metrics["summary"] is not model.metrics[0]

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())

        # The distinct top-level metric is serialized to the top-level metrics block.
        assert [m["name"] for m in data.get("metrics", [])] == ["summary"]
        assert data["metrics"][0]["expr"] == "orders.summary * 2"
        # And the export still re-parses cleanly.
        adapter.parse(output_file)

    def test_export_skips_auto_registered_metric_by_identity_not_name(self, adapter, tmp_path):
        """Auto-registered model metrics (same object) are still skipped at top level."""
        model = Model(
            name="orders",
            table="ORDERS",
            primary_key="id",
            metrics=[
                Metric(name="total_revenue", agg="sum", sql="amount"),
                Metric(name="revenue_yoy", type="time_comparison", base_metric="total_revenue", comparison_type="yoy"),
            ],
        )
        graph = SemanticGraph()
        graph.add_model(model)
        # The time_comparison metric is the same object registered at graph level.
        assert graph.metrics["revenue_yoy"] is model.metrics[1]

        output_file = tmp_path / "export.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())

        assert "metrics" not in data
        adapter.parse(output_file)

    def test_roundtrip_preserves_using_relationships_and_relationship_name(self, adapter, tmp_path):
        """A metric `using_relationships` and the named relationship it points to must survive.

        Snowflake relationship `name` is referenced by metric `using_relationships`.
        Both the relationship name and the metric reference must round-trip, and the
        aggregate metric carrying `using_relationships` must be exported as a metric
        (not a fact) so the key is not dropped on re-parse.
        """
        source = tmp_path / "rel.yaml"
        source.write_text(
            """
name: rel_test
tables:
  - name: orders
    base_table: {database: db, schema: s, table: orders}
    primary_key: {columns: [order_id]}
    dimensions:
      - {name: order_id, expr: order_id, data_type: number}
      - {name: customer_id, expr: customer_id, data_type: number}
    metrics:
      - name: distinct_orders
        expr: COUNT(DISTINCT order_id)
        using_relationships: [orders_to_customers]
  - name: customers
    base_table: {database: db, schema: s, table: customers}
    primary_key: {columns: [id]}
    dimensions:
      - {name: id, expr: id, data_type: number}
relationships:
  - name: orders_to_customers
    left_table: orders
    right_table: customers
    relationship_columns:
      - {left_column: customer_id, right_column: id}
    relationship_type: many_to_one
    join_type: left_outer
"""
        )

        graph = adapter.parse(source)
        rel = graph.models["orders"].relationships[0]
        assert rel.metadata["snowflake"]["name"] == "orders_to_customers"

        output_file = tmp_path / "out.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())

        # The relationship name is re-emitted so references stay resolvable.
        assert [r["name"] for r in data["relationships"]] == ["orders_to_customers"]

        # The aggregate metric carrying using_relationships goes to metrics, not facts.
        orders_table = next(t for t in data["tables"] if t["name"] == "orders")
        assert "facts" not in orders_table or all(f["name"] != "distinct_orders" for f in orders_table["facts"])
        exported_metric = next(m for m in orders_table["metrics"] if m["name"] == "distinct_orders")
        assert exported_metric["using_relationships"] == ["orders_to_customers"]

        # Re-parse preserves both the relationship name and the metric reference.
        graph2 = adapter.parse(output_file)
        rel2 = graph2.models["orders"].relationships[0]
        assert rel2.metadata["snowflake"]["name"] == "orders_to_customers"
        metric2 = graph2.models["orders"].get_metric("distinct_orders")
        assert metric2.metadata["snowflake"]["using_relationships"] == ["orders_to_customers"]

    def test_roundtrip_aggregate_metric_with_non_additive_dimensions_stays_metric(self, adapter, tmp_path):
        """A simple aggregate metric carrying non_additive_dimensions exports as a metric."""
        source = tmp_path / "na.yaml"
        source.write_text(
            """
name: na_test
tables:
  - name: accounts
    base_table: {database: db, schema: s, table: accounts}
    primary_key: {columns: [id]}
    dimensions:
      - {name: id, expr: id, data_type: number}
    metrics:
      - name: max_balance
        expr: MAX(balance)
        non_additive_dimensions:
          - {table: accounts, dimension: snapshot_date}
"""
        )

        graph = adapter.parse(source)
        output_file = tmp_path / "out.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())

        accounts = next(t for t in data["tables"] if t["name"] == "accounts")
        # Routed to metrics, not facts, so the metric-only key keeps it a metric.
        assert "facts" not in accounts or all(f["name"] != "max_balance" for f in accounts["facts"])
        exported = next(m for m in accounts["metrics"] if m["name"] == "max_balance")
        assert exported["non_additive_dimensions"][0]["dimension"] == "snapshot_date"

        graph2 = adapter.parse(output_file)
        metric2 = graph2.models["accounts"].get_metric("max_balance")
        assert metric2.metadata["snowflake"]["non_additive_dimensions"][0]["dimension"] == "snapshot_date"

    def test_roundtrip_private_access_modifier_maps_to_public_false(self, adapter, tmp_path):
        """access_modifier: private_access marks the field non-public and round-trips."""
        source = tmp_path / "priv.yaml"
        source.write_text(
            """
name: priv_test
tables:
  - name: orders
    base_table: {database: db, schema: s, table: orders}
    primary_key: {columns: [id]}
    dimensions:
      - name: ssn
        expr: ssn
        data_type: text
        access_modifier: private_access
      - name: status
        expr: status
        data_type: text
        access_modifier: public_access
"""
        )

        graph = adapter.parse(source)
        ssn = graph.models["orders"].get_dimension("ssn")
        status = graph.models["orders"].get_dimension("status")
        assert ssn.public is False
        assert status.public is True

        output_file = tmp_path / "out.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())
        orders = data["tables"][0]
        exported_ssn = next(d for d in orders["dimensions"] if d["name"] == "ssn")
        assert exported_ssn["access_modifier"] == "private_access"

        graph2 = adapter.parse(output_file)
        assert graph2.models["orders"].get_dimension("ssn").public is False

    def test_public_false_overrides_stale_public_access_metadata(self, adapter, tmp_path):
        """public=False must win over a public_access modifier carried in metadata.

        A field imported with access_modifier: public_access keeps that value in
        metadata. If a user later sets public=False (native YAML/API) and exports
        back to Snowflake, the field must become private_access, not stay public.
        """
        source = tmp_path / "pub.yaml"
        source.write_text(
            """
name: pub_test
tables:
  - name: orders
    base_table: {database: db, schema: s, table: orders}
    primary_key: {columns: [id]}
    dimensions:
      - name: ssn
        expr: ssn
        data_type: text
        access_modifier: public_access
    facts:
      - name: amount
        expr: amount
        data_type: number
        access_modifier: public_access
    metrics:
      - name: total
        expr: SUM(amount)
        access_modifier: public_access
        non_additive_dimensions:
          - {table: orders, dimension: snapshot_date}
"""
        )

        graph = adapter.parse(source)
        orders = graph.models["orders"]
        # Imported public_access is preserved in metadata.
        assert orders.get_dimension("ssn").metadata["snowflake"]["access_modifier"] == "public_access"
        assert orders.get_metric("amount").metadata["snowflake"]["access_modifier"] == "public_access"
        assert orders.get_metric("total").metadata["snowflake"]["access_modifier"] == "public_access"

        # User flips visibility to private via the native API.
        orders.get_dimension("ssn").public = False
        orders.get_metric("amount").public = False
        orders.get_metric("total").public = False

        output_file = tmp_path / "out.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())
        table = data["tables"][0]
        exported_dim = next(d for d in table["dimensions"] if d["name"] == "ssn")
        exported_fact = next(f for f in table["facts"] if f["name"] == "amount")
        exported_metric = next(m for m in table["metrics"] if m["name"] == "total")
        assert exported_dim["access_modifier"] == "private_access"
        assert exported_fact["access_modifier"] == "private_access"
        assert exported_metric["access_modifier"] == "private_access"

        # Re-parsing keeps them non-public.
        graph2 = adapter.parse(output_file)
        orders2 = graph2.models["orders"]
        assert orders2.get_dimension("ssn").public is False
        assert orders2.get_metric("amount").public is False
        assert orders2.get_metric("total").public is False

    def test_public_true_drops_stale_private_access_metadata(self, adapter, tmp_path):
        """public=True must win over a stale private_access modifier in metadata.

        A field imported as private (access_modifier: private_access) keeps that in
        metadata. If a user flips public back to True and exports, the stale
        private_access must be dropped so the native visibility flag unhides it.
        """
        source = tmp_path / "priv.yaml"
        source.write_text(
            """
name: priv_test
tables:
  - name: orders
    base_table: {database: db, schema: s, table: orders}
    primary_key: {columns: [id]}
    dimensions:
      - name: ssn
        expr: ssn
        data_type: text
        access_modifier: private_access
    facts:
      - name: amount
        expr: amount
        data_type: number
        access_modifier: private_access
    metrics:
      - name: total
        expr: SUM(amount)
        access_modifier: private_access
        non_additive_dimensions:
          - {table: orders, dimension: snapshot_date}
"""
        )

        graph = adapter.parse(source)
        orders = graph.models["orders"]
        # Imported as private.
        assert orders.get_dimension("ssn").public is False
        assert orders.get_dimension("ssn").metadata["snowflake"]["access_modifier"] == "private_access"

        # User unhides via the native API.
        orders.get_dimension("ssn").public = True
        orders.get_metric("amount").public = True
        orders.get_metric("total").public = True

        output_file = tmp_path / "out.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())
        table = data["tables"][0]
        exported_dim = next(d for d in table["dimensions"] if d["name"] == "ssn")
        exported_fact = next(f for f in table["facts"] if f["name"] == "amount")
        exported_metric = next(m for m in table["metrics"] if m["name"] == "total")
        # Stale private_access is dropped (default public), not re-emitted.
        assert exported_dim.get("access_modifier") != "private_access"
        assert exported_fact.get("access_modifier") != "private_access"
        assert exported_metric.get("access_modifier") != "private_access"

        # Re-parsing keeps them public.
        graph2 = adapter.parse(output_file)
        orders2 = graph2.models["orders"]
        assert orders2.get_dimension("ssn").public is True
        assert orders2.get_metric("amount").public is True
        assert orders2.get_metric("total").public is True

    def test_export_strips_model_placeholder_from_table_scoped_metric(self, adapter, tmp_path):
        """Table-scoped derived metrics must not leak {model} placeholders into Snowflake."""
        source = tmp_path / "ph.yaml"
        source.write_text(
            """
name: ph_test
tables:
  - name: orders
    base_table: {database: db, schema: s, table: orders}
    primary_key: {columns: [id]}
    dimensions:
      - {name: id, expr: id, data_type: number}
    facts:
      - {name: amount, expr: amount, data_type: number}
metrics:
  - name: avg_order
    table: orders
    expr: SUM(amount) / COUNT(id)
"""
        )

        graph = adapter.parse(source)
        # Internally the table-scoped expression is qualified for queryability.
        assert "{model}" in graph.models["orders"].get_metric("avg_order").sql

        output_file = tmp_path / "out.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())
        orders = next(t for t in data["tables"] if t["name"] == "orders")
        expr = next(m["expr"] for m in orders["metrics"] if m["name"] == "avg_order")
        assert "{model}" not in expr
        assert expr == "SUM(amount) / COUNT(id)"

    def test_export_top_level_ratio_keeps_model_qualifiers(self, adapter, tmp_path):
        """Graph-level ratio metrics keep model.field qualifiers for cross-table refs."""
        graph = SemanticGraph()
        graph.add_model(
            Model(
                name="orders",
                table="ORDERS",
                primary_key="id",
                metrics=[
                    Metric(name="revenue", agg="sum", sql="amount"),
                    Metric(name="order_count", agg="count"),
                ],
            )
        )
        graph.add_metric(Metric(name="aov", type="ratio", numerator="orders.revenue", denominator="orders.order_count"))

        output_file = tmp_path / "out.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())
        expr = next(m["expr"] for m in data["metrics"] if m["name"] == "aov")
        assert expr == "orders.revenue / NULLIF(orders.order_count, 0)"

    def test_parse_directory_attaches_top_level_metric_regardless_of_file_order(self, adapter, tmp_path):
        """A top-level metric must attach to its table even if defined in an earlier file."""
        # rglob visits files in sorted order, so a_metrics is parsed before z_tables.
        (tmp_path / "a_metrics.yaml").write_text(
            """
name: a_metrics
metrics:
  - name: avg_order
    table: orders
    expr: SUM(amount) / COUNT(order_id)
"""
        )
        (tmp_path / "z_tables.yaml").write_text(
            """
name: z_tables
tables:
  - name: orders
    base_table: {database: db, schema: s, table: orders}
    primary_key: {columns: [order_id]}
    dimensions:
      - {name: order_id, expr: order_id, data_type: number}
    facts:
      - {name: amount, expr: amount, data_type: number}
"""
        )

        graph = adapter.parse(tmp_path)

        # The metric attaches to its table (not the graph-level branch).
        orders = graph.models["orders"]
        assert "avg_order" in [m.name for m in orders.metrics]
        assert "avg_order" not in graph.metrics
        # Table-scoped: complex expression is qualified for queryability.
        assert "{model}" in orders.get_metric("avg_order").sql

        # Export drops the placeholder and keeps the metric under the orders table.
        output_file = tmp_path / "out.yaml"
        adapter.export(graph, output_file)
        data = yaml.safe_load(output_file.read_text())
        orders_table = next(t for t in data["tables"] if t["name"] == "orders")
        expr = next(m["expr"] for m in orders_table["metrics"] if m["name"] == "avg_order")
        assert "{model}" not in expr
