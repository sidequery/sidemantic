"""Tests for auto-discovery loaders."""

from sidemantic import SemanticLayer
from sidemantic.loaders import load_from_directory


def test_load_from_directory_supports_sql_yaml_python(tmp_path):
    """Load mixed Sidemantic SQL, YAML, and Python definitions from one directory."""
    sql_file = tmp_path / "orders.sidemantic.sql"
    sql_file.write_text(
        """
MODEL (name orders, table orders, primary_key order_id);
DIMENSION (name customer_id, type categorical, sql customer_id);
METRIC (name order_count, agg count);
"""
    )

    yaml_file = tmp_path / "customers.sidemantic.yaml"
    yaml_file.write_text(
        """
models:
  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: id
        type: categorical
        sql: id
"""
    )

    python_file = tmp_path / "events.sidemantic.py"
    python_file.write_text(
        """
from sidemantic import Dimension, Metric, Model

events = Model(
    name="events",
    table="events",
    primary_key="id",
    dimensions=[Dimension(name="id", type="categorical", sql="id")],
    metrics=[Metric(name="event_count", agg="count")],
)
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert {"orders", "customers", "events"}.issubset(set(layer.graph.models))

    orders = layer.graph.models["orders"]
    assert any(relationship.name == "customers" for relationship in orders.relationships)

    events = layer.graph.models["events"]
    assert events.table == "events"
    assert events.metrics[0].name == "event_count"
    assert getattr(events, "_source_format", None) == "Python"
    assert getattr(events, "_source_file", None) == "events.sidemantic.py"


def test_load_from_directory_extracts_python_models_from_layer_variable(tmp_path):
    """Load Python definitions when file creates its own SemanticLayer instance."""
    python_file = tmp_path / "definitions.py"
    python_file.write_text(
        """
from sidemantic import Model, SemanticLayer

layer = SemanticLayer()
Model(name="sessions", table="sessions", primary_key="id")
"""
    )

    target_layer = SemanticLayer()
    load_from_directory(target_layer, tmp_path)

    assert "sessions" in target_layer.graph.models
    assert target_layer.graph.models["sessions"].table == "sessions"


def test_load_from_directory_detects_yardstick_sql(tmp_path):
    """Load Yardstick SQL definitions using AS MEASURE syntax."""
    sql_file = tmp_path / "sales.sql"
    sql_file.write_text(
        """
CREATE VIEW sales_v AS
SELECT
    year,
    region,
    SUM(amount) AS
    MEASURE revenue
FROM sales;
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "sales_v" in layer.graph.models
    sales = layer.graph.models["sales_v"]
    assert sales.get_metric("revenue") is not None
    assert getattr(sales, "_source_format", None) == "Yardstick"
    assert getattr(sales, "_source_file", None) == "sales.sql"


def test_load_from_directory_finalizes_bsl_join_aliases_across_files(tmp_path):
    """BSL join aliases should work when the target model is in a separate file."""
    (tmp_path / "flights.yml").write_text(
        """
flights:
  table: flights
  dimensions:
    flight_id:
      expr: _.flight_id
      is_entity: true
    origin: _.origin
  measures:
    count: _.count()
  joins:
    origin_airport:
      model: airports
      type: one
      left_on: origin
      right_on: code
"""
    )
    (tmp_path / "airports.yml").write_text(
        """
airports:
  table: airports
  dimensions:
    code:
      expr: _.code
      is_entity: true
    name: _.name
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "origin_airport" in layer.graph.models
    assert layer.graph.find_relationship_path("flights", "origin_airport")

    sql = layer.compile(metrics=["flights.count"], dimensions=["origin_airport.name"])
    assert "origin_airport_cte" in sql
    assert "origin_airport_cte.code = flights_cte.origin" in sql


def test_load_from_directory_scopes_reused_bsl_join_aliases(tmp_path):
    """BSL join aliases are local to their source model, even across files."""
    (tmp_path / "orders.yml").write_text(
        """
orders:
  table: orders
  dimensions:
    order_id:
      expr: _.order_id
      is_entity: true
    user_id: _.user_id
  measures:
    count: _.count()
  joins:
    user:
      model: customers
      type: one
      left_on: user_id
      right_on: customer_id
"""
    )
    (tmp_path / "events.yml").write_text(
        """
events:
  table: events
  dimensions:
    event_id:
      expr: _.event_id
      is_entity: true
    account_id: _.account_id
  measures:
    count: _.count()
  joins:
    user:
      model: accounts
      type: one
      left_on: account_id
      right_on: account_id
"""
    )
    (tmp_path / "customers.yml").write_text(
        """
customers:
  table: customers
  dimensions:
    customer_id:
      expr: _.customer_id
      is_entity: true
    name: _.name
"""
    )
    (tmp_path / "accounts.yml").write_text(
        """
accounts:
  table: accounts
  dimensions:
    account_id:
      expr: _.account_id
      is_entity: true
    name: _.name
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "user" not in layer.graph.models
    assert layer.graph.models["orders_user"].table == "customers"
    assert layer.graph.models["events_user"].table == "accounts"
    assert layer.graph.find_relationship_path("orders", "orders_user")
    assert layer.graph.find_relationship_path("events", "events_user")

    orders_sql = layer.compile(metrics=["orders.count"], dimensions=["orders_user.name"])
    assert "orders_user_cte" in orders_sql
    assert "FROM customers" in orders_sql

    events_sql = layer.compile(metrics=["events.count"], dimensions=["events_user.name"])
    assert "events_user_cte" in events_sql
    assert "FROM accounts" in events_sql


def test_load_from_directory_preserves_snowflake_top_level_sections(tmp_path):
    """CLI-first load -> export-native must round-trip Snowflake Cortex top-level sections."""
    import yaml

    from sidemantic.adapters.sidemantic import SidemanticAdapter

    (tmp_path / "cortex.yaml").write_text(
        """
name: cortex
tables:
  - name: orders
    base_table:
      database: db
      schema: s
      table: orders
    primary_key:
      columns: [order_id]
    dimensions:
      - name: order_id
        expr: order_id
        data_type: number
    measures:
      - name: order_total
        expr: total
        data_type: number
        default_aggregation: sum
verified_queries:
  - name: total revenue
    question: what is the total revenue
    sql: "SELECT SUM(total) FROM orders"
custom_instructions: Prefer revenue.
module_custom_instructions:
  sql_generation: Use explicit columns.
"""
    )

    layer = SemanticLayer(auto_register=False)
    load_from_directory(layer, tmp_path)
    graph = layer.graph

    # Top-level sections reach layer.graph (both as metadata and dynamic attrs).
    assert graph.metadata["snowflake"]["verified_queries"]
    assert graph.metadata["snowflake"]["custom_instructions"] == "Prefer revenue."
    assert getattr(graph, "verified_queries", None)
    assert getattr(graph, "custom_instructions", None) == "Prefer revenue."

    # export-native emits a root metadata block carrying them.
    out = tmp_path / "native.yml"
    SidemanticAdapter().export(graph, out)
    data = yaml.safe_load(out.read_text())
    assert data["metadata"]["snowflake"]["custom_instructions"] == "Prefer revenue."

    # And a native re-parse keeps them on graph.metadata.
    graph2 = SidemanticAdapter().parse(out)
    assert graph2.metadata["snowflake"]["verified_queries"]


def test_load_from_directory_merges_snowflake_metadata_across_files(tmp_path):
    """Multi-file Cortex projects must accumulate top-level sections, not overwrite."""
    (tmp_path / "a.yaml").write_text(
        """
name: a
tables:
  - name: orders
    base_table:
      database: db
      schema: s
      table: orders
    primary_key:
      columns: [id]
    dimensions:
      - name: id
        expr: id
        data_type: number
verified_queries:
  - name: q1
    question: x
    sql: SELECT 1
custom_instructions: from A
"""
    )
    (tmp_path / "b.yaml").write_text(
        """
name: b
tables:
  - name: customers
    base_table:
      database: db
      schema: s
      table: customers
    primary_key:
      columns: [id]
    dimensions:
      - name: id
        expr: id
        data_type: number
verified_queries:
  - name: q2
    question: y
    sql: SELECT 2
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)
    graph = layer.graph

    merged = graph.metadata["snowflake"]["verified_queries"]
    assert sorted(q["name"] for q in merged) == ["q1", "q2"]
    # Dynamic attribute accumulates too.
    assert len(getattr(graph, "verified_queries", [])) == 2


def test_load_from_directory_attaches_snowflake_metric_to_table_in_another_file(tmp_path):
    """A Snowflake top-level metric attaches to its table even if defined in another file."""
    # File A is Snowflake-detected (tables + base_table) and carries a top-level
    # metric referencing `orders`, which lives in file B.
    (tmp_path / "a_model.yaml").write_text(
        """
name: a_model
tables:
  - name: products
    base_table:
      database: db
      schema: s
      table: products
    primary_key:
      columns: [id]
    dimensions:
      - name: id
        expr: id
        data_type: number
metrics:
  - name: avg_order
    table: orders
    expr: SUM(amount) / COUNT(order_id)
"""
    )
    (tmp_path / "b_model.yaml").write_text(
        """
name: b_model
tables:
  - name: orders
    base_table:
      database: db
      schema: s
      table: orders
    primary_key:
      columns: [order_id]
    dimensions:
      - name: order_id
        expr: order_id
        data_type: number
    facts:
      - name: amount
        expr: amount
        data_type: number
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)
    graph = layer.graph

    assert set(graph.models) == {"products", "orders"}
    orders = graph.models["orders"]
    assert "avg_order" in [m.name for m in orders.metrics]
    assert "avg_order" not in graph.metrics
    metric = orders.get_metric("avg_order")
    # Table-scoped: complex expression re-qualified for queryability.
    assert "{model}" in metric.sql
    # The internal pending marker is cleaned up after attachment.
    assert (metric.metadata or {}).get("snowflake", {}).get("pending_table") is None


def test_load_from_directory_detects_metric_only_snowflake_file(tmp_path):
    """A Cortex file with only top-level metrics (table + expr) is routed to Snowflake."""
    # Metric-only file (no tables section) parsed before the table file.
    (tmp_path / "a_metrics.yaml").write_text(
        """
metrics:
  - name: avg_order
    table: orders
    expr: SUM(amount) / COUNT(order_id)
"""
    )
    (tmp_path / "z_tables.yaml").write_text(
        """
name: tables_model
tables:
  - name: orders
    base_table:
      database: db
      schema: s
      table: orders
    primary_key:
      columns: [order_id]
    dimensions:
      - name: order_id
        expr: order_id
        data_type: number
    facts:
      - name: amount
        expr: amount
        data_type: number
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)
    graph = layer.graph

    orders = graph.models["orders"]
    assert "avg_order" in [m.name for m in orders.metrics]
    assert "avg_order" not in graph.metrics
    assert "{model}" in orders.get_metric("avg_order").sql


def test_load_from_directory_detects_mixed_snowflake_metrics_file(tmp_path):
    """A metrics-only Cortex file may mix table-scoped and tableless view metrics."""
    # No tables section; one metric has table (table-scoped), one omits it (graph-level).
    (tmp_path / "a_metrics.yaml").write_text(
        """
metrics:
  - name: avg_order
    table: orders
    expr: SUM(amount) / COUNT(order_id)
  - name: global_ratio
    expr: orders.revenue / orders.order_count
"""
    )
    (tmp_path / "z_tables.yaml").write_text(
        """
name: tables_model
tables:
  - name: orders
    base_table:
      database: db
      schema: s
      table: orders
    primary_key:
      columns: [order_id]
    dimensions:
      - name: order_id
        expr: order_id
        data_type: number
    facts:
      - name: amount
        expr: amount
        data_type: number
    metrics:
      - name: revenue
        expr: SUM(amount)
      - name: order_count
        expr: COUNT(order_id)
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)
    graph = layer.graph

    # Table-scoped metric attaches to its table; tableless metric stays graph-level.
    assert "avg_order" in [m.name for m in graph.models["orders"].metrics]
    assert "global_ratio" in graph.metrics
    assert "avg_order" not in graph.metrics


def test_load_from_directory_detects_view_metric_sidecar_with_snowflake_sections(tmp_path):
    """A tableless Cortex sidecar with verified_queries routes to Snowflake."""
    # Pure view-level metrics (no table) plus Snowflake-only top-level sections.
    (tmp_path / "a_sidecar.yaml").write_text(
        """
metrics:
  - name: global_ratio
    expr: orders.revenue / orders.order_count
verified_queries:
  - name: total revenue
    sql: SELECT SUM(amount) FROM orders
custom_instructions: Prefer revenue.
"""
    )
    (tmp_path / "z_tables.yaml").write_text(
        """
name: tm
tables:
  - name: orders
    base_table:
      database: db
      schema: s
      table: orders
    primary_key:
      columns: [order_id]
    dimensions:
      - name: order_id
        expr: order_id
        data_type: number
    facts:
      - name: amount
        expr: amount
        data_type: number
    metrics:
      - name: revenue
        expr: SUM(amount)
      - name: order_count
        expr: COUNT(order_id)
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)
    graph = layer.graph

    assert "global_ratio" in graph.metrics
    snowflake_meta = graph.metadata.get("snowflake", {})
    assert snowflake_meta.get("verified_queries")
    assert snowflake_meta.get("custom_instructions") == "Prefer revenue."
