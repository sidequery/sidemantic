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


def test_load_from_directory_detects_released_osi_json(tmp_path):
    """Released-spec OSI .json (dbt OSI consumer) is routed to the OSI adapter."""
    osi_dir = tmp_path / "OSI"
    osi_dir.mkdir()
    (osi_dir / "model.json").write_text(
        """
{
  "version": "0.1.1",
  "semantic_model": [
    {
      "name": "released_analytics",
      "datasets": [
        {
          "name": "orders",
          "source": "db.schema.fct_orders",
          "primary_key": ["order_id"],
          "fields": [
            {
              "name": "order_id",
              "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "order_id"}]}
            }
          ]
        }
      ],
      "metrics": [
        {
          "name": "order_count",
          "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "COUNT(*)"}]}
        }
      ]
    }
  ]
}
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models
    orders = layer.graph.models["orders"]
    assert orders.table.endswith("fct_orders")
    assert getattr(orders, "_source_format", None) == "OSI"
    assert "order_count" in layer.graph.metrics


def test_load_from_directory_skips_generated_osi_json(tmp_path):
    """A dbt-generated target/ OSI document must not be loaded as a source model."""

    def _osi_json(dataset_name: str, source: str) -> str:
        return f"""
{{
  "version": "0.1.1",
  "semantic_model": [
    {{
      "name": "analytics",
      "datasets": [
        {{
          "name": "{dataset_name}",
          "source": "{source}",
          "primary_key": ["id"],
          "fields": [
            {{
              "name": "id",
              "expression": {{"dialects": [{{"dialect": "ANSI_SQL", "expression": "id"}}]}}
            }}
          ]
        }}
      ]
    }}
  ]
}}
"""

    osi_dir = tmp_path / "OSI"
    osi_dir.mkdir()
    (osi_dir / "model.json").write_text(_osi_json("orders", "db.schema.fct_orders"))

    # Simulate a stale `dbt compile` artifact containing a deleted/old model.
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "osi_document.json").write_text(_osi_json("stale_orders", "db.schema.old_orders"))

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models
    assert "stale_orders" not in layer.graph.models
