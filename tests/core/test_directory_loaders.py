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


def test_load_from_directory_detects_multi_document_hex(tmp_path):
    """Multi-document (``---``-separated) typed Hex files load via auto-discovery.

    ``yaml.safe_load`` rejects multi-document files, so without explicit Hex
    detection the documented CLI workflow could not load current Hex projects.
    """
    hex_file = tmp_path / "subscriptions_project.yml"
    hex_file.write_text(
        """
id: subscriptions
type: model
base_sql_table: analytics.subscriptions
dimensions:
  - id: customer_id
    type: string
    unique: true
  - id: snapshot_date
    type: date
measures:
  - id: total_mrr
    func: sum
    of: mrr
  - id: current_mrr
    func: sum
    of: mrr
    semi_additive:
      over:
        - dimension: snapshot_date
          pick: max
---
id: revenue_overview
type: view
base: subscriptions
contents:
  - name: Revenue
    measures:
      - total_mrr
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    # Both the model and the table-less view resource are registered.
    assert "subscriptions" in layer.graph.models
    assert "revenue_overview" in layer.graph.models

    view = layer.graph.models["revenue_overview"]
    assert view.meta.get("hex_resource_type") == "view"
    assert view.table is None

    # The typed model's semi-additive config survives through the CLI load path.
    assert layer.graph.models["subscriptions"].get_metric("current_mrr").non_additive_dimension == "snapshot_date"


def test_load_from_directory_detects_exported_hex_view(tmp_path):
    """A standalone exported ``type: view`` Hex file is detected by auto-discovery."""
    view_file = tmp_path / "revenue_overview.yml"
    view_file.write_text(
        """
id: revenue_overview
type: view
base: subscriptions
contents:
  - name: Revenue
    measures:
      - total_mrr
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "revenue_overview" in layer.graph.models
    assert layer.graph.models["revenue_overview"].meta.get("hex_resource_type") == "view"


def test_load_from_directory_detects_query_backed_hex(tmp_path):
    """Untyped query-backed Hex models (``base_sql_query``) load via auto-discovery.

    ``HexAdapter`` accepts ``base_sql_query`` as well as ``base_sql_table``, but
    directory auto-discovery previously required ``base_sql_table`` to select the
    Hex adapter, so query-backed Hex models were silently skipped on the
    documented CLI/MCP load path.
    """
    hex_file = tmp_path / "support_tickets.yml"
    hex_file.write_text(
        """
id: support_tickets
base_sql_query: |
  SELECT id, customer_id, status
  FROM support.tickets
dimensions:
  - id: id
    type: number
    unique: true
  - id: customer_id
    type: number
  - id: status
    type: string
measures:
  - id: ticket_count
    func: count
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "support_tickets" in layer.graph.models
    model = layer.graph.models["support_tickets"]
    assert model._source_format == "Hex"
    # The query-backed model carries its SQL, not a physical table reference.
    assert model.sql is not None
    assert model.table is None
    assert model.get_metric("ticket_count") is not None


_HEX_VIEW_BASE_MODEL = """
id: subscriptions
type: model
base_sql_table: analytics.subscriptions
dimensions:
  - id: id
    type: number
    unique: true
measures:
  - id: total
    func: count
---
"""


def test_validate_directory_flags_missing_hex_view_base(tmp_path):
    """A Hex view without a `base` reference is reported as an error, not a pass.

    Views are exempt from the physical-source check, so an omitted base would
    otherwise let `sidemantic validate` report Validation Passed for an
    unresolvable view.
    """
    from sidemantic.validation_runner import validate_directory

    (tmp_path / "project.yml").write_text(
        _HEX_VIEW_BASE_MODEL
        + """
id: revenue_overview
type: view
contents:
  - name: Revenue
    measures:
      - total
"""
    )

    report = validate_directory(tmp_path)
    assert not report.passed
    assert any("must have a 'base'" in err and "revenue_overview" in err for err in report.errors)


def test_validate_directory_flags_unknown_hex_view_base(tmp_path):
    """A Hex view whose `base` names no loaded model is reported as an error."""
    from sidemantic.validation_runner import validate_directory

    (tmp_path / "project.yml").write_text(
        _HEX_VIEW_BASE_MODEL
        + """
id: revenue_overview
type: view
base: subscriptionz
contents:
  - name: Revenue
    measures:
      - total
"""
    )

    report = validate_directory(tmp_path)
    assert not report.passed
    assert any("subscriptionz" in err and "doesn't exist" in err for err in report.errors)


def test_validate_directory_flags_hex_view_without_contents(tmp_path):
    """A Hex view with a valid `base` but no `contents` is reported as an error.

    Hex views require `contents`; without this check a view that omits it would
    report Validation Passed because views are exempt from the source check.
    """
    from sidemantic.validation_runner import validate_directory

    (tmp_path / "project.yml").write_text(
        _HEX_VIEW_BASE_MODEL
        + """
id: revenue_overview
type: view
base: subscriptions
"""
    )

    report = validate_directory(tmp_path)
    assert not report.passed
    assert any("contents" in err and "revenue_overview" in err for err in report.errors)


def test_validate_directory_accepts_valid_hex_view_base(tmp_path):
    """A Hex view with a `base` naming a loaded model emits no view errors."""
    from sidemantic.validation_runner import validate_directory

    (tmp_path / "project.yml").write_text(
        _HEX_VIEW_BASE_MODEL
        + """
id: revenue_overview
type: view
base: subscriptions
contents:
  - name: Revenue
    measures:
      - total
"""
    )

    report = validate_directory(tmp_path)
    assert not any("view" in err.lower() for err in report.errors)


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


def test_load_from_directory_accepts_osi_dir_as_loader_root(tmp_path):
    """Pointing the loader straight at the ``OSI/`` directory routes its JSON to OSI.

    dbt users are told to drop released-spec OSI documents in ``<project>/OSI/``,
    so ``sidemantic validate OSI/`` points the loader root at that folder itself.
    The file then sits directly in the root (relative_parts == ("model.json",)),
    and it must still be detected as OSI rather than reporting "No models found".
    """
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
    # Load the OSI directory directly, not its parent project root.
    load_from_directory(layer, osi_dir)

    assert "orders" in layer.graph.models
    orders = layer.graph.models["orders"]
    assert orders.table.endswith("fct_orders")
    assert getattr(orders, "_source_format", None) == "OSI"
    assert "order_count" in layer.graph.metrics


def test_load_from_directory_accepts_osi_dir_root_subdirectory(tmp_path):
    """When the loader root IS the ``OSI/`` directory, a document in a SUBDIRECTORY
    of it must also load. ``validate OSI/`` and ``validate <project>`` (which rglobs
    and only checks the leading ``OSI/`` component) must accept the same nested file.
    """
    sub = tmp_path / "OSI" / "models"
    sub.mkdir(parents=True)
    (sub / "model.json").write_text(
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
    # Load the OSI directory directly; the document lives one level deeper.
    load_from_directory(layer, tmp_path / "OSI")

    assert "orders" in layer.graph.models
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


def test_load_from_directory_skips_osi_json_outside_osi_tree(tmp_path):
    """OSI-shaped JSON outside the project-root OSI/ tree is ignored.

    dbt's OSI consumer only scans ``<project_root>/OSI/``. An archived or scratch
    OSI document under another folder (or sitting at the project root) must not
    add stale models or collide with the real OSI/ sources during
    ``sidemantic validate .``.
    """

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

    # An archived OSI document under a non-OSI/ folder.
    backups_dir = tmp_path / "backups"
    backups_dir.mkdir()
    (backups_dir / "old_osi.json").write_text(_osi_json("stale_orders", "db.schema.old_orders"))

    # A scratch OSI document sitting directly at the project root.
    (tmp_path / "scratch_osi.json").write_text(_osi_json("scratch_orders", "db.schema.scratch_orders"))

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models
    assert "stale_orders" not in layer.graph.models
    assert "scratch_orders" not in layer.graph.models


def test_load_from_directory_surfaces_malformed_osi_json(tmp_path):
    """Malformed OSI JSON is reported as a parse error, not silently skipped."""
    import pytest

    osi_dir = tmp_path / "OSI"
    osi_dir.mkdir()
    # OSI text markers (semantic_model + datasets) present, but the JSON is
    # truncated/malformed (trailing comma, missing closing braces).
    (osi_dir / "model.json").write_text(
        """
{
  "version": "0.1.1",
  "semantic_model": [
    {
      "name": "broken",
      "datasets": [
        {
          "name": "orders",
          "source": "db.schema.fct_orders",
        }
"""
    )

    layer = SemanticLayer()
    with pytest.raises(ValueError, match="model.json"):
        load_from_directory(layer, tmp_path, strict=True)

    # Non-strict mode must not raise and must not load anything from the bad file.
    non_strict_layer = SemanticLayer()
    load_from_directory(non_strict_layer, tmp_path, strict=False)
    assert "orders" not in non_strict_layer.graph.models
