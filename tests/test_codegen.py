"""Tests for `sidemantic gen types` / `sidemantic gen sql` TypeScript codegen."""

from pathlib import Path

from typer.testing import CliRunner

from sidemantic import Dimension, Metric, Model, Parameter, SemanticLayer, load_from_directory
from sidemantic.cli import app
from sidemantic.codegen import (
    _output_columns,
    analyze_sql,
    build_client_schema,
    expand_sources,
    extract_sql_literals,
    generate_client_schema_ts,
    generate_sql_types_ts,
)

runner = CliRunner()

EXAMPLE_DIR = Path(__file__).resolve().parents[1] / "examples" / "headless_dashboard"

MODELS_YAML = """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
      - name: created_at
        type: time
        granularity: day
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
        sql: id
"""


def _orders_layer() -> SemanticLayer:
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="created_at", type="time", granularity="day"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count", sql="id"),
            ],
        )
    )
    return layer


def _orders_layer_with_top_metric() -> SemanticLayer:
    layer = _orders_layer()
    layer.add_metric(
        Metric(
            name="finance.revenue_per_order",
            type="ratio",
            numerator="orders.revenue",
            denominator="orders.order_count",
        )
    )
    return layer


def _orders_default_time_layer() -> SemanticLayer:
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            default_time_dimension="created_at",
            default_grain="month",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="created_at", type="time", granularity="day"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    return layer


def _two_model_layer() -> SemanticLayer:
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="customer_count", agg="count", sql="id")],
        )
    )
    return layer


class _Explanation:
    """Minimal stand-in for RewriteExplanation for unit-testing column aliasing."""

    def __init__(self, dimensions, metrics, aliases=None):
        self.dimensions = dimensions
        self.metrics = metrics
        self.aliases = aliases or {}


def _write_models(tmp_path: Path) -> Path:
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "models.yml").write_text(MODELS_YAML)
    return models_dir


# --- gen types (structured client schema) ---


def test_build_client_schema_resolves_field_types():
    schema = build_client_schema(_orders_layer())
    orders = schema["models"]["orders"]
    assert orders["metrics"]["revenue"] == {"agg": "sum", "ts": "number"}
    assert orders["metrics"]["order_count"]["ts"] == "number"
    assert orders["dimensions"]["status"] == {"kind": "categorical", "ts": "string"}
    assert orders["dimensions"]["created_at"]["kind"] == "time"
    assert orders["dimensions"]["created_at"]["ts"] == "string"
    assert "month" in orders["dimensions"]["created_at"]["grains"]
    assert schema["topMetrics"] == []


def test_build_client_schema_preserves_top_metric_names():
    schema = build_client_schema(_orders_layer_with_top_metric())
    assert schema["topMetrics"] == ["finance.revenue_per_order"]


def test_build_client_schema_keeps_top_metric_sharing_model_metric_name():
    # A graph-level metric whose name collides with a model metric leaf name must stay in
    # topMetrics (the runtime resolves the bare ref) instead of being dropped by a name filter.
    layer = _orders_layer()  # orders.metrics: revenue, order_count
    layer.graph.add_metric(
        Metric(name="revenue", type="ratio", numerator="orders.revenue", denominator="orders.order_count")
    )
    schema = build_client_schema(layer)
    assert "revenue" in schema["topMetrics"]


def test_generate_client_schema_ts_emits_as_const():
    ts = generate_client_schema_ts(_orders_layer(), include_yaml=False)
    assert ts.startswith("/* Generated by `sidemantic gen types`")
    assert "export const schema = {" in ts
    assert "} as const;" in ts
    assert '"ts": "number"' in ts
    assert "SCHEMA_YAML" not in ts

    with_yaml = generate_client_schema_ts(_orders_layer(), include_yaml=True)
    assert "export const SCHEMA_YAML =" in with_yaml


def test_gen_types_cli_writes_file(tmp_path):
    models_dir = _write_models(tmp_path)
    out = tmp_path / "schema.ts"
    result = runner.invoke(app, ["gen", "types", "-m", str(models_dir), "--out", str(out)])
    assert result.exit_code == 0, result.output
    text = out.read_text()
    assert "export const schema" in text
    assert '"ts": "number"' in text
    assert "export const SCHEMA_YAML =" in text  # included by default


def test_gen_types_cli_no_yaml(tmp_path):
    models_dir = _write_models(tmp_path)
    result = runner.invoke(app, ["gen", "types", "-m", str(models_dir), "--no-yaml"])
    assert result.exit_code == 0, result.output
    assert "SCHEMA_YAML" not in result.output


def test_gen_types_cli_accepts_model_file(tmp_path):
    models_file = _write_models(tmp_path) / "models.yml"
    result = runner.invoke(app, ["gen", "types", "-m", str(models_file), "--no-yaml"])
    assert result.exit_code == 0, result.output
    assert "export const schema" in result.output


def test_gen_types_cli_model_file_ignores_siblings(tmp_path):
    # A single-file -m must load only that file: a sibling model is not pulled in,
    # and an unrelated broken draft beside it does not fail the valid request.
    models_dir = _write_models(tmp_path)
    (models_dir / "customers.yml").write_text(
        "models:\n  - name: customers\n    table: customers\n    primary_key: id\n"
        "    dimensions: [{name: tier, type: categorical, sql: tier}]\n"
    )
    (models_dir / "broken.yml").write_text("models:\n  - name: oops\n    table: [unclosed\n")
    result = runner.invoke(app, ["gen", "types", "-m", str(models_dir / "models.yml"), "--no-yaml"])
    assert result.exit_code == 0, result.output
    assert '"orders"' in result.output
    assert "customers" not in result.output


def test_gen_types_cli_python_model_file_keeps_parent_context(tmp_path):
    # A single Python semantic file is loaded in place (its real parent on sys.path), so a
    # sibling import still resolves — loading it via a temp-dir copy would break that import.
    (tmp_path / "helper.py").write_text("TABLE = 'orders'\n")
    (tmp_path / "sidemantic.py").write_text(
        "from helper import TABLE\n"
        "from sidemantic import Dimension, Metric, Model\n"
        "orders = Model(name='orders', table=TABLE, primary_key='id',\n"
        "  dimensions=[Dimension(name='region', type='categorical', sql='region')],\n"
        "  metrics=[Metric(name='revenue', agg='sum', sql='amount')])\n"
    )
    result = runner.invoke(app, ["gen", "types", "-m", str(tmp_path / "sidemantic.py"), "--no-yaml"])
    assert result.exit_code == 0, result.output
    assert '"orders"' in result.output


# --- gen sql (sqlx-style typed semantic SQL) ---


def test_extract_sql_literals_skips_dynamic_and_object_args(tmp_path):
    src = tmp_path / "q.ts"
    src.write_text(
        "db.query(`SELECT orders.status FROM orders`);\n"
        'db.query("SELECT orders.revenue FROM orders");\n'
        "db.query(`SELECT ${dynamic} FROM orders`);\n"
        'client.query({ metrics: ["orders.revenue"] });\n'
    )
    literals = extract_sql_literals(expand_sources([src]))
    assert "SELECT orders.status FROM orders" in literals
    assert "SELECT orders.revenue FROM orders" in literals
    assert all("${" not in literal for literal in literals)
    assert len(literals) == 2


def test_extract_sql_literals_ignores_commented_out_calls(tmp_path):
    src = tmp_path / "q.ts"
    src.write_text(
        "db.query(`SELECT orders.status FROM orders`);\n"
        '// db.query("SELECT orders.revnue FROM orders");\n'
        '/* db.query("SELECT bogus FROM nope") */\n'
        "db.query(\"SELECT orders.region FROM orders WHERE url LIKE 'http://x'\");\n"
    )
    literals = extract_sql_literals(expand_sources([src]))
    assert "SELECT orders.status FROM orders" in literals
    # `//` inside a quoted SQL string is not treated as a comment.
    assert "SELECT orders.region FROM orders WHERE url LIKE 'http://x'" in literals
    # Commented-out calls (line + block) are not scanned.
    assert not any("revnue" in literal for literal in literals)
    assert not any("bogus" in literal for literal in literals)


def test_analyze_sql_types_rows_and_params():
    layer = _orders_layer()
    columns, params = analyze_sql(
        layer,
        "SELECT orders.status, orders.revenue FROM orders WHERE orders.created_at >= {{ start }}",
    )
    assert ("status", "string") in columns
    assert ("revenue", "number") in columns
    assert params == {"start": "string"}


def test_generate_sql_types_ts_shapes_interface():
    ts = generate_sql_types_ts(_orders_layer(), ["SELECT orders.status, orders.revenue FROM orders"])
    assert "export interface GeneratedQueries" in ts
    assert '"status": string' in ts
    assert '"revenue": number' in ts
    assert "export const queryParamTypes" in ts


def test_generate_sql_types_ts_includes_default_time_dimension():
    ts = generate_sql_types_ts(_orders_default_time_layer(), ["SELECT orders.revenue FROM orders"])
    assert '"created_at__month": string' in ts
    assert '"revenue": number' in ts


def test_generate_sql_types_ts_preserves_top_level_dotted_metric_alias():
    ts = generate_sql_types_ts(_orders_layer_with_top_metric(), ["SELECT finance.revenue_per_order FROM orders"])
    assert '"finance.revenue_per_order": number' in ts
    assert '"revenue_per_order": number' not in ts


def test_generate_sql_types_ts_emits_unquoted_param_metadata():
    layer = _orders_layer()
    layer.graph.add_parameter(Parameter(name="table_name", type="unquoted"))
    ts = generate_sql_types_ts(layer, ["SELECT orders.revenue FROM orders WHERE orders.status = {{ table_name }}"])
    assert '"table_name": string' in ts
    assert '"table_name": "unquoted"' in ts


def test_output_columns_renames_leaf_collisions():
    layer = _two_model_layer()
    columns = _output_columns(
        layer.graph,
        _Explanation(["orders.region", "customers.region"], ["orders.revenue"]),
    )
    names = [name for name, _ in columns]
    assert "orders_region" in names
    assert "customers_region" in names
    assert "revenue" in names  # unique leaf keeps the bare alias


def test_gen_sql_cli_writes_typed_bindings(tmp_path):
    models_dir = _write_models(tmp_path)
    src = tmp_path / "queries.ts"
    src.write_text('export const a = () => db.query("SELECT orders.status, orders.revenue FROM orders");')
    out = tmp_path / "queries.generated.ts"
    result = runner.invoke(app, ["gen", "sql", "-m", str(models_dir), str(src), "--out", str(out)])
    assert result.exit_code == 0, result.output
    text = out.read_text()
    assert "export interface GeneratedQueries" in text
    assert '"status": string' in text
    assert '"revenue": number' in text


def test_gen_sql_cli_accepts_model_file(tmp_path):
    models_file = _write_models(tmp_path) / "models.yml"
    src = tmp_path / "queries.ts"
    src.write_text('db.query("SELECT orders.status FROM orders");')
    result = runner.invoke(app, ["gen", "sql", "-m", str(models_file), str(src)])
    assert result.exit_code == 0, result.output
    assert '"status": string' in result.output


def test_gen_sql_cli_preserves_explicit_alias(tmp_path):
    models_dir = _write_models(tmp_path)
    src = tmp_path / "aliased.ts"
    src.write_text('db.query("SELECT orders.revenue AS sales, orders.status FROM orders");')
    out = tmp_path / "aliased.generated.ts"
    result = runner.invoke(app, ["gen", "sql", "-m", str(models_dir), str(src), "--out", str(out)])
    assert result.exit_code == 0, result.output
    text = out.read_text()
    assert '"sales": number' in text  # explicit alias becomes the row key
    assert '"revenue"' not in text


def test_gen_sql_cli_rejects_unknown_reference(tmp_path):
    models_dir = _write_models(tmp_path)
    src = tmp_path / "bad.ts"
    src.write_text("db.query(`SELECT orders.revnue FROM orders`);")
    result = runner.invoke(app, ["gen", "sql", "-m", str(models_dir), str(src)])
    assert result.exit_code != 0
    assert "not found" in result.output.lower() or "revnue" in result.output


def test_gen_sql_cli_requires_sources(tmp_path):
    models_dir = _write_models(tmp_path)
    result = runner.invoke(app, ["gen", "sql", "-m", str(models_dir)])
    assert result.exit_code != 0


# --- committed example stays in sync ---


def test_committed_headless_dashboard_codegen_in_sync():
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    load_from_directory(layer, EXAMPLE_DIR)

    client_generated = (EXAMPLE_DIR / "sidemantic.client.generated.ts").read_text()
    assert client_generated == generate_client_schema_ts(layer, include_yaml=False)

    literals = extract_sql_literals(expand_sources([EXAMPLE_DIR / "queries.ts"]))
    queries_generated = (EXAMPLE_DIR / "sidemantic.queries.generated.ts").read_text()
    assert queries_generated == generate_sql_types_ts(layer, literals)


def test_apply_default_time_dimensions_resolves_graph_metric_owner():
    from sidemantic.codegen import _apply_default_time_dimensions, _metric_owner_models

    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            default_time_dimension="created_at",
            default_grain="month",
            dimensions=[Dimension(name="created_at", type="time", granularity="day")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount"), Metric(name="cnt", agg="count", sql="id")],
        )
    )
    # Namespaced top-level metric whose namespace ("finance") is not a model.
    layer.graph.add_metric(
        Metric(name="finance.revenue_per_order", type="ratio", numerator="orders.revenue", denominator="orders.cnt")
    )
    graph = layer.graph

    assert _metric_owner_models(graph, "finance.revenue_per_order") == {"orders"}
    # The owner model's default time dimension is pulled in (previously skipped because the
    # namespace was mistaken for a model).
    assert _apply_default_time_dimensions(graph, ["finance.revenue_per_order"], []) == ["orders.created_at__month"]
