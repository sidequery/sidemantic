from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import yaml

from sidemantic import SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.introspection import describe_graph
from sidemantic.dax.modeling import DaxModelingError

pytest.importorskip("sidemantic_dax")


def _write_native_dax_model(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: category
        type: categorical
      - name: doubled_amount
        type: numeric
        dax: "'sales'[amount] * 2"
    metrics:
      - name: revenue
        dax: "SUM('sales'[amount])"
"""
    )
    return path


def test_native_sidemantic_dax_authoring_lowers_and_preserves_source(tmp_path):
    layer = SemanticLayer.from_yaml(_write_native_dax_model(tmp_path))
    sales = layer.get_model("sales")

    doubled = sales.get_dimension("doubled_amount")
    assert doubled.sql == "(amount * 2)"
    assert doubled.dax == "'sales'[amount] * 2"
    assert doubled.expression_language == "dax"

    revenue = sales.get_metric("revenue")
    assert revenue.agg == "sum"
    assert revenue.sql == "amount"
    assert revenue.dax == "SUM('sales'[amount])"
    assert revenue.expression_language == "dax"

    sidemantic_sql = layer.compile(metrics=["sales.revenue"], dimensions=["sales.category"])
    assert "SUM(sales_cte.revenue_raw)" in sidemantic_sql


def test_native_sidemantic_expression_language_dax_uses_sql_text_as_dax_source(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    metrics:
      - name: revenue
        expression_language: dax
        sql: "SUM('sales'[amount])"
"""
    )

    layer = SemanticLayer.from_yaml(path)
    revenue = layer.get_model("sales").get_metric("revenue")

    assert revenue.agg == "sum"
    assert revenue.sql == "amount"
    assert revenue.dax == "SUM('sales'[amount])"
    assert revenue.expression_language == "dax"


def test_native_sidemantic_public_false_round_trips_for_model_items(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: internal_category
        type: categorical
        sql: category
        public: false
    metrics:
      - name: internal_revenue
        dax: "SUM('sales'[amount])"
        public: false
metrics:
  - name: global_internal
    type: derived
    sql: "1"
    public: false
"""
    )

    graph = SidemanticAdapter().parse(path)
    sales = graph.models["sales"]
    assert sales.get_dimension("internal_category").public is False
    assert sales.get_metric("internal_revenue").public is False
    assert graph.metrics["global_internal"].public is False

    output = tmp_path / "exported.yml"
    SidemanticAdapter().export(graph, output)
    exported = yaml.safe_load(output.read_text())
    exported_dimension = exported["models"][0]["dimensions"][0]
    exported_metric = exported["models"][0]["metrics"][0]
    exported_graph_metric = exported["metrics"][0]
    assert exported_dimension["public"] is False
    assert exported_metric["public"] is False
    assert exported_graph_metric["public"] is False


def test_native_sidemantic_graph_metric_expression_language_dax_lowers_and_exports(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: amount
        type: numeric
metrics:
  - name: revenue
    expression_language: dax
    sql: "SUM('sales'[amount])"
"""
    )

    graph = SidemanticAdapter().parse(path)
    revenue = graph.metrics["revenue"]

    assert revenue.agg == "sum"
    assert revenue.sql == "amount"
    assert revenue.dax == "SUM('sales'[amount])"
    assert revenue.expression_language == "dax"
    assert getattr(revenue, "_dax_lowered") is True

    output = tmp_path / "exported.yml"
    SidemanticAdapter().export(graph, output)
    exported = yaml.safe_load(output.read_text())
    exported_metric = exported["metrics"][0]
    assert exported_metric["dax"] == "SUM('sales'[amount])"
    assert exported_metric["expression_language"] == "dax"
    assert "sql" not in exported_metric


def test_native_sidemantic_graph_metric_expression_language_dax_requires_single_model_context(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
  - name: returns
    table: returns
    primary_key: id
metrics:
  - name: revenue
    expression_language: dax
    sql: "SUM('sales'[amount])"
"""
    )

    with pytest.raises(DaxModelingError, match="DAX graph metric 'revenue' needs a model context"):
        SidemanticAdapter().parse(path)


def test_native_sidemantic_expression_language_dax_for_dimensions_and_models(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: amount_doubled
        type: numeric
        expression_language: dax
        sql: "'sales'[amount] * 2"
  - name: positive_sales
    primary_key: id
    expression_language: dax
    sql: "FILTER('sales', 'sales'[amount] > 0)"
    dimensions:
      - name: id
        type: numeric
"""
    )

    graph = SidemanticAdapter().parse(path)
    amount_doubled = graph.models["sales"].get_dimension("amount_doubled")
    positive_sales = graph.models["positive_sales"]

    assert amount_doubled.sql == "(amount * 2)"
    assert amount_doubled.dax == "'sales'[amount] * 2"
    assert amount_doubled.expression_language == "dax"
    assert positive_sales.sql == "SELECT * FROM sales WHERE (amount > 0)"
    assert positive_sales.dax == "FILTER('sales', 'sales'[amount] > 0)"
    assert positive_sales.expression_language == "dax"


def test_native_sidemantic_model_level_dax_calculated_table_lowers(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: id
        type: numeric
      - name: amount
        type: numeric
  - name: positive_sales
    primary_key: id
    dax: "FILTER('sales', 'sales'[amount] > 0)"
    dimensions:
      - name: id
        type: numeric
"""
    )

    graph = SidemanticAdapter().parse(path)
    positive_sales = graph.models["positive_sales"]
    assert positive_sales.sql == "SELECT * FROM sales WHERE (amount > 0)"
    assert positive_sales.table is None
    assert positive_sales.dax == "FILTER('sales', 'sales'[amount] > 0)"
    assert positive_sales.expression_language == "dax"
    assert getattr(positive_sales, "_dax_required_models") == ["sales"]

    description = describe_graph(graph)
    positive_sales_info = next(model for model in description["models"] if model["name"] == "positive_sales")
    assert positive_sales_info["kind"] == "calculated_table"
    assert positive_sales_info["calculated_table"] is True
    assert positive_sales_info["dax"] == "FILTER('sales', 'sales'[amount] > 0)"
    assert positive_sales_info["original_expression"] == "FILTER('sales', 'sales'[amount] > 0)"
    assert positive_sales_info["dax_lowered"] is True
    assert positive_sales_info["dax_required_models"] == ["sales"]


def test_native_sidemantic_model_level_dax_overrides_table_source(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: id
        type: numeric
      - name: amount
        type: numeric
  - name: positive_sales
    table: should_not_remain
    primary_key: id
    expression_language: dax
    sql: "FILTER('sales', 'sales'[amount] > 0)"
    dimensions:
      - name: id
        type: numeric
"""
    )

    graph = SidemanticAdapter().parse(path)
    positive_sales = graph.models["positive_sales"]

    assert positive_sales.table is None
    assert positive_sales.sql == "SELECT * FROM sales WHERE (amount > 0)"
    assert positive_sales.dax == "FILTER('sales', 'sales'[amount] > 0)"
    assert describe_graph(graph)["models"][1]["kind"] == "calculated_table"


def test_native_sidemantic_model_level_dax_surfaces_cross_join_warnings(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: id
        type: numeric
  - name: products
    table: products
    primary_key: id
    dimensions:
      - name: category
        type: categorical
  - name: sales_products
    primary_key: id
    dax: "SUMMARIZECOLUMNS(sales[id], products[category])"
    dimensions:
      - name: id
        type: numeric
"""
    )

    graph = SidemanticAdapter().parse(path)
    warnings = getattr(graph, "import_warnings")

    assert graph.models["sales_products"].sql == (
        "SELECT sales.id, products.category FROM sales CROSS JOIN products GROUP BY sales.id, products.category"
    )
    assert warnings == [
        {
            "code": "dax_unrelated_cross_join",
            "context": "calculated_table",
            "model": "sales_products",
            "name": "sales_products",
            "message": (
                "DAX query cross joins unrelated table 'products' with 'sales' because no relationship path is defined"
            ),
        }
    ]


def test_native_sidemantic_export_preserves_dax_sources(tmp_path):
    layer = SemanticLayer.from_yaml(_write_native_dax_model(tmp_path))
    output = tmp_path / "exported.yml"

    SidemanticAdapter().export(layer.graph, output)

    exported = yaml.safe_load(output.read_text())
    sales = exported["models"][0]
    exported_dimension = next(dim for dim in sales["dimensions"] if dim["name"] == "doubled_amount")
    exported_metric = next(metric for metric in sales["metrics"] if metric["name"] == "revenue")

    assert exported_dimension["dax"] == "'sales'[amount] * 2"
    assert exported_dimension["expression_language"] == "dax"
    assert "sql" not in exported_dimension
    assert exported_metric["dax"] == "SUM('sales'[amount])"
    assert exported_metric["expression_language"] == "dax"
    assert "sql" not in exported_metric


def test_native_sidemantic_graph_metric_dax_lowers_exports_and_describes(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: category
        type: categorical
metrics:
  - name: revenue
    dax: "SUM('sales'[amount])"
"""
    )
    layer = SemanticLayer.from_yaml(path)

    revenue = layer.graph.metrics["revenue"]
    assert revenue.agg == "sum"
    assert revenue.sql == "amount"
    assert revenue.dax == "SUM('sales'[amount])"
    assert revenue.expression_language == "dax"

    compiled = layer.compile(metrics=["revenue"], dimensions=["sales.category"])
    assert "SUM(amount) AS revenue" in compiled

    description = layer.describe_models()
    graph_metric = description["metrics"][0]
    assert graph_metric["name"] == "revenue"
    assert graph_metric["source_format"] == "Sidemantic"
    assert graph_metric["source_file"] == "models.yml"
    assert graph_metric["dax"] == "SUM('sales'[amount])"
    assert graph_metric["original_expression"] == "SUM('sales'[amount])"
    assert graph_metric["dax_lowered"] is True
    assert graph_metric["faithful_lowering"] is True

    output = tmp_path / "exported.yml"
    SidemanticAdapter().export(layer.graph, output)
    exported = yaml.safe_load(output.read_text())
    assert exported["metrics"] == [
        {
            "name": "revenue",
            "dax": "SUM('sales'[amount])",
            "expression_language": "dax",
            "agg": "sum",
        }
    ]


def test_native_sidemantic_dax_authoring_rejects_invalid_dax(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    metrics:
      - name: revenue
        dax: "SUM("
"""
    )

    with pytest.raises(DaxModelingError, match="Could not parse DAX metric 'sales.revenue'"):
        SidemanticAdapter().parse(path)


@pytest.mark.parametrize(
    "yaml_text",
    [
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: doubled_amount
        type: numeric
        expression_language: sql
        dax: "'sales'[amount] * 2"
""",
        """
models:
  - name: sales
    table: sales
    primary_key: id
    metrics:
      - name: revenue
        expression_language: sql
        dax: "SUM('sales'[amount])"
""",
        """
models:
  - name: positive_sales
    primary_key: id
    expression_language: sql
    dax: "FILTER('sales', 'sales'[amount] > 0)"
""",
        """
models:
  - name: sales
    table: sales
    primary_key: id
metrics:
  - name: revenue
    expression_language: sql
    dax: "SUM('sales'[amount])"
""",
    ],
)
def test_native_sidemantic_dax_authoring_rejects_dax_source_with_sql_language(tmp_path, yaml_text):
    path = tmp_path / "models.yml"
    path.write_text(yaml_text)

    with pytest.raises(DaxModelingError, match="defines dax but expression_language='sql'"):
        SidemanticAdapter().parse(path)


def test_native_sidemantic_dax_authoring_requires_dax_extra(monkeypatch, tmp_path):
    import builtins

    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    metrics:
      - name: revenue
        dax: "SUM('sales'[amount])"
"""
    )

    real_import = builtins.__import__

    def _block_sidemantic_dax(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "sidemantic_dax" or name.startswith("sidemantic_dax."):
            raise ImportError("simulated missing sidemantic_dax")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _block_sidemantic_dax)

    with pytest.raises(DaxModelingError, match="sidemantic_dax is required for DAX model definitions"):
        SidemanticAdapter().parse(path)


@pytest.mark.parametrize(
    ("yaml_text", "message"),
    [
        (
            """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: bad_dimension
        type: numeric
        dax: "SUM("
""",
            "Could not parse DAX dimension 'sales.bad_dimension'",
        ),
        (
            """
models:
  - name: bad_table
    primary_key: id
    dax: "FILTER("
""",
            "Could not parse DAX model 'bad_table'",
        ),
    ],
)
def test_native_sidemantic_dax_authoring_rejects_invalid_dax_at_load_boundaries(tmp_path, yaml_text, message):
    path = tmp_path / "models.yml"
    path.write_text(yaml_text)

    with pytest.raises(DaxModelingError, match=message):
        SidemanticAdapter().parse(path)


@pytest.mark.parametrize(
    ("yaml_text", "message"),
    [
        (
            """
models:
  - name: sales
    table: sales
    primary_key: id
    metrics:
      - name: bad_metric
        dax: "UNKNOWNFUNC('sales'[amount])"
""",
            "DAX metric 'sales.bad_metric' is unsupported",
        ),
        (
            """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: bad_dimension
        type: numeric
        dax: "UNKNOWNFUNC('sales'[amount])"
""",
            "DAX dimension 'sales.bad_dimension' is unsupported",
        ),
        (
            """
models:
  - name: sales
    table: sales
    primary_key: id
  - name: bad_table
    primary_key: id
    dax: "UNKNOWNTABLEFN('sales')"
""",
            "DAX model 'bad_table' is unsupported",
        ),
        (
            """
models:
  - name: sales
    table: sales
    primary_key: id
  - name: returns
    table: returns
    primary_key: id
metrics:
  - name: bad_graph_metric
    dax: "SUM('sales'[amount])"
""",
            "DAX graph metric 'bad_graph_metric' needs a model context",
        ),
    ],
)
def test_native_sidemantic_dax_authoring_rejects_valid_unsupported_dax_at_load_boundaries(tmp_path, yaml_text, message):
    path = tmp_path / "models.yml"
    path.write_text(yaml_text)

    with pytest.raises(DaxModelingError, match=message):
        SidemanticAdapter().parse(path)


def test_semantic_layer_compile_and_query_dax_use_model_metrics(tmp_path):
    layer = SemanticLayer.from_yaml(_write_native_dax_model(tmp_path))
    layer.adapter.execute("CREATE TABLE sales (id INTEGER, category VARCHAR, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO sales VALUES (1, 'A', 10), (2, 'A', 5), (3, 'B', 7)")

    dax = """
    EVALUATE
        SUMMARIZECOLUMNS(
            'sales'[category],
            "Revenue", [revenue]
        )
    ORDER BY 'sales'[category] ASC
    """
    sql = layer.compile_dax_query(dax)
    assert "SUM(sales.amount) AS Revenue" in sql
    assert "revenue AS Revenue" not in sql

    rows = layer.query_dax(dax).fetchall()
    assert rows == [("A", 15.0), ("B", 7.0)]

    dry_run = layer.run_dax_query(dax, dry_run=True)
    assert dry_run == {
        "sql": sql,
        "rows": [],
        "row_count": 0,
        "warnings": [],
        "import_warnings": [],
    }

    payload = layer.run_dax_query(dax)
    assert payload["sql"] == sql
    assert payload["rows"] == [{"category": "A", "Revenue": 15.0}, {"category": "B", "Revenue": 7.0}]
    assert payload["row_count"] == 2
    assert payload["warnings"] == []
    assert payload["import_warnings"] == []
    json.dumps(payload)


def test_semantic_layer_dax_query_payload_preserves_translation_warnings(monkeypatch):
    layer = SemanticLayer()
    graph_warning = {"code": "query_warning", "message": "graph-level warning"}
    evaluate_warning = {"code": "evaluate_warning", "message": "evaluate-level warning"}

    monkeypatch.setattr(
        layer,
        "translate_dax_query",
        lambda _dax: SimpleNamespace(
            evaluates=[SimpleNamespace(sql="SELECT 1 AS one", warnings=[evaluate_warning])],
            warnings=[graph_warning],
        ),
    )

    payload = layer.compile_dax_query_payload('EVALUATE ROW("one", 1)')
    assert payload == {
        "sql": "SELECT 1 AS one",
        "warnings": [graph_warning, evaluate_warning],
        "import_warnings": [],
    }
    assert layer.run_dax_query('EVALUATE ROW("one", 1)', dry_run=True)["warnings"] == [
        graph_warning,
        evaluate_warning,
    ]


def test_semantic_layer_dax_query_payload_warns_on_unrelated_cross_join(tmp_path):
    path = tmp_path / "models.yml"
    path.write_text(
        """
models:
  - name: sales
    table: sales
    primary_key: id
    dimensions:
      - name: product_key
        type: categorical
  - name: products
    table: products
    primary_key: product_key
    dimensions:
      - name: category
        type: categorical
"""
    )
    layer = SemanticLayer.from_yaml(path)

    payload = layer.compile_dax_query_payload("EVALUATE SUMMARIZECOLUMNS('sales'[product_key], 'products'[category])")

    assert payload["sql"] == (
        "SELECT sales.product_key, products.category FROM sales CROSS JOIN products "
        "GROUP BY sales.product_key, products.category"
    )
    assert payload["warnings"] == [
        {
            "code": "dax_unrelated_cross_join",
            "context": "query",
            "base_table": "sales",
            "table": "products",
            "message": (
                "DAX query cross joins unrelated table 'products' with 'sales' because no relationship path is defined"
            ),
        }
    ]
    assert payload["import_warnings"] == []


def test_semantic_layer_describe_models_exposes_dax_metadata(tmp_path):
    layer = SemanticLayer.from_yaml(_write_native_dax_model(tmp_path))

    description = layer.describe_models()
    sales = description["models"][0]
    revenue = next(metric for metric in sales["metrics"] if metric["name"] == "revenue")
    doubled = next(dimension for dimension in sales["dimensions"] if dimension["name"] == "doubled_amount")

    assert description["import_warnings"] == []
    assert sales["kind"] == "table"
    assert "calculated_table" not in sales
    assert sales["source_format"] == "Sidemantic"
    assert sales["source_file"] == "models.yml"
    assert revenue["dax"] == "SUM('sales'[amount])"
    assert revenue["source_format"] == "Sidemantic"
    assert revenue["source_file"] == "models.yml"
    assert revenue["original_expression"] == "SUM('sales'[amount])"
    assert revenue["dax_lowered"] is True
    assert revenue["faithful_lowering"] is True
    assert revenue["public"] is True
    assert doubled["dax"] == "'sales'[amount] * 2"
    assert doubled["source_format"] == "Sidemantic"
    assert doubled["source_file"] == "models.yml"
    assert doubled["faithful_lowering"] is True


def test_semantic_layer_describe_models_marks_import_warning_status(tmp_path):
    layer = SemanticLayer.from_yaml(_write_native_dax_model(tmp_path))
    layer.graph.import_warnings = [
        {
            "code": "dax_translation_fallback",
            "context": "measure",
            "name": "revenue",
            "message": "simulated warning",
        }
    ]

    revenue = next(metric for metric in layer.describe_models()["models"][0]["metrics"] if metric["name"] == "revenue")

    assert revenue["unsupported"] is True
    assert revenue["faithful_lowering"] is False
    assert revenue["import_warnings"][0]["code"] == "dax_translation_fallback"
