"""Tests for directory loaders."""

import builtins
import sys
from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.loaders import load_from_directory


def test_load_from_directory_does_not_require_antlr4_without_antlr_formats(tmp_path, monkeypatch):
    """Cube loading should work even when ANTLR runtime is unavailable."""
    fixture_file = Path(__file__).parent / "fixtures" / "cube" / "orders.yml"
    (tmp_path / "orders.yml").write_text(fixture_file.read_text())

    monkeypatch.delitem(sys.modules, "sidemantic.adapters.holistics", raising=False)
    for module_name in list(sys.modules):
        if module_name == "antlr4" or module_name.startswith("antlr4."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    real_import = builtins.__import__

    def blocked_antlr4_import(name, *args, **kwargs):
        if name == "antlr4" or name.startswith("antlr4."):
            raise ImportError("simulated missing antlr4")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_antlr4_import)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models


def test_load_from_directory_strict_raises_on_detected_parse_error(tmp_path):
    """Strict loading fails instead of returning a partial graph."""
    (tmp_path / "good.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
"""
    )
    (tmp_path / "bad.yml").write_text(
        """
models:
  - name: broken
    table: [
"""
    )

    layer = SemanticLayer()
    with pytest.raises(ValueError, match="Could not parse .*bad.yml"):
        load_from_directory(layer, tmp_path)

    assert not layer.graph.models


def test_load_from_directory_lenient_mode_skips_detected_parse_error(tmp_path):
    """Lenient loading remains available as an explicit opt-in."""
    (tmp_path / "good.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
"""
    )
    (tmp_path / "bad.yml").write_text(
        """
models:
  - name: broken
    table: [
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=False)

    assert set(layer.graph.models) == {"orders"}


def test_load_from_directory_resolves_native_inheritance_across_files(tmp_path):
    (tmp_path / "base.yml").write_text(
        """
version: 1
models:
  - name: base_orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"""
    )
    (tmp_path / "child.yml").write_text(
        """
version: 1
models:
  - name: paid_orders
    extends: base_orders
    dimensions:
      - name: paid_at
        type: time
        sql: paid_at
        granularity: day
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    paid_orders = layer.graph.models["paid_orders"]
    assert paid_orders.table == "orders"
    assert paid_orders.primary_key == "id"
    assert paid_orders.extends is None
    assert paid_orders.get_dimension("status") is not None
    assert paid_orders.get_dimension("paid_at") is not None
    assert paid_orders.get_metric("revenue") is not None


def test_load_from_directory_resolves_native_metric_inheritance_after_model_merge(tmp_path):
    (tmp_path / "base.yml").write_text(
        """
version: 1
models:
  - name: base_orders
    table: orders
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"""
    )
    (tmp_path / "child.yml").write_text(
        """
version: 1
models:
  - name: paid_orders
    extends: base_orders
    metrics:
      - name: paid_revenue
        extends: revenue
        filters:
          - status = 'paid'
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    paid_revenue = layer.graph.models["paid_orders"].get_metric("paid_revenue")
    assert paid_revenue is not None
    assert paid_revenue.extends is None
    assert paid_revenue.agg == "sum"
    assert paid_revenue.sql == "amount"
    assert paid_revenue.filters == ["status = 'paid'"]


def test_load_from_directory_detects_native_metrics_only_file(tmp_path):
    (tmp_path / "models.yml").write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
"""
    )
    (tmp_path / "metrics.yml").write_text(
        """
version: 1
metrics:
  - name: finance.revenue_per_order
    type: ratio
    numerator: orders.revenue
    denominator: orders.order_count
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    metric = layer.graph.metrics["finance.revenue_per_order"]
    assert metric.numerator == "orders.revenue"
    assert metric.denominator == "orders.order_count"


def test_load_from_directory_detects_unversioned_native_metrics_only_file(tmp_path):
    """Native metrics-only files without a version key must not route to MetricFlow."""
    (tmp_path / "models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
"""
    )
    (tmp_path / "metrics.yml").write_text(
        """
metrics:
  - name: total_revenue
    sql: orders.revenue

  - name: completion_rate
    type: ratio
    numerator: orders.revenue
    denominator: orders.order_count

  - name: revenue_per_order
    type: derived
    sql: total_revenue / order_count
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    metric = layer.graph.metrics["completion_rate"]
    assert metric._source_format == "Sidemantic"
    assert metric.numerator == "orders.revenue"
    assert metric.denominator == "orders.order_count"
    assert layer.graph.metrics["total_revenue"].sql == "orders.revenue"
    assert layer.graph.metrics["revenue_per_order"].type == "derived"


def test_load_from_directory_loads_ecommerce_example():
    """The ecommerce example uses unversioned native metric files and must load."""
    examples_dir = Path(__file__).parent.parent / "examples" / "ecommerce" / "models"

    layer = SemanticLayer()
    load_from_directory(layer, examples_dir)

    assert "orders" in layer.graph.models
    metric = layer.graph.metrics["completion_rate"]
    assert metric.numerator == "orders.completed_orders"
    assert metric.denominator == "orders.order_count"


def test_load_from_directory_routes_metricflow_metrics_only_file_to_metricflow(tmp_path):
    """MetricFlow metrics files keep routing to MetricFlow via type_params."""
    (tmp_path / "metrics.yml").write_text(
        """
metrics:
  - name: revenue_per_order
    type: ratio
    type_params:
      numerator:
        name: revenue
      denominator:
        name: order_count
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    metric = layer.graph.metrics["revenue_per_order"]
    assert metric._source_format == "MetricFlow"
    assert metric.numerator == "revenue"
    assert metric.denominator == "order_count"


def test_load_from_directory_resolves_native_graph_metric_inheritance_across_files(tmp_path):
    (tmp_path / "base_metrics.yml").write_text(
        """
version: 1
metrics:
  - name: gross_revenue
    agg: sum
    sql: orders.amount
"""
    )
    (tmp_path / "child_metrics.yml").write_text(
        """
version: 1
metrics:
  - name: paid_revenue
    extends: gross_revenue
    filters:
      - orders.status = 'paid'
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    metric = layer.graph.metrics["paid_revenue"]
    assert metric.extends is None
    assert metric.agg == "sum"
    assert metric.sql == "orders.amount"
    assert metric.filters == ["orders.status = 'paid'"]


def test_load_from_directory_strict_raises_on_missing_native_graph_metric_parent(tmp_path):
    (tmp_path / "metrics.yml").write_text(
        """
version: 1
metrics:
  - name: paid_revenue
    extends: missing_revenue
    filters:
      - orders.status = 'paid'
"""
    )

    layer = SemanticLayer()
    with pytest.raises(ValueError, match="Native metric 'paid_revenue' extends unknown metric 'missing_revenue'"):
        load_from_directory(layer, tmp_path)


def test_load_from_directory_strict_raises_on_missing_native_parent(tmp_path):
    (tmp_path / "child.yml").write_text(
        """
version: 1
models:
  - name: paid_orders
    extends: missing_base
    table: orders
"""
    )

    layer = SemanticLayer()
    with pytest.raises(ValueError, match="Native model 'paid_orders' extends unknown model 'missing_base'"):
        load_from_directory(layer, tmp_path)

    assert not layer.graph.models


def test_native_inheritance_does_not_register_model_metrics_globally(tmp_path):
    (tmp_path / "models.yml").write_text(
        """
models:
  - name: base_orders
    table: orders
    primary_key: order_id
    metrics:
      - name: margin_label
        type: derived
        sql: "'margin'"

  - name: orders
    extends: base_orders
    table: orders
    primary_key: order_id
"""
    )

    layer = SemanticLayer(auto_register=True)
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models
    assert layer.graph.models["orders"].get_metric("margin_label") is not None
    assert "margin_label" not in layer.graph.metrics


def test_load_from_directory_lenient_surfaces_adapter_parse_failures(tmp_path, monkeypatch):
    from sidemantic.adapters.sidemantic import SidemanticAdapter

    (tmp_path / "broken.yml").write_text("models:\n  - name: broken\n")

    def _raise_parse_failure(self, path):
        raise ValueError("simulated native yaml failure")

    monkeypatch.setattr(SidemanticAdapter, "parse", _raise_parse_failure)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=False)

    warnings = layer.describe_models()["import_warnings"]
    assert warnings == [
        {
            "code": "adapter_parse_error",
            "context": "loader",
            "source_format": "Sidemantic",
            "source_file": "broken.yml",
            "message": "simulated native yaml failure",
        }
    ]


def test_load_from_directory_strict_raises_tmdl_project_parse_failures(tmp_path, monkeypatch):
    from sidemantic.adapters.tmdl import TMDLAdapter

    tmdl_file = tmp_path / "definition" / "tables" / "Sales.tmdl"
    tmdl_file.parent.mkdir(parents=True)
    tmdl_file.write_text("table Sales\n")

    def _raise_parse_failure(self, path):
        raise ValueError("simulated tmdl failure")

    monkeypatch.setattr(TMDLAdapter, "parse", _raise_parse_failure)

    layer = SemanticLayer()
    with pytest.raises(ValueError, match="Could not parse .*definition"):
        load_from_directory(layer, tmp_path)


def test_load_from_directory_lenient_does_not_partially_parse_tmdl_project_after_project_failure(tmp_path, monkeypatch):
    from sidemantic.adapters.tmdl import TMDLAdapter
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    definition_dir = tmp_path / "definition"
    tmdl_file = definition_dir / "tables" / "Sales.tmdl"
    tmdl_file.parent.mkdir(parents=True)
    tmdl_file.write_text("table Sales\n")

    calls: list[Path] = []

    def _parse_project_only(self, path):
        source = Path(path)
        calls.append(source)
        if source.is_dir():
            raise ValueError("simulated project-level failure")
        graph = SemanticGraph()
        graph.add_model(Model(name="PartialSales", table="sales", primary_key="id"))
        return graph

    monkeypatch.setattr(TMDLAdapter, "parse", _parse_project_only)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=False)

    assert calls == [definition_dir]
    assert layer.graph.models == {}
    assert layer.describe_models()["import_warnings"] == [
        {
            "code": "tmdl_parse_error",
            "context": "loader",
            "source_format": "TMDL",
            "source_file": "definition",
            "message": "simulated project-level failure",
        }
    ]


def test_load_from_directory_strips_template_join_after_native_overwrite(tmp_path):
    """A LookML explore join to an `extension: required` template must be stripped even when a later
    native model of the same name overwrites the template (so the join does not silently retarget it)."""
    (tmp_path / "a.model.lkml").write_text(
        "view: base {\n"
        "  extension: required\n"
        "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }\n"
        "}\n"
        "view: orders {\n"
        "  sql_table_name: orders ;;\n"
        "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }\n"
        "  dimension: ref_col { type: number  sql: ${TABLE}.ref_col ;; }\n"
        "}\n"
        "explore: orders {\n"
        "  join: base { sql_on: ${orders.ref_col} = ${base.id} ;; relationship: many_to_one }\n"
        "}\n"
    )
    (tmp_path / "native.py").write_text(
        "from sidemantic import Model, Dimension\n"
        "base = Model(name='base', table='real_base', primary_key='id',\n"
        "             dimensions=[Dimension(name='id', type='numeric', sql='id')])\n"
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=False)

    orders = layer.graph.models.get("orders")
    assert orders is not None
    # The template-defined explore join is stripped; it must not point at the overwriting real model.
    assert "base" not in {r.name for r in (orders.relationships or [])}
    # The real model still loads.
    assert layer.graph.models["base"].table == "real_base"


def test_load_from_directory_preserves_native_join_to_overwritten_template_name(tmp_path):
    """A NATIVE relationship authored for the replacement model must survive, only LookML template
    joins to the overwritten name are stripped."""
    (tmp_path / "a.model.lkml").write_text(
        "view: customer {\n"
        "  extension: required\n"
        "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }\n"
        "}\n"
    )
    (tmp_path / "native.py").write_text(
        "from sidemantic import Model, Dimension, Relationship\n"
        "customer = Model(name='customer', table='real_customer', primary_key='id',\n"
        "                 dimensions=[Dimension(name='id', type='numeric', sql='id')])\n"
        "orders = Model(name='orders', table='orders', primary_key='id',\n"
        "  dimensions=[Dimension(name='id', type='numeric', sql='id'), Dimension(name='cust_ref', type='numeric', sql='cust_ref')],\n"
        "  relationships=[Relationship(name='customer', type='many_to_one', foreign_key='cust_ref')])\n"
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=False)

    orders = layer.graph.models.get("orders")
    assert orders is not None
    # The native-authored relationship to the real customer is preserved, not stripped as a template join.
    assert "customer" in {r.name for r in (orders.relationships or [])}
    assert layer.graph.models["customer"].table == "real_customer"


def test_load_from_directory_no_fk_inference_to_unincluded_lookml_view(tmp_path):
    """FK inference must not join to a LookML view outside every model's include closure.

    An archived `customers` view no model includes is kept (so an imperfect include never silently
    drops a view), but Looker cannot see it, so `orders.customer_id -> customers` must NOT be
    inferred against it. An INCLUDED customers view is still inferred normally."""
    (tmp_path / "views").mkdir()
    (tmp_path / "archive").mkdir()
    (tmp_path / "orders.model.lkml").write_text('include: "/views/orders.view.lkml"\n')
    (tmp_path / "views" / "orders.view.lkml").write_text(
        "view: orders {\n  sql_table_name: orders ;;\n"
        "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }\n"
        "  dimension: customer_id { type: number  sql: ${TABLE}.customer_id ;; }\n}\n"
    )
    (tmp_path / "archive" / "customers.view.lkml").write_text(
        "view: customers { dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; } }\n"
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=False)

    orders = layer.graph.models.get("orders")
    assert orders is not None
    # No inferred join to the unincluded archived customers view.
    assert "customers" not in {r.name for r in (orders.relationships or [])}


def test_load_from_directory_hides_extension_required_view_with_table(tmp_path):
    """An `extension: required` base that declares a sql_table_name is still hidden (not queryable).

    Looker uses such a reusable base only through `extends`, so the loader must not register it as
    a CLI model even though it has a table. Its child still inherits its fields."""
    (tmp_path / "m.model.lkml").write_text(
        'include: "*.view.lkml"\n'
        "view: base {\n  extension: required\n  sql_table_name: real_base ;;\n"
        "  dimension: id { primary_key: yes  type: number  sql: ${TABLE}.id ;; }\n"
        "  dimension: shared { type: string  sql: ${TABLE}.shared ;; }\n}\n"
        "view: child {\n  extends: [base]\n  sql_table_name: child_t ;;\n"
        "  dimension: cid { primary_key: yes  type: number  sql: ${TABLE}.cid ;; }\n}\n"
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=False)

    assert "base" not in layer.graph.models  # abstract base hidden despite its table
    child = layer.graph.models.get("child")
    assert child is not None
    assert child.get_dimension("shared") is not None  # child still inherited the base's fields


def test_duplicate_model_name_records_fidelity_note_and_warns(tmp_path):
    """Two files defining the same model name shadow one another: note + warning."""
    import warnings

    from sidemantic.fidelity import capture_import_report

    (tmp_path / "a.yml").write_text(
        """
models:
  - name: orders
    table: orders_a
    primary_key: id
"""
    )
    (tmp_path / "b.yml").write_text(
        """
models:
  - name: orders
    table: orders_b
    primary_key: id
"""
    )

    layer = SemanticLayer()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with capture_import_report() as report:
            load_from_directory(layer, tmp_path)

    dup_notes = [n for n in report.notes if n.construct == "duplicate_model"]
    assert len(dup_notes) == 1
    note = dup_notes[0]
    assert note.severity == "dropped"
    # Detail names both the shadowed and the surviving file.
    assert "a.yml" in note.detail and "b.yml" in note.detail

    shadow_warnings = [w for w in caught if "shadowed" in str(w.message)]
    assert shadow_warnings, "expected a stdlib warning about the shadowed model"


def test_parse_error_formats_pydantic_validation_error_one_line(tmp_path):
    """A bad pydantic field yields a compact ``loc: message`` line, not a raw dump."""
    (tmp_path / "orders.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    metrics:
      - name: total
        agg: notavalidagg
"""
    )

    layer = SemanticLayer()
    with pytest.raises(ValueError) as exc_info:
        load_from_directory(layer, tmp_path, strict=True)

    message = str(exc_info.value)
    # Existing "Could not parse <path>:" prefix is preserved.
    assert "Could not parse" in message and "orders.yml" in message
    # Dotted loc + message, not the multi-line pydantic banner.
    assert "agg:" in message
    assert "Input should be" in message
    assert "validation error" not in message.lower()


def test_unsupported_derived_table_records_fidelity_note(tmp_path):
    """A LookML derived_table with no extractable SQL is dropped with a note."""
    from sidemantic.fidelity import capture_import_report

    (tmp_path / "pdt.view.lkml").write_text(
        "view: pdt_summary {\n"
        "  derived_table: {\n"
        "    datagroup_trigger: my_datagroup\n"
        "  }\n"
        "  dimension: id {\n"
        "    type: number\n"
        "    primary_key: yes\n"
        "    sql: ${TABLE}.id ;;\n"
        "  }\n"
        "}\n"
    )

    layer = SemanticLayer(auto_register=False)
    with capture_import_report() as report:
        load_from_directory(layer, tmp_path, strict=False)

    notes = [n for n in report.notes if n.construct == "derived_table"]
    assert len(notes) == 1
    assert notes[0].severity == "dropped"
    assert "pdt_summary" in notes[0].detail
    # The unsupported derived table is not registered as a queryable model.
    assert "pdt_summary" not in layer.graph.models
