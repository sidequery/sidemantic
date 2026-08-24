"""Malloy source invariants and visibility mappings."""

from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.malloy import MalloyAdapter, MalloySchemaExposureError
from sidemantic.core.semantic_layer import SchemaIntrospectionError, SecurityError
from sidemantic.fidelity import capture_import_report


def _parse(tmp_path: Path, text: str, *, strict: bool = False, warn_on_errors: bool = False):
    path = tmp_path / "model.malloy"
    path.write_text(text)
    adapter = MalloyAdapter(strict=strict, warn_on_errors=warn_on_errors)
    return adapter, adapter.parse(path)


def test_source_where_is_conjunctive_invariant_and_executes(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: orders is duckdb.table('orders') extend {
  where: tenant_id = 1
  where: active
  dimension: status is status
  measure: revenue is sum(amount)
}
""",
    )
    model = graph.get_model("orders")

    assert model.invariant_filters == ["tenant_id = 1", "active"]
    assert model.segments == []

    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("create table orders (tenant_id int, active boolean, status text, amount int)")
    layer.adapter.execute(
        "insert into orders values (1, true, 'kept', 10), (1, false, 'inactive', 20), (2, true, 'other', 30)"
    )
    layer.add_model(model)
    assert layer.query(metrics=["orders.revenue"], dimensions=["orders.status"]).fetchall() == [("kept", 10)]

    exported = tmp_path / "roundtrip.malloy"
    adapter.export(graph, exported)
    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported).get_model("orders")
    assert reparsed.invariant_filters == ["tenant_id = 1", "active"]
    assert exported.read_text().count("  where:") == 2


def test_field_and_measure_access_labels_map_and_roundtrip(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: orders is duckdb.table('orders') extend {
  private dimension: secret_label is concat(secret, '!')
  internal measure: internal_revenue is sum(amount)
  measure: public_count is count()
}
""",
    )
    model = graph.get_model("orders")
    secret = model.get_dimension("secret_label")
    internal = model.get_metric("internal_revenue")

    assert secret.public is False
    assert secret.metadata["malloy_access"] == "private"
    assert internal.public is False
    assert internal.visibility == "internal"
    assert model.get_metric("public_count").public is True
    assert adapter.unsupported_features == []

    exported = tmp_path / "roundtrip.malloy"
    adapter.export(graph, exported)
    text = exported.read_text()
    assert "private dimension:" in text
    assert "internal measure:" in text
    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported).get_model("orders")
    assert reparsed.get_dimension("secret_label").public is False
    assert reparsed.get_metric("internal_revenue").visibility == "internal"


def test_private_and_internal_rename_visibility_executes_and_roundtrips(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: accounts is duckdb.table('accounts') extend {
  private rename: secret is secret_token
  internal rename: staff_region is region
}
""",
    )
    model = graph.get_model("accounts")

    assert model.get_dimension("secret").public is False
    assert model.get_dimension("secret").metadata["malloy_access"] == "private"
    assert model.get_dimension("staff_region").public is False
    assert model.get_dimension("staff_region").metadata["malloy_access"] == "internal"
    assert set(model.schema_exposure.private) == {
        "secret",
        "secret_token",
        "staff_region",
        "region",
    }
    assert any("internal renamed dimension" in issue for _, issue in adapter.unsupported_features)

    layer = SemanticLayer(auto_register=False, engine="python", enforce_visibility=True)
    layer.adapter.execute("create table accounts (account_id int, region text, secret_token text)")
    layer.adapter.execute("insert into accounts values (1, 'west', 'hidden')")
    layer.add_model(model)
    assert {dimension.name for dimension in model.dimensions} == {
        "account_id",
        "secret",
        "staff_region",
    }
    with pytest.raises(SecurityError, match="secret"):
        layer.compile(dimensions=["accounts.secret"])
    with pytest.raises(SecurityError, match="staff_region"):
        layer.compile(dimensions=["accounts.staff_region"])

    exported = tmp_path / "rename-roundtrip.malloy"
    adapter.export(graph, exported)
    text = exported.read_text()
    assert "private rename:" in text
    assert "internal rename:" in text
    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported).get_model("accounts")
    assert reparsed.get_dimension("secret").public is False
    assert reparsed.get_dimension("staff_region").metadata["malloy_access"] == "internal"


def test_intrinsic_columns_and_explicit_primary_key_execute(tmp_path):
    _, graph = _parse(
        tmp_path,
        """source: orders is duckdb.table('orders') extend {
  primary_key: order_id
  measure: revenue is sum(amount)
}
""",
    )
    model = graph.get_model("orders")
    assert model.auto_dimensions is True
    assert model.schema_exposure.include_primary_key is True

    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("create table orders (order_id int, region text, amount int)")
    layer.adapter.execute("insert into orders values (1, 'west', 10), (2, 'east', 20)")
    layer.add_model(model)

    assert model.get_dimension("region") is not None
    assert model.get_dimension("order_id") is not None
    assert layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
        order_by=["orders.region"],
    ).fetchall() == [
        ("east", 20),
        ("west", 10),
    ]
    assert layer.query(dimensions=["orders.order_id"]).fetchall() == [(1,), (2,)]


def test_accept_except_and_private_filter_before_schema_exposure(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: accounts is duckdb.table('accounts') extend {
  primary_key: account_id
  private dimension: secret_token is secret_token
  accept: account_id, region, balance, secret_token
  except: balance
}
""",
    )
    model = graph.get_model("accounts")
    assert model.schema_exposure.accept == ["account_id", "region"]
    assert model.schema_exposure.except_fields == []
    assert model.schema_exposure.private == ["secret_token"]
    assert adapter.unsupported_features == []

    layer = SemanticLayer(auto_register=False, engine="python", enforce_visibility=True)
    layer.adapter.execute("create table accounts (account_id int, region text, balance int, secret_token text)")
    layer.adapter.execute("insert into accounts values (1, 'west', 10, 'hidden')")
    layer.add_model(model)

    assert {dimension.name for dimension in model.dimensions} == {
        "account_id",
        "region",
        "secret_token",
    }
    assert layer.query(dimensions=["accounts.region"]).fetchall() == [("west",)]
    with pytest.raises(ValueError, match="balance"):
        layer.compile(dimensions=["accounts.balance"])
    with pytest.raises(SecurityError, match="secret_token"):
        layer.compile(dimensions=["accounts.secret_token"])

    exported = tmp_path / "roundtrip.malloy"
    adapter.export(graph, exported)
    text = exported.read_text()
    assert "accept: account_id, region, balance, secret_token" in text
    assert "except: balance" in text
    assert "private dimension:" in text

    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported).get_model("accounts")
    assert reparsed.auto_dimensions is True
    assert reparsed.schema_exposure.accept == ["account_id", "region"]
    assert reparsed.schema_exposure.private == ["secret_token"]
    assert reparsed.get_dimension("secret_token").public is False


def test_inherited_accept_survives_flattened_export_reparse(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: base is duckdb.table('accounts') extend {
  accept: account_id, region
}
source: child is base extend {
  timezone: 'UTC'
}
""",
    )
    assert graph.get_model("base").schema_exposure.accept == ["account_id", "region"]
    assert graph.get_model("child").schema_exposure.accept == ["account_id", "region"]

    exported = tmp_path / "inherited-accept.malloy"
    adapter.export(graph, exported)
    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported)
    child = reparsed.get_model("child")
    assert child.schema_exposure.accept == ["account_id", "region"]
    assert "source: child is duckdb.table('accounts')" in exported.read_text()


def test_inherited_child_accept_narrows_parent_accept(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: base is duckdb.table('accounts') extend {
  accept: account_id, region, email
}
source: child is base extend {
  accept: account_id, region
}
""",
    )

    assert graph.get_model("child").schema_exposure.accept == ["account_id", "region"]

    exported = tmp_path / "composed-accept.malloy"
    adapter.export(graph, exported)
    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported)
    assert reparsed.get_model("child").schema_exposure.accept == ["account_id", "region"]


def test_inherited_child_except_and_private_compose_with_parent(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: base is duckdb.table('accounts') extend {
  except: parent_secret
  private dimension: parent_private is parent_private
}
source: child is base extend {
  except: child_secret
  private dimension: child_private is child_private
}
""",
    )

    exposure = graph.get_model("child").schema_exposure
    assert exposure.except_fields == ["parent_secret", "child_secret"]
    assert exposure.private == ["parent_private", "child_private"]

    exported = tmp_path / "composed-except.malloy"
    adapter.export(graph, exported)
    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported)
    reparsed_exposure = reparsed.get_model("child").schema_exposure
    assert reparsed_exposure.except_fields == ["parent_secret", "child_secret"]
    assert reparsed_exposure.private == ["parent_private", "child_private"]


def test_strict_schema_introspection_failure_and_lenient_unsafe_shape_report(tmp_path):
    _, graph = _parse(
        tmp_path,
        "source: missing is duckdb.table('missing_table')\n",
        strict=True,
    )
    model = graph.get_model("missing")
    assert model.schema_exposure.strict is True
    layer = SemanticLayer(auto_register=False, engine="python")
    with pytest.raises(SchemaIntrospectionError, match="missing"):
        layer.add_model(model)

    with capture_import_report() as report:
        adapter, virtual_graph = _parse(
            tmp_path,
            "source: events is duckdb.virtual('event_stream')\n",
        )
    virtual = virtual_graph.get_model("events")
    assert virtual.auto_dimensions is False
    assert virtual.schema_exposure is None
    assert any("cannot be safely introspected" in issue for _, issue in adapter.unsupported_features)
    assert report.is_blocked
    assert report.features[0].feature == "malloy_unsupported_feature"

    with pytest.raises(MalloySchemaExposureError, match="cannot be safely introspected"):
        _parse(
            tmp_path,
            "source: events is duckdb.virtual('event_stream')\n",
            strict=True,
        )


def test_include_access_modifier_is_reported_not_silently_claimed(tmp_path):
    source = """source: base is duckdb.table('base') extend {
  dimension: secret is secret
}
source: derived is base include { private: secret }
"""
    with capture_import_report() as report:
        adapter, graph = _parse(tmp_path, source)

    assert graph.get_model("derived") is not None
    assert any("include access modifiers cannot be represented" in issue for _, issue in adapter.unsupported_features)
    assert report.is_blocked

    with pytest.raises(MalloySchemaExposureError, match="include access modifiers"):
        _parse(tmp_path, source, strict=True)


def test_private_join_is_omitted_lenient_and_rejected_strict(tmp_path):
    source = """source: customers is duckdb.table('customers')
source: orders is duckdb.table('orders') extend {
  private join_one: customers with customer_id
}
"""
    with capture_import_report() as report:
        adapter, graph = _parse(tmp_path, source)

    assert graph.get_model("orders").relationships == []
    assert any("private join" in issue for _, issue in adapter.unsupported_features)
    assert report.is_blocked

    with pytest.raises(MalloySchemaExposureError, match="private join"):
        _parse(tmp_path, source, strict=True)


@pytest.mark.parametrize(
    ("unsafe_source", "feature", "message"),
    [
        (
            """source: unsafe is base -> { group_by: id } extend {
  dimension: leaked is id
}
""",
            "malloy_source_pipeline_rejected",
            "query pipeline",
        ),
        (
            """source: unsafe is compose(base, other) extend {
  dimension: leaked is id
}
""",
            "malloy_source_compose_rejected",
            "compose",
        ),
        (
            """source: unsafe() is duckdb.table('unsafe') extend {
  dimension: leaked is id
}
""",
            "malloy_source_parameters_rejected",
            "declares parameters",
        ),
        (
            """source: unsafe is base(limit is 10) extend {
  dimension: leaked is id
}
""",
            "malloy_source_arguments_rejected",
            "source with arguments",
        ),
        (
            '''source: unsafe is duckdb.sql("""
  select * from %{ base }
""") extend {
  dimension: leaked is id
}
''',
            "malloy_sql_interpolation_rejected",
            "SQL interpolation",
        ),
        (
            """source: unsafe is `from`(base) extend {
  dimension: leaked is id
}
""",
            "malloy_source_from_query_rejected",
            "defined from a query",
        ),
    ],
    ids=[
        "pipeline",
        "compose",
        "declaration-parameters",
        "invocation-arguments",
        "sql-interpolation",
        "source-from-query",
    ],
)
def test_unsafe_source_shapes_are_atomically_omitted_lenient(
    tmp_path,
    unsafe_source,
    feature,
    message,
):
    source = (
        "source: base is duckdb.table('base') extend { dimension: id is id }\n"
        "source: other is duckdb.table('other') extend { dimension: id is id }\n"
        + unsafe_source
        + "source: safe_after is duckdb.table('safe_after') extend { dimension: id is id }\n"
    )

    with capture_import_report() as report:
        adapter, graph = _parse(tmp_path, source)

    assert adapter.errors == []
    assert set(graph.models) == {"base", "other", "safe_after"}
    assert "unsafe" not in graph.models
    assert any(message in issue for _, issue in adapter.unsupported_features)
    assert any(item.feature == feature and item.status == "rejected" for item in report.features)


@pytest.mark.parametrize(
    ("unsafe_source", "message"),
    [
        ("source: unsafe is base -> { group_by: id }\n", "query pipeline"),
        ("source: unsafe is compose(base, other)\n", "compose"),
        (
            "source: unsafe() is duckdb.table('unsafe')\n",
            "declares parameters",
        ),
        ("source: unsafe is base(limit is 10)\n", "source with arguments"),
        (
            'source: unsafe is duckdb.sql("""select * from %{ base }""")\n',
            "SQL interpolation",
        ),
        (
            "source: unsafe is `from`(base)\n",
            "defined from a query",
        ),
    ],
    ids=[
        "pipeline",
        "compose",
        "declaration-parameters",
        "invocation-arguments",
        "sql-interpolation",
        "source-from-query",
    ],
)
def test_unsafe_source_shapes_raise_in_strict_mode(tmp_path, unsafe_source, message):
    source = (
        "source: base is duckdb.table('base') extend { dimension: id is id }\n"
        "source: other is duckdb.table('other') extend { dimension: id is id }\n" + unsafe_source
    )

    with pytest.raises(MalloySchemaExposureError, match=message):
        _parse(tmp_path, source, strict=True)


def test_reserved_from_query_source_is_omitted_after_lenient_parser_recovery(tmp_path):
    source = """source: base is duckdb.table('base') extend { dimension: id is id }
source: unsafe is from(base -> { group_by: id })
source: safe_after is duckdb.table('safe_after') extend { dimension: id is id }
"""

    with capture_import_report() as report:
        adapter, graph = _parse(tmp_path, source)

    assert set(graph.models) == {"base", "safe_after"}
    assert adapter.errors
    assert any(
        item.feature == "malloy_source_from_query_rejected" and item.status == "rejected" for item in report.features
    )


def test_legacy_source_refinement_remains_supported(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: base is duckdb.table('base') extend {
  dimension: id is id
}
source: refined is base + {
  dimension: label is label
}
""",
    )

    refined = graph.get_model("refined")
    assert refined.extends == "base"
    assert adapter.unsupported_features == []
