"""Malloy module binding, visibility, and local path-resolution contracts."""

from pathlib import Path

import pytest

from sidemantic.adapters.malloy import MalloyAdapter
from sidemantic.adapters.malloy_modules import MalloyModuleResolver, MalloyResolutionError
from sidemantic.fidelity import capture_import_report


def _source(name: str, table: str | None = None) -> str:
    return f"source: {name} is duckdb.table('{table or name}') extend {{ primary_key: id dimension: id is id }}\n"


def test_alias_uses_local_name_then_exported_name(tmp_path):
    (tmp_path / "base.malloy").write_text(_source("customers"))
    root = tmp_path / "root.malloy"
    root.write_text("import { crm_customers is customers } from 'base.malloy'\n" + _source("orders"))

    graph = MalloyAdapter(strict=True).parse(root)

    assert set(graph.models) == {"crm_customers", "orders"}
    assert graph.get_model("crm_customers").table == "customers"


def test_explicit_export_is_complete_allowlist(tmp_path):
    (tmp_path / "library.malloy").write_text(
        _source("private_helper") + _source("published") + "export { published }\n"
    )
    root = tmp_path / "root.malloy"
    root.write_text("import 'library.malloy'\n" + _source("root_source"))

    graph = MalloyAdapter(strict=True).parse(root)

    assert set(graph.models) == {"published", "root_source"}


def test_multiple_exports_union_and_imported_name_can_be_reexported(tmp_path):
    (tmp_path / "raw.malloy").write_text(_source("raw_customers"))
    (tmp_path / "library.malloy").write_text(
        "import { customers is raw_customers } from 'raw.malloy'\n"
        + _source("orders")
        + "export { customers }\nexport { orders }\n"
    )
    root = tmp_path / "root.malloy"
    root.write_text("import 'library.malloy'\n")

    graph = MalloyAdapter(strict=True).parse(root)

    assert set(graph.models) == {"customers", "orders"}
    assert graph.get_model("customers").table == "raw_customers"


def test_selective_import_cannot_reach_private_name(tmp_path):
    (tmp_path / "library.malloy").write_text(
        _source("private_helper") + _source("published") + "export { published }\n"
    )
    root = tmp_path / "root.malloy"
    root.write_text("import { private_helper } from 'library.malloy'\n" + _source("safe"))

    with pytest.raises(MalloyResolutionError, match="not exported"):
        MalloyAdapter(strict=True).parse(root)


def test_export_forward_reference_is_error(tmp_path):
    root = tmp_path / "root.malloy"
    root.write_text("export { later }\n" + _source("later"))

    with pytest.raises(MalloyResolutionError, match="has not been defined or imported yet"):
        MalloyAdapter(strict=True).parse(root)


def test_source_forward_reference_is_error(tmp_path):
    root = tmp_path / "root.malloy"
    root.write_text("source: child is parent extend { dimension: x is x }\n" + _source("parent"))

    with pytest.raises(MalloyResolutionError, match="unavailable source 'parent'"):
        MalloyAdapter(strict=True).parse(root)


def test_aliased_cross_file_inheritance_keeps_raw_child_and_dependency(tmp_path):
    (tmp_path / "base.malloy").write_text(_source("base", "physical"))
    root = tmp_path / "root.malloy"
    root.write_text(
        "import { imported_base is base } from 'base.malloy'\n"
        "source: child is imported_base extend { dimension: extra is extra }\n"
    )

    graph = MalloyAdapter(strict=True).parse(root)

    assert set(graph.models) == {"imported_base", "child"}
    assert graph.get_model("child").extends == "imported_base"
    assert graph.get_model("child").primary_key is None
    assert graph.get_model("imported_base").primary_key == "id"


@pytest.mark.parametrize("specifier", ["https://example.com/a.malloy", "file:///tmp/a.malloy", "/tmp/a.malloy"])
def test_nonlocal_imports_are_rejected(tmp_path, specifier):
    root = tmp_path / "root.malloy"
    root.write_text(f"import '{specifier}'\n" + _source("safe"))

    with pytest.raises(MalloyResolutionError, match="only project-relative local Malloy imports"):
        MalloyAdapter(strict=True).parse(root)


def test_import_root_escape_is_rejected(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (tmp_path / "outside.malloy").write_text(_source("outside"))
    root = project / "root.malloy"
    root.write_text("import '../outside.malloy'\n" + _source("safe"))

    with pytest.raises(MalloyResolutionError, match="outside Malloy project root"):
        MalloyAdapter(strict=True, import_root=project).parse(root)


def test_missing_import_leniently_keeps_independent_source_and_reports(tmp_path):
    root = tmp_path / "root.malloy"
    root.write_text("import 'missing.malloy'\n" + _source("safe"))

    with capture_import_report() as report:
        graph = MalloyAdapter(strict=False, warn_on_errors=False).parse(root)

    assert set(graph.models) == {"safe"}
    assert report.is_blocked
    assert report.features[0].feature == "malloy_import_missing"
    assert graph.import_warnings[0]["code"] == "malloy_import_missing"


def test_import_cycle_reports_complete_chain(tmp_path):
    a = tmp_path / "a.malloy"
    b = tmp_path / "b.malloy"
    a.write_text("import 'b.malloy'\n" + _source("a"))
    b.write_text("import 'a.malloy'\n" + _source("b"))

    with pytest.raises(MalloyResolutionError) as exc_info:
        MalloyAdapter(strict=True).parse(a)

    assert exc_info.value.code == "malloy_import_cycle"
    assert exc_info.value.chain == (a.resolve(), b.resolve(), a.resolve())


def test_inheritance_cycle_across_files_is_rejected(tmp_path):
    a = tmp_path / "a.malloy"
    b = tmp_path / "b.malloy"
    a.write_text("import { b } from 'b.malloy'\nsource: a is b extend {}\n")
    b.write_text("import { a } from 'a.malloy'\nsource: b is a extend {}\n")

    # The module cycle is diagnosed before an unsafe inheritance fixed point can
    # be invented.
    with pytest.raises(MalloyResolutionError) as exc_info:
        MalloyAdapter(strict=True).parse(a)
    assert exc_info.value.code == "malloy_import_cycle"


def test_duplicate_source_and_import_conflict_are_rejected(tmp_path):
    duplicate = tmp_path / "duplicate.malloy"
    duplicate.write_text(_source("same", "one") + _source("same", "two"))
    with pytest.raises(MalloyResolutionError, match="cannot redefine 'same'"):
        MalloyAdapter(strict=True).parse(duplicate)

    (tmp_path / "base.malloy").write_text(_source("same"))
    conflict = tmp_path / "conflict.malloy"
    conflict.write_text("import { same } from 'base.malloy'\n" + _source("same", "local"))
    with pytest.raises(MalloyResolutionError, match="cannot redefine 'same'"):
        MalloyAdapter(strict=True).parse(conflict)


def test_distinct_directory_sources_cannot_share_flat_graph_name(tmp_path):
    (tmp_path / "a.malloy").write_text(_source("same", "one"))
    (tmp_path / "b.malloy").write_text(_source("same", "two"))

    with pytest.raises(MalloyResolutionError, match="both require graph name 'same'"):
        MalloyAdapter(strict=True).parse(tmp_path)


def test_lenient_flat_name_conflict_drops_ambiguous_name(tmp_path):
    (tmp_path / "a.malloy").write_text(_source("same", "one"))
    (tmp_path / "b.malloy").write_text(_source("same", "two"))

    with capture_import_report() as report:
        graph = MalloyAdapter(strict=False, warn_on_errors=False).parse(tmp_path)

    assert "same" not in graph.models
    assert report.is_blocked
    assert any(feature.feature == "malloy_flat_name_conflict" for feature in report.features)


def test_module_is_parsed_once_and_provenance_is_defining_file(tmp_path):
    base = tmp_path / "base.malloy"
    base.write_text(_source("customers"))
    root = tmp_path / "root.malloy"
    root.write_text(
        "import { first_copy is customers, second_copy is customers } from 'base.malloy'\n" + _source("root")
    )
    adapter = MalloyAdapter(strict=True)
    original = adapter._parse_module
    calls: list[Path] = []

    def counting_parse(path: Path):
        calls.append(path)
        return original(path)

    adapter._parse_module = counting_parse
    graph = adapter.parse(root)

    assert calls.count(base.resolve()) == 1
    assert graph.get_model("first_copy")._source_file == str(base.resolve())
    assert graph.get_model("second_copy")._source_file == str(base.resolve())


def test_type_given_and_query_symbols_follow_export_visibility(tmp_path):
    library = tmp_path / "library.malloy"
    library.write_text(
        _source("safe") + "type: hidden_type is { value::string }\n"
        "type: public_type is { value::number }\n"
        "given: public_arg::string\n"
        "query: public_query is safe -> { group_by: id }\n"
        "export { public_type, public_arg, public_query }\n"
    )
    root = tmp_path / "root.malloy"
    root.write_text(
        "import { local_type is public_type, local_arg is public_arg, local_query is public_query } "
        "from 'library.malloy'\n"
    )

    with capture_import_report() as report:
        adapter = MalloyAdapter(strict=True, warn_on_errors=False)
        graph = adapter.parse(root)

    assert set(graph.models) == {"safe"}
    assert adapter.user_types == {"local_type": "{ value::number }"}
    assert adapter.given == {"local_arg": "string"}
    assert set(graph.saved_queries) == {"local_query"}
    assert graph.get_saved_query("local_query").dimensions == ["id"]
    assert not any(feature.feature == "malloy_query_execution_unsupported" for feature in report.features)


def test_query_only_import_uses_source_binding_from_defining_module(tmp_path):
    (tmp_path / "data.malloy").write_text(_source("orders", "physical_orders"))
    (tmp_path / "library.malloy").write_text(
        "import { scoped_orders is orders } from 'data.malloy'\n"
        "query: published is scoped_orders -> { group_by: id }\n"
        "export { published }\n"
    )
    root = tmp_path / "root.malloy"
    root.write_text("import { local_query is published } from 'library.malloy'\n")

    adapter = MalloyAdapter(strict=True)
    resolution = MalloyModuleResolver(adapter._parse_module, strict=True, import_root=tmp_path).resolve([root])
    graph = adapter.parse(root)

    assert set(graph.models) == {"scoped_orders"}
    assert graph.get_model("scoped_orders").table == "physical_orders"
    assert set(graph.saved_queries) == {"local_query"}
    assert graph.get_explore("__malloy_local_query").model == "scoped_orders"
    assert resolution.queries["local_query"].source_model == "scoped_orders"
    assert resolution.queries["local_query"].location.path == (tmp_path / "library.malloy").resolve()


def test_imported_query_does_not_rebind_missing_source_in_entry_scope(tmp_path):
    (tmp_path / "library.malloy").write_text("query: published is orders -> { group_by: id }\nexport { published }\n")
    root = tmp_path / "root.malloy"
    root.write_text("import { local_query is published } from 'library.malloy'\n" + _source("orders"))

    with pytest.raises(MalloyResolutionError) as exc_info:
        MalloyAdapter(strict=True).parse(root)

    assert exc_info.value.code == "malloy_query_source_reference_not_found"
    assert "in its defining scope" in str(exc_info.value)


def test_lenient_imported_query_with_unbound_source_is_omitted(tmp_path):
    (tmp_path / "library.malloy").write_text("query: published is orders -> { group_by: id }\nexport { published }\n")
    root = tmp_path / "root.malloy"
    root.write_text("import { local_query is published } from 'library.malloy'\n" + _source("orders"))

    with capture_import_report() as report:
        graph = MalloyAdapter(strict=False, warn_on_errors=False).parse(root)

    assert "local_query" not in graph.saved_queries
    assert any(feature.feature == "malloy_query_source_reference_not_found" for feature in report.features)
    assert any(warning["code"] == "malloy_query_source_reference_not_found" for warning in graph.import_warnings)


def test_role_alias_dependency_uses_canonical_related_model(tmp_path):
    library = tmp_path / "library.malloy"
    library.write_text(
        _source("customers") + "source: orders is duckdb.table('orders') extend {\n"
        "  primary_key: id\n"
        "  join_one: buyer is customers with customer_id\n"
        "}\n"
        "export { orders }\n"
    )
    root = tmp_path / "root.malloy"
    root.write_text("import { orders } from 'library.malloy'\n")

    graph = MalloyAdapter(strict=True).parse(root)

    assert set(graph.models) == {"customers", "orders"}
    relationship = graph.get_model("orders").relationships[0]
    assert relationship.name == "buyer"
    assert relationship.related_model == "customers"


def test_lenient_import_conflict_invalidates_binding_and_dependent(tmp_path):
    (tmp_path / "a.malloy").write_text(_source("base", "one"))
    (tmp_path / "b.malloy").write_text(_source("base", "two"))
    root = tmp_path / "root.malloy"
    root.write_text(
        "import { shared is base } from 'a.malloy'\n"
        "import { shared is base } from 'b.malloy'\n"
        "source: child is shared extend { dimension: x is x }\n" + _source("safe")
    )

    with capture_import_report() as report:
        graph = MalloyAdapter(strict=False, warn_on_errors=False).parse(root)

    assert set(graph.models) == {"safe"}
    assert any(feature.feature == "malloy_import_name_conflict" for feature in report.features)
    assert any(feature.feature == "malloy_source_reference_not_found" for feature in report.features)


def test_lenient_duplicate_source_invalidates_ambiguous_name(tmp_path):
    root = tmp_path / "root.malloy"
    root.write_text(_source("same", "one") + _source("same", "two") + _source("safe"))

    graph = MalloyAdapter(strict=False, warn_on_errors=False).parse(root)

    assert set(graph.models) == {"safe"}
