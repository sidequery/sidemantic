"""Installed Rust SQL transport acceptance with independently expected populations."""

import csv
import io
import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from sidemantic import SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.cli import app
from sidemantic.core.security import SecurityPolicy
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.core.transport_security import rewrite_transport_sql
from sidemantic.sql.query_rewriter import QueryRewriter
from sidemantic.validation import QueryValidationError

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def installed_rust():
    rust = pytest.importorskip("sidemantic_rs", reason="Policy CLI acceptance requires the real Rust extension")
    assert callable(rust.rewrite_with_semantic_input_context)


def make_layer(source, *, engine="rust", fallback=False):
    layer = SemanticLayer(engine=engine, fallback=fallback, auto_register=False, enforce_visibility=True)
    layer.graph = SidemanticAdapter().parse(FIXTURES / f"{source}.yml")
    return layer


def rewrite(layer, sql, attributes):
    return rewrite_transport_sql(layer, sql, user_attributes=attributes, transport="policy acceptance")


@pytest.mark.parametrize("mode", ["query", "dry-run", "rewrite"])
def test_cli_authorized_context_reaches_rust(mode, tmp_path):
    database = tmp_path / "data.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute((FIXTURES / "policy_aggregate.sql").read_text())
    attributes = tmp_path / "attributes.json"
    attributes.write_text(json.dumps({"tenant": 1}))
    sql = "select revenue_per_quota, net_of_quota from metrics"
    arguments = [
        "--verbose",
        "rewrite" if mode == "rewrite" else "query",
        sql,
        "--models",
        str(FIXTURES / "policy_aggregate.yml"),
        "--engine",
        "rust",
        "--no-fallback",
        "--enforce-visibility",
        "--user-attrs-file",
        str(attributes),
    ]
    if mode != "rewrite":
        arguments.extend(["--db", str(database)])
    if mode == "dry-run":
        arguments.append("--dry-run")
    result = CliRunner().invoke(app, arguments)
    assert result.exit_code == 0, result.output
    assert "Engine: rust" in result.stderr
    if mode == "query":
        records = list(csv.DictReader(io.StringIO(result.stdout)))
        assert len(records) == 1
        assert float(records[0]["revenue_per_quota"]) == 30
        assert float(records[0]["net_of_quota"]) == 174
    else:
        with duckdb.connect(str(database)) as connection:
            result_set = connection.execute(result.stdout)
            assert [column[0] for column in result_set.description] == ["revenue_per_quota", "net_of_quota"]
            assert result_set.fetchall() == [(30.0, 174)]


@pytest.mark.parametrize("engine", ["rust", "auto"])
@pytest.mark.parametrize(
    "source,sql,attributes",
    [
        ("policy_literals", "select denied.total from metrics", {}),
        ("policy_literals", "select quoted.total from metrics", None),
        ("policy_aggregate", "select purchases.secret_cost from metrics", {"tenant": 1}),
    ],
)
def test_cli_policy_denial_emits_no_sql_or_results(engine, source, sql, attributes, tmp_path):
    arguments = [
        "rewrite",
        sql,
        "--models",
        str(FIXTURES / f"{source}.yml"),
        "--engine",
        engine,
        "--fallback",
        "--enforce-visibility",
    ]
    if attributes is not None:
        attributes_file = tmp_path / "attributes.json"
        attributes_file.write_text(json.dumps(attributes))
        arguments.extend(["--user-attrs-file", str(attributes_file)])
    result = CliRunner().invoke(app, arguments)
    assert result.exit_code != 0
    assert result.stdout == ""
    assert "Using Python engine" not in result.stderr


@pytest.mark.parametrize("engine,fallback", [("rust", False), ("rust", True), ("auto", True)])
@pytest.mark.parametrize(
    "source,sql,attributes",
    [
        ("policy_literals", "select denied.total from metrics", {}),
        ("policy_literals", "select admin_only.total from metrics", {"role": "viewer"}),
        ("policy_literals", "select admin_only.total from metrics", {}),
        ("policy_literals", "select quoted.total from metrics", None),
        ("policy_literals", "select quoted.total from metrics", {}),
        ("policy_aggregate", "select purchases.secret_cost from metrics", {"tenant": 1}),
        ("policy_aggregate", "select purchases.cost, purchases.revenue from metrics", {"tenant": 1}),
        ("policy_aggregate", "select purchases.revenue from metrics where purchases.cost > 0", {"tenant": 1}),
        ("policy_aggregate", "select purchases.revenue from metrics order by purchases.secret_cost", {"tenant": 1}),
    ],
)
def test_security_failures_never_enter_python_fallback(engine, fallback, source, sql, attributes, monkeypatch):
    def forbidden_fallback(*args, **kwargs):
        pytest.fail("A security failure entered the Python SQL fallback")

    monkeypatch.setattr(QueryRewriter, "_rewrite_python", forbidden_fallback)
    monkeypatch.setattr(QueryRewriter, "_explain_python", forbidden_fallback)
    layer = make_layer(source, engine=engine, fallback=fallback)
    try:
        with pytest.raises(SecurityError):
            rewrite(layer, sql, attributes)
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine,fallback", [("rust", False), ("rust", True), ("auto", True)])
@pytest.mark.parametrize("clause", ["where", "having"])
@pytest.mark.parametrize("field", ["purchases.amount", "amount", "unknown_owner.amount"])
def test_undeclared_filter_columns_are_validation_errors_without_fallback(engine, fallback, clause, field, monkeypatch):
    def forbidden_fallback(*args, **kwargs):
        pytest.fail("An undeclared SQL filter field entered the Python fallback")

    monkeypatch.setattr(QueryRewriter, "_rewrite_python", forbidden_fallback)
    monkeypatch.setattr(QueryRewriter, "_explain_python", forbidden_fallback)
    layer = make_layer("policy_aggregate", engine=engine, fallback=fallback)
    try:
        # amount exists physically but is absent from the public semantic fields.
        with pytest.raises(QueryValidationError):
            rewrite(layer, f"select purchases.revenue from metrics {clause} {field} > 100", {"tenant": 1})
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("model", ["quoted", "unquoted"])
@pytest.mark.parametrize("subject,total", [("O'Brien", 9), ("x' OR '1'='1", 7), ("missing'; --", None)])
def test_sql_policy_attributes_are_literals(model, subject, total):
    layer = make_layer("policy_literals")
    try:
        layer.adapter.execute((FIXTURES / "migration_seed.sql").read_text())
        generated = rewrite(layer, f"select {model}.total from metrics", {"subject": subject})
        assert layer.last_engine_selection["engine"] == "rust"
        assert layer.adapter.execute(generated).fetchall() == [(total,)]
    finally:
        layer.adapter.close()


def test_sql_join_policy_excludes_orphans_and_other_tenants():
    layer = make_layer("policy_join")
    try:
        layer.adapter.execute((FIXTURES / "migration_seed.sql").read_text())
        generated = rewrite(layer, "select accounts.label, purchases.total from metrics", {"tenant": 1})
        assert layer.adapter.execute(generated).fetchall() == [("visible", 10)]
        assert layer.last_engine_selection["engine"] == "rust"
    finally:
        layer.adapter.close()


def test_declared_left_join_preserves_base_rows_with_secured_details():
    layer = make_layer("policy_join")
    try:
        layer.graph.models["purchases"].relationships[0].metadata = {"bsl_how": "left"}
        layer.adapter.execute((FIXTURES / "migration_seed.sql").read_text())
        generated = rewrite(
            layer,
            "select purchases.id, accounts.label, purchases.total from metrics order by purchases.id",
            {"tenant": 1},
        )
        assert layer.adapter.execute(generated).fetchall() == [
            (1, "visible", 10),
            (2, None, 20),
            (3, None, 30),
            (4, None, 40),
        ]
    finally:
        layer.adapter.close()


def test_policy_applies_independently_to_both_relationship_roles():
    layer = make_layer("native/roles")
    try:
        layer.graph.models["airports"].security = SecurityPolicy(row_filters=["city != {{ user.excluded }}"])
        layer.adapter.execute((FIXTURES / "seed.sql").read_text())
        sql = (
            "select origin.city as departure, destination.city as arrival, flights.flight_count "
            "from metrics order by destination.city"
        )
        assert layer.adapter.execute(rewrite(layer, sql, {"excluded": "JFK"})).fetchall() == [("SFO", "LAX", 1)]
        assert layer.adapter.execute(rewrite(layer, sql, {"excluded": "SFO"})).fetchall() == []
    finally:
        layer.adapter.close()


def test_secured_rollup_request_uses_authorized_source_rows():
    layer = make_layer("native/restricted")
    try:
        layer.use_preaggregations = True
        layer.adapter.execute((FIXTURES / "seed.sql").read_text())
        # The unscoped materialized rollup is 930; tenant 1's source rows total 30.
        generated = rewrite(layer, "select restricted.revenue from metrics", {"tenant": 1})
        assert layer.adapter.execute(generated).fetchall() == [(30,)]
        assert layer.last_engine_selection["engine"] == "rust"
    finally:
        layer.adapter.close()


@pytest.mark.parametrize(
    "sql",
    [
        "select * from policy_values",
        "select allowed.total from metrics left join policy_values on true",
        "select allowed.total, (select max(amount) from policy_values) from metrics",
    ],
)
def test_sql_physical_sources_cannot_bypass_policy(sql):
    layer = make_layer("policy_literals", engine="auto", fallback=True)
    try:
        with pytest.raises(SecurityError):
            rewrite(layer, sql, {})
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("mode", ["query", "rewrite"])
@pytest.mark.parametrize("contents", ["[]", "null", '"tenant"', "{broken"])
def test_cli_rejects_non_object_attribute_files(mode, contents, tmp_path):
    attributes = tmp_path / "attributes.json"
    attributes.write_text(contents)
    result = CliRunner().invoke(
        app,
        [
            mode,
            "select quoted.total from metrics",
            "--models",
            str(FIXTURES / "policy_literals.yml"),
            "--engine",
            "rust",
            "--user-attrs-file",
            str(attributes),
        ],
    )
    assert result.exit_code != 0
    assert "JSON" in result.output or "object" in result.output


@pytest.mark.parametrize("engine,fallback", [("rust", False), ("auto", True)])
def test_unsupported_policy_sql_shape_has_explicit_selection(engine, fallback):
    from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError

    layer = make_layer("policy_literals", engine=engine, fallback=fallback)
    try:
        layer.adapter.execute((FIXTURES / "migration_seed.sql").read_text())
        sql = "select quoted.total from quoted"
        if fallback:
            generated = rewrite(layer, sql, {"subject": "O'Brien"})
            assert layer.adapter.execute(generated).fetchall() == [(9,)]
            assert layer.last_engine_selection["engine"] == "python"
            assert "rewrite.policy_select_shape" in layer.last_engine_selection["reason"]
        else:
            with pytest.raises(UnsupportedSemanticFeaturesError, match="rewrite.policy_select_shape"):
                rewrite(layer, sql, {"subject": "O'Brien"})
    finally:
        layer.adapter.close()


def test_scoped_filters_and_alias_pagination_keep_authorized_rows():
    layer = make_layer("policy_aggregate")
    try:
        layer.adapter.execute((FIXTURES / "policy_aggregate.sql").read_text())
        sql = (
            "select purchases.id as purchase, accounts.tier, purchases.revenue as total from metrics "
            "where purchases.id > 1 having purchases.revenue >= 60 "
            "order by total desc limit 1 offset 0"
        )
        # Purchases 4/5 are outside the account policy; 6 violates the invariant.
        assert layer.adapter.execute(rewrite(layer, sql, {"tenant": 1})).fetchall() == [(3, "business", 60)]
    finally:
        layer.adapter.close()
