"""Persistent recipes follow the same catalog identities as database views."""

import pytest

from tests.interchange.ossie.test_latest_discovery import _layer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    instance = _layer(request.param)
    try:
        yield instance
    finally:
        instance.adapter.close()


def _create(layer, name, threshold=0):
    layer.sql(f"CREATE OR REPLACE VIEW {name} AS SELECT sum_sal AS pay FROM scott_model.emp WHERE sal>{threshold}")


def _total(layer, name):
    return layer.sql(f"SELECT AGG(pay) FROM {name}").fetchall()


@pytest.mark.parametrize("created,dropped", [("salaries", "main.salaries"), ("main.salaries", "salaries")])
def test_drop_equivalent_qualification_removes_recipe_and_cached_rewrite(layer, created, dropped):
    _create(layer, created)
    assert _total(layer, "salaries") == [(29025,)]
    version = layer.graph._version
    layer.sql(f"DROP VIEW {dropped}")
    assert not layer.graph.metadata["ossie_views"]
    assert not layer.graph.metadata["ossie_view_columns"]
    assert layer.graph._version > version
    with pytest.raises(Exception, match="(?i)(does not exist|unknown|measure|table)"):
        _total(layer, "salaries")


@pytest.mark.parametrize("created,replaced", [("salaries", "main.salaries"), ("main.salaries", "salaries")])
def test_replacement_uses_one_recipe_for_every_qualification(layer, created, replaced):
    _create(layer, created)
    assert _total(layer, "salaries") == [(29025,)]
    _create(layer, replaced, 2000)
    expected = layer.adapter.execute("SELECT SUM(sal) FROM scott.emp WHERE sal>2000").fetchall()
    for name in ("salaries", "main.salaries", '"memory"."main"."SALARIES"'):
        assert _total(layer, name) == expected
    assert len(layer.graph.metadata["ossie_views"]) == 1


def test_schema_search_path_changes_binding_and_invalidates_cached_query(layer):
    layer.adapter.execute("CREATE SCHEMA reporting")
    _create(layer, "main.salaries")
    _create(layer, "reporting.salaries", 2000)
    assert _total(layer, "salaries") == [(29025,)]
    layer.adapter.execute("SET search_path='reporting,main'")
    expected = layer.adapter.execute("SELECT SUM(sal) FROM scott.emp WHERE sal>2000").fetchall()
    assert _total(layer, "salaries") == expected
    assert layer.sql("DESCRIBE salaries").fetchall()[0][1].endswith("MEASURE")
    layer.sql("DROP VIEW salaries")
    assert _total(layer, "salaries") == [(29025,)]
    assert len(layer.graph.metadata["ossie_views"]) == 1
    # A physical relation earlier in the path must shadow the semantic view.
    layer.adapter.execute("CREATE TABLE reporting.salaries (pay INTEGER)")
    layer.adapter.execute("INSERT INTO reporting.salaries VALUES (42)")
    assert layer.sql("SELECT pay FROM salaries").fetchall() == [(42,)]
    assert layer.sql("DESCRIBE salaries").fetchall()[0][1] == "INTEGER"


def test_unqualified_create_uses_selected_schema_and_nested_view_keeps_binding(layer):
    layer.adapter.execute("CREATE SCHEMA reporting")
    layer.adapter.execute("SET schema='reporting'")
    _create(layer, "salaries", 2000)
    layer.sql("CREATE VIEW summary AS SELECT pay FROM salaries")
    expected = _total(layer, "summary")
    layer.adapter.execute("SET schema='main'")
    _create(layer, "salaries")
    assert _total(layer, "reporting.summary") == expected
    assert _total(layer, "salaries") == [(29025,)]
    layer.sql("DROP VIEW reporting.salaries")
    assert len(layer.graph.metadata["ossie_views"]) == 2


def test_other_catalogs_and_quoted_dotted_names_keep_separate_identities(layer):
    layer.adapter.execute("ATTACH ':memory:' AS other")
    layer.adapter.execute("CREATE SCHEMA v")
    _create(layer, "main.salaries")
    _create(layer, "other.main.salaries", 2000)
    _create(layer, '"v.salaries"', 1000)
    _create(layer, "v.salaries", 2000)
    expected_other = _total(layer, "other.main.salaries")
    assert _total(layer, "other.salaries") == expected_other
    metadata = layer.sql(
        "SELECT table_catalog, data_type FROM information_schema.columns "
        "WHERE table_schema='main' AND table_name='salaries' ORDER BY table_catalog"
    ).fetchall()
    assert metadata == [("memory", "DECIMAL MEASURE"), ("other", "DECIMAL MEASURE")]
    layer.sql("DROP VIEW main.salaries")
    assert _total(layer, "other.main.salaries") == expected_other
    assert layer.sql("DESCRIBE other.main.salaries").fetchall()[0][1].endswith("MEASURE")
    quoted_total = _total(layer, '"v.salaries"')
    layer.sql("DROP VIEW v.salaries")
    assert _total(layer, 'main."v.salaries"') == quoted_total
    assert len(layer.graph.metadata["ossie_views"]) == 2


def test_discovery_does_not_remove_physical_tables_in_other_catalogs(layer):
    layer.adapter.execute("ATTACH ':memory:' AS other")
    layer.adapter.execute("CREATE TABLE other.main.salaries (amount INTEGER)")
    _create(layer, "salaries")
    assert layer.sql(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog='other' AND table_schema='main' AND table_name='salaries'"
    ).fetchall() == [("amount", "INTEGER")]


def test_failed_and_noop_ddl_keep_recipes_and_effective_binding_version(layer):
    _create(layer, "salaries")
    assert _total(layer, "salaries") == [(29025,)]
    version = layer.graph._version
    registry = dict(layer.graph.metadata["ossie_views"])
    assert _total(layer, "main.salaries") == [(29025,)]
    layer.sql("DROP VIEW IF EXISTS missing")
    layer.sql("CREATE VIEW IF NOT EXISTS main.salaries AS SELECT 1 AS pay")
    with pytest.raises(Exception):
        layer.sql("CREATE VIEW main.salaries AS SELECT sum_sal AS pay FROM scott_model.emp")
    with pytest.raises(Exception):
        layer.sql("DROP VIEW missing")
    assert layer.graph.metadata["ossie_views"] == registry
    assert layer.graph._version == version
    assert _total(layer, "salaries") == [(29025,)]


def test_binding_preserves_reference_aliases_and_cte_names(layer):
    _create(layer, "salaries")
    layer.sql("CREATE VIEW summary AS SELECT salaries.pay FROM salaries")
    assert _total(layer, "summary") == [(29025,)]
    layer.sql(
        "CREATE VIEW cte_summary AS WITH salaries AS "
        "(SELECT sum_sal AS pay FROM scott_model.emp WHERE sal>2000) SELECT pay FROM salaries"
    )
    expected = layer.adapter.execute("SELECT SUM(sal) FROM scott.emp WHERE sal>2000").fetchall()
    assert _total(layer, "cte_summary") == expected


def test_view_definition_resolves_its_target_schema_before_session_path(layer):
    layer.adapter.execute("CREATE SCHEMA reporting")
    _create(layer, "main.salaries")
    _create(layer, "reporting.salaries", 2000)
    layer.sql("CREATE VIEW reporting.summary AS SELECT pay FROM salaries")
    expected = layer.adapter.execute("SELECT SUM(sal) FROM scott.emp WHERE sal>2000").fetchall()
    assert _total(layer, "reporting.summary") == expected


def test_schema_qualified_drop_honors_temporary_view_shadowing(layer):
    _create(layer, "salaries")
    layer.sql("CREATE TEMP VIEW salaries AS SELECT sum_sal AS pay FROM scott_model.emp WHERE sal>2000")
    expected = layer.adapter.execute("SELECT SUM(sal) FROM scott.emp WHERE sal>2000").fetchall()
    assert _total(layer, "main.salaries") == expected
    layer.sql("DROP VIEW main.salaries")
    assert _total(layer, "salaries") == [(29025,)]
    assert len(layer.graph.metadata["ossie_views"]) == 1


def test_stored_recipe_keeps_physical_binding_after_schema_change(layer):
    layer.adapter.execute("CREATE SCHEMA one")
    layer.adapter.execute("CREATE SCHEMA two")
    layer.adapter.execute("CREATE TABLE one.raw AS SELECT 10 AS amount")
    layer.adapter.execute("CREATE TABLE two.raw AS SELECT 100 AS amount")
    layer.adapter.execute("SET schema='one'")
    layer.sql("CREATE VIEW main.v AS SELECT TO_MEASURE(SUM(amount)) AS revenue FROM raw")
    assert layer.sql("SELECT AGG(revenue) FROM main.v").fetchall() == [(10,)]
    layer.adapter.execute("SET schema='two'")
    assert layer.sql("SELECT AGG(revenue) FROM main.v").fetchall() == [(10,)]


@pytest.mark.parametrize("drop", ["DROP VIEW gone.salaries", "DROP SCHEMA gone CASCADE"])
def test_external_drop_prunes_recipe_discovery_and_cached_rewrite(layer, drop):
    layer.adapter.execute("CREATE SCHEMA gone")
    _create(layer, "gone.salaries")
    assert _total(layer, "gone.salaries") == [(29025,)]
    version = layer.graph._version
    layer.adapter.execute(drop)
    with pytest.raises(Exception, match="(?i)(does not exist|unknown|measure|table)"):
        _total(layer, "gone.salaries")
    assert not layer.graph.metadata["ossie_views"]
    assert not layer.graph.metadata["ossie_view_columns"]
    assert layer.graph._version > version
    assert (
        layer.sql(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='gone' AND table_name='salaries'"
        ).fetchall()
        == []
    )


def test_ambiguous_catalog_and_schema_name_cannot_select_semantic_recipe(layer):
    layer.adapter.execute("CREATE SCHEMA aux")
    _create(layer, "memory.aux.salaries")
    layer.adapter.execute("ATTACH ':memory:' AS aux")
    layer.adapter.execute("CREATE TABLE aux.main.salaries AS SELECT 7 AS pay")
    with pytest.raises(Exception):
        _total(layer, "aux.salaries")
    assert _total(layer, "memory.aux.salaries") == [(29025,)]
    assert layer.sql("SELECT pay FROM aux.main.salaries").fetchall() == [(7,)]


def test_bound_view_discovery_takes_precedence_over_same_named_model(layer):
    _create(layer, "main.emp")
    assert _total(layer, "emp") == [(29025,)]
    unqualified = layer.sql("DESCRIBE emp").fetchall()
    qualified = layer.sql("DESCRIBE main.emp").fetchall()
    assert unqualified == qualified
    assert [(row[0], row[1]) for row in unqualified] == [("pay", "DECIMAL MEASURE")]
