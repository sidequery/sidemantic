"""Regression tests for Cube adapter correctness fixes (import crash-risks + SQL normalization).

Each test corresponds to a verified finding from the Cube correctness audit:

- #1  ``_normalize_cube_sql`` now rewrites dotted self-references ``{CUBE.col}`` / ``${CUBE.col}``
      (and the ``cube_name`` equivalents) to ``{model}.col`` across dimensions/measures/segments/filters.
- #4  Cube's camelCase join relationship values (belongsTo/hasMany/hasOne) no longer crash the parser.
- #6  Sub-day pre-aggregation granularities (minute/second) no longer raise a validation error.
- #27 ``build_range_start``/``build_range_end`` present with a null value no longer crashes.
"""

import tempfile
import warnings
from pathlib import Path

import pytest
import yaml

from sidemantic import SemanticLayer
from sidemantic.adapters.cube import CubeAdapter, CubeImportWarning, _model_placeholder_to_cube, _normalize_cube_sql
from sidemantic.core.preagg_matcher import GRANULARITY_HIERARCHY
from sidemantic.core.relationship import Relationship


def test_derived_export_only_rewrites_executable_semantic_member_references():
    convert = CubeAdapter._derived_sql_to_cube
    known = {"orders.total"}

    assert convert('"orders.total" + orders.total', known) == '"orders.total" + ${orders.total}'
    assert convert("`orders.total` + orders.total", known) == "`orders.total` + ${orders.total}"
    assert convert("[orders.total] + orders.total", known) == "[orders.total] + ${orders.total}"
    assert convert("orders.total /* orders.total */", known) == "${orders.total} /* orders.total */"
    assert convert("orders.total = $$orders.total$$", known) == "${orders.total} = $$orders.total$$"
    assert convert("orders.total = $tag$orders.total$tag$", known) == ("${orders.total} = $tag$orders.total$tag$")
    assert convert("orders . total", known) == "${orders . total}"
    assert convert("orders/* member */.total", known) == "${orders/* member */.total}"
    assert convert("(SELECT orders.total FROM audit AS orders)", known) == (
        "(SELECT orders.total FROM audit AS orders)"
    )
    assert convert("(SELECT orders.total FROM audit AS ORDERS)", known) == (
        "(SELECT orders.total FROM audit AS ORDERS)"
    )


def test_model_placeholder_export_uses_the_same_sql_lexical_boundary():
    sql = "{model}.amount = '{model}' AND \"{model}\" = `{model}` AND [{model}] = $$ {model} $$ /* {model} */"

    assert _model_placeholder_to_cube(sql) == (
        "${CUBE}.amount = '{model}' AND \"{model}\" = `{model}` AND [{model}] = $$ {model} $$ /* {model} */"
    )
    assert _model_placeholder_to_cube("${model}.amount + {model}.tax") == "${model}.amount + ${CUBE}.tax"
    assert _model_placeholder_to_cube("{model}.x /* outer /* inner */ {model}.y */") == (
        "${CUBE}.x /* outer /* inner */ {model}.y */"
    )


def _parse(yaml_text: str):
    adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yaml_text)
        path = Path(f.name)
    try:
        return adapter.parse(path)
    finally:
        path.unlink()


def _export(graph):
    adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        out = Path(f.name)
    try:
        adapter.export(graph, out)
        return yaml.safe_load(out.read_text())
    finally:
        out.unlink()


# --------------------------------------------------------------------------------------
# #1 - dotted self-reference normalization
# --------------------------------------------------------------------------------------


def test_normalize_dotted_cube_ref_unit():
    # Self-reference (by CUBE and by name) rewritten to {model}; foreign-cube refs preserved.
    assert _normalize_cube_sql("{CUBE.amount}", "orders") == "{model}.amount"
    assert _normalize_cube_sql("${CUBE.amount}", "orders") == "{model}.amount"
    assert _normalize_cube_sql("${CUBE}.amount", "orders") == "{model}.amount"
    assert _normalize_cube_sql("{orders.amount}", "orders") == "{model}.amount"
    assert _normalize_cube_sql("{CUBE.a} / {CUBE.b}", "orders") == "{model}.a / {model}.b"
    # Bare {CUBE} still collapses to {model}.
    assert _normalize_cube_sql("{CUBE}", "orders") == "{model}"
    # Other-cube refs and already-normalized SQL are left untouched (idempotent).
    assert _normalize_cube_sql("{CUBE.a} + {other.b}", "orders") == "{model}.a + {other.b}"
    assert _normalize_cube_sql("{model}.a", "orders") == "{model}.a"


def test_dotted_cube_ref_normalized_in_dimension_measure_segment_filter():
    graph = _parse(
        """
cubes:
  - name: users
    sql_table: users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name_upper
        sql: "{CUBE.user_name}"
        type: string
    measures:
      - name: admin_count
        type: count
        filters:
          - sql: "{CUBE.user_type} = 'admin'"
    segments:
      - name: active
        sql: "{CUBE.status} = 'active'"
"""
    )
    model = graph.get_model("users")
    dim = next(d for d in model.dimensions if d.name == "name_upper")
    assert dim.sql == "{model}.user_name"
    seg = next(s for s in model.segments if s.name == "active")
    assert seg.sql == "{model}.status = 'active'"
    measure = next(m for m in model.metrics if m.name == "admin_count")
    assert measure.filters[0] == "{model}.user_type = 'admin'"


def test_dotted_cube_ref_generates_executable_sql():
    # Before the fix the {CUBE.amount} placeholder survived into the SQL and DuckDB parsed
    # it as STRUCT(...); now it resolves to a real column reference.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: total_doubled
        type: sum
        sql: "{CUBE.amount} * 2"
"""
    )
    layer = SemanticLayer()
    layer.graph = graph
    layer.adapter.execute("CREATE TABLE orders (id INT, amount INT)")
    layer.adapter.execute("INSERT INTO orders VALUES (1, 10), (2, 20)")

    sql = layer.compile(metrics=["orders.total_doubled"])
    rows = layer.adapter.execute(sql).fetchall()
    assert rows[0][0] == 60  # (10 + 20) * 2


# --------------------------------------------------------------------------------------
# #4 - camelCase join relationship values
# --------------------------------------------------------------------------------------


def test_belongs_to_maps_to_many_to_one():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: customers
        relationship: belongsTo
        sql: "${CUBE}.customer_id = ${customers.id}"
  - name: customers
    sql_table: customers
"""
    )
    rel = graph.get_model("orders").relationships[0]
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "customer_id"
    assert rel.primary_key == "id"


def test_has_many_maps_to_one_to_many():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: line_items
        relationship: hasMany
        sql: "${CUBE}.id = ${line_items.order_id}"
  - name: line_items
    sql_table: line_items
"""
    )
    rel = graph.get_model("orders").relationships[0]
    assert rel.type == "one_to_many"
    assert rel.foreign_key == "order_id"
    assert rel.primary_key == "id"


def test_has_one_maps_to_one_to_one():
    graph = _parse(
        """
cubes:
  - name: users
    sql_table: users
    joins:
      - name: profiles
        relationship: hasOne
        sql: "${CUBE}.id = ${profiles.user_id}"
  - name: profiles
    sql_table: profiles
"""
    )
    rel = graph.get_model("users").relationships[0]
    assert rel.type == "one_to_one"
    # one_to_one preserves the explicit condition rather than splitting into keys.
    assert rel.sql == "{from}.id = {to}.user_id"


def test_unknown_relationship_warns_and_defaults_to_many_to_one():
    adapter = CubeAdapter()
    yaml_text = """
cubes:
  - name: x
    sql_table: x
    joins:
      - name: y
        relationship: sideways
        sql: "${CUBE}.y_id = ${y.id}"
  - name: y
    sql_table: y
"""
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yaml_text)
        path = Path(f.name)
    try:
        with pytest.warns(UserWarning, match="Unknown Cube join relationship"):
            graph = adapter.parse(path)
    finally:
        path.unlink()
    rel = graph.get_model("x").relationships[0]
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "y_id"


# --------------------------------------------------------------------------------------
# #6 - sub-day pre-aggregation granularities
# --------------------------------------------------------------------------------------


def test_preagg_sub_day_granularity_does_not_crash():
    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: by_minute
        type: rollup
        measures:
          - CUBE.count
        time_dimension: CUBE.ts
        granularity: minute
"""
    )
    preagg = graph.get_model("events").pre_aggregations[0]
    assert preagg.granularity == "minute"


def test_granularity_hierarchy_includes_sub_day():
    assert GRANULARITY_HIERARCHY["minute"] > GRANULARITY_HIERARCHY["hour"]
    assert GRANULARITY_HIERARCHY["second"] > GRANULARITY_HIERARCHY["minute"]


# --------------------------------------------------------------------------------------
# #27 - build range present with a null value
# --------------------------------------------------------------------------------------


def test_preagg_null_build_range_does_not_crash():
    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: main
        type: rollup
        measures:
          - CUBE.count
        time_dimension: CUBE.ts
        granularity: day
        build_range_start:
        build_range_end:
"""
    )
    preagg = graph.get_model("events").pre_aggregations[0]
    assert preagg.build_range_start is None
    assert preagg.build_range_end is None


# --------------------------------------------------------------------------------------
# Import-fidelity batch: measures
# --------------------------------------------------------------------------------------


def test_rolling_window_leading_offset_preserved_and_warns():
    with pytest.warns(CubeImportWarning, match="leading/offset"):
        graph = _parse(
            """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: trailing_7
        type: sum
        sql: amount
        rolling_window:
          trailing: 7 day
      - name: shifted
        type: sum
        sql: amount
        rolling_window:
          trailing: 7 day
          offset: start
          leading: 2 day
"""
        )
    model = graph.get_model("events")
    plain = next(m for m in model.metrics if m.name == "trailing_7")
    shifted = next(m for m in model.metrics if m.name == "shifted")
    # The two measures no longer collapse to identical configs.
    assert shifted.meta["cube_internal"]["rolling_window_offset"] == "start"
    assert shifted.meta["cube_internal"]["rolling_window_leading"] == "2 day"
    assert (plain.meta or {}).get("cube_internal") is None


def test_cross_cube_measure_ref_resolved_in_derived():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: total
        type: sum
        sql: amount
  - name: line_items
    sql_table: line_items
    measures:
      - name: count
        type: count
      - name: ratio_to_orders
        type: number
        sql: "${count} / ${orders.total}"
"""
    )
    measure = next(m for m in graph.get_model("line_items").metrics if m.name == "ratio_to_orders")
    # Local ref -> cube.measure; cross-cube ref kept verbatim (was left unresolved before).
    assert measure.sql == "line_items.count / orders.total"


def test_count_distinct_approx_maps_to_first_class_agg():
    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: uniq
        type: count_distinct_approx
        sql: user_id
"""
    )
    assert graph.get_model("events").get_metric("uniq").agg == "approx_count_distinct"


# --------------------------------------------------------------------------------------
# Import-fidelity batch: dimensions
# --------------------------------------------------------------------------------------


def test_boolean_dimension_imports_as_boolean():
    graph = _parse(
        """
cubes:
  - name: users
    sql_table: users
    dimensions:
      - name: active
        sql: is_active
        type: boolean
"""
    )
    assert graph.get_model("users").get_dimension("active").type == "boolean"


def test_geo_dimension_preserved_and_warns():
    with pytest.warns(CubeImportWarning, match="type geo"):
        graph = _parse(
            """
cubes:
  - name: stores
    sql_table: stores
    dimensions:
      - name: location
        type: geo
        latitude:
          sql: "{CUBE}.lat"
        longitude:
          sql: "{CUBE}.lng"
"""
        )
    dim = graph.get_model("stores").get_dimension("location")
    assert dim.meta["cube_internal"]["cube_type"] == "geo"
    assert dim.meta["cube_internal"]["latitude"] == {"sql": "{CUBE}.lat"}
    assert dim.meta["cube_internal"]["longitude"] == {"sql": "{CUBE}.lng"}


def test_sub_query_dimension_preserved_and_warns():
    with pytest.warns(CubeImportWarning, match="sub_query"):
        graph = _parse(
            """
cubes:
  - name: users
    sql_table: users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: total_orders
        type: number
        sub_query: true
        sql: "{orders.count}"
"""
        )
    # Stored under the adapter-owned namespace so export can lift it back to the top-level
    # Cube `sub_query` property rather than demoting it to meta.sub_query.
    assert graph.get_model("users").get_dimension("total_orders").meta["cube_internal"]["sub_query"] is True


def test_custom_granularity_definitions_preserved_and_warns():
    with pytest.warns(CubeImportWarning, match="custom granularities"):
        graph = _parse(
            """
cubes:
  - name: sales
    sql_table: sales
    dimensions:
      - name: created
        type: time
        sql: created_at
        granularities:
          - name: fiscal_year
            interval: 1 year
            offset: -3 months
"""
        )
    dim = graph.get_model("sales").get_dimension("created")
    assert dim.supported_granularities == ["fiscal_year"]
    assert dim.meta["custom_granularities"][0]["interval"] == "1 year"


# --------------------------------------------------------------------------------------
# Import-fidelity batch: pre-aggregations
# --------------------------------------------------------------------------------------
def test_rollup_join_rollups_preserved_and_warns():
    with pytest.warns(CubeImportWarning, match="rollup_join"):
        graph = _parse(
            """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: joined
        type: rollupJoin
        rollups:
          - other_cube.some_rollup
"""
        )
    preagg = graph.get_model("events").pre_aggregations[0]
    assert preagg.type == "rollup_join"
    assert preagg.rollups == ["other_cube.some_rollup"]


# --------------------------------------------------------------------------------------
# Export round-trip
# --------------------------------------------------------------------------------------


def test_export_converts_model_placeholder_to_cube():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: big
        sql: "{CUBE.amount}"
        type: number
    measures:
      - name: total
        type: sum
        sql: "{CUBE.amount}"
      - name: admins
        type: count
        filters:
          - sql: "{CUBE.kind} = 'admin'"
"""
    )
    cube = next(c for c in _export(graph)["cubes"] if c["name"] == "orders")
    big = next(d for d in cube["dimensions"] if d["name"] == "big")
    total = next(m for m in cube["measures"] if m["name"] == "total")
    admins = next(m for m in cube["measures"] if m["name"] == "admins")
    assert big["sql"] == "${CUBE}.amount"
    assert total["sql"] == "${CUBE}.amount"
    assert admins["filters"] == [{"sql": "${CUBE}.kind = 'admin'"}]


def test_export_cumulative_keeps_aggregation_type():
    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: rolling_total
        type: sum
        sql: amount
        rolling_window:
          trailing: 7 day
"""
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "rolling_total")
    assert m["type"] == "sum"  # not the non-aggregating "number"
    assert m["rolling_window"] == {"trailing": "7 day"}


def test_export_does_not_fabricate_drill_members():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: country
        sql: country
        type: string
      - name: state
        sql: state
        type: string
    measures:
      - name: count
        type: count
"""
    )
    # Create a hierarchy so the old code would have fabricated drill_members.
    graph.get_model("orders").get_dimension("state").parent = "country"
    count = next(m for m in _export(graph)["cubes"][0]["measures"] if m["name"] == "count")
    assert "drill_members" not in count


def test_boolean_dimension_round_trips_to_cube():
    graph = _parse(
        """
cubes:
  - name: users
    sql_table: users
    dimensions:
      - name: active
        sql: is_active
        type: boolean
"""
    )
    active = next(d for d in _export(graph)["cubes"][0]["dimensions"] if d["name"] == "active")
    assert active["type"] == "boolean"


def test_export_omits_many_to_many_with_warning():
    graph = _parse(
        """
cubes:
  - name: a
    sql_table: a
  - name: b
    sql_table: b
"""
    )
    graph.get_model("a").relationships.append(Relationship(name="b", type="many_to_many", foreign_key="b_id"))
    with pytest.warns(CubeImportWarning, match="many_to_many"):
        exported = _export(graph)
    cube_a = next(c for c in exported["cubes"] if c["name"] == "a")
    assert all(j["name"] != "b" for j in cube_a.get("joins", []))


def test_export_omits_cross_join_with_warning():
    graph = _parse(
        """
cubes:
  - name: a
    sql_table: a
  - name: b
    sql_table: b
"""
    )
    graph.get_model("a").relationships.append(Relationship(name="b", type="cross"))
    with pytest.warns(CubeImportWarning, match="cross"):
        exported = _export(graph)
    cube_a = next(c for c in exported["cubes"] if c["name"] == "a")
    assert all(j["name"] != "b" for j in cube_a.get("joins", []))


def test_dotted_sql_round_trips_through_export():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: total
        type: sum
        sql: "{CUBE.amount}"
"""
    )
    exported = _export(graph)
    adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yaml.safe_dump(exported))
        path = Path(f.name)
    try:
        reimported = adapter.parse(path)
    finally:
        path.unlink()
    # {CUBE.amount} -> {model}.amount -> ${CUBE}.amount -> {model}.amount
    assert reimported.get_model("orders").get_metric("total").sql == "{model}.amount"


# --------------------------------------------------------------------------------------
# Governance / visibility warning sweep
# --------------------------------------------------------------------------------------


def test_cube_access_policy_preserved_and_warns():
    with pytest.warns(CubeImportWarning, match="access_policy"):
        graph = _parse(
            """
cubes:
  - name: orders
    sql_table: orders
    access_policy:
      - role: admin
        row_level:
          filters: []
    measures:
      - name: count
        type: count
"""
        )
    assert graph.get_model("orders").meta["cube_internal"]["top_level"]["access_policy"][0]["role"] == "admin"


def test_cube_public_false_preserved_and_warns():
    with pytest.warns(CubeImportWarning, match="visibility"):
        graph = _parse(
            """
cubes:
  - name: internal
    sql_table: internal
    public: false
    measures:
      - name: count
        type: count
"""
        )
    assert graph.get_model("internal").meta["cube_internal"]["top_level"]["public"] is False


def test_js_cube_files_skipped_with_warning(tmp_path):
    (tmp_path / "model.yml").write_text(
        "cubes:\n  - name: orders\n    sql_table: orders\n    measures:\n      - name: count\n        type: count\n"
    )
    (tmp_path / "Orders.js").write_text("cube('Orders', {});")
    adapter = CubeAdapter()
    with pytest.warns(CubeImportWarning, match="JavaScript"):
        graph = adapter.parse(tmp_path)
    assert "orders" in graph.models


def test_js_scan_prunes_dependency_dirs(tmp_path):
    # A large node_modules/dist tree must not be walked just to emit the .js warning.
    (tmp_path / "model.yml").write_text(
        "cubes:\n  - name: orders\n    sql_table: orders\n    measures:\n      - name: count\n        type: count\n"
    )
    nm = tmp_path / "node_modules" / "pkg"
    nm.mkdir(parents=True)
    (nm / "index.js").write_text("module.exports = {};")
    adapter = CubeAdapter()
    with warnings.catch_warnings():
        # node_modules is pruned, so its .js does not trigger the warning (would error here).
        warnings.simplefilter("error", CubeImportWarning)
        graph = adapter.parse(tmp_path)
    assert "orders" in graph.models


def test_view_access_policy_and_description_preserved():
    with pytest.warns(CubeImportWarning, match="access_policy"):
        graph = _parse(
            """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
views:
  - name: orders_view
    description: "Customer-facing orders"
    access_policy:
      - role: viewer
        row_level:
          filters: []
    cubes:
      - join_path: orders
        includes: "*"
"""
        )
    view = graph.get_model("orders_view")
    assert view.description == "Customer-facing orders"
    assert view.meta["access_policy"][0]["role"] == "viewer"


# --------------------------------------------------------------------------------------
# Adversarial-review follow-ups (regression + crash guards + export round-trip)
# --------------------------------------------------------------------------------------


def test_derived_self_cube_dotted_measure_ref_resolves_like_bare():
    # Regression: the dotted self-cube form must resolve to MEASURE refs, identical to
    # the bare form -- not to {model}.col column refs (which point at nothing).
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: a
        type: sum
        sql: x
      - name: b
        type: sum
        sql: y
      - name: dotted
        type: number
        sql: "${CUBE.a} + ${CUBE.b}"
      - name: bare
        type: number
        sql: "${a} + ${b}"
"""
    )
    model = graph.get_model("orders")
    assert model.get_metric("dotted").sql == "orders.a + orders.b"
    assert model.get_metric("bare").sql == "orders.a + orders.b"


def test_non_string_relationship_warns_and_defaults():
    with pytest.warns(UserWarning, match="Unknown Cube join relationship"):
        graph = _parse(
            """
cubes:
  - name: a
    sql_table: a
    joins:
      - name: b
        relationship: [bad, value]
        sql: "${CUBE}.b_id = ${b.id}"
  - name: b
    sql_table: b
"""
        )
    assert graph.get_model("a").relationships[0].type == "many_to_one"


def test_granularities_with_bare_string_does_not_crash():
    graph = _parse(
        """
cubes:
  - name: sales
    sql_table: sales
    dimensions:
      - name: created
        type: time
        sql: created_at
        granularities:
          - just_a_string
          - name: fiscal
            interval: 1 year
"""
    )
    assert graph.get_model("sales").get_dimension("created").supported_granularities == ["fiscal"]


def test_export_preserves_custom_aggregate_type():
    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: p50
        type: number_agg
        sql: "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {CUBE.latency})"
"""
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "p50")
    assert m["type"] == "number_agg"  # not the lossy "count"
    assert "PERCENTILE_CONT" in m["sql"]


def test_rank_measure_round_trips_to_top_level():
    # rank imports with agg="count" fallback + meta cube_type; export must re-emit
    # type: rank (not count) and lift order_by/reduce_by back to top-level Cube keys.
    graph = _parse(
        """
cubes:
  - name: leaderboard
    sql_table: leaderboard
    measures:
      - name: position
        type: rank
        order_by:
          - sql: "{CUBE}.score"
            dir: desc
"""
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "position")
    assert m["type"] == "rank"
    assert m["order_by"] == [{"sql": "{CUBE}.score", "dir": "desc"}]
    assert "cube_type" not in (m.get("meta") or {})


def test_preagg_rollup_join_camelcase_type_round_trips():
    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: joined
        type: rollupJoin
        rollups:
          - other.rollup
"""
    )
    p = _export(graph)["cubes"][0]["pre_aggregations"][0]
    # Exported with Cube's camelCase type (not the snake-case internal form) and rollups.
    assert p["type"] == "rollupJoin"
    # A cross-cube rollup ref stays qualified as-is; CUBE.other.rollup would read as a
    # current-cube member and break the join.
    assert p["rollups"] == ["other.rollup"]


def test_governance_round_trips_to_top_level_cube_keys():
    graph = _parse(
        """
cubes:
  - name: internal
    sql_table: internal
    public: false
    access_policy:
      - role: admin
    measures:
      - name: count
        type: count
"""
    )
    cube = _export(graph)["cubes"][0]
    assert cube["public"] is False
    assert cube["access_policy"] == [{"role": "admin"}]
    assert "public" not in (cube.get("meta") or {})
    assert "cube_top_level" not in (cube.get("meta") or {})


def test_export_does_not_lift_arbitrary_user_meta_keys():
    # A user meta key named like a Cube field must NOT be promoted to a top-level control.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    meta:
      public: internal
      title: My Orders
    measures:
      - name: count
        type: count
"""
    )
    cube = _export(graph)["cubes"][0]
    # The user meta stays under meta, not lifted to top-level Cube fields.
    assert cube["meta"]["public"] == "internal"
    assert cube["meta"]["title"] == "My Orders"
    assert "public" not in {k: v for k, v in cube.items() if k != "meta"}
    assert "title" not in {k: v for k, v in cube.items() if k != "meta"}


def test_cross_cube_derived_measure_round_trips_to_cube_refs():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: total
        type: sum
        sql: amount
  - name: line_items
    sql_table: line_items
    measures:
      - name: count
        type: count
      - name: per_order
        type: number
        sql: "${count} + ${orders.total}"
"""
    )
    m = next(x for x in _export(graph)["cubes"] if x["name"] == "line_items")
    per_order = next(x for x in m["measures"] if x["name"] == "per_order")
    # Both the local and cross-cube member refs come back as Cube ${...} references.
    assert per_order["sql"] == "${line_items.count} + ${orders.total}"


def test_export_rolling_window_leading_offset_not_duplicated_in_meta():
    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: shifted
        type: sum
        sql: amount
        rolling_window:
          trailing: 7 day
          leading: 2 day
          offset: start
"""
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "shifted")
    assert m["rolling_window"]["leading"] == "2 day"
    assert m["rolling_window"]["offset"] == "start"
    assert "rolling_window_leading" not in (m.get("meta") or {})
    assert "cube_internal" not in (m.get("meta") or {})


def test_export_does_not_misread_user_measure_meta_cube_type():
    # A normal measure whose own meta happens to use the name "cube_type" must not be
    # reinterpreted as a rank/number_agg measure; the marker lives under cube_internal.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: total
        type: sum
        sql: amount
        meta:
          cube_type: rank
"""
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "total")
    assert m["type"] == "sum"  # not misread as "rank"
    assert m["meta"]["cube_type"] == "rank"  # user meta preserved verbatim


def test_export_does_not_wrap_non_member_dotted_sql():
    # Hand-authored derived SQL referencing non-member table.column must NOT be wrapped as a
    # Cube member reference (only known semantic members are wrapped).
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="ratio", type="derived", sql="external.rate - other.fee")],
        )
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "ratio")
    # Neither external.rate nor other.fee is a known member, so they stay as plain SQL.
    assert m["sql"] == "external.rate - other.fee"


def test_derived_no_dollar_member_refs_resolve():
    # Cube supports the no-dollar reference form; derived re-resolution must handle it too.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: total
        type: sum
        sql: amount
  - name: line_items
    sql_table: line_items
    measures:
      - name: cnt
        type: count
      - name: combined
        type: number
        sql: "{CUBE.cnt} + {orders.total}"
"""
    )
    m = graph.get_model("line_items").get_metric("combined")
    assert m.sql == "line_items.cnt + orders.total"


def test_derived_export_does_not_wrap_member_inside_string_literal():
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[
                Metric(name="total", agg="sum", sql="amount"),
                Metric(
                    name="flagged",
                    type="derived",
                    sql="CASE WHEN kind = 'orders.total' THEN 1 ELSE orders.total END",
                ),
            ],
        )
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "flagged")
    # The string literal is left intact; only the real member reference is wrapped.
    assert m["sql"] == "CASE WHEN kind = 'orders.total' THEN 1 ELSE ${orders.total} END"


def test_user_meta_cube_internal_non_dict_does_not_crash():
    # A measure needing internal markers (rank) whose own meta uses a non-dict
    # "cube_internal" must not crash the import (the namespace is replaced, not dict()'d).
    graph = _parse(
        """
cubes:
  - name: leaderboard
    sql_table: leaderboard
    measures:
      - name: position
        type: rank
        meta:
          cube_internal: keepme
        order_by:
          - sql: "{CUBE}.score"
"""
    )
    m = graph.get_model("leaderboard").get_metric("position")
    assert m.meta["cube_internal"]["cube_type"] == "rank"


def test_user_meta_cube_top_level_preserved_alongside_governance():
    # A cube with a preserved top-level field (public:false) whose own meta also has a
    # "cube_top_level" key must keep the user value verbatim (not clobber it) and stash the
    # governance separately under the adapter-owned cube_internal.top_level namespace.
    graph = _parse(
        """
cubes:
  - name: internal
    sql_table: internal
    public: false
    meta:
      cube_top_level: keepme
    measures:
      - name: count
        type: count
"""
    )
    m = graph.get_model("internal")
    assert m.meta["cube_top_level"] == "keepme"  # user value preserved, not overwritten
    assert m.meta["cube_internal"]["top_level"]["public"] is False  # governance stashed separately


def test_user_meta_cube_top_level_dict_round_trips_verbatim():
    # Codex P2: ordinary user metadata named cube_top_level (with no real top-level governance
    # on the cube) must NOT be lifted to top-level Cube fields on export; it round-trips verbatim.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    meta:
      cube_top_level:
        public: internal
    measures:
      - name: count
        type: count
"""
    )
    cube = _export(graph)["cubes"][0]
    assert "public" not in cube  # not promoted to a real top-level control
    assert cube["meta"]["cube_top_level"] == {"public": "internal"}


def test_geo_dimension_round_trips_to_cube():
    graph = _parse(
        """
cubes:
  - name: stores
    sql_table: stores
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: location
        type: geo
        latitude:
          sql: "{CUBE}.lat"
        longitude:
          sql: "{CUBE}.lng"
"""
    )
    cube = _export(graph)["cubes"][0]
    loc = next(d for d in cube["dimensions"] if d["name"] == "location")
    assert loc["type"] == "geo"
    assert loc["latitude"] == {"sql": "{CUBE}.lat"}
    assert loc["longitude"] == {"sql": "{CUBE}.lng"}
    # The internal markers are not left behind in exported meta.
    assert "cube_type" not in (loc.get("meta") or {})
    assert "latitude" not in (loc.get("meta") or {})


def test_user_meta_cube_type_geo_not_exported_as_geo_dimension():
    # A normal string dimension whose own meta uses cube_type=geo must not be re-emitted as a
    # Cube geo dimension (the geo marker lives under the internal namespace, not raw meta).
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: kind
        sql: kind
        type: string
        meta:
          cube_type: geo
"""
    )
    kind = next(d for d in _export(graph)["cubes"][0]["dimensions"] if d["name"] == "kind")
    assert kind["type"] == "string"
    assert kind["meta"]["cube_type"] == "geo"


def test_export_preserves_user_measure_cube_internal_meta():
    # A plain measure with its own (non-adapter) cube_internal meta must round-trip.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: total
        type: sum
        sql: amount
        meta:
          cube_internal: keepme
"""
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "total")
    assert m["meta"]["cube_internal"] == "keepme"


def test_export_keeps_user_cube_internal_keys_alongside_adapter_markers():
    # rank sets adapter markers under cube_internal; a user key in the same namespace must
    # survive export while the adapter markers are stripped.
    graph = _parse(
        """
cubes:
  - name: board
    sql_table: board
    measures:
      - name: rnk
        type: rank
        meta:
          cube_internal:
            mykey: 1
        order_by:
          - sql: "{CUBE}.score"
"""
    )
    m = next(x for x in _export(graph)["cubes"][0]["measures"] if x["name"] == "rnk")
    assert m["type"] == "rank"
    assert m["meta"]["cube_internal"] == {"mykey": 1}


def test_derived_trailing_cube_column_refs_stay_columns():
    # ${CUBE}.col (trailing form) is a COLUMN ref, not a member; it must become {model}.col,
    # not a metric dependency like orders.col.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: margin
        type: number
        sql: "${CUBE}.gross - ${CUBE}.discount"
"""
    )
    assert graph.get_model("orders").get_metric("margin").sql == "{model}.gross - {model}.discount"


def test_complete_expression_measure_not_materialized_or_matched():
    # A complete-expression measure (agg=None, sql_is_complete) listed in a rollup must not
    # crash materialization, must not be stored as a re-aggregatable column, and must not be
    # routed to the rollup (it is computed from source at query time).
    from sidemantic.core.dimension import Dimension
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.pre_aggregation import PreAggregation
    from sidemantic.core.preagg_matcher import PreAggregationMatcher

    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=[
            Metric(name="cnt", agg="count"),
            Metric(
                name="p95",
                sql="PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {model}.latency)",
                sql_is_complete=True,
            ),
        ],
        pre_aggregations=[PreAggregation(name="r", type="rollup", measures=["cnt", "p95"], dimensions=["status"])],
    )
    preagg = model.pre_aggregations[0]
    sql = preagg.generate_materialization_sql(model)
    assert "cnt_raw" in sql  # plain aggregate is materialized
    assert "p95_raw" not in sql  # complete-expression measure is skipped
    assert "PERCENTILE_CONT" not in sql
    matcher = PreAggregationMatcher(model)
    assert matcher.can_satisfy_query(preagg, ["p95"], ["status"]) is False
    assert matcher.can_satisfy_query(preagg, ["cnt"], ["status"]) is True


def test_derived_resolve_skips_string_literals():
    # A member-looking token inside a single-quoted literal must not be rewritten.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
      - name: pending_count
        type: number
        sql: "CASE WHEN status = '{pending}' THEN ${count} ELSE 0 END"
"""
    )
    m = graph.get_model("orders").get_metric("pending_count")
    assert m.sql == "CASE WHEN status = '{pending}' THEN orders.count ELSE 0 END"


def test_rollup_skips_derived_measure_in_materialization():
    # A derived (agg=None, not sql_is_complete) measure listed in a rollup must be skipped,
    # not emitted as raw metric references from the source table.
    from sidemantic.core.dimension import Dimension
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.pre_aggregation import PreAggregation

    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="margin", type="derived", sql="{model}.revenue - {model}.cost"),
        ],
        pre_aggregations=[
            PreAggregation(name="r", type="rollup", measures=["revenue", "margin"], dimensions=["status"])
        ],
    )
    sql = model.pre_aggregations[0].generate_materialization_sql(model)
    assert "revenue_raw" in sql
    assert "margin_raw" not in sql


def test_user_meta_cube_internal_geo_not_exported_as_geo():
    # A string dimension whose own meta spoofs cube_internal.cube_type=geo (without
    # latitude/longitude) must not be re-emitted as a Cube geo dimension.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: kind
        sql: kind
        type: string
        meta:
          cube_internal:
            cube_type: geo
"""
    )
    kind = next(d for d in _export(graph)["cubes"][0]["dimensions"] if d["name"] == "kind")
    assert kind["type"] == "string"


def test_normalize_skips_cube_refs_in_string_literals():
    # A Cube-ref-looking string literal must not be rewritten by the normalizer (dim/segment/
    # filter SQL), while real refs outside the literal still normalize.
    assert (
        _normalize_cube_sql("CASE WHEN label = '{CUBE.status}' THEN 1 ELSE 0 END", "orders")
        == "CASE WHEN label = '{CUBE.status}' THEN 1 ELSE 0 END"
    )
    assert _normalize_cube_sql("{CUBE.x} = '{CUBE.y}'", "orders") == "{model}.x = '{CUBE.y}'"


def test_original_sql_preagg_round_trips_with_sql():
    # camelCase originalSql imports to original_sql, preserves its sql, and exports back.
    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: base
        type: originalSql
        sql: "SELECT * FROM events WHERE active"
"""
    )
    preagg = graph.get_model("events").pre_aggregations[0]
    assert preagg.type == "original_sql"
    assert preagg.sql == "SELECT * FROM events WHERE active"
    p = _export(graph)["cubes"][0]["pre_aggregations"][0]
    assert p["type"] == "originalSql"
    assert p["sql"] == "SELECT * FROM events WHERE active"


def test_export_does_not_double_wrap_existing_cube_placeholders():
    # An inline-aggregate calculated measure with an existing ${cross.cube} placeholder must
    # not be double-wrapped into ${${...}} on export.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - {name: status, sql: status, type: string}
  - name: line_items
    sql_table: line_items
    measures:
      - name: paid_count
        type: number
        sql: "COUNT(*) FILTER (WHERE ${orders.status} = 'paid')"
"""
    )
    m = next(c for c in _export(graph)["cubes"] if c["name"] == "line_items")["measures"][0]
    assert m["sql"] == "COUNT(*) FILTER (WHERE ${orders.status} = 'paid')"
    assert "${${" not in m["sql"]


def test_cross_cube_trailing_column_ref_translated_to_member():
    # ${other}.col (cross-cube trailing form) references the joined cube's column. Sidemantic
    # calculated measures reference members, not raw joined columns, and an un-resolved ${other}
    # placeholder compiles to invalid SQL (a struct literal). It is translated to the cross-cube
    # member form other.col -- the same as the ${other.col} in-brace form -- so it is executable.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - {name: amount, type: sum, sql: amt}
  - name: line_items
    sql_table: line_items
    joins:
      - name: orders
        relationship: belongsTo
        sql: "${CUBE}.order_id = ${orders.id}"
    measures:
      - name: derived_x
        type: number
        sql: "${orders}.amount * 2"
"""
    )
    m = graph.get_model("line_items").get_metric("derived_x")
    assert m.sql == "orders.amount * 2"
    # It compiles to valid SQL (joins orders, references its measure) -- not a ${...} struct literal.
    layer = SemanticLayer()
    layer.graph = graph
    compiled = layer.compile(metrics=["line_items.derived_x"])
    assert "${orders}" not in compiled and "{'orders'" not in compiled
    assert "SUM(orders_cte.amount_raw)" in compiled


def test_rollup_with_only_unmaterializable_measures_is_rejected():
    # A rollup that lists only agg=None measures and declares no dimensions/time dimension would
    # render "SELECT  FROM ... GROUP BY " (invalid SQL). generate_materialization_sql must reject
    # it with a clear error instead of emitting un-runnable SQL.
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.pre_aggregation import PreAggregation

    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        metrics=[Metric(name="p95", sql="PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY x)", sql_is_complete=True)],
        pre_aggregations=[PreAggregation(name="bad", type="rollup", measures=["p95"])],
    )
    with pytest.raises(ValueError, match="no materializable columns"):
        model.pre_aggregations[0].generate_materialization_sql(model)


def test_cross_cube_rollup_ref_round_trips():
    # rollupLambda/rollupJoin constituents: a self rollup is unqualified and gets the CUBE prefix
    # on export, but an already-qualified cross-cube ref must stay as-is (CUBE.other.x would be
    # read by Cube as a current-cube member, breaking the cross-cube join/lambda).
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.pre_aggregation import PreAggregation
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="line_items",
            table="line_items",
            primary_key="id",
            metrics=[Metric(name="cnt", agg="count")],
            pre_aggregations=[
                PreAggregation(name="self_rollup", type="rollup", measures=["cnt"]),
                PreAggregation(name="lam", type="lambda", rollups=["self_rollup", "visitors.for_join"]),
            ],
        )
    )
    preaggs = {p["name"]: p for p in _export(graph)["cubes"][0]["pre_aggregations"]}
    assert preaggs["lam"]["rollups"] == ["CUBE.self_rollup", "visitors.for_join"]


def test_export_preserves_model_placeholder_inside_string_literal():
    # Codex P2: a literal "{model}" inside a single-quoted string must NOT be rewritten to
    # "${CUBE}" on export (import already skips quoted literals, so this is required for a
    # faithful round-trip). Real {model} self-refs outside quotes still convert.
    from sidemantic.adapters.cube import _model_placeholder_to_cube

    assert _model_placeholder_to_cube("CASE WHEN label = '{model}' THEN 1 END") == (
        "CASE WHEN label = '{model}' THEN 1 END"
    )
    assert _model_placeholder_to_cube("{model}.amount > 0") == "${CUBE}.amount > 0"
    assert _model_placeholder_to_cube("{model}.x = '{model}'") == "${CUBE}.x = '{model}'"

    # End-to-end: a dimension whose SQL compares against the literal '{model}' round-trips.
    from sidemantic.core.dimension import Dimension
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="flag", type="categorical", sql="CASE WHEN tag = '{model}' THEN 1 END")],
        )
    )
    dim = next(d for d in _export(graph)["cubes"][0]["dimensions"] if d["name"] == "flag")
    assert dim["sql"] == "CASE WHEN tag = '{model}' THEN 1 END"


def test_original_sql_preagg_cube_refs_normalized_and_materializable():
    # Codex P2: an originalSql pre-agg whose custom sql uses Cube self-refs (${CUBE}) must be
    # normalized on import and resolve to executable SQL at materialization (no ${CUBE} reaching
    # the database), and round-trip back to ${CUBE} on export.
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: base
        type: originalSql
        sql: "SELECT ${CUBE}.id, ${CUBE.amount} AS amount FROM ${CUBE}"
"""
    )
    model = graph.get_model("orders")
    preagg = model.pre_aggregations[0]
    # Stored with the {model} placeholder, like every other normalized Cube SQL field.
    assert preagg.sql == "SELECT {model}.id, {model}.amount AS amount FROM {model}"
    # Materialization substitutes {model} with the real table -- no placeholder reaches the DB.
    mat = preagg.generate_materialization_sql(model)
    assert "{model}" not in mat and "${CUBE}" not in mat
    assert mat == "SELECT orders.id, orders.amount AS amount FROM orders"
    # Export converts {model} back to ${CUBE}.
    exported = next(c for c in _export(graph)["cubes"] if c["name"] == "orders")["pre_aggregations"][0]
    assert exported["sql"] == "SELECT ${CUBE}.id, ${CUBE}.amount AS amount FROM ${CUBE}"


def test_original_sql_preagg_on_sql_backed_model_materializes_executable():
    # Codex P2 follow-up: when the cube is defined with `sql:` (not sql_table), originalSql
    # materialization must expose the model SQL as an aliased CTE so {model}.col qualifiers stay
    # valid -- inlining "(SELECT ...).col" is invalid SQL. Verify it actually runs in DuckDB.
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.pre_aggregation import PreAggregation

    model = Model(
        name="orders",
        sql="SELECT 1 AS id, 10 AS amount UNION ALL SELECT 2, 20",
        primary_key="id",
        metrics=[Metric(name="count", agg="count")],
        pre_aggregations=[
            PreAggregation(name="base", type="original_sql", sql="SELECT {model}.id, {model}.amount FROM {model}")
        ],
    )
    mat = model.pre_aggregations[0].generate_materialization_sql(model)
    # The model SQL becomes an aliased CTE and {model} qualifiers point at that alias -- not a
    # bare "(SELECT ...).col", which is invalid SQL.
    assert mat.startswith("WITH orders__base AS (")
    assert "FROM orders__base" in mat
    assert "orders__base.id" in mat and "orders__base.amount" in mat
    assert "{model}" not in mat

    import duckdb

    rows = duckdb.connect().execute(f"SELECT * FROM (\n{mat}\n) t ORDER BY id").fetchall()
    assert rows == [(1, 10), (2, 20)]


def test_inline_agg_measure_not_routed_to_preaggregation():
    # Codex P1: a type:number measure with an inline aggregate and no column dependencies
    # (e.g. COUNT(*)) is agg=None and skipped by the materializer. The matcher must NOT route a
    # query for it to a rollup that lists it -- doing so would recompute COUNT(*) over rollup rows
    # instead of source rows. Marking it sql_is_complete keeps it out of routing.
    from sidemantic.core.pre_aggregation import PreAggregation
    from sidemantic.core.preagg_matcher import PreAggregationMatcher

    graph = _parse(
        """
cubes:
  - name: events
    sql_table: events
    dimensions:
      - {name: status, sql: status, type: string}
    measures:
      - name: total_rows
        type: number
        sql: "COUNT(*)"
"""
    )
    model = graph.get_model("events")
    m = model.get_metric("total_rows")
    assert m.agg is None
    assert m.sql_is_complete is True
    # A dimension-only rollup that lists the measure must not be considered able to satisfy it.
    preagg = PreAggregation(name="by_status", type="rollup", measures=["total_rows"], dimensions=["status"])
    matcher = PreAggregationMatcher(model)
    assert matcher.can_satisfy_query(preagg, ["total_rows"], ["status"]) is False


def test_sub_query_dimension_round_trips_to_top_level():
    # Codex P2: a Cube `sub_query: true` dimension must round-trip to the top-level Cube property,
    # not to meta.sub_query (which would demote a measure-as-dimension to a plain SQL dimension).
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        graph = _parse(
            """
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: total_amount
        type: number
        sub_query: true
        sql: "${line_items.total}"
"""
        )
    dim = graph.get_model("orders").get_dimension("total_amount")
    assert dim.meta["cube_internal"]["sub_query"] is True
    assert "sub_query" not in {k for k in dim.meta if k != "cube_internal"}
    exported = next(d for d in _export(graph)["cubes"][0]["dimensions"] if d["name"] == "total_amount")
    assert exported["sub_query"] is True
    assert "sub_query" not in (exported.get("meta") or {})
