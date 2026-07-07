"""Import of Cube access_policy and Rill security blocks into SecurityPolicy, plus the uri dimension flag."""

import warnings

from sidemantic.adapters.cube import CubeAdapter, _access_policy_to_security
from sidemantic.adapters.rill import RillAdapter
from sidemantic.core.dimension import Dimension


def test_cube_access_policy_row_filters_map_to_security():
    policy, unmapped = _access_policy_to_security(
        [
            {
                "role": "*",
                "row_level": {
                    "filters": [
                        {"member": "{CUBE}.region", "operator": "equals", "values": ["US"]},
                        {"member": "status", "operator": "in", "values": ["active", "trial"]},
                    ]
                },
            }
        ]
    )
    assert policy is not None
    assert policy.row_filters == ["region = 'US' AND status IN ('active', 'trial')"]
    assert unmapped == set()


def test_cube_filter_operators_and_escaping():
    policy, _ = _access_policy_to_security(
        [{"row_level": {"filters": [{"member": "name", "operator": "equals", "values": ["O'Brien"]}]}}]
    )
    # Single quotes are doubled so the literal cannot break out of the fragment.
    assert policy.row_filters == ["name = 'O''Brien'"]

    for operator, values, expected in [
        ("notEquals", ["x"], "col != 'x'"),
        ("notIn", ["a", "b"], "col NOT IN ('a', 'b')"),
        ("gt", [5], "col > 5"),
        ("set", [], "col IS NOT NULL"),
        ("notSet", [], "col IS NULL"),
        ("contains", ["ab"], "col LIKE '%ab%'"),
    ]:
        policy, _ = _access_policy_to_security(
            [{"row_level": {"filters": [{"member": "col", "operator": operator, "values": values}]}}]
        )
        assert policy is not None and policy.row_filters == [expected], operator


def test_cube_unmapped_constructs_reported():
    policy, unmapped = _access_policy_to_security(
        [
            {
                "role": "manager",
                "conditions": [{"if": "something"}],
                "row_level": {"filters": [{"member": "x", "operator": "equals", "values": ["1"]}]},
            }
        ]
    )
    assert policy is not None
    assert "role" in unmapped
    assert "conditions" in unmapped


def test_cube_import_sets_model_security_and_warns():
    yaml_src = """
cubes:
  - name: orders
    sql_table: orders
    access_policy:
      - role: "*"
        row_level:
          filters:
            - member: region
              operator: equals
              values: ["US"]
    dimensions:
      - name: region
        sql: region
        type: string
    measures:
      - name: count
        type: count
"""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "orders.yml"
        p.write_text(yaml_src)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            graph = CubeAdapter().parse(p)
    model = graph.get_model("orders")
    assert model.security is not None
    assert model.security.row_filters == ["region = 'US'"]


def test_rill_security_block_maps_to_security():
    adapter = RillAdapter()
    security = adapter._parse_security({"access": "{{ .user.admin }}", "row_filter": "region = '{{ .user.region }}'"})
    assert security is not None
    # Go-template .user.* refs become sidemantic's user.* Jinja namespace.
    assert security.access == "{{ user.admin }}"
    assert security.row_filters == ["region = '{{ user.region }}'"]


def test_rill_security_none_when_absent():
    assert RillAdapter()._parse_security(None) is None
    assert RillAdapter()._parse_security({}) is None


def test_uri_dimension_flag():
    d = Dimension(name="homepage", type="categorical", uri=True)
    assert d.uri is True
    # Default is False and round-trips through model_dump.
    assert Dimension(name="x", type="categorical").uri is False
    assert d.model_dump()["uri"] is True
