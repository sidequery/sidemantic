"""Named many-to-many roles retain independent junction and metric populations."""

import copy
import json
from collections import Counter

import duckdb
import pytest


@pytest.fixture
def rust():
    return pytest.importorskip("sidemantic_rs")


def source():
    return {
        "version": 1,
        "input_dialect": "duckdb",
        "models": [
            {
                "name": "orders",
                "table": "orders",
                "primary_key": "id",
                "dimensions": [{"name": "id", "type": "numeric"}],
                "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}],
                "relationships": [
                    {
                        "name": role,
                        "target_model": "tags",
                        "type": "many_to_many",
                        "through": "links",
                        "through_foreign_key": "order_id",
                        "related_foreign_key": key,
                        "edge_id": role,
                    }
                    for role, key in [("primary_tags", "primary_tag"), ("secondary_tags", "secondary_tag")]
                ],
            },
            {
                "name": "tags",
                "table": "tags",
                "primary_key": "id",
                "dimensions": [{"name": "name", "type": "categorical"}],
            },
            {"name": "links", "table": "links", "primary_key": "id"},
        ],
    }


def execute(rust, model, dimensions, **query):
    sql = rust.compile_with_semantic_input(
        json.dumps(model),
        json.dumps(
            {
                "metrics": ["orders.revenue"],
                "dimensions": dimensions,
                **query,
            }
        ),
    )
    with duckdb.connect() as connection:
        connection.execute(
            "create table orders(id integer, amount integer); insert into orders values (1,10),(2,20),(3,30)"
        )
        connection.execute(
            "create table tags(id integer, name varchar); insert into tags values (1,'x'),(2,'y'),(3,'z')"
        )
        connection.execute(
            "create table links(id integer, order_id integer, primary_tag integer, secondary_tag integer, tenant varchar, enabled boolean)"
        )
        connection.execute(
            "insert into links values (1,1,1,2,'a',true),(2,1,1,2,'a',true),(3,1,2,1,'b',true),(4,2,2,1,'a',true),(5,2,3,3,'a',false),(6,1,3,3,'b',true),(7,2,null,null,'a',true)"
        )
        connection.execute("alter table orders add column tenant varchar default 'a'")
        connection.execute("alter table tags add column tenant varchar default 'a'")
        return Counter(connection.execute(sql).fetchall())


def test_role_fanout_deduplicates_at_order_key(rust):
    assert execute(rust, source(), ["primary_tags.name"]) == Counter({("x", 10), ("y", 30), ("z", 30), (None, 50)})


@pytest.mark.parametrize(
    "role,expected",
    [
        ("primary_tags", {("x", 10), ("y", 20), (None, 50)}),
        ("secondary_tags", {("y", 10), ("x", 20), (None, 50)}),
    ],
)
def test_bridge_policies_bind_each_role(rust, role, expected):
    model = source()
    model["models"][2]["security"] = {"row_filters": ["tenant = {{ user.tenant }}"]}
    model["models"][2]["invariant_filters"] = ["enabled"]
    assert execute(rust, model, [f"{role}.name"], user_attributes={"tenant": "a"}) == Counter(expected)


def test_alternate_roles_do_not_share_junction_keys(rust):
    model = source()
    model["models"][2]["security"] = {"row_filters": ["tenant = {{ user.tenant }}"]}
    model["models"][2]["invariant_filters"] = ["enabled"]
    assert execute(
        rust, model, ["primary_tags.name", "secondary_tags.name"], user_attributes={"tenant": "a"}
    ) == Counter(
        {
            ("x", "y", 10),
            ("y", "x", 20),
            ("y", None, 20),
            (None, "x", 20),
            (None, None, 50),
        }
    )


def test_role_filter_preserves_source_grain(rust):
    assert execute(rust, source(), ["primary_tags.name"], filters=["primary_tags.name = 'x'"]) == Counter({("x", 10)})


@pytest.mark.parametrize("index", [0, 1, 2])
def test_access_denial_on_every_population(rust, index):
    model = source()
    model["models"][index]["security"] = {"access": False}
    with pytest.raises(Exception, match="denied"):
        execute(rust, model, ["primary_tags.name"], user_attributes={})


def test_inactive_role_is_excluded(rust):
    model = source()
    model["models"][0]["relationships"][0]["active"] = False
    with pytest.raises(Exception, match="primary_tags"):
        execute(rust, model, ["primary_tags.name"])
    assert execute(rust, model, ["secondary_tags.name"]) == Counter({("x", 30), ("y", 10), ("z", 30), (None, 50)})


def test_duplicate_role_identity_is_rejected(rust):
    model = source()
    model["models"][0]["relationships"].append(copy.deepcopy(model["models"][0]["relationships"][0]))
    with pytest.raises(Exception, match="more than once"):
        execute(rust, model, ["primary_tags.name"])


@pytest.mark.parametrize(
    "mutation,expected",
    [
        ("missing_bridge", "unknown bridge"),
        ("missing_key", "explicit through_foreign_key"),
        ("arity", "junction key arity"),
        ("no_through", "without_through"),
    ],
)
def test_incomplete_junction_contract_is_rejected(rust, mutation, expected):
    model = source()
    relationship = model["models"][0]["relationships"][0]
    if mutation == "missing_bridge":
        relationship["through"] = "missing"
    elif mutation == "missing_key":
        del relationship["through_foreign_key"]
    elif mutation == "arity":
        relationship["through_foreign_key"] = ["tenant", "order_id"]
    elif mutation == "no_through":
        del relationship["through"]
    with pytest.raises(Exception, match=expected):
        execute(rust, model, ["primary_tags.name"])


def test_bridge_join_retains_bridge_keys_when_custom_sql_is_present(rust):
    model = source()
    # Python uses the declared bridge path; direct-only custom SQL does not
    # replace either bridge predicate.
    model["models"][0]["relationships"][0]["sql"] = "{from}.id = {to}.id"
    assert execute(rust, model, ["primary_tags.name"]) == Counter({("x", 10), ("y", 30), ("z", 30), (None, 50)})


def test_target_measures_retain_target_key_grain(rust):
    model = source()
    model["models"][1]["metrics"] = [{"name": "sum_ids", "agg": "sum", "sql": "id"}]
    assert execute(rust, model, ["orders.id"], metrics=["primary_tags.sum_ids"]) == Counter({(1, 6), (2, 5), (3, None)})


def test_composite_junction_keys_do_not_cross_tenants(rust):
    model = source()
    model["models"][0]["primary_key"] = ["tenant", "id"]
    model["models"][1]["primary_key"] = ["tenant", "id"]
    for relationship in model["models"][0]["relationships"]:
        relationship["through_foreign_key"] = ["tenant", "order_id"]
        relationship["related_foreign_key"] = ["tenant", relationship["related_foreign_key"]]
    assert execute(rust, model, ["orders.id", "primary_tags.name"], metrics=[], ungrouped=True) == Counter(
        [
            (1, "x"),
            (1, "x"),
            (2, "y"),
            (2, "z"),
            (2, None),
            (3, None),
        ]
    )
    assert execute(rust, model, ["primary_tags.name"]) == Counter({("x", 10), ("y", 20), ("z", 20), (None, 50)})
