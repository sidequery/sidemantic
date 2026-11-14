"""Every YAML and SQL example in docs/native-format.md loads and validates.

Complete documents (YAML with a top-level ``models`` key, and SQL definition
files) are validated as written. Fragments are placed into a small fixture
project chosen by their top-level keys, so each fragment shape has one explicit
wrapper below. A fragment shape without a wrapper fails the test rather than
being skipped.
"""

import copy
import re
from pathlib import Path

import pytest
import yaml

from sidemantic.validation_runner import validate_directory

DOC = Path(__file__).resolve().parents[1] / "docs" / "native-format.md"

# The models, fields, and metrics that the doc's fragments reference.
FIXTURE_PROJECT = yaml.safe_load(
    """
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - {name: order_id, type: categorical}
      - {name: line_item_id, type: categorical}
      - {name: customer_id, type: categorical}
      - {name: user_id, type: categorical}
      - {name: status, type: categorical}
      - {name: event_type, type: categorical}
      - {name: platform, type: categorical}
      - {name: created_at, type: time, granularity: day}
      - {name: updated_at, type: time, granularity: day}
    metrics:
      - {name: total_revenue, agg: sum, sql: amount}
      - {name: refunds, agg: sum, sql: refund_amount}
      - {name: order_count, agg: count}
  - name: customers
    table: customers
    primary_key: customer_id
  - name: order_items
    table: order_items
    primary_key: [order_id, line_item_id]
  - name: products
    table: products
    primary_key: product_id
"""
)

TOP_LEVEL_FRAGMENT_KEYS = {"version", "parameters"}
MODEL_LIST_KEYS = {"dimensions", "metrics", "relationships", "segments", "pre_aggregations"}
MODEL_SCALAR_KEYS = {"table", "primary_key", "primary_key_columns", "freshness"}
PRE_AGGREGATION_KEYS = {"refresh_key", "indexes"}


def _examples(language: str) -> list:
    """Return the doc's fenced code blocks for ``language``, each id'd by its line number."""
    text = DOC.read_text()
    examples = []
    for match in re.finditer(rf"^```{language}\n(.*?)^```", text, re.S | re.M):
        line = text.count("\n", 0, match.start()) + 1
        examples.append(pytest.param(match.group(1), id=f"line-{line}"))
    return examples


def _replace_by_name(existing: list[dict], fragment_items: list[dict]) -> list[dict]:
    names = {item["name"] for item in fragment_items}
    return [item for item in existing if item["name"] not in names] + fragment_items


def _wrap_fragment(fragment: dict) -> dict:
    project = copy.deepcopy(FIXTURE_PROJECT)
    orders = project["models"][0]
    keys = set(fragment)

    if keys <= TOP_LEVEL_FRAGMENT_KEYS:
        project.update(fragment)
    elif keys <= MODEL_LIST_KEYS | MODEL_SCALAR_KEYS:
        for key, value in fragment.items():
            orders[key] = _replace_by_name(orders.get(key, []), value) if key in MODEL_LIST_KEYS else value
        if "primary_key_columns" in fragment:
            # The explicit list form replaces primary_key rather than adding to it.
            del orders["primary_key"]
    elif keys == {"sql"}:
        orders["dimensions"].append({"name": "doc_example_dimension", **fragment})
    elif keys <= PRE_AGGREGATION_KEYS:
        orders["pre_aggregations"] = [
            {
                "name": "doc_example_rollup",
                "measures": ["total_revenue"],
                "dimensions": ["status"],
                "time_dimension": "created_at",
                "granularity": "day",
                **fragment,
            }
        ]
    else:
        raise AssertionError(f"No fixture wrapper for a fragment with keys {sorted(keys)}; add one to this test")
    return project


def _assert_valid(directory: Path) -> None:
    report = validate_directory(directory)
    assert report.errors == []


@pytest.mark.parametrize("example", _examples("yaml"))
def test_native_format_yaml_example_validates(example: str, tmp_path: Path) -> None:
    document = yaml.safe_load(example)
    assert isinstance(document, dict)
    if "models" not in document:
        example = yaml.safe_dump(_wrap_fragment(document), sort_keys=False)
    (tmp_path / "models.yml").write_text(example)
    _assert_valid(tmp_path)


@pytest.mark.parametrize("example", _examples("sql"))
def test_native_format_sql_example_validates(example: str, tmp_path: Path) -> None:
    (tmp_path / "orders.sql").write_text(example)
    _assert_valid(tmp_path)


def test_native_format_doc_has_examples() -> None:
    assert len(_examples("yaml")) >= 20
    assert len(_examples("sql")) >= 2
