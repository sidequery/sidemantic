"""Source-backed semantic cases with independent DuckDB result contracts."""

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from sidemantic import SemanticLayer
from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.ossie import OssieAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError, graph_to_semantic_input
from sidemantic.validation import QueryValidationError

FIXTURES = Path(__file__).parent / "fixtures"
CASES = yaml.safe_load((FIXTURES / "cases.yml").read_text())
ERROR_TYPES = {"QueryValidationError": QueryValidationError, "SecurityError": SecurityError}


def load_source(source):
    adapter = {"native": SidemanticAdapter, "cube": CubeAdapter, "ossie": OssieAdapter}[source.split("/")[0]]()
    return adapter.parse(FIXTURES / f"{source}.yml")


def normalize(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_compiler_results(case, engine):
    if engine == "rust":
        pytest.importorskip("sidemantic_rs", reason="The real Rust extension must be installed for acceptance")
    layer = SemanticLayer(engine=engine, auto_register=False)
    try:
        layer.graph = load_source(case["source"])
        if case["source"] == "ossie":
            # Ossie lowering deliberately retains aggregate SQL as an opaque
            # graph metric; handing it off must not extract or rebind its body.
            payload = graph_to_semantic_input(layer.graph)
            metric = payload["metrics"][0]
            assert metric["sql_is_complete"] is True
            assert metric["sql"] == "SUM(CASE WHEN orders.amount >= 100 THEN orders.amount ELSE 0 END)"
            assert metric["metadata"]["ossie_expression_dialect"] == "ANSI_SQL"
            assert payload["metric_owners"] == {}
        layer.adapter.execute((FIXTURES / "seed.sql").read_text())
        if engine == "rust" and (required := case.get("rust_unsupported")):
            with pytest.raises(UnsupportedSemanticFeaturesError) as raised:
                layer.compile(**case["query"])
            assert set(required) <= set(raised.value.capabilities)
            return
        if error := case.get("error"):
            with pytest.raises(ERROR_TYPES[error["type"]], match=error["match"]):
                layer.compile(**case["query"])
            return

        sql = layer.compile(**case["query"])
        result = layer.adapter.execute(sql)
        assert [column[0] for column in result.description] == case["columns"]
        assert [[normalize(value) for value in row] for row in result.fetchall()] == case["rows"]
    finally:
        layer.adapter.close()
