from __future__ import annotations

import re
from pathlib import Path

import pytest
import sidemantic_dax

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = ROOT / "tests" / "dax" / "fixtures" / "query-docs" / "queries.txt"


def _parse_query(text: str):
    try:
        return sidemantic_dax.parse_query(text)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise


def _load_blocks(path: Path) -> list[tuple[str, str]]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in path.read_text().splitlines():
        if line.strip() == "---":
            if current:
                blocks.append(current)
                current = []
            continue
        current.append(line)
    if current:
        blocks.append(current)

    out: list[tuple[str, str]] = []
    for block in blocks:
        source = "<unknown>"
        expr_lines: list[str] = []
        for line in block:
            if line.startswith("# source:"):
                source = line.replace("# source:", "", 1).strip()
                continue
            expr_lines.append(line)
        query = "\n".join(expr_lines).strip()
        if query:
            out.append((source, query))
    return out


def _queries_with(keyword: str) -> list[tuple[str, str]]:
    keyword_upper = keyword.upper()
    pattern = rf"\b{re.escape(keyword_upper)}\b"
    return [(source, query) for source, query in _load_blocks(FIXTURE_PATH) if re.search(pattern, query.upper())]


def test_parse_query_corpus_evaluate_examples():
    for source, query in _queries_with("EVALUATE"):
        parsed = _parse_query(query)
        assert parsed.evaluates, f"no evaluates parsed for {source}"


def test_parse_query_corpus_define_examples():
    for source, query in _queries_with("DEFINE"):
        parsed = _parse_query(query)
        assert parsed.define is not None, f"define block missing for {source}"
        assert parsed.define.defs, f"no define defs parsed for {source}"


def test_parse_query_corpus_order_by_examples():
    for source, query in _queries_with("ORDER BY"):
        parsed = _parse_query(query)
        assert parsed.evaluates, f"no evaluates parsed for {source}"
        assert any(stmt.order_by for stmt in parsed.evaluates), f"order by missing for {source}"


def test_parse_query_corpus_start_at_examples():
    for source, query in _queries_with("START AT"):
        parsed = _parse_query(query)
        assert parsed.evaluates, f"no evaluates parsed for {source}"
        assert any(stmt.start_at for stmt in parsed.evaluates), f"start at missing for {source}"
