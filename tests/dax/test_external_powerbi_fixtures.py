from __future__ import annotations

import re
from pathlib import Path

import pytest
import sidemantic_dax

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "fixtures" / "external_powerbi" / "marfolger-powerbi-dax" / "business_logic_DAX.txt"


def _parse_expression(expression: str):
    try:
        return sidemantic_dax.parse_expression(expression)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise


def _load_measure_assignments(path: Path) -> list[tuple[str, str]]:
    measures: list[tuple[str, str]] = []
    current_name: str | None = None
    current_lines: list[str] = []

    for line in path.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("//"):
            continue
        match = re.match(r"^([^=]+?)\s=\s*$", line)
        if match and not line.startswith((" ", "\t")):
            if current_name is not None:
                measures.append((current_name, "\n".join(current_lines).strip()))
            current_name = match.group(1).strip()
            current_lines = []
            continue
        if current_name is not None:
            current_lines.append(line)

    if current_name is not None:
        measures.append((current_name, "\n".join(current_lines).strip()))

    return measures


def test_external_powerbi_dax_measure_file_parses():
    measures = _load_measure_assignments(FIXTURE)

    assert [name for name, _expr in measures] == [
        "Total Revenue",
        "Revenue MoM Growth %",
        "Avg Turnaround Days",
        "Is High Value Client",
        "Pickup Preference %",
    ]

    for name, expression in measures:
        parsed = _parse_expression(expression)
        assert parsed is not None, f"failed to parse {name}"
