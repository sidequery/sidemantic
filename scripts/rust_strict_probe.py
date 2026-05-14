#!/usr/bin/env python3
"""Run a fast strict-mode probe and emit a subsystem failure matrix."""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path


@dataclass
class SkipReason:
    reason: str
    count: int
    category: str


@dataclass
class ProbeResult:
    group: str
    status: str
    returncode: int
    failed_tests: int
    passed_tests: int
    skipped_tests: int
    deselected_tests: int
    skip_reasons: list[SkipReason]
    skip_category_counts: dict[str, int]
    strict_subsystem: str | None
    summary: str


def parse_counts(text: str) -> tuple[int, int, int, int]:
    failed = 0
    passed = 0
    skipped = 0
    deselected = 0
    for value, key in re.findall(r"(\d+)\s+(failed|passed|skipped|deselected)", text):
        if key == "failed":
            failed += int(value)
        elif key == "passed":
            passed += int(value)
        elif key == "skipped":
            skipped += int(value)
        elif key == "deselected":
            deselected += int(value)
    return failed, passed, skipped, deselected


def parse_summary(text: str) -> str:
    summary = ""
    for line in text.splitlines():
        if line.startswith("FAILED ") or line.startswith("ERROR ") or line.startswith("="):
            continue
        if "failed" in line or "passed" in line or "skipped" in line or "deselected" in line:
            summary = line.strip()
    return summary


def parse_skip_reasons(text: str) -> list[tuple[int, str]]:
    results: list[tuple[int, str]] = []
    for line in text.splitlines():
        if not line.startswith("SKIPPED ["):
            continue
        match = re.match(r"^SKIPPED \[(\d+)\]\s+(.+)$", line)
        if not match:
            continue
        count = int(match.group(1))
        detail = match.group(2).strip()
        if ": " in detail:
            _, reason = detail.rsplit(": ", 1)
        else:
            reason = detail
        results.append((count, reason.strip()))
    return results


def categorize_skip_reason(reason: str) -> str:
    lowered = reason.lower()
    if "examples not found" in lowered:
        return "fixture_gap"
    if "not currently supported" in lowered or "not directly supported" in lowered:
        return "capability_gap"
    if (
        (lowered.startswith("set ") and "_test=1" in lowered)
        or "run docker compose" in lowered
        or "run docker compose up -d" in lowered
        or "requires real" in lowered
        or "tested in integration tests" in lowered
        or "integration tests" in lowered
    ):
        return "integration_scope"
    if (
        "requires" in lowered
        or "required" in lowered
        or "could not import" in lowered
        or "no module named" in lowered
        or "adbc_driver_manager" in lowered
    ):
        return "external_dependency"
    if (
        "credential" in lowered
        or "network" in lowered
        or "service unavailable" in lowered
        or "connection refused" in lowered
    ):
        return "external_environment"
    return "unclassified"


def parse_strict_subsystem(text: str) -> str | None:
    matches = re.findall(r"\[rust-strict:([^\]]+)\]", text)
    if not matches:
        return None
    # Pytest tracebacks include source lines like f"[rust-strict:{subsystem}]".
    # Prefer concrete evaluated values over template placeholders.
    for value in reversed(matches):
        if "{" not in value and "}" not in value:
            return value
    return matches[-1]


def run_group(
    group_name: str,
    paths: list[str],
    env: dict[str, str],
    *,
    clear_default_addopts: bool = False,
) -> ProbeResult:
    cmd = ["uv", "run", "pytest", "-q", "-rs", "--maxfail=1"]
    if clear_default_addopts:
        cmd.extend(["-o", "addopts="])
    cmd.extend(paths)
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    output = f"{proc.stdout}\n{proc.stderr}"
    failed, passed, skipped, deselected = parse_counts(output)
    skip_reasons_raw = parse_skip_reasons(output)
    skip_reason_counter: Counter[str] = Counter()
    for count, reason in skip_reasons_raw:
        skip_reason_counter[reason] += count
    skip_reasons = [
        SkipReason(
            reason=reason,
            count=count,
            category=categorize_skip_reason(reason),
        )
        for reason, count in sorted(
            skip_reason_counter.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]
    skip_category_counts: Counter[str] = Counter()
    for item in skip_reasons:
        skip_category_counts[item.category] += item.count
    strict_subsystem = parse_strict_subsystem(output)

    status = "pass" if proc.returncode == 0 else "fail"
    summary = parse_summary(output)
    if strict_subsystem:
        summary = f"{summary} [strict:{strict_subsystem}]".strip()

    return ProbeResult(
        group=group_name,
        status=status,
        returncode=proc.returncode,
        failed_tests=failed,
        passed_tests=passed,
        skipped_tests=skipped,
        deselected_tests=deselected,
        skip_reasons=skip_reasons,
        skip_category_counts=dict(skip_category_counts),
        strict_subsystem=strict_subsystem,
        summary=summary,
    )


def build_groups() -> list[tuple[str, list[str]]]:
    tests_dir = Path("tests")
    groups: list[tuple[str, list[str]]] = []

    root_files = sorted(str(p) for p in tests_dir.glob("test_*.py"))
    if root_files:
        groups.append(("root", root_files))

    for d in sorted(p for p in tests_dir.iterdir() if p.is_dir()):
        if d.name in {"__pycache__", "fixtures"}:
            continue
        groups.append((d.name, [str(d)]))

    return groups


def update_parity_matrix(payload: dict[str, object]) -> None:
    parity_path = Path("docs") / "rust-parity-matrix.json"
    if not parity_path.exists():
        return

    data = json.loads(parity_path.read_text())
    evidence = data.setdefault("evidence", {})
    aggregate = payload["aggregate"]
    criterion_1_gate = payload["criterion_1_gate"]

    summary: dict[str, str] = {}
    skip_classification: dict[str, object] = {}
    for group in payload["groups"]:
        summary[group["group"]] = group["summary"]
        if group["skipped_tests"] or group["deselected_tests"]:
            skip_classification[group["group"]] = {
                "skipped_tests": group["skipped_tests"],
                "deselected_tests": group["deselected_tests"],
                "skip_category_counts": group["skip_category_counts"],
                "skip_reasons": group["skip_reasons"],
            }

    today = date.today().isoformat()
    evidence["latest_strict_probe_summary"] = summary
    evidence["latest_strict_probe_aggregate"] = aggregate
    evidence["latest_strict_probe_criterion_1_gate"] = criterion_1_gate
    evidence["latest_strict_probe_skip_classification"] = skip_classification
    evidence["latest_strict_probe_command"] = "uv run scripts/rust_strict_probe.py"
    evidence["latest_strict_probe_date"] = today

    acceptance = data.get("acceptance_criteria_status", {})
    criterion_1 = acceptance.get("criterion_1_full_strict_no_fallback_suite", {})
    if isinstance(criterion_1, dict):
        criterion_1["status"] = criterion_1_gate["status"]
        probe_state = "green" if criterion_1_gate["all_groups_pass"] else "failing"
        criterion_1["evidence"] = (
            f"Strict grouped probe is {probe_state}; "
            f"blocking skipped={criterion_1_gate['blocking_skipped_tests']}, "
            f"deselected={criterion_1_gate['blocking_deselected_tests']}; "
            f"unavoidable_skips={criterion_1_gate['unavoidable_skip_count']}, "
            f"known_blocking_skips={criterion_1_gate['known_blocking_skip_count']}, "
            f"ambiguous_skips={criterion_1_gate['ambiguous_skip_count']}, "
            f"ambiguous_deselected={criterion_1_gate['ambiguous_deselected_count']}."
        )
        acceptance["criterion_1_full_strict_no_fallback_suite"] = criterion_1
        data["acceptance_criteria_status"] = acceptance

    data["last_updated"] = today
    parity_path.write_text(json.dumps(data, indent=2))


def main() -> None:
    env = os.environ.copy()
    env["SIDEMANTIC_RS_STRICT_SUBSYSTEMS"] = "all"
    env["SIDEMANTIC_RS_REWRITER"] = "1"
    env["SIDEMANTIC_RS_NO_FALLBACK"] = "1"

    groups = build_groups()
    results = [
        run_group(
            name,
            paths,
            env,
            clear_default_addopts=(name == "db"),
        )
        for name, paths in groups
    ]
    total_failed = sum(r.failed_tests for r in results)
    total_passed = sum(r.passed_tests for r in results)
    total_skipped = sum(r.skipped_tests for r in results)
    total_deselected = sum(r.deselected_tests for r in results)
    all_groups_pass = all(r.status == "pass" for r in results)

    skip_category_counts: Counter[str] = Counter()
    for result in results:
        for key, value in result.skip_category_counts.items():
            skip_category_counts[key] += value

    classified_skip_count = sum(skip_category_counts.values())
    if classified_skip_count < total_skipped:
        skip_category_counts["unclassified"] += total_skipped - classified_skip_count

    unavoidable_skip_count = (
        skip_category_counts.get("external_dependency", 0)
        + skip_category_counts.get("external_environment", 0)
        + skip_category_counts.get("integration_scope", 0)
    )
    known_blocking_skip_count = skip_category_counts.get("capability_gap", 0) + skip_category_counts.get(
        "fixture_gap", 0
    )
    ambiguous_skip_count = skip_category_counts.get("unclassified", 0)
    deselected_category_counts = {}
    if total_deselected > 0:
        deselected_category_counts = {
            "integration_scope": total_deselected,
        }
    ambiguous_deselected_count = 0
    if not all_groups_pass:
        criterion_1_status = "fail"
    elif total_skipped == 0 and total_deselected == 0:
        criterion_1_status = "pass"
    else:
        criterion_1_status = "partial"

    out_path = Path("docs") / "rust-strict-failure-matrix.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "strict_env": {
            "SIDEMANTIC_RS_STRICT_SUBSYSTEMS": env["SIDEMANTIC_RS_STRICT_SUBSYSTEMS"],
            "SIDEMANTIC_RS_REWRITER": env["SIDEMANTIC_RS_REWRITER"],
            "SIDEMANTIC_RS_NO_FALLBACK": env["SIDEMANTIC_RS_NO_FALLBACK"],
        },
        "groups": [asdict(r) for r in results],
        "aggregate": {
            "all_groups_pass": all_groups_pass,
            "total_failed_tests": total_failed,
            "total_passed_tests": total_passed,
            "total_skipped_tests": total_skipped,
            "total_deselected_tests": total_deselected,
            "skip_category_counts": dict(skip_category_counts),
            "deselected_category_counts": deselected_category_counts,
            "unavoidable_skip_count": unavoidable_skip_count,
            "known_blocking_skip_count": known_blocking_skip_count,
            "ambiguous_skip_count": ambiguous_skip_count,
            "ambiguous_deselected_count": ambiguous_deselected_count,
        },
        "execution_gate": {
            "status": "pass" if all_groups_pass else "fail",
            "requires_all_groups_pass": True,
            "notes": "Probe command exit status tracks strict runnable test failures. Skip coverage remains classified in criterion_1_gate.",
        },
        "criterion_1_gate": {
            "status": criterion_1_status,
            "all_groups_pass": all_groups_pass,
            "requires_zero_skips_and_deselected_for_full_pass": True,
            "blocking_skipped_tests": total_skipped,
            "blocking_deselected_tests": total_deselected,
            "unavoidable_skip_count": unavoidable_skip_count,
            "known_blocking_skip_count": known_blocking_skip_count,
            "ambiguous_skip_count": ambiguous_skip_count,
            "known_blocking_deselected_count": total_deselected,
            "ambiguous_deselected_count": ambiguous_deselected_count,
            "deselected_scope_note": (
                "Probe runs db group with '-o addopts=' to avoid pyproject marker "
                "deselection; non-zero deselected indicates an explicit test selection "
                "filter outside this probe."
            ),
            "notes": (
                "Criterion 1 is only full-pass when strict suite is green with zero skipped and zero deselected tests."
            ),
        },
    }
    out_path.write_text(json.dumps(payload, indent=2))
    update_parity_matrix(payload)

    print(f"Wrote {out_path}")
    for r in results:
        print(f"{r.group:12} {r.status:4} {r.summary}")
    if not all_groups_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
