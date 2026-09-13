"""Execute the release input boundary without running builds or publishing."""

import os
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/duckdb-extension-release.yml"


@pytest.mark.parametrize(
    "version",
    ["v1.5.4", 'v1.5.5"; touch injected; #', "$(touch injected)", "v1.5.5\nextra=value"],
)
def test_release_rejects_unsupported_input_without_shell_execution(tmp_path, version):
    workflow = yaml.safe_load(WORKFLOW.read_text())
    step = next(step for step in workflow["jobs"]["build"]["steps"] if step.get("id") == "duckdb")
    output = tmp_path / "output"
    output.touch()
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        cwd=tmp_path,
        env={
            **os.environ,
            "EVENT_NAME": "workflow_dispatch",
            "REQUESTED_VERSION": version,
            "GITHUB_OUTPUT": str(output),
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert not (tmp_path / "injected").exists()
    assert output.read_text() == ""


@pytest.mark.parametrize("event", ["workflow_dispatch", "push"])
def test_release_accepts_supported_version(tmp_path, event):
    workflow = yaml.safe_load(WORKFLOW.read_text())
    step = next(step for step in workflow["jobs"]["build"]["steps"] if step.get("id") == "duckdb")
    output = tmp_path / "output"
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        cwd=tmp_path,
        env={**os.environ, "EVENT_NAME": event, "REQUESTED_VERSION": "v1.5.5", "GITHUB_OUTPUT": str(output)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.read_text() == "version=v1.5.5\n"
