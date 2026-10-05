"""Execute the release input boundary without running builds or publishing."""

import hashlib
import os
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/duckdb-extension-release.yml"
PLATFORMS = ["linux_amd64", "linux_arm64", "osx_amd64", "osx_arm64"]


def workflow_step(job, name):
    workflow = yaml.safe_load(WORKFLOW.read_text())
    return next(step for step in workflow["jobs"][job]["steps"] if step.get("name") == name)


@pytest.mark.parametrize(
    "version",
    ["v1.5.4", 'v1.5.6"; touch injected; #', "$(touch injected)", "v1.5.6\nextra=value"],
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
        env={**os.environ, "EVENT_NAME": event, "REQUESTED_VERSION": "v1.5.6", "GITHUB_OUTPUT": str(output)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.read_text() == "version=v1.5.6\n"


@pytest.mark.parametrize("platform", PLATFORMS)
def test_release_packages_matching_platform_with_verifiable_checksum(tmp_path, platform):
    extension = tmp_path / "build/release/extension/sidemantic/sidemantic.duckdb_extension"
    extension.parent.mkdir(parents=True)
    extension.write_bytes(b"extension payload")
    step = workflow_step("build", "Package extension artifact")
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        cwd=tmp_path,
        env={**os.environ, "DUCKDB_VERSION": "v1.5.6", "PLATFORM": platform},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    asset = tmp_path / "dist" / f"sidemantic-duckdb-1.5.6-{platform}.duckdb_extension"
    assert asset.read_bytes() == extension.read_bytes()
    assert asset.with_suffix(".duckdb_extension.sha256").read_text() == (
        f"{hashlib.sha256(extension.read_bytes()).hexdigest()}  {asset.name}\n"
    )


@pytest.mark.parametrize("failure", ["mismatched_tag", "missing_platform", "corrupt_payload", "existing_release", None])
def test_publication_checks_provenance_and_all_payloads_before_creating_release(tmp_path, failure):
    dist = tmp_path / "dist"
    dist.mkdir()
    for platform in PLATFORMS:
        asset = dist / f"sidemantic-duckdb-1.5.6-{platform}.duckdb_extension"
        payload = platform.encode()
        asset.write_bytes(payload)
        asset.with_suffix(".duckdb_extension.sha256").write_text(
            f"{hashlib.sha256(payload).hexdigest()}  {asset.name}\n"
        )
    if failure == "missing_platform":
        asset.unlink()
    elif failure == "corrupt_payload":
        asset.write_bytes(b"changed")
    (tmp_path / "LICENSE").write_text("Test license\n")
    commands = tmp_path / "bin"
    commands.mkdir()
    git = commands / "git"
    git.write_text('#!/bin/sh\nif [ "$1" = rev-parse ]; then echo "$TAG_COMMIT"; fi\n')
    git.chmod(0o755)
    gh = commands / "gh"
    gh.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$GH_ARGS"\nexit "$GH_STATUS"\n')
    gh.chmod(0o755)
    args = tmp_path / "gh-args"
    step = workflow_step("github-release", "Verify and publish immutable artifacts")
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{commands}{os.pathsep}{os.environ['PATH']}",
            "TAG": "sidemantic-duckdb-v0.1.0",
            "RELEASE_COMMIT": "built-commit",
            "TAG_COMMIT": "other-commit" if failure == "mismatched_tag" else "built-commit",
            "GH_ARGS": str(args),
            "GH_STATUS": "1" if failure == "existing_release" else "0",
        },
        capture_output=True,
        text=True,
    )
    if failure:
        assert result.returncode != 0
    else:
        assert result.returncode == 0, result.stderr
        assert len((dist / "SHA256SUMS").read_text().splitlines()) == len(PLATFORMS)
        assert (dist / "LICENSE").read_text() == "Test license\n"
    if failure in {"mismatched_tag", "missing_platform", "corrupt_payload"}:
        assert not args.exists()
    else:
        arguments = args.read_text().splitlines()
        assert arguments[:3] == ["release", "create", "sidemantic-duckdb-v0.1.0"]
        assert "--verify-tag" in arguments
        assert "--clobber" not in arguments
        assert sum(arg.endswith(".duckdb_extension") for arg in arguments) == len(PLATFORMS)


@pytest.mark.parametrize("matching_commit", [True, False])
def test_publication_verifies_remote_annotated_tag_without_replacing_checkout_ref(tmp_path, matching_commit):
    def git(directory, *arguments):
        return subprocess.run(
            [
                "git",
                "-c",
                "user.name=Release test",
                "-c",
                "user.email=release@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "tag.gpgSign=false",
                *arguments,
            ],
            cwd=directory,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-q")
    (source / "LICENSE").write_text("Initial license\n")
    git(source, "add", "LICENSE")
    git(source, "commit", "-qm", "Initial source")
    earlier_commit = git(source, "rev-parse", "HEAD")
    (source / "LICENSE").write_text("Release license\n")
    git(source, "commit", "-qam", "Release source")
    tagged_commit = git(source, "rev-parse", "HEAD")
    tag = "sidemantic-duckdb-v0.1.0"
    git(source, "tag", "-a", tag, "-m", "Release")
    checkout = tmp_path / "checkout"
    git(tmp_path, "clone", "-q", "--no-tags", str(source), str(checkout))
    built_commit = tagged_commit if matching_commit else earlier_commit
    git(checkout, "checkout", "-q", "--detach", built_commit)
    # actions/checkout can synthesize a lightweight ref at the built commit.
    git(checkout, "tag", tag, built_commit)
    dist = checkout / "dist"
    dist.mkdir()
    for platform in PLATFORMS:
        asset = dist / f"sidemantic-duckdb-1.5.6-{platform}.duckdb_extension"
        asset.write_bytes(platform.encode())
        asset.with_suffix(".duckdb_extension.sha256").write_text(
            f"{hashlib.sha256(asset.read_bytes()).hexdigest()}  {asset.name}\n"
        )
    commands = tmp_path / "bin"
    commands.mkdir()
    gh = commands / "gh"
    gh.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$GH_ARGS"\n')
    gh.chmod(0o755)
    args = tmp_path / "gh-args"
    step = workflow_step("github-release", "Verify and publish immutable artifacts")
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        cwd=checkout,
        env={
            **os.environ,
            "PATH": f"{commands}{os.pathsep}{os.environ['PATH']}",
            "TAG": tag,
            "RELEASE_COMMIT": built_commit,
            "GH_ARGS": str(args),
        },
        capture_output=True,
        text=True,
    )
    assert (result.returncode == 0) == matching_commit, result.stderr
    assert args.exists() == matching_commit
    assert git(checkout, "rev-parse", f"refs/tags/{tag}") == built_commit
