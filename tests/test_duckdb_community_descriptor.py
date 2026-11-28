import subprocess
from pathlib import Path

import pytest
import yaml

from scripts.duckdb_community_descriptor import ROOT, TEMPLATE, git, render_descriptor


@pytest.fixture
def release_repo(tmp_path: Path) -> Path:
    git(tmp_path, "init", "--quiet")
    files = {
        TEMPLATE: (ROOT / TEMPLATE).read_text(),
        "sqlctx-rs/Cargo.toml": '[package]\nversion = "1.2.3"\n',
        "sqlctx-rs/Cargo.lock": "version = 4\n",
        "sqlctx-rs/include/sqlctx.h": "/* public API */\n",
        "sqlctx-duckdb/Makefile": "all:\n",
        "sqlctx-duckdb/CMakeLists.txt": "project(sqlctx)\n",
        "sqlctx-duckdb/extension_config.cmake": "duckdb_extension_load(sqlctx)\n",
        "sqlctx-duckdb/LICENSE": "MIT\n",
        "LICENSE": "AGPL-3.0-only\n",
    }
    for name, content in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    git(tmp_path, "add", ".")
    git(tmp_path, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "Release")
    return tmp_path


def test_descriptor_pins_both_hosts_to_committed_monorepo(release_repo: Path) -> None:
    commit = git(release_repo, "rev-parse", "HEAD")
    # Uncommitted metadata must not describe bytes that the registry will not fetch.
    (release_repo / "sqlctx-rs/Cargo.toml").write_text('[package]\nversion = "9.9.9"\n')
    descriptor = yaml.safe_load(render_descriptor(release_repo, commit))
    assert descriptor["repo"] == {
        "github": "sidequery/sqlctx",
        "build_directory": "sqlctx-duckdb",
        "ref": commit,
        "variegata": commit,
    }
    assert descriptor["extension"]["version"] == "1.2.3"
    assert descriptor["extension"]["requires_toolchains"] == "rust"
    assert descriptor["extension"]["license"] == "AGPL-3.0-only AND MIT"
    assert "@COMMIT@" not in render_descriptor(release_repo, commit)


def test_descriptor_rejects_missing_sibling_crate(release_repo: Path) -> None:
    git(release_repo, "rm", "sqlctx-rs/include/sqlctx.h")
    git(
        release_repo,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Incomplete",
    )
    with pytest.raises(subprocess.CalledProcessError):
        render_descriptor(release_repo, "HEAD")


def test_descriptor_rejects_unknown_ref(release_repo: Path) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        render_descriptor(release_repo, "missing-release")
