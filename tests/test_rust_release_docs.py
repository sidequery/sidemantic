import re
import shlex
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def assert_file_contains(path: str, expected: list[str]) -> None:
    contents = (ROOT / path).read_text()

    for needle in expected:
        assert needle in contents, f"{path} is missing {needle!r}"


def test_rust_native_runtime_release_workflows_are_documented() -> None:
    assert_file_contains(
        ".github/workflows/rust-runtime-release.yml",
        [
            "name: Rust Runtime Release",
            "cargo package --locked --no-verify",
            "cargo publish --locked --dry-run",
            "CARGO_REGISTRY_TOKEN",
        ],
    )
    assert_file_contains(
        ".github/workflows/sidemantic-rs-wheels.yml",
        [
            "name: sidemantic-rs Python Wheels",
            "uvx maturin sdist --out dist",
            "MATURIN_PYPI_TOKEN",
        ],
    )
    assert_file_contains(
        ".github/workflows/duckdb-extension-release.yml",
        [
            "name: DuckDB Extension Release",
            "duckdb_version",
            "make test",
            "sidemantic.duckdb_extension",
        ],
    )


def test_rust_native_runtime_release_docs_cover_install_and_compatibility() -> None:
    packaging = (ROOT / "docs/rust-native-runtime-packaging.md").read_text()
    # Check real install targets and the pinned ABI, rather than editorial headings.
    commands = [
        shlex.split(line)
        for block in re.findall(r"```bash\n(.*?)```", packaging, re.S)
        for line in block.splitlines()
        if line.strip()
    ]
    cargo_install = next(command for command in commands if command[:2] == ["cargo", "install"])
    crate = ROOT / cargo_install[cargo_install.index("--path") + 1]
    assert (crate / "Cargo.toml").is_file()
    assert "--locked" in cargo_install
    binding_install = next(command for command in commands if command[:2] == ["uv", "add"])
    assert (ROOT / binding_install[2] / "pyproject.toml").is_file()
    makefile = (ROOT / "sidemantic-duckdb/Makefile").read_text()
    version = re.search(r"^SUPPORTED_DUCKDB_VERSION := (\S+)$", makefile, re.M).group(1)
    assert f"`{version}`" in packaging
    for workflow in re.findall(r"`([\w-]+\.yml)`", packaging):
        assert (ROOT / ".github/workflows" / workflow).is_file()
    assert_file_contains(
        "docs/duckdb-extension.md",
        [
            "# DuckDB Extension",
            "Build From Source",
            "sidemantic_load_file",
            "DuckDB extension artifacts are ABI-sensitive",
        ],
    )


def test_rust_runtime_conversion_example_validates_a_directory() -> None:
    assert_file_contains(
        "docs/rust-runtime.md",
        [
            "--output ./native-models/sidemantic.yml",
            "sidemantic validate ./native-models --engine rust",
        ],
    )


def test_release_wheel_smoke_installs_the_artifact_that_is_uploaded() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/sidemantic-rs-wheels.yml").read_text())
    job = workflow["jobs"]["build-wheels"]
    steps = job["steps"]
    upload_index, upload = next(
        (index, step) for index, step in enumerate(steps) if step.get("uses", "").startswith("actions/upload-artifact@")
    )
    smoke_index, smoke = next(
        (index, step) for index, step in enumerate(steps) if "python_wheel_smoke.py" in step.get("run", "")
    )
    command = shlex.split(smoke["run"])
    assert command[:2] == ["uv", "run"]
    assert "--no-project" in command
    working_directory = smoke.get(
        "working-directory", job.get("defaults", {}).get("run", {}).get("working-directory", ".")
    )
    installed = (ROOT / working_directory / command[command.index("--with") + 1]).resolve()
    uploaded = (ROOT / upload["with"]["path"]).resolve()
    assert installed == uploaded
    assert smoke_index < upload_index
    assert (ROOT / working_directory / command[-1]).is_file()
