"""Exercise uv's local wheel reuse with the native packages' real cache keys."""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from textwrap import dedent

import pytest
import yaml


@pytest.mark.parametrize("restored", [True, False])
def test_imported_rust_cache_archive_is_removed_without_losing_build_state(tmp_path, restored):
    repo = Path(__file__).resolve().parents[1]
    action = yaml.safe_load((repo / ".github/actions/setup-rust-cache/action.yml").read_text())
    steps = action["runs"]["steps"]
    cleanup_index = next(i for i, step in enumerate(steps) if step.get("name") == "Remove imported Rust cache archive")
    # A failed upstream import must stop setup before any cleanup takes place.
    assert steps[cleanup_index - 1]["id"] == "cache"
    assert steps[cleanup_index].get("if", "success()") == "success()"
    assert steps[cleanup_index]["env"]["MBX_CACHE_DIR"] == steps[cleanup_index - 1]["env"]["MBX_CACHE_DIR"]

    cache = tmp_path / "cache with spaces" / "actions"
    cache.mkdir(parents=True)
    archive = cache / "github-actions-cache-v1.tar"
    if restored:
        archive.write_bytes(b"imported transport archive")
    retained = [cache / "objects" / "compiler-object", tmp_path / "target" / "compiled-library"]
    for path in retained:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"reusable build state")

    # Substitute only mbx's directory lookup; execute the action's real shell.
    result = subprocess.run(
        [
            "bash",
            "-e",
            "-c",
            'mbx() { test "$*" = "cache dir"; printf "%s\\n" "$TEST_CACHE_DIR"; }\n' + steps[cleanup_index]["run"],
        ],
        env={**os.environ, "TEST_CACHE_DIR": str(cache)},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert not archive.exists()
    for path in retained:
        assert path.read_bytes() == b"reusable build state"


@pytest.mark.parametrize(
    ("package", "changed_file"),
    [
        ("sidemantic-rs", "src/lib.rs"),
        ("sidemantic-rs", "build.rs"),
        ("sidemantic-rs", "../Cargo.lock"),
        ("crates/dax-pyo3", "src/lib.rs"),
        ("crates/dax-pyo3", "../dax-parser/src/functions.rs"),
        ("crates/dax-pyo3", "python/sidemantic_dax/__init__.py"),
    ],
)
def test_native_source_changes_invalidate_uv_wheels(tmp_path, package, changed_file):
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required to exercise its wheel cache")

    repo = Path(__file__).resolve().parents[1]
    project = tmp_path / "source" / package
    project.mkdir(parents=True)
    source = project / changed_file
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("first source\n")
    cache_keys = (repo / package / "pyproject.toml").read_text().split("[tool.uv]\n", 1)[1]
    (project / "pyproject.toml").write_text(
        '[build-system]\nrequires = []\nbuild-backend = "backend"\nbackend-path = ["."]\n'
        '[project]\nname = "cache-probe"\nversion = "1.0"\n'
        f"[tool.uv]\n{cache_keys}"
    )
    # A dependency-free PEP 517 backend stands in for maturin. The observable
    # wheel contents prove when uv reuses a wheel and when it calls the backend;
    # no Rust compiler or network is needed for this invalidation contract.
    (project / "backend.py").write_text(
        dedent(
            f"""\
            from pathlib import Path
            from zipfile import ZipFile

            def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
                name = "cache_probe-1.0-py3-none-any.whl"
                source = Path({changed_file!r}).read_text()
                with ZipFile(Path(wheel_directory) / name, "w") as wheel:
                    wheel.writestr("cache_probe.py", source)
                    wheel.writestr(
                        "cache_probe-1.0.dist-info/METADATA",
                        "Metadata-Version: 2.1\\nName: cache-probe\\nVersion: 1.0\\n",
                    )
                    wheel.writestr(
                        "cache_probe-1.0.dist-info/WHEEL",
                        "Wheel-Version: 1.0\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n",
                    )
                    wheel.writestr("cache_probe-1.0.dist-info/RECORD", "")
                counter = Path("build-count")
                previous = int(counter.read_text()) if counter.exists() else 0
                counter.write_text(str(previous + 1))
                return name
            """
        )
    )

    requirement = tmp_path / "requirements.txt"
    requirement.write_text(f"cache-probe @ {project.as_uri()}\n")

    def install(attempt):
        target = tmp_path / f"installed-{attempt}"
        result = subprocess.run(
            [
                uv,
                "pip",
                "install",
                "--python",
                sys.executable,
                "--target",
                str(target),
                "--no-deps",
                "--no-index",
                "--requirement",
                str(requirement),
            ],
            env={**os.environ, "UV_CACHE_DIR": str(tmp_path / "cache")},
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        return (target / "cache_probe.py").read_text()

    assert install(1) == "first source\n"
    first_builds = (project / "build-count").read_text()
    if package == "crates/dax-pyo3":
        # Import and editable-build outputs must not invalidate their own wheel.
        generated = project / "python/sidemantic_dax"
        (generated / "__pycache__").mkdir(parents=True, exist_ok=True)
        (generated / "__pycache__/__init__.pyc").write_bytes(b"bytecode")
        (generated / "_native.so").write_bytes(b"compiled extension")
    assert install(2) == "first source\n"
    assert (project / "build-count").read_text() == first_builds

    source.write_text("changed source\n")
    # uv's file keys use mtimes; avoid depending on filesystem clock resolution.
    changed_at = time.time() + 2
    os.utime(source, (changed_at, changed_at))
    assert install(3) == "changed source\n"
    assert int((project / "build-count").read_text()) > int(first_builds)
