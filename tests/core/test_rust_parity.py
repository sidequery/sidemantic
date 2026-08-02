"""Tests for Rust strict-mode parity matrix gating."""

import json

import pytest

import sidemantic.rust_parity as rust_parity


@pytest.fixture(autouse=True)
def _reset_rust_parity_caches():
    rust_parity._load_parity_matrix.cache_clear()
    rust_parity.strict_targets.cache_clear()
    yield
    rust_parity._load_parity_matrix.cache_clear()
    rust_parity.strict_targets.cache_clear()


def _write_matrix(tmp_path, payload: dict) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "rust-parity-matrix.json").write_text(json.dumps(payload))


def _use_tmp_matrix(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(rust_parity, "_repo_root", lambda: tmp_path)
    rust_parity._load_parity_matrix.cache_clear()


def test_require_rust_subsystem_passes_for_rust_backed_target(monkeypatch, tmp_path):
    _write_matrix(
        tmp_path,
        {"subsystems": {"sql_generator_entrypoint": {"status": "rust_backed"}}},
    )
    _use_tmp_matrix(monkeypatch, tmp_path)
    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "sql_generator_entrypoint")
    rust_parity.strict_targets.cache_clear()

    rust_parity.require_rust_subsystem("sql_generator_entrypoint", "compile")


def test_repo_matrix_declares_rust_backed_strict_subsystems():
    matrix = rust_parity._load_parity_matrix()
    subsystems = matrix["subsystems"]

    assert matrix["runtime_contract"] == "docs/runtime-conformance.yml"
    assert matrix["runtime_lifecycle"] == "experimental"
    assert subsystems["sql_generator_entrypoint"]["status"] == "rust_backed"
    assert subsystems["semantic_core_query_validation"]["status"] == "rust_backed"
    assert subsystems["semantic_sql_rewriter"]["status"] == "rust_backed_opt_in"


def test_require_rust_subsystem_ignores_non_strict_targets(monkeypatch, tmp_path):
    _write_matrix(
        tmp_path,
        {"subsystems": {"semantic_sql_rewriter": {"status": "rust_backed_opt_in"}}},
    )
    _use_tmp_matrix(monkeypatch, tmp_path)
    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "sql_generator_entrypoint")
    rust_parity.strict_targets.cache_clear()

    rust_parity.require_rust_subsystem("semantic_sql_rewriter", "rewrite")


def test_require_rust_subsystem_fails_for_non_rust_backed_strict_target(monkeypatch, tmp_path):
    _write_matrix(
        tmp_path,
        {"subsystems": {"semantic_sql_rewriter": {"status": "rust_backed_opt_in"}}},
    )
    _use_tmp_matrix(monkeypatch, tmp_path)
    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "semantic_sql_rewriter")
    rust_parity.strict_targets.cache_clear()

    with pytest.raises(RuntimeError, match=r"\[rust-strict:semantic_sql_rewriter\].*rust_backed_opt_in"):
        rust_parity.require_rust_subsystem("semantic_sql_rewriter", "rewrite")


def test_require_rust_subsystem_all_targets_every_subsystem(monkeypatch, tmp_path):
    _write_matrix(
        tmp_path,
        {"subsystems": {"sql_generator_entrypoint": {"status": "rust_backed"}}},
    )
    _use_tmp_matrix(monkeypatch, tmp_path)
    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "all")
    rust_parity.strict_targets.cache_clear()

    with pytest.raises(RuntimeError, match=r"\[rust-strict:semantic_sql_rewriter\].*python_only"):
        rust_parity.require_rust_subsystem("semantic_sql_rewriter", "rewrite")


def test_strict_targets_parses_comma_separated_values(monkeypatch):
    monkeypatch.setenv(
        "SIDEMANTIC_RS_STRICT_SUBSYSTEMS",
        " sql_generator_entrypoint, semantic_core_query_validation ,,",
    )
    rust_parity.strict_targets.cache_clear()

    assert rust_parity.strict_targets() == {
        "sql_generator_entrypoint",
        "semantic_core_query_validation",
    }
    assert rust_parity.is_strict_for("semantic_core_query_validation") is True
    assert rust_parity.is_strict_for("semantic_sql_rewriter") is False


def test_missing_matrix_falls_back_to_python_only(monkeypatch, tmp_path):
    _use_tmp_matrix(monkeypatch, tmp_path)
    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "sql_generator_entrypoint")
    rust_parity.strict_targets.cache_clear()

    with pytest.raises(RuntimeError, match="python_only"):
        rust_parity.require_rust_subsystem("sql_generator_entrypoint", "compile")


def test_invalid_matrix_fails_with_source_context(monkeypatch, tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "rust-parity-matrix.json").write_text("{")
    _use_tmp_matrix(monkeypatch, tmp_path)
    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "sql_generator_entrypoint")
    rust_parity.strict_targets.cache_clear()

    with pytest.raises(rust_parity.ParityMatrixError, match="rust-parity-matrix.json") as error:
        rust_parity.require_rust_subsystem("sql_generator_entrypoint", "compile")

    assert isinstance(error.value.__cause__, json.JSONDecodeError)
