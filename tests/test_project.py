from pathlib import Path

import pytest

from sidemantic.project import ProjectContext, ProjectResolutionError, ensure_mutually_exclusive


def test_discovers_config_upward_and_resolves_config_paths_from_project_root(tmp_path):
    (tmp_path / "models").mkdir()
    (tmp_path / "data").mkdir()
    database = tmp_path / "data" / "warehouse.duckdb"
    database.touch()
    (tmp_path / "sidemantic.yaml").write_text(
        "models_dir: models\nconnection:\n  type: duckdb\n  path: data/warehouse.duckdb\n"
    )
    nested = tmp_path / "work" / "nested"
    nested.mkdir(parents=True)

    project = ProjectContext.discover(nested)

    assert project.root == tmp_path
    assert project.resolve_models() == tmp_path / "models"
    resolved = project.resolve_connection(required=True)
    assert resolved is not None
    assert resolved.database == database
    assert resolved.source == "config"


def test_explicit_paths_override_config_and_are_relative_to_invocation_dir(tmp_path):
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "configured-models").mkdir()
    (project_root / "sidemantic.yaml").write_text("models_dir: configured-models\n")
    nested = project_root / "work"
    nested.mkdir()
    explicit_models = nested / "local-models"
    explicit_models.mkdir()
    explicit_database = nested / "local.duckdb"
    explicit_database.touch()

    project = ProjectContext.discover(nested)

    assert project.resolve_models("local-models") == explicit_models
    resolved = project.resolve_connection(database="local.duckdb")
    assert resolved is not None
    assert resolved.database == explicit_database
    assert resolved.source == "--db"


def test_models_convention_prefers_models_directory_then_root(tmp_path):
    project = ProjectContext.discover(tmp_path)
    assert project.resolve_models() == tmp_path

    models = tmp_path / "models"
    models.mkdir()
    assert project.resolve_models() == models


def test_conventional_project_is_discovered_upward_without_config(tmp_path):
    models = tmp_path / "models"
    nested = tmp_path / "work" / "nested"
    models.mkdir()
    nested.mkdir(parents=True)

    project = ProjectContext.discover(nested)

    assert project.root == tmp_path
    assert project.resolve_models() == models


def test_config_without_models_dir_uses_conventional_models_directory(tmp_path):
    models = tmp_path / "models"
    models.mkdir()
    (tmp_path / "sidemantic.yaml").write_text("runtime:\n  engine: python\n")

    assert ProjectContext.discover(tmp_path).resolve_models() == models


@pytest.mark.parametrize("suffix", ["yml", "yaml", "json"])
def test_dashboard_conventional_discovery(tmp_path, suffix):
    dashboard = tmp_path / f"dashboard.{suffix}"
    dashboard.write_text("{}")

    assert ProjectContext.discover(tmp_path).resolve_dashboard() == dashboard


def test_dashboard_discovery_rejects_ambiguity(tmp_path):
    (tmp_path / "dashboard.yml").touch()
    (tmp_path / "dashboard.json").touch()

    with pytest.raises(ProjectResolutionError, match="Multiple dashboard specs found"):
        ProjectContext.discover(tmp_path).resolve_dashboard()


def test_dashboard_config_path_is_relative_to_config_root(tmp_path):
    specs = tmp_path / "specs"
    specs.mkdir()
    dashboard = specs / "sales.yml"
    dashboard.touch()
    (tmp_path / "sidemantic.yaml").write_text("dashboard: specs/sales.yml\n")
    nested = tmp_path / "nested"
    nested.mkdir()

    assert ProjectContext.discover(nested).resolve_dashboard() == dashboard


def test_database_discovery_accepts_db_and_duckdb_and_rejects_ambiguity(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    first = data / "analytics.duckdb"
    first.touch()
    project = ProjectContext.discover(tmp_path)

    resolved = project.resolve_connection(required=True)
    assert resolved is not None
    assert resolved.database == first
    assert resolved.source == "project data"

    (data / "warehouse.db").touch()
    with pytest.raises(ProjectResolutionError, match="Multiple databases found"):
        project.resolve_connection(required=True)


def test_connection_models_path_is_relative_to_invocation_dir(tmp_path, monkeypatch):
    invocation_dir = tmp_path / "invocation"
    models_dir = invocation_dir / "local-models"
    data_dir = models_dir / "data"
    data_dir.mkdir(parents=True)
    database = data_dir / "analytics.duckdb"
    database.touch()
    unrelated_cwd = tmp_path / "elsewhere"
    unrelated_cwd.mkdir()
    project = ProjectContext.discover(invocation_dir)

    monkeypatch.chdir(unrelated_cwd)
    resolved = project.resolve_connection(models="local-models", required=True)

    assert resolved is not None
    assert resolved.database == database


def test_explicit_connection_and_database_are_mutually_exclusive(tmp_path):
    with pytest.raises(ProjectResolutionError, match="--connection and --db are mutually exclusive"):
        ProjectContext.discover(tmp_path).resolve_connection(connection="duckdb:///:memory:", database="data.db")

    with pytest.raises(ProjectResolutionError, match="--connection and --database are mutually exclusive"):
        ensure_mutually_exclusive("uri", Path("db"), database_option="--database")


def test_explicit_missing_or_malformed_config_is_fatal(tmp_path):
    with pytest.raises(ProjectResolutionError, match="Config file not found"):
        ProjectContext.discover(tmp_path, "missing.yml")

    malformed = tmp_path / "broken.yml"
    malformed.write_text("models_dir: [")
    with pytest.raises(ProjectResolutionError, match="Could not load config"):
        ProjectContext.discover(tmp_path, malformed)


def test_optional_resources_can_be_absent(tmp_path):
    project = ProjectContext.discover(tmp_path)

    assert project.resolve_dashboard(required=False) is None
    assert project.resolve_connection(required=False) is None
