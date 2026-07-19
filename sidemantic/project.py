"""Project discovery and CLI default resolution.

This module keeps filesystem convention in one place so commands do not each
invent a different meaning for ``.``.  It deliberately has no Typer dependency;
the CLI can translate :class:`ProjectResolutionError` into its normal error UI.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sidemantic.config import SidemanticConfig, build_connection_string, find_config, get_init_sql, load_config


class ProjectResolutionError(ValueError):
    """Raised when project defaults are invalid or ambiguous."""


@dataclass(frozen=True)
class ResolvedConnection:
    """A connection selected from CLI arguments, config, or project data."""

    connection: str
    source: str
    database: Path | None = None
    init_sql: list[str] | None = None


def ensure_mutually_exclusive(
    connection: str | None,
    database: str | Path | None,
    *,
    connection_option: str = "--connection",
    database_option: str = "--db",
) -> None:
    """Reject two command-line options that select the same resource."""

    if connection is not None and database is not None:
        raise ProjectResolutionError(f"{connection_option} and {database_option} are mutually exclusive")


def _resolve_cli_path(path: str | Path, start_dir: Path) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = start_dir / candidate
    return candidate.resolve()


def _require_path(path: Path, description: str) -> Path:
    if not path.exists():
        raise ProjectResolutionError(f"{description} not found: {path}")
    return path


def _load_config_values(config_path: Path) -> dict[str, Any]:
    if config_path.suffix.lower() == ".json":
        import json

        with config_path.open() as config_file:
            values = json.load(config_file)
    else:
        import yaml

        with config_path.open() as config_file:
            values = yaml.safe_load(config_file)
    return values if isinstance(values, dict) else {}


def _find_conventional_root(start: Path) -> Path:
    current = start
    while True:
        has_dashboard = any(
            (current / name).is_file() for name in ("dashboard.yml", "dashboard.yaml", "dashboard.json")
        )
        has_database = any(
            path.is_file() for pattern in ("*.db", "*.duckdb") for path in (current / "data").glob(pattern)
        )
        if (current / "models").is_dir() or has_dashboard or has_database:
            return current
        if current.parent == current:
            return start
        current = current.parent


@dataclass(frozen=True)
class ProjectContext:
    """Discovered Sidemantic project and its shared command defaults."""

    start_dir: Path
    root: Path
    config_path: Path | None = None
    config: SidemanticConfig | None = None
    config_values: dict[str, Any] | None = None

    @classmethod
    def discover(
        cls,
        start_dir: str | Path | None = None,
        config_path: str | Path | None = None,
    ) -> ProjectContext:
        """Discover a project upward from ``start_dir``.

        An explicitly requested config is authoritative: a missing or malformed
        file is an error rather than a warning followed by unrelated defaults.
        """

        start = Path(start_dir or Path.cwd()).expanduser().resolve()
        if not start.exists():
            raise ProjectResolutionError(f"Start directory not found: {start}")
        if start.is_file():
            start = start.parent

        if config_path is not None:
            selected_config = _resolve_cli_path(config_path, start)
            _require_path(selected_config, "Config file")
        else:
            selected_config = find_config(start)

        if selected_config is None:
            return cls(start_dir=start, root=_find_conventional_root(start))

        try:
            config = load_config(selected_config)
        except Exception as exc:
            raise ProjectResolutionError(f"Could not load config {selected_config}: {exc}") from exc

        return cls(
            start_dir=start,
            root=selected_config.parent.resolve(),
            config_path=selected_config.resolve(),
            config=config,
            config_values=_load_config_values(selected_config),
        )

    def resolve_models(self, explicit: str | Path | None = None) -> Path:
        """Resolve models using CLI, config, ``models/``, then project root."""

        if explicit is not None:
            return _require_path(_resolve_cli_path(explicit, self.start_dir), "Models path")

        if self.config is not None and "models_dir" in (self.config_values or {}):
            # load_config() has already made this path relative to the config root.
            return _require_path(Path(self.config.models_dir).resolve(), "Configured models path")

        conventional = self.root / "models"
        if conventional.is_dir():
            return conventional.resolve()
        return _require_path(self.root.resolve(), "Project root")

    def resolve_dashboard(
        self,
        explicit: str | Path | None = None,
        *,
        required: bool = True,
    ) -> Path | None:
        """Resolve a dashboard spec, rejecting ambiguous conventional files."""

        if explicit is not None:
            return _require_path(_resolve_cli_path(explicit, self.start_dir), "Dashboard spec")

        configured = (self.config_values or {}).get("dashboard")
        if configured:
            path = Path(configured).expanduser()
            if not path.is_absolute():
                path = self.root / path
            return _require_path(path.resolve(), "Configured dashboard spec")

        matches = [
            path
            for path in (self.root / "dashboard.yml", self.root / "dashboard.yaml", self.root / "dashboard.json")
            if path.is_file()
        ]
        if len(matches) > 1:
            choices = ", ".join(str(path) for path in matches)
            raise ProjectResolutionError(f"Multiple dashboard specs found: {choices}; pass one explicitly")
        if matches:
            return matches[0].resolve()
        if required:
            raise ProjectResolutionError(
                f"No dashboard spec found in {self.root}; expected dashboard.yml, dashboard.yaml, or dashboard.json"
            )
        return None

    def resolve_connection(
        self,
        *,
        connection: str | None = None,
        database: str | Path | None = None,
        models: str | Path | None = None,
        required: bool = False,
        discover: bool = True,
    ) -> ResolvedConnection | None:
        """Resolve a connection using CLI, config, then exactly one ``data`` DB."""

        ensure_mutually_exclusive(connection, database)
        if connection is not None:
            return ResolvedConnection(connection=connection, source="--connection")
        if database is not None:
            path = _require_path(_resolve_cli_path(database, self.start_dir), "Database")
            return ResolvedConnection(connection=f"duckdb:///{path}", database=path, source="--db")

        if self.config is not None and self.config.connection is not None:
            database_path = None
            connection_string = build_connection_string(self.config)
            if connection_string.startswith("duckdb:///"):
                raw_path = connection_string.removeprefix("duckdb:///")
                if raw_path != ":memory:":
                    database_path = Path(raw_path)
            return ResolvedConnection(
                connection=connection_string,
                database=database_path,
                init_sql=get_init_sql(self.config),
                source="config",
            )

        if not discover:
            return None

        data_dirs = [self.root / "data"]
        if models is not None:
            models_path = _resolve_cli_path(models, self.start_dir)
            if models_path.is_dir():
                data_dirs.append(models_path / "data")
                if models_path.name == "models":
                    data_dirs.append(models_path.parent / "data")
        matches = sorted(
            {
                path.resolve()
                for data_dir in data_dirs
                for pattern in ("*.db", "*.duckdb")
                for path in data_dir.glob(pattern)
                if path.is_file()
            },
            key=lambda path: str(path),
        )
        if len(matches) > 1:
            choices = ", ".join(str(path) for path in matches)
            raise ProjectResolutionError(f"Multiple databases found: {choices}; pass --db or --connection")
        if matches:
            path = matches[0]
            return ResolvedConnection(connection=f"duckdb:///{path}", database=path, source="project data")
        if required:
            locations = ", ".join(str(path) for path in dict.fromkeys(data_dirs))
            raise ProjectResolutionError(
                f"No database connection configured and no .db or .duckdb file found in: {locations}"
            )
        return None
