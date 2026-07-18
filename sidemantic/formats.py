"""Semantic format registry and conversion helpers.

This module describes the import/export capabilities already implemented by the
adapter package.  It deliberately keeps adapter imports lazy so listing formats
does not pull optional parser dependencies into core Sidemantic imports.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sidemantic.core.semantic_graph import SemanticGraph


class SourceKind(str, Enum):
    """Filesystem shapes accepted by a format parser."""

    FILE = "file"
    DIRECTORY = "directory"
    FILE_OR_DIRECTORY = "file_or_directory"


class OutputKind(str, Enum):
    """Filesystem shapes emitted by a format exporter."""

    FILE = "file"
    DIRECTORY = "directory"
    FILE_OR_DIRECTORY = "file_or_directory"


@dataclass(frozen=True)
class SemanticFormat:
    """One semantic-model interchange format and its adapter capabilities."""

    name: str
    adapter_module: str
    adapter_class: str
    aliases: tuple[str, ...] = ()
    extensions: tuple[str, ...] = ()
    source_kind: SourceKind = SourceKind.FILE_OR_DIRECTORY
    output_kind: OutputKind | None = None

    @property
    def supports_import(self) -> bool:
        return True

    @property
    def supports_export(self) -> bool:
        return self.output_kind is not None

    def create_adapter(self) -> Any:
        """Construct this format's adapter without eagerly importing others."""
        module = import_module(self.adapter_module)
        adapter_type = getattr(module, self.adapter_class)
        return adapter_type()


class UnknownFormatError(ValueError):
    """Raised when a format name or alias is not registered."""


class UnsupportedFormatOperationError(ValueError):
    """Raised when a registered format cannot perform an operation."""


_FORMATS = (
    SemanticFormat(
        "atscale-sml",
        "sidemantic.adapters.atscale_sml",
        "AtScaleSMLAdapter",
        aliases=("atscale", "sml"),
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.DIRECTORY,
    ),
    SemanticFormat(
        "bsl",
        "sidemantic.adapters.bsl",
        "BSLAdapter",
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.FILE_OR_DIRECTORY,
    ),
    SemanticFormat(
        "cube",
        "sidemantic.adapters.cube",
        "CubeAdapter",
        aliases=("cubejs", "cube-js"),
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.FILE,
    ),
    SemanticFormat(
        "gooddata",
        "sidemantic.adapters.gooddata",
        "GoodDataAdapter",
        aliases=("good-data",),
        extensions=(".json",),
        output_kind=OutputKind.FILE,
    ),
    SemanticFormat(
        "graphene",
        "sidemantic.adapters.graphene",
        "GrapheneAdapter",
        aliases=("gsql",),
        extensions=(".gsql",),
    ),
    SemanticFormat(
        "hex",
        "sidemantic.adapters.hex",
        "HexAdapter",
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.FILE_OR_DIRECTORY,
    ),
    SemanticFormat(
        "holistics",
        "sidemantic.adapters.holistics",
        "HolisticsAdapter",
        aliases=("aml",),
        extensions=(".aml",),
        output_kind=OutputKind.FILE_OR_DIRECTORY,
    ),
    SemanticFormat(
        "lookml",
        "sidemantic.adapters.lookml",
        "LookMLAdapter",
        aliases=("looker",),
        extensions=(".lkml",),
        output_kind=OutputKind.FILE,
    ),
    SemanticFormat(
        "malloy",
        "sidemantic.adapters.malloy",
        "MalloyAdapter",
        extensions=(".malloy",),
        output_kind=OutputKind.FILE,
    ),
    SemanticFormat(
        "metricflow",
        "sidemantic.adapters.metricflow",
        "MetricFlowAdapter",
        aliases=("dbt", "dbt-semantic-layer"),
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.FILE,
    ),
    SemanticFormat(
        "omni",
        "sidemantic.adapters.omni",
        "OmniAdapter",
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.DIRECTORY,
    ),
    SemanticFormat(
        "osi",
        "sidemantic.adapters.osi",
        "OSIAdapter",
        aliases=("open-semantic-interchange",),
        extensions=(".yml", ".yaml", ".json"),
        output_kind=OutputKind.FILE,
    ),
    SemanticFormat(
        "rill",
        "sidemantic.adapters.rill",
        "RillAdapter",
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.DIRECTORY,
    ),
    SemanticFormat(
        "sidemantic",
        "sidemantic.adapters.sidemantic",
        "SidemanticAdapter",
        aliases=("native",),
        extensions=(".yml", ".yaml", ".sql"),
        source_kind=SourceKind.FILE,
        output_kind=OutputKind.FILE,
    ),
    SemanticFormat(
        "snowflake",
        "sidemantic.adapters.snowflake",
        "SnowflakeAdapter",
        aliases=("cortex", "snowflake-cortex"),
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.FILE,
    ),
    SemanticFormat(
        "superset",
        "sidemantic.adapters.superset",
        "SupersetAdapter",
        extensions=(".yml", ".yaml"),
        output_kind=OutputKind.FILE_OR_DIRECTORY,
    ),
    SemanticFormat(
        "tableau",
        "sidemantic.adapters.tableau",
        "TableauAdapter",
        extensions=(".tds", ".twb", ".tdsx", ".twbx"),
    ),
    SemanticFormat(
        "thoughtspot",
        "sidemantic.adapters.thoughtspot",
        "ThoughtSpotAdapter",
        aliases=("thought-spot", "tml"),
        extensions=(".tml", ".yml", ".yaml"),
        output_kind=OutputKind.FILE_OR_DIRECTORY,
    ),
    SemanticFormat(
        "tmdl",
        "sidemantic.adapters.tmdl",
        "TMDLAdapter",
        aliases=("powerbi", "power-bi"),
        extensions=(".tmdl",),
        output_kind=OutputKind.FILE_OR_DIRECTORY,
    ),
    SemanticFormat(
        "yardstick",
        "sidemantic.adapters.yardstick",
        "YardstickAdapter",
        aliases=("yardstick-sql",),
        extensions=(".sql",),
    ),
)


def semantic_formats() -> tuple[SemanticFormat, ...]:
    """Return registered formats in stable display order."""
    return _FORMATS


def get_semantic_format(name: str, *, operation: str | None = None) -> SemanticFormat:
    """Resolve a canonical format name or alias.

    ``operation`` may be ``"import"`` or ``"export"`` and produces a focused
    error when a known format does not support that direction.
    """
    normalized = name.strip().lower().replace("_", "-")
    spec = next(
        (candidate for candidate in _FORMATS if normalized == candidate.name or normalized in candidate.aliases),
        None,
    )
    if spec is None:
        known = ", ".join(candidate.name for candidate in _FORMATS)
        raise UnknownFormatError(f"Unknown semantic format '{name}'. Available formats: {known}")

    if operation == "export" and not spec.supports_export:
        raise UnsupportedFormatOperationError(f"Format '{spec.name}' supports import but not export")
    if operation not in (None, "import", "export"):
        raise ValueError(f"Unknown format operation '{operation}'")
    return spec


def load_semantic_source(
    source: str | Path,
    *,
    source_format: str = "auto",
) -> SemanticGraph:
    """Load one semantic source using auto-discovery or an explicit adapter.

    File inputs are always exact: auto-discovery parses only the named file and
    never scans its siblings. Directory inputs retain the existing project-wide
    discovery behavior.
    """
    source_path = Path(source)
    if not source_path.exists():
        raise FileNotFoundError(f"Semantic source does not exist: {source_path}")

    if source_format.strip().lower() == "auto":
        from sidemantic.core.semantic_layer import SemanticLayer
        from sidemantic.loaders import load_from_directory, load_from_file

        layer = SemanticLayer()
        if source_path.is_file():
            load_from_file(layer, source_path)
        else:
            load_from_directory(layer, source_path)
        return layer.graph

    spec = get_semantic_format(source_format, operation="import")
    _validate_source_kind(source_path, spec)
    from sidemantic.loaders import parse_with_adapter

    return parse_with_adapter(spec.create_adapter(), source_path)


def export_semantic_graph(
    graph: SemanticGraph,
    output: str | Path,
    *,
    target_format: str = "sidemantic",
) -> None:
    """Export a graph through a registered format adapter."""
    spec = get_semantic_format(target_format, operation="export")
    spec.create_adapter().export(graph, Path(output))


def convert_semantic_source(
    source: str | Path,
    output: str | Path,
    *,
    source_format: str = "auto",
    target_format: str = "sidemantic",
) -> SemanticGraph:
    """Load an exact source, export it, and return the intermediate graph."""
    graph = load_semantic_source(source, source_format=source_format)
    export_semantic_graph(graph, output, target_format=target_format)
    return graph


def _validate_source_kind(source: Path, spec: SemanticFormat) -> None:
    if source.is_file() and spec.source_kind == SourceKind.DIRECTORY:
        raise ValueError(f"Format '{spec.name}' requires a directory source")
    if source.is_dir() and spec.source_kind == SourceKind.FILE:
        raise ValueError(f"Format '{spec.name}' requires a file source")
