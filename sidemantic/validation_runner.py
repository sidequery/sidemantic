"""Noninteractive semantic layer validation."""

from dataclasses import dataclass, field
from pathlib import Path

from sidemantic import SemanticLayer, load_from_directory
from sidemantic.validation import validate_metric, validate_model, validate_model_warnings


@dataclass
class ValidationReport:
    directory: Path
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.errors


def validate_directory(directory: str | Path) -> ValidationReport:
    """Load and validate semantic layer definitions from a directory."""
    directory = Path(directory)
    report = ValidationReport(directory=directory)

    layer = SemanticLayer()
    load_from_directory(layer, str(directory))

    if not layer.graph.models:
        report.errors.append("No models found in directory")
        return report

    report.info.append(f"Loaded {len(layer.graph.models)} models")

    for model_name, model in layer.graph.models.items():
        report.errors.extend(validate_model(model))
        report.warnings.extend(validate_model_warnings(model))

        if not model.dimensions:
            report.warnings.append(f"Model '{model_name}' has no dimensions")
        if not model.metrics:
            report.warnings.append(f"Model '{model_name}' has no metrics")

        for metric in model.metrics:
            report.errors.extend(validate_metric(metric, layer.graph))

        for rel in model.relationships:
            if rel.name not in layer.graph.models:
                report.errors.append(f"Model '{model_name}' has relationship to '{rel.name}' which doesn't exist")

        # Hex ``view`` resources reference a base model by name and carry their
        # own ``contents``. Both are required by the Hex spec, but views are
        # exempt from the physical-source check in ``validate_model``, so a
        # missing/misspelled base or absent contents would otherwise pass
        # silently on the CLI validation path.
        model_meta = getattr(model, "meta", None) or {}
        if model_meta.get("hex_resource_type") == "view":
            base = model_meta.get("base")
            if not base:
                report.errors.append(f"Hex view '{model_name}' must have a 'base' model reference defined")
            elif base not in layer.graph.models:
                report.errors.append(f"Hex view '{model_name}' references base model '{base}' which doesn't exist")
            if not model_meta.get("contents"):
                report.errors.append(f"Hex view '{model_name}' must have non-empty 'contents' defined")

    for metric in layer.graph.metrics.values():
        report.errors.extend(validate_metric(metric, layer.graph))

    if len(layer.graph.models) > 1:
        orphaned = []
        for model_name, model in layer.graph.models.items():
            has_outgoing = bool(model.relationships)
            has_incoming = any(
                any(rel.name == model_name for rel in other.relationships)
                for other_name, other in layer.graph.models.items()
                if other_name != model_name
            )
            if not has_outgoing and not has_incoming:
                orphaned.append(model_name)

        if orphaned:
            report.warnings.append(f"Orphaned models (no relationships): {', '.join(orphaned)}")

    total_dims = sum(len(model.dimensions) for model in layer.graph.models.values())
    total_metrics = sum(len(model.metrics) for model in layer.graph.models.values())
    total_rels = sum(len(model.relationships) for model in layer.graph.models.values())

    report.info.append(f"Total dimensions: {total_dims}")
    report.info.append(f"Total metrics: {total_metrics}")
    report.info.append(f"Total relationships: {total_rels}")

    return report
