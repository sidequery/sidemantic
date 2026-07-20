"""Safe planning helpers for pre-aggregation management commands."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.core.preagg_recommender import PreAggRecommendation, PreAggregationRecommender
from sidemantic.yaml_compat import safe_load as _yaml_safe_load

RefreshMode = Literal["full", "incremental", "merge", "engine"]


class PreAggregationTargetError(ValueError):
    """Raised when a model or pre-aggregation target is missing or ambiguous."""


@dataclass(frozen=True)
class ModelDefinitionLocation:
    """The unique YAML location of a model definition."""

    path: Path
    index: int


@dataclass(frozen=True)
class RecommendationApplyResult:
    """Summary of a recommendation application plan or write."""

    added: int
    skipped: int
    changed_files: tuple[Path, ...]


def resolve_refresh_mode(preagg: PreAggregation, requested: str | None = "auto") -> RefreshMode:
    """Resolve ``auto`` from the pre-aggregation's declared refresh strategy.

    A definition opts into incremental refresh through
    ``refresh_key.incremental``. Definitions without that opt-in receive a full
    refresh. Explicit modes always win.
    """

    normalized = (requested or "auto").lower()
    if normalized == "auto":
        return "incremental" if preagg.refresh_key and preagg.refresh_key.incremental else "full"
    if normalized not in {"full", "incremental", "merge", "engine"}:
        raise ValueError(f"Invalid refresh mode '{requested}'. Expected auto, full, incremental, merge, or engine.")
    return normalized  # type: ignore[return-value]


def resolve_preaggregation_targets(
    models: Mapping[str, Any],
    *,
    model_name: str | None = None,
    preagg_name: str | None = None,
) -> list[tuple[str, Any, PreAggregation]]:
    """Select refresh targets deterministically and reject ambiguous names."""

    if model_name and model_name not in models:
        available = ", ".join(sorted(models)) or "(none)"
        raise PreAggregationTargetError(f"Unknown model '{model_name}'. Available models: {available}")

    selected_models = [(model_name, models[model_name])] if model_name else sorted(models.items())
    targets: list[tuple[str, Any, PreAggregation]] = []
    for current_name, model in selected_models:
        for candidate in sorted(model.pre_aggregations, key=lambda item: item.name):
            if preagg_name is None or candidate.name == preagg_name:
                targets.append((current_name, model, candidate))

    if preagg_name and not targets:
        scope = f"model '{model_name}'" if model_name else "the loaded models"
        raise PreAggregationTargetError(f"Unknown pre-aggregation '{preagg_name}' in {scope}")

    if preagg_name and not model_name and len(targets) > 1:
        matches = ", ".join(f"{name}.{preagg.name}" for name, _, preagg in targets)
        raise PreAggregationTargetError(
            f"Pre-aggregation name '{preagg_name}' is ambiguous: {matches}. Pass --model to select one."
        )

    return targets


def apply_recommendations_to_yaml(
    directory: Path,
    recommendations: Iterable[PreAggRecommendation],
    recommender: PreAggregationRecommender,
    *,
    dry_run: bool = False,
) -> RecommendationApplyResult:
    """Add recommendations atomically without duplicating or replacing definitions.

    Model names must resolve to exactly one YAML definition. An identical
    recommendation is skipped, while a name collision with different content is
    rejected instead of silently overwriting the user's definition.
    """

    documents, locations = _load_model_documents(directory)
    added = 0
    skipped = 0
    changed: set[Path] = set()

    ordered = sorted(
        recommendations,
        key=lambda rec: (rec.pattern.model, rec.suggested_name, -rec.estimated_benefit_score),
    )
    for recommendation in ordered:
        model_name = recommendation.pattern.model
        model_locations = locations.get(model_name, [])
        if not model_locations:
            raise PreAggregationTargetError(f"Could not find YAML definition for model '{model_name}'")
        if len(model_locations) > 1:
            paths = ", ".join(str(location.path) for location in model_locations)
            raise PreAggregationTargetError(
                f"Model '{model_name}' is defined more than once ({paths}); refusing to choose a target"
            )

        location = model_locations[0]
        model_definition = documents[location.path]["models"][location.index]
        existing = model_definition.setdefault("pre_aggregations", [])
        payload = _recommendation_payload(recommender, recommendation)

        matches = [definition for definition in existing if definition.get("name") == payload["name"]]
        if matches:
            if any(_canonical_definition(definition) == _canonical_definition(payload) for definition in matches):
                skipped += 1
                continue
            raise PreAggregationTargetError(
                f"Pre-aggregation '{model_name}.{payload['name']}' already exists with a different definition; "
                "refusing to overwrite it"
            )

        existing.append(payload)
        added += 1
        changed.add(location.path)

    changed_files = tuple(sorted(changed))
    if not dry_run:
        for path in changed_files:
            _atomic_yaml_write(path, documents[path])

    return RecommendationApplyResult(added=added, skipped=skipped, changed_files=changed_files)


def _load_model_documents(
    directory: Path,
) -> tuple[dict[Path, dict[str, Any]], dict[str, list[ModelDefinitionLocation]]]:
    if not directory.exists():
        raise FileNotFoundError(f"Models directory does not exist: {directory}")
    if not directory.is_dir():
        raise NotADirectoryError(f"Models path is not a directory: {directory}")

    documents: dict[Path, dict[str, Any]] = {}
    locations: dict[str, list[ModelDefinitionLocation]] = {}
    yaml_files = sorted({*directory.rglob("*.yml"), *directory.rglob("*.yaml")})
    for path in yaml_files:
        loaded = _yaml_safe_load(path.read_text())
        if loaded is None:
            continue
        if not isinstance(loaded, dict):
            continue
        documents[path] = loaded
        models = loaded.get("models", [])
        if not isinstance(models, list):
            continue
        for index, definition in enumerate(models):
            if not isinstance(definition, dict) or not isinstance(definition.get("name"), str):
                continue
            locations.setdefault(definition["name"], []).append(ModelDefinitionLocation(path, index))

    return documents, locations


def _recommendation_payload(
    recommender: PreAggregationRecommender, recommendation: PreAggRecommendation
) -> dict[str, Any]:
    definition = recommender.generate_preagg_definition(recommendation)
    return definition.model_dump(
        mode="json",
        exclude_none=True,
        exclude_defaults=True,
        exclude={"rollups", "union_with_source_data", "scheduled_refresh"},
    )


def _canonical_definition(definition: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize defaults so hand-written and generated definitions compare equally."""

    return PreAggregation.model_validate(definition).model_dump(mode="json", exclude_none=True)


def _atomic_yaml_write(path: Path, document: dict[str, Any]) -> None:
    rendered = yaml.safe_dump(document, sort_keys=False, default_flow_style=False)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as temporary:
            temporary.write(rendered)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
