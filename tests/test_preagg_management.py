from pathlib import Path

import pytest

from sidemantic.core.pre_aggregation import PreAggregation, RefreshKey
from sidemantic.core.preagg_management import (
    PreAggregationTargetError,
    apply_recommendations_to_yaml,
    resolve_preaggregation_targets,
    resolve_refresh_mode,
)
from sidemantic.core.preagg_recommender import (
    PreAggRecommendation,
    PreAggregationRecommender,
    QueryPattern,
)


def _recommendation(model: str = "orders", name: str = "by_status") -> PreAggRecommendation:
    return PreAggRecommendation(
        pattern=QueryPattern(
            model=model,
            metrics=frozenset({f"{model}.revenue"}),
            dimensions=frozenset({f"{model}.status"}),
            granularities=frozenset(),
        ),
        suggested_name=name,
        query_count=20,
        estimated_benefit_score=0.8,
    )


def _write_model(path: Path, *, model: str = "orders", preaggregations: str = "") -> None:
    path.write_text(
        f"""models:
  - name: {model}
    table: {model}
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    dimensions:
      - name: status
        type: categorical
{preaggregations}"""
    )


def test_auto_refresh_mode_uses_definition_strategy():
    full = PreAggregation(name="full", measures=["revenue"])
    incremental = PreAggregation(
        name="incremental",
        measures=["revenue"],
        refresh_key=RefreshKey(incremental=True),
    )

    assert resolve_refresh_mode(full, "auto") == "full"
    assert resolve_refresh_mode(incremental, "auto") == "incremental"
    assert resolve_refresh_mode(incremental, "merge") == "merge"


def test_preaggregation_refresh_accepts_auto():
    preagg = PreAggregation(name="rollup", measures=["revenue"])

    class Result:
        def fetchone(self):
            return (1,)

    class Connection:
        def __init__(self):
            self.statements = []

        def execute(self, sql):
            self.statements.append(sql)
            return Result()

    connection = Connection()

    result = preagg.refresh(connection, "SELECT 1", "rollup_table", mode="auto")

    assert result.mode == "full"
    assert connection.statements[:2] == [
        "DROP TABLE IF EXISTS rollup_table",
        "CREATE TABLE rollup_table AS SELECT 1",
    ]


def test_recommendation_application_is_idempotent_and_preserves_existing_definitions(tmp_path):
    model_file = tmp_path / "orders.yml"
    _write_model(
        model_file,
        preaggregations="""    pre_aggregations:
      - name: existing
        measures: [revenue]
""",
    )
    recommender = PreAggregationRecommender()

    first = apply_recommendations_to_yaml(tmp_path, [_recommendation()], recommender)
    second = apply_recommendations_to_yaml(tmp_path, [_recommendation()], recommender)

    assert first.added == 1
    assert second.added == 0
    assert second.skipped == 1
    contents = model_file.read_text()
    assert contents.count("name: by_status") == 1
    assert "name: existing" in contents


def test_recommendation_application_rejects_conflicting_name(tmp_path):
    _write_model(
        tmp_path / "orders.yml",
        preaggregations="""    pre_aggregations:
      - name: by_status
        measures: [revenue]
        dimensions: [different]
""",
    )

    with pytest.raises(PreAggregationTargetError, match="different definition"):
        apply_recommendations_to_yaml(
            tmp_path,
            [_recommendation()],
            PreAggregationRecommender(),
        )


def test_recommendation_application_rejects_duplicate_model_definitions(tmp_path):
    _write_model(tmp_path / "one.yml")
    _write_model(tmp_path / "two.yml")

    with pytest.raises(PreAggregationTargetError, match="defined more than once"):
        apply_recommendations_to_yaml(
            tmp_path,
            [_recommendation()],
            PreAggregationRecommender(),
        )


def test_target_resolution_requires_model_for_duplicate_preaggregation_names():
    class Model:
        def __init__(self):
            self.pre_aggregations = [PreAggregation(name="daily", measures=["count"])]

    with pytest.raises(PreAggregationTargetError, match="ambiguous"):
        resolve_preaggregation_targets({"orders": Model(), "users": Model()}, preagg_name="daily")

    targets = resolve_preaggregation_targets(
        {"orders": Model(), "users": Model()},
        model_name="orders",
        preagg_name="daily",
    )
    assert [(name, preagg.name) for name, _, preagg in targets] == [("orders", "daily")]
