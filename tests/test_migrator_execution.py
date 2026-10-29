"""Execute the generated models and queries together against the original SQL."""

import pytest

from sidemantic import Metric, Model, SemanticLayer
from sidemantic.core.migrator import Migrator


@pytest.fixture
def layer():
    layer = SemanticLayer(auto_register=False)
    layer.conn.execute("CREATE TABLE sales(amount INTEGER, status VARCHAR, category VARCHAR)")
    layer.conn.execute(
        "INSERT INTO sales VALUES (4, 'paid', 'a'), (10, 'pending', 'a'), "
        "(20, 'pending', 'b'), (30, 'pending', 'b'), (NULL, 'paid', 'b')"
    )
    yield layer
    layer.conn.close()


def assert_migration_executes(layer, queries):
    expected = [layer.conn.execute(query).fetchall() for query in queries]
    migrator = Migrator(layer)
    report = migrator.analyze_queries(queries)
    assert report.parseable_queries == len(queries)
    # Calling either generator first must make the same naming decisions.
    rewritten = migrator.generate_rewritten_queries(report)
    models = migrator.generate_models(report)
    assert rewritten == migrator.generate_rewritten_queries(report)
    for model in models.values():
        layer.add_model(Model(**model))
    for metric in migrator.generate_graph_metrics(report, models):
        layer.add_metric(Metric(**metric))
    for index, rows in enumerate(expected, 1):
        assert layer.sql(rewritten[f"query_{index}"]).fetchall() == rows
    return migrator, report, models


def test_filter_populations_and_nulls_survive_migration(layer):
    queries = [
        "SELECT category, SUM(s.amount) FILTER (WHERE s.status = 'paid') AS revenue, "
        "SUM(s.amount) FILTER (WHERE s.status = 'pending') AS pending, "
        "SUM(s.amount) AS total, COUNT(*) FILTER (WHERE s.status = 'paid') AS paid_rows, "
        "COUNT(s.amount) FILTER (WHERE s.status = 'paid') AS paid_values "
        "FROM sales s GROUP BY category ORDER BY category",
        "SELECT SUM(amount) FILTER (WHERE status = 'missing') AS missing FROM sales",
        "SELECT SUM(amount * 2) FILTER (WHERE amount > 2 * 3) AS large_amount FROM sales",
        "SELECT SUM(amount) FILTER (WHERE status = 'paid'), SUM(amount) FILTER (WHERE status = 'pending') FROM sales",
    ]
    _, _, models = assert_migration_executes(layer, queries)
    complete_metrics = [m for m in models["sales"]["metrics"] if m.get("sql_is_complete")]
    assert len(complete_metrics) == 6
    # Re-analysis must recognize these exact populations in the installed model.
    assert Migrator(layer).analyze_queries(queries).rewritable_queries == len(queries)


@pytest.mark.parametrize("percentile", [0.1, 0.5, 0.9])
def test_approximate_percentile_argument_survives_migration(layer, percentile):
    assert_migration_executes(
        layer,
        [
            f"SELECT APPROX_QUANTILE(s.amount, {percentile}) AS percentile, MEDIAN(s.amount) AS median FROM sales s",
            f"SELECT APPROX_QUANTILE(amount, {percentile}) FILTER (WHERE status = 'pending') AS filtered FROM sales",
        ],
    )


def test_different_percentiles_remain_distinct_in_one_report(layer):
    _, _, models = assert_migration_executes(
        layer,
        [
            "SELECT APPROX_QUANTILE(amount, 0.9) AS p90, APPROX_QUANTILE(amount, 0.5) AS p50 FROM sales",
            "SELECT APPROX_QUANTILE(amount, 0.9) AS percentile FROM sales",
        ],
    )
    assert len(models["sales"]["metrics"]) == 2


def test_report_wide_aliases_collisions_and_shared_aggregates(layer):
    _, _, models = assert_migration_executes(
        layer,
        [
            "SELECT SUM(amount) AS revenue FROM sales",
            "SELECT SUM(amount) AS total FROM sales",
            "SELECT SUM(amount) AS total FROM sales",
            "SELECT SUM(amount) AS first_total, SUM(amount) AS second_total FROM sales",
            "SELECT AVG(amount) AS total FROM sales",
            "SELECT COUNT(*) AS sum_amount FROM sales",
            "SELECT category, SUM(amount) AS revenue FROM sales GROUP BY category ORDER BY revenue DESC",
            "SELECT SUM(amount) AS revenue, SUM(amount) / COUNT(*) AS average FROM sales",
            "SELECT SUM(amount) AS average FROM sales",
        ],
    )
    metrics = models["sales"]["metrics"]
    assert len({metric["name"] for metric in metrics}) == len(metrics)
    assert len([metric for metric in metrics if metric.get("agg") == "sum"]) == 1


def test_alias_collision_with_canonical_name_does_not_drop_aggregate(layer):
    assert_migration_executes(
        layer,
        [
            "SELECT COUNT(*) AS sum_amount, SUM(amount) FROM sales",
            "SELECT SUM(amount) AS sum_amount, AVG(amount) AS sum_amount FROM sales",
        ],
    )


def test_filtered_parameterized_aggregates_with_generated_join(layer):
    layer.conn.execute("ALTER TABLE sales ADD COLUMN category_id INTEGER")
    layer.conn.execute("UPDATE sales SET category_id = CASE category WHEN 'a' THEN 1 ELSE 2 END")
    layer.conn.execute("CREATE TABLE categories(id INTEGER PRIMARY KEY, weight INTEGER)")
    layer.conn.execute("INSERT INTO categories VALUES (1, 1), (2, 2)")
    assert_migration_executes(
        layer,
        [
            "SELECT c.weight, APPROX_QUANTILE(s.amount, 0.9) FILTER (WHERE s.status = 'pending') AS p90 "
            "FROM sales s JOIN categories c ON s.category_id = c.id GROUP BY c.weight ORDER BY c.weight",
        ],
    )


def test_filtered_parameterized_aggregates_resolve_graph_references(layer):
    migrator = Migrator(layer)
    report = migrator.analyze_queries(
        [
            "SELECT APPROX_QUANTILE(s.amount, 0.9) FILTER (WHERE s.status = 'pending') / "
            "COUNT(DISTINCT c.id) FILTER (WHERE c.weight > 0) AS ratio "
            "FROM sales s JOIN categories c ON s.category_id = c.id",
        ]
    )
    models = migrator.generate_models(report)
    graph_metrics = migrator.generate_graph_metrics(report, models)
    assert len(graph_metrics) == 1
    numerator = models["sales"]["metrics"][0]["name"]
    denominator = models["categories"]["metrics"][0]["name"]
    assert graph_metrics[0]["sql"] == f"sales.{numerator} / categories.{denominator}"
