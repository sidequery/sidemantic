"""Behavior tests for the optional Textual workbench surface."""

from __future__ import annotations

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

from sidemantic.validation_runner import ValidationReport
from sidemantic.workbench import run_validation, run_workbench
from sidemantic.workbench.app import SidequeryWorkbench
from sidemantic.workbench.validation_app import ValidationApp


def test_run_workbench_builds_and_runs_application(monkeypatch, tmp_path: Path):
    calls: list[tuple[Path, bool, str | None]] = []

    class FakeWorkbench:
        def __init__(self, directory: Path, *, demo_mode: bool, connection: str | None):
            calls.append((directory, demo_mode, connection))

        def run(self) -> None:
            calls.append((Path("run"), False, None))

    monkeypatch.setitem(sys.modules, "sidemantic.workbench.app", SimpleNamespace(SidequeryWorkbench=FakeWorkbench))

    run_workbench(tmp_path, demo_mode=True, connection="duckdb:///warehouse.db")

    assert calls == [(tmp_path, True, "duckdb:///warehouse.db"), (Path("run"), False, None)]


def test_run_validation_builds_and_runs_application(monkeypatch, tmp_path: Path):
    calls: list[tuple[Path, bool] | str] = []

    class FakeValidationApp:
        def __init__(self, directory: Path, *, verbose: bool):
            calls.append((directory, verbose))

        def run(self) -> None:
            calls.append("run")

    monkeypatch.setitem(
        sys.modules,
        "sidemantic.workbench.validation_app",
        SimpleNamespace(ValidationApp=FakeValidationApp),
    )

    run_validation(tmp_path, verbose=True)

    assert calls == [(tmp_path, True), "run"]


def test_validation_app_renders_failures_warnings_and_info(monkeypatch, tmp_path: Path):
    report = ValidationReport(
        directory=tmp_path,
        errors=["Metric revenue is invalid"],
        warnings=["Model has no description"],
        info=["Loaded 1 models"],
    )
    rendered: list[str] = []
    result_widget = SimpleNamespace(update=rendered.append)
    app = ValidationApp(tmp_path, verbose=True)

    monkeypatch.setattr("sidemantic.workbench.validation_app.validate_directory", lambda _directory: report)
    monkeypatch.setattr(app, "query_one", lambda *_args, **_kwargs: result_widget)

    app.on_mount()

    assert app.errors == report.errors
    assert app.warnings == report.warnings
    assert app.info == report.info
    assert "Metric revenue is invalid" in rendered[0]
    assert "Model has no description" in rendered[0]
    assert "Loaded 1 models" in rendered[0]
    assert "Validation Failed" in rendered[0]


def test_validation_app_success_summary_includes_info(monkeypatch, tmp_path: Path):
    report = ValidationReport(directory=tmp_path, info=["Loaded 2 models"])
    rendered: list[str] = []
    result_widget = SimpleNamespace(update=rendered.append)
    app = ValidationApp(tmp_path)

    monkeypatch.setattr("sidemantic.workbench.validation_app.validate_directory", lambda _directory: report)
    monkeypatch.setattr(app, "query_one", lambda *_args, **_kwargs: result_widget)

    app.on_mount()

    assert "Loaded 2 models" in rendered[0]
    assert "Validation Passed" in rendered[0]


def test_workbench_mounts_models_and_executes_semantic_query(tmp_path: Path):
    (tmp_path / "orders.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
""".strip()
        + "\n",
        encoding="utf-8",
    )
    app = SidequeryWorkbench(tmp_path, connection="duckdb:///:memory:")

    async def exercise() -> None:
        async with app.run_test(size=(140, 50)) as pilot:
            assert app.layer is not None
            assert str(app.query_one("#tree").label) == "Models (1)"

            app.layer.adapter.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
            app.layer.adapter.execute("INSERT INTO orders VALUES (1, 'paid'), (2, 'paid'), (3, 'pending')")

            editor = app.query_one("#editor-timeseries").query_one("TextArea")
            editor.text = """
SELECT orders.status, orders.order_count
FROM orders
ORDER BY orders.order_count DESC
""".strip()

            app.action_run_query()
            await pilot.pause()

            assert app.last_result == {
                "columns": ["status", "order_count"],
                "rows": [("paid", 2), ("pending", 1)],
            }
            assert "COUNT" in app.last_rendered_sql
            assert app.query_one("#results-table").row_count == 2

            await pilot.click("#btn-chart")
            assert app.query_one("#chart-view").styles.display == "block"
            assert app.query_one("#table-view").styles.display == "none"

            await pilot.click("#btn-custom")
            assert app.query_one("#editor-custom").styles.display == "block"
            assert app.last_result is None

    # A few integration tests intentionally leave an event loop active in the
    # main pytest thread. Run Textual's test harness in an isolated thread so
    # this behavior test stays independent of full-suite ordering.
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(asyncio.run, exercise()).result()
