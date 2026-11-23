from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "plugins/sqlctx/skills/webapp-builder/assets/ui-dist"


def test_only_product_webapp_owns_component_source() -> None:
    component_assets = ROOT / "plugins/sqlctx/skills/webapp-builder/assets/components"
    assert not list(component_assets.rglob("*.tsx"))
    assert not list(component_assets.rglob("*.js"))
    assert (ROOT / "webapp/src/components/MetricCard.tsx").is_file()
    assert (ROOT / "webapp/src/components/Leaderboard.tsx").is_file()


def test_distribution_contains_react_and_static_builds() -> None:
    assert (DIST / "sqlctx-ui.js").stat().st_size > 1_000
    assert (DIST / "sqlctx-ui-static.js").stat().st_size > 1_000
    assert (DIST / "sqlctx-ui.css").stat().st_size > 1_000


def test_wasm_uses_exact_static_distribution() -> None:
    wasm = ROOT / "examples/sqlctx_wasm_demo/src/components/sqlctx"
    for filename in ("sqlctx-ui-static.js", "sqlctx-ui.css"):
        assert (wasm / filename).read_bytes() == (DIST / filename).read_bytes()


def test_copy_script_distributes_built_artifacts(tmp_path: Path) -> None:
    script = ROOT / "plugins/sqlctx/skills/webapp-builder/scripts/copy_components.py"
    spec = importlib.util.spec_from_file_location("copy_components", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    args = SimpleNamespace(kind="static", components=["all"], target=tmp_path, force=False, dry_run=False)
    assert len(module.check_components(args)) == 2
    module.copy_components(args)
    assert module.check_components(args) == []


def test_copy_script_lists_generated_distribution(capsys) -> None:
    script = ROOT / "plugins/sqlctx/skills/webapp-builder/scripts/copy_components.py"
    spec = importlib.util.spec_from_file_location("copy_components_list", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    module._list_components()
    listing = json.loads(capsys.readouterr().out)
    assert listing["react-tailwind"]["files"] == ["sqlctx-ui.css", "sqlctx-ui.js"]
    assert listing["static"]["files"] == ["sqlctx-ui-static.js", "sqlctx-ui.css"]
