#!/usr/bin/env python3
"""Rill Demo: Export sidemantic models to Rill and run in Docker.

This demo:
1. Loads a sidemantic YAML definition
2. Exports to a complete Rill project (sources, models, metrics_views)
3. Runs Rill Developer in Docker to visualize the dashboard

Prerequisites:
- Docker installed and running

Usage:
    uv run examples/rill_demo/run_demo.py
"""

import shutil
import subprocess
import sys
from pathlib import Path

# Add project root to path for local development
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def main():
    demo_dir = Path(__file__).parent
    sidemantic_yaml = demo_dir / "sidemantic.yaml"
    rill_project_dir = demo_dir / "rill_project"

    print("=" * 60)
    print("  Sidemantic to Rill Demo")
    print("=" * 60)

    # Step 1: Load sidemantic YAML
    print("\n[1/4] Loading sidemantic.yaml...")
    from sidemantic.adapters.sidemantic import SidemanticAdapter

    adapter = SidemanticAdapter()
    graph = adapter.parse(sidemantic_yaml)
    print(f"      Loaded {len(graph.models)} model(s)")
    for model_name, model in graph.models.items():
        print(f"      - {model_name}: {len(model.dimensions)} dimensions, {len(model.metrics)} metrics")

    # Step 2: Export to Rill
    print("\n[2/4] Exporting to Rill project...")
    from sidemantic.adapters.rill import RillAdapter

    # Clean previous output
    if rill_project_dir.exists():
        shutil.rmtree(rill_project_dir)

    rill_adapter = RillAdapter()
    rill_adapter.export(graph, rill_project_dir, project_name="Sidemantic Adtech Demo", full_project=True)

    # List generated files
    print(f"      Generated Rill project at: {rill_project_dir}")
    for file in sorted(rill_project_dir.rglob("*")):
        if file.is_file():
            rel_path = file.relative_to(rill_project_dir)
            print(f"      - {rel_path}")

    # Step 3: Check Docker
    print("\n[3/4] Checking Docker...")
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            print("      ERROR: Docker is not running. Please start Docker and try again.")
            sys.exit(1)
        print("      Docker is running")
    except FileNotFoundError:
        print("      ERROR: Docker is not installed. Please install Docker and try again.")
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("      ERROR: Docker timed out. Please ensure Docker is running.")
        sys.exit(1)

    # Step 4: Run Rill in Docker (fresh install via rill.sh)
    print("\n[4/4] Starting Rill Developer in Docker...")
    print("      Installing latest Rill in container...")
    print("      Port: http://localhost:9009")
    print()
    print("=" * 60)
    print("  Open http://localhost:9009 in your browser")
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    print()

    # Use Ubuntu container, install Rill fresh, then run it
    # This ensures we get the latest version instead of stale Docker Hub image
    try:
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-it",
                "-p",
                "9009:9009",
                "-v",
                f"{rill_project_dir.absolute()}:/project",
                "-w",
                "/project",
                "ubuntu:22.04",
                "bash",
                "-c",
                "apt-get update -qq && apt-get install -qq -y curl unzip git > /dev/null 2>&1 && "
                "ARCH=$(uname -m | sed 's/aarch64/arm64/' | sed 's/x86_64/amd64/') && "
                "VERSION=$(curl -sL https://cdn.rilldata.com/rill/latest.txt) && "
                "curl -sL https://cdn.rilldata.com/rill/${VERSION}/rill_linux_${ARCH}.zip -o /tmp/rill.zip && "
                "unzip -q /tmp/rill.zip -d /usr/local/bin && "
                "chmod +x /usr/local/bin/rill && "
                "rill start --no-open",
            ],
        )
    except KeyboardInterrupt:
        print("\n\nStopping Rill...")


if __name__ == "__main__":
    main()
