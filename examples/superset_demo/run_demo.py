#!/usr/bin/env python3
# /// script
# dependencies = ["sidemantic"]
# ///
"""Superset Demo: Export sidemantic models to Superset with DuckDB.

This demo:
1. Loads a sidemantic YAML definition
2. Exports to Superset dataset YAML format
3. Starts Superset via docker-compose with DuckDB
4. Creates a DuckDB database with sample data from parquet
5. Registers the database and dataset in Superset

Prerequisites:
- Docker installed and running

Usage:
    git clone https://github.com/sidequery/sidemantic && cd sidemantic
    uv run examples/superset_demo/run_demo.py
"""

import shutil
import subprocess
import sys
import time
from pathlib import Path

# Add project root to path for local development
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

PARQUET_URL = "https://storage.googleapis.com/rilldata-public/bids_data.parquet"


def wait_for_superset(max_wait: int = 300) -> bool:
    """Wait for Superset to be healthy."""
    import http.client
    import urllib.error
    import urllib.request

    start = time.time()
    while time.time() - start < max_wait:
        try:
            req = urllib.request.urlopen("http://localhost:8088/health", timeout=5)
            if req.status == 200:
                return True
        except (
            urllib.error.URLError,
            ConnectionRefusedError,
            http.client.RemoteDisconnected,
            OSError,
        ):
            pass
        time.sleep(5)
    return False


def main():
    demo_dir = Path(__file__).parent
    sidemantic_yaml = demo_dir / "sidemantic.yaml"
    superset_datasets_dir = demo_dir / "superset_datasets"

    print("=" * 60)
    print("  Sidemantic to Superset Demo")
    print("=" * 60)

    # Step 1: Load sidemantic YAML
    print("\n[1/7] Loading sidemantic.yaml...")
    from sidemantic.adapters.sidemantic import SidemanticAdapter

    adapter = SidemanticAdapter()
    graph = adapter.parse(sidemantic_yaml)
    print(f"      Loaded {len(graph.models)} model(s)")
    for model_name, model in graph.models.items():
        print(f"      - {model_name}: {len(model.dimensions)} dimensions, {len(model.metrics)} metrics")

    # Step 2: Export to Superset
    print("\n[2/7] Exporting to Superset dataset format...")
    from sidemantic.adapters.superset import SupersetAdapter

    # Clean previous output
    if superset_datasets_dir.exists():
        shutil.rmtree(superset_datasets_dir)
    superset_datasets_dir.mkdir()

    superset_adapter = SupersetAdapter()
    superset_adapter.export(graph, superset_datasets_dir)

    # List generated files
    print(f"      Generated Superset datasets at: {superset_datasets_dir}")
    for file in sorted(superset_datasets_dir.rglob("*")):
        if file.is_file():
            rel_path = file.relative_to(superset_datasets_dir)
            print(f"      - {rel_path}")

    # Step 3: Check Docker
    print("\n[3/7] Checking Docker...")
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

    # Step 4: Start Superset stack
    print("\n[4/7] Starting Superset with DuckDB...")
    print("      This may take a few minutes on first run (installing duckdb)...")
    result = subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=demo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"      ERROR: Failed to start docker-compose:\n{result.stderr}")
        sys.exit(1)
    print("      Services started")

    # Step 5: Wait for Superset to be ready
    print("\n[5/7] Waiting for Superset to be ready...")
    print("      (This can take 2-3 minutes while DuckDB installs)")
    if not wait_for_superset(max_wait=300):
        print("      ERROR: Superset did not become healthy in time.")
        print("      Check logs with: docker compose logs superset")
        sys.exit(1)
    print("      Superset is ready")

    # Step 6: Create DuckDB database and load data
    print("\n[6/7] Creating DuckDB database and loading data...")
    duckdb_script = f"""
import duckdb
conn = duckdb.connect('/app/superset_home/demo.duckdb')
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("CREATE TABLE IF NOT EXISTS bids AS SELECT * FROM '{PARQUET_URL}'")
print(f"Loaded {{conn.execute('SELECT COUNT(*) FROM bids').fetchone()[0]}} rows")
conn.close()
"""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "superset", "python", "-c", duckdb_script],
        cwd=demo_dir,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        print(f"      WARNING: Failed to create DuckDB database:\n{result.stderr}")
    else:
        print(f"      {result.stdout.strip()}")

    # Step 7: Register DuckDB database, dataset with metrics, and dashboard
    print("\n[7/7] Registering database, dataset, and dashboard...")

    # Run inside container where requests is available
    register_script = """
import requests
import json

session = requests.Session()
base_url = "http://127.0.0.1:8088"

# Login
login_resp = session.post(f"{base_url}/api/v1/security/login", json={
    "username": "admin",
    "password": "admin",
    "provider": "db"
})
if login_resp.status_code != 200:
    print(f"Login failed: {login_resp.text}")
    exit(1)

access_token = login_resp.json()["access_token"]
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Get CSRF token
csrf_resp = session.get(f"{base_url}/api/v1/security/csrf_token/", headers=headers)
csrf_token = csrf_resp.json().get("result")
if csrf_token:
    headers["X-CSRFToken"] = csrf_token

# Create DuckDB database connection
db_payload = {
    "database_name": "DuckDB Demo",
    "sqlalchemy_uri": "duckdb:////app/superset_home/demo.duckdb",
    "expose_in_sqllab": True,
    "allow_ctas": True,
    "allow_cvas": True,
    "allow_dml": True
}
db_resp = session.post(f"{base_url}/api/v1/database/", headers=headers, json=db_payload)
if db_resp.status_code == 201:
    db_id = db_resp.json()["id"]
    print(f"Created database with id={db_id}")
elif "already exists" in db_resp.text.lower():
    dbs = session.get(f"{base_url}/api/v1/database/", headers=headers).json()
    db_id = next((d["id"] for d in dbs.get("result", []) if d["database_name"] == "DuckDB Demo"), None)
    print(f"Database already exists with id={db_id}")
else:
    print(f"Failed to create database: {db_resp.text}")
    exit(1)

# Create dataset for the bids table
dataset_payload = {
    "database": db_id,
    "table_name": "bids",
    "schema": "main"
}
ds_resp = session.post(f"{base_url}/api/v1/dataset/", headers=headers, json=dataset_payload)
if ds_resp.status_code == 201:
    ds_id = ds_resp.json()["id"]
    print(f"Created dataset with id={ds_id}")
elif "already exists" in ds_resp.text.lower():
    # Get existing dataset
    datasets = session.get(f"{base_url}/api/v1/dataset/", headers=headers).json()
    ds_id = next((d["id"] for d in datasets.get("result", []) if d["table_name"] == "bids"), None)
    print(f"Dataset already exists with id={ds_id}")
else:
    print(f"Failed to create dataset: {ds_resp.text}")
    exit(1)

# Define metrics from sidemantic model
metrics = [
    {"metric_name": "ad_spend", "verbose_name": "Ad Spend ($)", "expression": "SUM(media_spend_usd)/1000"},
    {"metric_name": "bids", "verbose_name": "Bids", "expression": "SUM(bid_cnt)"},
    {"metric_name": "impressions", "verbose_name": "Impressions", "expression": "SUM(imp_cnt)"},
    {"metric_name": "win_rate", "verbose_name": "Win Rate", "expression": "SUM(imp_cnt)*1.0/SUM(bid_cnt)"},
    {"metric_name": "clicks", "verbose_name": "Clicks", "expression": "SUM(click_reg_cnt)"},
    {"metric_name": "ctr", "verbose_name": "CTR", "expression": "SUM(click_reg_cnt)*1.0/SUM(imp_cnt)"},
    {"metric_name": "avg_bid_price", "verbose_name": "Avg Bid Price ($)", "expression": "SUM(bid_price_usd)*1.0/SUM(bid_cnt)/1000"},
    {"metric_name": "ecpm", "verbose_name": "eCPM ($)", "expression": "SUM(media_spend_usd)*1.0/1000/SUM(imp_cnt)"},
    {"metric_name": "video_starts", "verbose_name": "Video Starts", "expression": "SUM(video_start_cnt)"},
    {"metric_name": "video_completes", "verbose_name": "Video Completes", "expression": "SUM(video_complete_cnt)"},
    {"metric_name": "video_completion_rate", "verbose_name": "Video Completion Rate", "expression": "SUM(video_complete_cnt)*1.0/SUM(video_start_cnt)"},
]

# Update dataset with metrics
update_payload = {"metrics": metrics}
update_resp = session.put(f"{base_url}/api/v1/dataset/{ds_id}", headers=headers, json=update_payload)
if update_resp.status_code == 200:
    print(f"Added {len(metrics)} metrics to dataset")
else:
    print(f"Failed to add metrics: {update_resp.text[:200]}")

# Create charts
chart_ids = []

# Chart 1: Ad Spend over time
chart1 = {
    "slice_name": "Ad Spend Over Time",
    "viz_type": "echarts_timeseries_line",
    "datasource_id": ds_id,
    "datasource_type": "table",
    "params": json.dumps({
        "datasource": f"{ds_id}__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": "__time",
        "time_grain_sqla": "P1D",
        "metrics": ["ad_spend"],
        "groupby": [],
        "row_limit": 10000,
    })
}
resp = session.post(f"{base_url}/api/v1/chart/", headers=headers, json=chart1)
if resp.status_code == 201:
    chart_ids.append(resp.json()["id"])
    print(f"Created chart: Ad Spend Over Time")

# Chart 2: Impressions by Device OS
chart2 = {
    "slice_name": "Impressions by Device OS",
    "viz_type": "pie",
    "datasource_id": ds_id,
    "datasource_type": "table",
    "params": json.dumps({
        "datasource": f"{ds_id}__table",
        "viz_type": "pie",
        "metric": "impressions",
        "groupby": ["device_os"],
        "row_limit": 10,
    })
}
resp = session.post(f"{base_url}/api/v1/chart/", headers=headers, json=chart2)
if resp.status_code == 201:
    chart_ids.append(resp.json()["id"])
    print(f"Created chart: Impressions by Device OS")

# Chart 3: Win Rate by Advertiser
chart3 = {
    "slice_name": "Win Rate by Advertiser",
    "viz_type": "echarts_timeseries_bar",
    "datasource_id": ds_id,
    "datasource_type": "table",
    "params": json.dumps({
        "datasource": f"{ds_id}__table",
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "advertiser_name",
        "metrics": ["win_rate"],
        "groupby": [],
        "row_limit": 20,
        "order_desc": True,
    })
}
resp = session.post(f"{base_url}/api/v1/chart/", headers=headers, json=chart3)
if resp.status_code == 201:
    chart_ids.append(resp.json()["id"])
    print(f"Created chart: Win Rate by Advertiser")

# Chart 4: CTR by Campaign
chart4 = {
    "slice_name": "CTR by Campaign",
    "viz_type": "echarts_timeseries_bar",
    "datasource_id": ds_id,
    "datasource_type": "table",
    "params": json.dumps({
        "datasource": f"{ds_id}__table",
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "campaign_name",
        "metrics": ["ctr"],
        "groupby": [],
        "row_limit": 15,
        "order_desc": True,
    })
}
resp = session.post(f"{base_url}/api/v1/chart/", headers=headers, json=chart4)
if resp.status_code == 201:
    chart_ids.append(resp.json()["id"])
    print(f"Created chart: CTR by Campaign")

# Chart 5: Video Completion Rate by Creative Type
chart5 = {
    "slice_name": "Video Completion Rate by Creative",
    "viz_type": "echarts_timeseries_bar",
    "datasource_id": ds_id,
    "datasource_type": "table",
    "params": json.dumps({
        "datasource": f"{ds_id}__table",
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "creative_type",
        "metrics": ["video_completion_rate"],
        "groupby": [],
        "row_limit": 10,
        "order_desc": True,
    })
}
resp = session.post(f"{base_url}/api/v1/chart/", headers=headers, json=chart5)
if resp.status_code == 201:
    chart_ids.append(resp.json()["id"])
    print(f"Created chart: Video Completion Rate by Creative")

# Chart 6: Big Number - Total Ad Spend
chart6 = {
    "slice_name": "Total Ad Spend",
    "viz_type": "big_number_total",
    "datasource_id": ds_id,
    "datasource_type": "table",
    "params": json.dumps({
        "datasource": f"{ds_id}__table",
        "viz_type": "big_number_total",
        "metric": "ad_spend",
    })
}
resp = session.post(f"{base_url}/api/v1/chart/", headers=headers, json=chart6)
if resp.status_code == 201:
    chart_ids.append(resp.json()["id"])
    print(f"Created chart: Total Ad Spend")

# Create dashboard with all charts
if chart_ids:
    # Build position JSON for dashboard layout
    positions = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": "Adtech Bids Dashboard"}}
    }

    # Add charts to grid
    row_id = 0
    for i, chart_id in enumerate(chart_ids):
        chart_key = f"CHART-{chart_id}"
        row_key = f"ROW-{row_id}"

        # Create row if needed (2 charts per row)
        if i % 2 == 0:
            positions[row_key] = {
                "type": "ROW",
                "id": row_key,
                "children": [],
                "parents": ["GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            }
            positions["GRID_ID"]["children"].append(row_key)

        # Add chart to row
        positions[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "children": [],
            "parents": [row_key],
            "meta": {
                "width": 6,
                "height": 50,
                "chartId": chart_id
            }
        }
        positions[row_key]["children"].append(chart_key)

        if i % 2 == 1:
            row_id += 1

    if len(chart_ids) % 2 == 1:
        row_id += 1

    # Create dashboard with json_metadata that includes native filter config
    json_metadata = {
        "native_filter_configuration": [],
        "chart_configuration": {},
        "color_scheme": "",
        "refresh_frequency": 0,
        "shared_label_colors": {},
        "color_scheme_domain": [],
        "expanded_slices": {},
        "label_colors": {},
        "timed_refresh_immune_slices": [],
        "default_filters": "{}",
        "filter_scopes": {}
    }

    dashboard_payload = {
        "dashboard_title": "Adtech Bids Dashboard",
        "slug": "adtech-bids",
        "position_json": json.dumps(positions),
        "json_metadata": json.dumps(json_metadata),
        "published": True
    }
    dash_resp = session.post(f"{base_url}/api/v1/dashboard/", headers=headers, json=dashboard_payload)
    if dash_resp.status_code == 201:
        dash_id = dash_resp.json()["id"]
        print(f"Created dashboard: Adtech Bids Dashboard")

        # Now update dashboard to add the charts as slices
        # Use embedded dashboard endpoint to add charts
        update_payload = {
            "json_metadata": json.dumps({
                **json_metadata,
                "positions": positions
            })
        }
        session.put(f"{base_url}/api/v1/dashboard/{dash_id}", headers=headers, json=update_payload)
    else:
        print(f"Dashboard creation: {dash_resp.text[:200]}")
"""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "superset", "python", "-c", register_script],
        cwd=demo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"      WARNING: Registration issue:\n{result.stderr}")
    else:
        for line in result.stdout.strip().split("\n"):
            print(f"      {line}")

    print()
    print("=" * 60)
    print("  Superset is running with DuckDB!")
    print()
    print("  URL:       http://localhost:8088")
    print("  Dashboard: http://localhost:8088/superset/dashboard/adtech-bids/")
    print("  Username:  admin")
    print("  Password:  admin")
    print()
    print("  The 'bids' dataset has 11 metrics ready to explore.")
    print()
    print("  To stop:  docker compose down")
    print("  To stop and remove data:  docker compose down -v")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
