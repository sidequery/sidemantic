import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import os as _os
    import shutil
    import subprocess
    import sys
    from pathlib import Path

    def _run(cmd: str) -> None:
        print(f">> {cmd}")
        subprocess.check_call(cmd, shell=True)

    is_colab = "COLAB_RELEASE_TAG" in _os.environ or "google.colab" in sys.modules
    repo_root = Path.cwd()
    is_repo = (repo_root / "pyproject.toml").exists() and (repo_root / "sidemantic").exists()

    if is_repo:
        if shutil.which("uv"):
            _run('uv pip install -e ".[widget]"')
        else:
            _run('pip install -e ".[widget]"')
    else:
        if shutil.which("uv"):
            _run('uv pip install "sidemantic[widget]"')
        else:
            _run('pip install "sidemantic[widget]"')

    if is_colab:
        from google.colab import output

        output.enable_custom_widget_manager()
    return


@app.cell
def _():
    import marimo

    return marimo


@app.cell
def _(marimo):
    marimo.md(
        """
# Sidemantic Widget Demo

- Auctions example with **auto pre-aggregations**
- Foursquare Places example using **remote pre-agg DuckDB**
"""
    )
    return


@app.cell
def _(marimo):
    marimo.md("## Auctions: auto pre-aggregations")
    return


@app.cell
def _():
    from pathlib import Path as _Path

    db_path = _Path("examples/widget_demo.db")
    return db_path


@app.cell
def _(db_path):
    import duckdb as _duckdb

    # Load real auction data (~1.4M rows)
    conn = _duckdb.connect(str(db_path))
    conn.execute("install httpfs")
    conn.execute("load httpfs")
    _tables = {
        row[0] for row in conn.execute("select table_name from duckdb_tables() where schema_name = 'main'").fetchall()
    }
    if "auctions" not in _tables:
        conn.execute(
            """
        create table auctions as
        select *
        from 'https://sampledata.sidequery.dev/sidemantic-demo/auction_data.parquet'
        --cross join range(2)
        """
        )

    return conn


@app.cell
def _(
    db_path,
):
    from sidemantic import Dimension, Metric, Model, SemanticLayer
    from sidemantic.db.duckdb import DuckDBAdapter
    from sidemantic.widget import MetricsExplorer

    # Semantic model for auction analytics
    auction_model = Model(
        name="auctions",
        table="auctions_with_id",
        primary_key="id",
        default_time_dimension="__time",
        dimensions=[
            Dimension(name="__time", type="time", granularity="day"),
            Dimension(name="device_type", type="categorical"),
            Dimension(name="device_os", type="categorical"),
            Dimension(name="app_or_site", type="categorical"),
            Dimension(name="auction_type", type="categorical"),
            Dimension(name="bid_floor_bucket", type="categorical"),
            Dimension(name="ad_position", type="categorical"),
            Dimension(name="platform_browser", type="categorical"),
            Dimension(name="device_region", type="categorical"),
            Dimension(name="app_site_cat", type="categorical"),
        ],
        metrics=[
            Metric(name="row_count", agg="count"),
            Metric(name="bid_requests", agg="sum", sql="bid_request_cnt"),
            Metric(name="bid_floor_requests", agg="sum", sql="has_bid_floor_cnt"),
            Metric(name="avg_bid_floor", agg="avg", sql="bid_floor"),
            Metric(name="max_bid_floor", agg="max", sql="bid_floor"),
        ],
    )

    adapter = DuckDBAdapter(str(db_path))
    _tables = {
        row[0]
        for row in adapter.conn.execute("select table_name from duckdb_tables() where schema_name = 'main'").fetchall()
    }
    if "auctions_with_id" not in _tables:
        adapter.conn.execute(
            """
            create table auctions_with_id as
            select row_number() over () as id, *
            from auctions
        """
        )

    layer = SemanticLayer(connection=adapter, use_preaggregations=True)
    layer.add_model(auction_model)

    widget = MetricsExplorer(
        layer,
        metrics=[
            "auctions.row_count",
            "auctions.bid_requests",
            "auctions.bid_floor_requests",
            "auctions.avg_bid_floor",
            "auctions.max_bid_floor",
        ],
        dimensions=[
            "auctions.device_type",
            "auctions.device_os",
            "auctions.app_or_site",
            "auctions.auction_type",
            "auctions.bid_floor_bucket",
            "auctions.ad_position",
            "auctions.platform_browser",
        ],
        auto_preaggregations=True,
    )
    widget
    return adapter, auction_model, layer, widget


@app.cell
def _(marimo):
    marimo.md("## Foursquare Places: remote pre-agg DuckDB")
    return


@app.cell
def _():
    fsq_preagg_url = "https://sampledata.sidequery.dev/sidemantic/foursquare_preagg.db"
    return fsq_preagg_url


@app.cell
def _(fsq_preagg_url):
    from pathlib import Path as _Path

    target_path = _Path("examples/foursquare_preagg.db")
    return target_path


@app.cell
def _(marimo, target_path):
    from datetime import datetime as _dt

    if target_path.exists():
        _size_mb = target_path.stat().st_size / 1_000_000
        _updated = _dt.fromtimestamp(target_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        status = marimo.md(f"Pre-agg DB status: local file found ({_size_mb:.1f} MB, mtime {_updated})")
    else:
        status = marimo.md("Pre-agg DB status: local file not found")
    status
    return


@app.cell
def _(marimo, target_path):
    if target_path.exists():
        label = "Re-download Foursquare pre-agg DB (overwrite)"
    else:
        label = "Download Foursquare pre-agg DB"

    download = marimo.ui.run_button(label=label)
    download
    return download


@app.cell
def _(download, fsq_preagg_url, marimo, target_path):
    if download.value:
        import urllib.request

        target_path.parent.mkdir(parents=True, exist_ok=True)

        _total = None
        try:
            req = urllib.request.Request(fsq_preagg_url, method="HEAD")
            with urllib.request.urlopen(req) as resp:
                length = resp.headers.get("Content-Length")
                if length and length.isdigit():
                    _total = int(length)
        except Exception:
            _total = None

        with urllib.request.urlopen(fsq_preagg_url) as resp:
            if _total:
                with (
                    marimo.status.progress_bar(total=_total, title="Downloading pre-agg DB") as _bar,
                    open(target_path, "wb") as f,
                ):
                    while True:
                        chunk = resp.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        _bar.update(increment=len(chunk))
            else:
                with marimo.status.progress_bar(title="Downloading pre-agg DB") as _bar, open(target_path, "wb") as f:
                    while True:
                        chunk = resp.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        _bar.update()

        _size_mb = target_path.stat().st_size / 1_000_000
        marimo.md(f"Downloaded to `{target_path}` ({_size_mb:.1f} MB)")
    return


@app.cell
def _(download, fsq_preagg_url, marimo, target_path):
    if target_path.exists():
        marimo.md(f"Using local pre-agg DB: `{target_path}`")
        fsq_source = str(target_path)
    else:
        marimo.md(f"Using remote pre-agg DB: `{fsq_preagg_url}`")
        fsq_source = fsq_preagg_url
    return fsq_source


@app.cell
def _(fsq_source):
    from sidemantic.db.duckdb import DuckDBAdapter

    adapter_fsq_preagg = DuckDBAdapter(":memory:")
    adapter_fsq_preagg.conn.execute("install httpfs")
    adapter_fsq_preagg.conn.execute("load httpfs")
    adapter_fsq_preagg.conn.execute(f"attach '{fsq_source}' as fsq (read_only)")
    marimo.output.clear()
    return adapter_fsq_preagg


@app.cell
def _(
    adapter_fsq_preagg,
):
    from sidemantic import Dimension, Metric, Model, PreAggregation, SemanticLayer
    from sidemantic.widget import MetricsExplorer

    places_model_preagg = Model(
        name="places",
        table="places",
        primary_key="rowid",
        default_time_dimension="date_created",
        dimensions=[
            Dimension(name="date_created", type="time", granularity="day"),
            Dimension(name="country", type="categorical"),
            Dimension(name="region", type="categorical"),
            Dimension(name="admin_region", type="categorical"),
            Dimension(name="category_l1", type="categorical"),
            Dimension(name="category_l2", type="categorical"),
            Dimension(name="category_l3", type="categorical"),
        ],
        metrics=[
            Metric(name="row_count", agg="count"),
            Metric(name="sum_latitude", agg="sum", sql="latitude"),
            Metric(name="sum_longitude", agg="sum", sql="longitude"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_metrics",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
            ),
            PreAggregation(
                name="daily_all_dims",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=[
                    "country",
                    "region",
                    "admin_region",
                    "category_l1",
                    "category_l2",
                    "category_l3",
                ],
            ),
            PreAggregation(
                name="by_country",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["country"],
            ),
            PreAggregation(
                name="by_region",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["region"],
            ),
            PreAggregation(
                name="by_admin_region",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["admin_region"],
            ),
            PreAggregation(
                name="by_category_l1",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l1"],
            ),
            PreAggregation(
                name="by_category_l2",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l2"],
            ),
            PreAggregation(
                name="by_category_l3",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l3"],
            ),
        ],
    )

    layer_fsq_preagg = SemanticLayer(
        connection=adapter_fsq_preagg,
        use_preaggregations=True,
        preagg_database="fsq",
        preagg_schema="main",
    )
    layer_fsq_preagg.add_model(places_model_preagg)

    widget_fsq = MetricsExplorer(
        layer_fsq_preagg,
        metrics=[
            "places.row_count",
            "places.sum_latitude",
            "places.sum_longitude",
        ],
        dimensions=[
            "places.country",
            "places.region",
            "places.admin_region",
            "places.category_l1",
            "places.category_l2",
            "places.category_l3",
        ],
    )
    widget_fsq
    return layer_fsq_preagg, places_model_preagg, widget_fsq


if __name__ == "__main__":
    app.run()
