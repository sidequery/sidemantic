import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
    # Sidemantic Widget Demo

    - Auctions example with **auto pre-aggregations**
    - Optional Foursquare Places example using a **remote pre-aggregated DuckDB file** via DuckDB `httpfs` + `attach`
    """
    )
    return


@app.cell
def _():
    import urllib.request as urllib_request
    from datetime import datetime
    from pathlib import Path
    from urllib.error import HTTPError, URLError

    from sidemantic import Dimension, Metric, Model, PreAggregation, SemanticLayer
    from sidemantic.db.duckdb import DuckDBAdapter
    from sidemantic.widget import MetricsExplorer

    examples_dir = Path(__file__).resolve().parent
    datetime_cls = datetime
    http_error_cls = HTTPError
    url_error_cls = URLError
    dimension_cls = Dimension
    metric_cls = Metric
    model_cls = Model
    preaggregation_cls = PreAggregation
    semantic_layer_cls = SemanticLayer
    duckdb_adapter_cls = DuckDBAdapter
    metrics_explorer_cls = MetricsExplorer
    return (
        datetime_cls,
        dimension_cls,
        duckdb_adapter_cls,
        examples_dir,
        http_error_cls,
        metric_cls,
        metrics_explorer_cls,
        model_cls,
        preaggregation_cls,
        semantic_layer_cls,
        url_error_cls,
        urllib_request,
    )


@app.cell
def _(mo):
    mo.md("""## Auctions: auto pre-aggregations""")
    return


@app.cell
def _(duckdb_adapter_cls):
    auction_parquet_url = "https://sampledata.sidequery.dev/sidemantic-demo/auction_data.parquet"

    adapter_auctions = duckdb_adapter_cls(":memory:")
    try:
        adapter_auctions.conn.execute("load httpfs")
    except Exception:
        adapter_auctions.conn.execute("install httpfs")
        adapter_auctions.conn.execute("load httpfs")

    adapter_auctions.conn.execute(
        f"create or replace view auctions as select * from read_parquet('{auction_parquet_url}')"
    )
    adapter_auctions.conn.execute(
        """
        create or replace view auctions_with_id as
        select row_number() over () as id, *
        from auctions
        """
    )
    return adapter_auctions, auction_parquet_url


@app.cell
def _(auction_parquet_url, mo):
    mo.md(f"""Auction data source: `{auction_parquet_url}`""")
    return


@app.cell
def _(
    adapter_auctions,
    dimension_cls,
    metric_cls,
    metrics_explorer_cls,
    model_cls,
    semantic_layer_cls,
):
    auction_model = model_cls(
        name="auctions",
        table="auctions_with_id",
        primary_key="id",
        default_time_dimension="__time",
        dimensions=[
            dimension_cls(name="__time", type="time", granularity="day"),
            dimension_cls(name="device_type", type="categorical"),
            dimension_cls(name="device_os", type="categorical"),
            dimension_cls(name="app_or_site", type="categorical"),
            dimension_cls(name="auction_type", type="categorical"),
            dimension_cls(name="bid_floor_bucket", type="categorical"),
            dimension_cls(name="ad_position", type="categorical"),
            dimension_cls(name="platform_browser", type="categorical"),
            dimension_cls(name="device_region", type="categorical"),
            dimension_cls(name="app_site_cat", type="categorical"),
        ],
        metrics=[
            metric_cls(name="row_count", agg="count"),
            metric_cls(name="bid_requests", agg="sum", sql="bid_request_cnt"),
            metric_cls(name="bid_floor_requests", agg="sum", sql="has_bid_floor_cnt"),
            metric_cls(name="avg_bid_floor", agg="avg", sql="bid_floor"),
            metric_cls(name="max_bid_floor", agg="max", sql="bid_floor"),
        ],
    )

    layer = semantic_layer_cls(connection=adapter_auctions, use_preaggregations=True)
    layer.add_model(auction_model)

    widget = metrics_explorer_cls(
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
    return


@app.cell
def _(mo):
    show_fsq = mo.ui.checkbox(
        label="Enable Foursquare demo",
        value=False,
    )
    show_fsq
    return (show_fsq,)


@app.cell
def _(mo, show_fsq):
    fsq_ok = show_fsq.value
    mo.stop(
        not fsq_ok,
        mo.md("Foursquare demo is disabled."),
    )
    mo.md(
        """
## Foursquare Places: remote pre-agg DuckDB

Warning: the Foursquare pre-agg DuckDB is large (multi‑GB) and is not required for the widget demo below.

This section demonstrates using an existing pre-aggregated database over HTTP. It does **not** demonstrate
`auto_preaggregations=True` generating pre-aggregations for you.
"""
    )
    return (fsq_ok,)


@app.cell
def _(fsq_ok, mo):
    mo.stop(not fsq_ok)
    fsq_preagg_url = "https://sampledata.sidequery.dev/sidemantic/foursquare_preagg.db"
    return (fsq_preagg_url,)


@app.cell
def _(examples_dir, fsq_ok, mo):
    mo.stop(not fsq_ok)
    target_path = examples_dir / "foursquare_preagg.db"
    return (target_path,)


@app.cell
def _(
    fsq_ok,
    fsq_preagg_url,
    http_error_cls,
    mo,
    url_error_cls,
    urllib_request,
):
    mo.stop(not fsq_ok)
    size_mb = None
    try:
        _req = urllib_request.Request(fsq_preagg_url, method="HEAD")
        with urllib_request.urlopen(_req) as _resp:
            _length = _resp.headers.get("Content-Length")
            if _length and _length.isdigit():
                size_mb = int(_length) / 1_000_000
    except (http_error_cls, url_error_cls, Exception):
        size_mb = None

    if size_mb is None:
        mo.md("Remote pre-agg DB size: unknown (server did not provide Content-Length)")
    else:
        mo.md(f"Remote pre-agg DB size: ~{size_mb:,.0f} MB")
    return


@app.cell
def _(datetime_cls, fsq_ok, mo, target_path):
    mo.stop(not fsq_ok)
    if target_path.exists():
        _size_mb = target_path.stat().st_size / 1_000_000
        _updated = datetime_cls.fromtimestamp(target_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        mo.md(f"Pre-agg DB status: local file found ({_size_mb:.1f} MB, mtime {_updated})")
    else:
        mo.md("Pre-agg DB status: local file not found")
    return


@app.cell
def _(fsq_ok, mo, target_path):
    mo.stop(not fsq_ok)
    label = (
        "Re-download Foursquare pre-agg DB (overwrite)" if target_path.exists() else "Download Foursquare pre-agg DB"
    )
    download = mo.ui.run_button(label=label)
    download
    return (download,)


@app.cell
def _(
    datetime_cls,
    download,
    fsq_preagg_url,
    fsq_ok,
    http_error_cls,
    mo,
    target_path,
    url_error_cls,
    urllib_request,
):
    mo.stop(not fsq_ok)
    if download.value:
        target_path.parent.mkdir(parents=True, exist_ok=True)

        total = None
        try:
            _req = urllib_request.Request(fsq_preagg_url, method="HEAD")
            with urllib_request.urlopen(_req) as _resp:
                _length = _resp.headers.get("Content-Length")
                if _length and _length.isdigit():
                    total = int(_length)
        except (http_error_cls, url_error_cls, Exception):
            total = None

        with urllib_request.urlopen(fsq_preagg_url) as _resp:
            if total:
                with (
                    mo.status.progress_bar(total=total, title="Downloading pre-agg DB") as bar,
                    open(target_path, "wb") as f,
                ):
                    while True:
                        chunk = _resp.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        bar.update(increment=len(chunk))
            else:
                with mo.status.progress_bar(title="Downloading pre-agg DB") as bar, open(target_path, "wb") as f:
                    while True:
                        chunk = _resp.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        bar.update()

        _size_mb = target_path.stat().st_size / 1_000_000
        _updated = datetime_cls.fromtimestamp(target_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        mo.md(f"Downloaded to `{target_path}` ({_size_mb:.1f} MB, mtime {_updated})")
    return


@app.cell
def _(fsq_ok, fsq_preagg_url, mo, target_path):
    mo.stop(not fsq_ok)
    fsq_source = None
    if target_path.exists():
        mo.md(f"Using local pre-agg DB: `{target_path}`")
        fsq_source = str(target_path)
    else:
        mo.md(f"Using remote pre-agg DB: `{fsq_preagg_url}`")
        fsq_source = fsq_preagg_url
    return (fsq_source,)


@app.cell
def _(duckdb_adapter_cls, fsq_ok, fsq_source, mo):
    mo.stop(not fsq_ok)
    adapter_fsq_preagg = duckdb_adapter_cls(":memory:")
    try:
        adapter_fsq_preagg.conn.execute("load httpfs")
    except Exception:
        adapter_fsq_preagg.conn.execute("install httpfs")
        adapter_fsq_preagg.conn.execute("load httpfs")
    adapter_fsq_preagg.conn.execute(f"attach '{fsq_source}' as fsq (read_only)")
    mo.output.clear()
    return (adapter_fsq_preagg,)


@app.cell
def _(
    adapter_fsq_preagg,
    dimension_cls,
    fsq_ok,
    metric_cls,
    metrics_explorer_cls,
    model_cls,
    mo,
    preaggregation_cls,
    semantic_layer_cls,
):
    mo.stop(not fsq_ok)
    places_model_preagg = model_cls(
        name="places",
        table="places",
        primary_key="rowid",
        default_time_dimension="date_created",
        dimensions=[
            dimension_cls(name="date_created", type="time", granularity="day"),
            dimension_cls(name="country", type="categorical"),
            dimension_cls(name="region", type="categorical"),
            dimension_cls(name="admin_region", type="categorical"),
            dimension_cls(name="category_l1", type="categorical"),
            dimension_cls(name="category_l2", type="categorical"),
            dimension_cls(name="category_l3", type="categorical"),
        ],
        metrics=[
            metric_cls(name="row_count", agg="count"),
            metric_cls(name="sum_latitude", agg="sum", sql="latitude"),
            metric_cls(name="sum_longitude", agg="sum", sql="longitude"),
        ],
        pre_aggregations=[
            preaggregation_cls(
                name="daily_metrics",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
            ),
            preaggregation_cls(
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
            preaggregation_cls(
                name="by_country",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["country"],
            ),
            preaggregation_cls(
                name="by_region",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["region"],
            ),
            preaggregation_cls(
                name="by_admin_region",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["admin_region"],
            ),
            preaggregation_cls(
                name="by_category_l1",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l1"],
            ),
            preaggregation_cls(
                name="by_category_l2",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l2"],
            ),
            preaggregation_cls(
                name="by_category_l3",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l3"],
            ),
        ],
    )

    layer_fsq_preagg = semantic_layer_cls(
        connection=adapter_fsq_preagg,
        use_preaggregations=True,
        preagg_database="fsq",
        preagg_schema="main",
    )
    layer_fsq_preagg.add_model(places_model_preagg)

    widget_fsq = metrics_explorer_cls(
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
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
