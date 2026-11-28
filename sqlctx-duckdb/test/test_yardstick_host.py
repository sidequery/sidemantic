"""Run both extensions' measure-aware views in one dynamically loaded host."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("first", ["sqlctx", "yardstick"])
def test_measure_views_with_yardstick(tmp_path, first):
    if not os.environ.get("YARDSTICK_EXTENSION"):
        pytest.skip("Set YARDSTICK_EXTENSION to a Yardstick binary built for this host")
    paths = {
        "yardstick": str(Path(os.environ["YARDSTICK_EXTENSION"]).resolve()),
        "sqlctx": str(Path(os.environ["SQLCTX_DUCKDB_EXTENSION"]).resolve()),
    }
    second = "sqlctx" if first == "yardstick" else "yardstick"
    # The build's CLI can statically preload SQLCtx. Use the matching Python
    # host in an isolated process so these are actual, independent load orders.
    script = r'''
import duckdb
import json
import sys

con = duckdb.connect(config={"allow_unsigned_extensions": True})
for path in json.loads(sys.argv[1]):
    con.execute("load '" + path.replace("'", "''") + "'")
if duckdb.__version__.startswith("2."):
    con.execute("set active_grammar_extensions=['sqlctx','yardstick']")
con.execute("create table facts(id int, region varchar, amount int)")
con.execute("insert into facts values (1,'west',10), (2,'west',30), (3,'east',80)")
con.execute("create view yard_sales as select region, sum(amount) as measure revenue from facts")
con.execute("""
    model sales from facts (
        primary key (id)
        region
        sum(amount) as revenue
    )
""")
con.execute("create semantic view report as select region, revenue from sales")
queries = [
    "select region, aggregate(revenue) as total from yard_sales order by region",
    "select region, revenue from report order by region",
    "select revenue from report",
    "select 'AS MEASURE AGGREGATE(revenue)' as words",
    "select revenue from report -- AS MEASURE AGGREGATE(revenue)\n",
    "select revenue /* AS MEASURE AGGREGATE(revenue) */ from report",
    'select revenue as "AS MEASURE" from report',
    "select aggregate([1,2,3], 'sum')",
    "select aggregate(list(amount), 'sum') from facts",
    'select "aggregate"([1,2,3], \'sum\')',
    "semantic select sales.revenue from sales",
]
print(json.dumps([con.execute(query).fetchall() for query in queries]))
'''
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps([paths[first], paths[second]])],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [
        [["east", 80], ["west", 40]],
        [["east", 80], ["west", 40]],
        [[120]],
        [["AS MEASURE AGGREGATE(revenue)"]],
        [[120]],
        [[120]],
        [[120]],
        [[6]],
        [[120]],
        [[6]],
        [[120]],
    ]
