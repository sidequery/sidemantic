"""Tests for adapter integration across CLI, workbench, and server components.

These tests verify that internal tools (CLI, workbench, server) go through
the adapter interface rather than using raw connections directly.
"""

from unittest.mock import MagicMock

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


class TestSemanticLayerAdapterInterface:
    """Tests for SemanticLayer using adapter interface."""

    def test_layer_adapter_execute(self):
        """Test that SemanticLayer.adapter.execute() works correctly."""
        layer = SemanticLayer(connection="duckdb:///:memory:")
        layer.add_model(
            Model(
                name="test",
                table="test_table",
                dimensions=[Dimension(name="id", sql="id", type="categorical")],
                metrics=[Metric(name="count", agg="count")],
            )
        )

        # Create table using adapter
        layer.adapter.execute("CREATE TABLE test_table (id INTEGER)")
        layer.adapter.execute("INSERT INTO test_table VALUES (1), (2), (3)")

        # Verify data was inserted
        result = layer.adapter.execute("SELECT COUNT(*) FROM test_table")
        count = result.fetchone()[0]
        assert count == 3

    def test_layer_adapter_executemany(self):
        """Test that SemanticLayer.adapter.executemany() works correctly."""
        layer = SemanticLayer(connection="duckdb:///:memory:")

        # Create table
        layer.adapter.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")

        # Insert using executemany
        layer.adapter.executemany(
            "INSERT INTO test_table VALUES (?, ?)",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        )

        # Verify data was inserted
        result = layer.adapter.execute("SELECT COUNT(*) FROM test_table")
        count = result.fetchone()[0]
        assert count == 3

    def test_layer_adapter_fetch_record_batch(self):
        """Test that adapter results support fetch_record_batch()."""
        pytest.importorskip("pyarrow")
        layer = SemanticLayer(connection="duckdb:///:memory:")

        layer.adapter.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
        layer.adapter.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")

        result = layer.adapter.execute("SELECT * FROM test_table")
        reader = result.fetch_record_batch()

        # Read all batches
        table = reader.read_all()
        assert table.num_rows == 2
        assert "id" in table.column_names
        assert "name" in table.column_names


class TestServerConnectionAdapterIntegration:
    """Tests for server connection handler using adapter."""

    def test_connection_handler_uses_adapter_for_dml(self):
        """Test that SemanticLayerConnection uses layer.adapter.execute for DML."""
        pytest.importorskip("riffq")
        pytest.importorskip("pyarrow")

        from sidemantic.server.connection import SemanticLayerConnection

        adapter_calls = []

        # Create a mock layer with tracking adapter
        class MockResult:
            def fetch_record_batch(self):
                import pyarrow as pa

                return pa.RecordBatchReader.from_batches(pa.schema([("ok", pa.int64())]), [])

        class MockAdapter:
            dialect = "duckdb"

            def execute(self, sql):
                adapter_calls.append(sql)
                return MockResult()

        class MockGraph:
            models = {}
            metrics = []

        class MockLayer:
            adapter = MockAdapter()
            graph = MockGraph()
            dialect = "duckdb"

        # Create connection handler
        conn = SemanticLayerConnection(
            connection_id=1,
            executor=MagicMock(),
            layer=MockLayer(),
        )

        # Mock send_reader
        conn.send_reader = lambda reader, callback: callback(True)

        # Test _handle_query directly for SET command
        conn._handle_query("SET client_encoding = 'UTF8'", lambda x: None)

        # Should have called adapter.execute
        assert len(adapter_calls) > 0
        assert "SELECT 1" in adapter_calls[0]

    def test_connection_handler_uses_adapter_for_system_queries(self):
        """Test that system query handlers use layer.adapter.execute."""
        pytest.importorskip("riffq")
        pytest.importorskip("pyarrow")

        from sidemantic.server.connection import SemanticLayerConnection

        adapter_calls = []

        class MockResult:
            def fetch_record_batch(self):
                import pyarrow as pa

                return pa.RecordBatchReader.from_batches(pa.schema([("word", pa.string())]), [])

        class MockAdapter:
            dialect = "duckdb"

            def execute(self, sql):
                adapter_calls.append(sql)
                return MockResult()

        class MockGraph:
            models = {}
            metrics = []

        class MockLayer:
            adapter = MockAdapter()
            graph = MockGraph()
            dialect = "duckdb"

        conn = SemanticLayerConnection(
            connection_id=1,
            executor=MagicMock(),
            layer=MockLayer(),
        )

        conn.send_reader = lambda reader, callback: callback(True)

        # Test pg_get_keywords system query
        conn._handle_query("SELECT * FROM pg_get_keywords()", lambda x: None)

        # Should have called adapter.execute with duckdb_keywords
        assert any("duckdb_keywords" in sql for sql in adapter_calls)

    def test_connection_handler_uses_adapter_for_pg_namespace(self):
        """Test that pg_namespace queries use layer.adapter.execute."""
        pytest.importorskip("riffq")
        pytest.importorskip("pyarrow")

        from sidemantic.server.connection import SemanticLayerConnection

        adapter_calls = []

        class MockResult:
            def fetch_record_batch(self):
                import pyarrow as pa

                return pa.RecordBatchReader.from_batches(pa.schema([("nspname", pa.string())]), [])

        class MockAdapter:
            dialect = "duckdb"

            def execute(self, sql):
                adapter_calls.append(sql)
                return MockResult()

        class MockGraph:
            models = {}
            metrics = []

        class MockLayer:
            adapter = MockAdapter()
            graph = MockGraph()
            dialect = "duckdb"

        conn = SemanticLayerConnection(
            connection_id=1,
            executor=MagicMock(),
            layer=MockLayer(),
        )

        conn.send_reader = lambda reader, callback: callback(True)

        # Test pg_catalog.pg_namespace query
        conn._handle_query("SELECT * FROM pg_catalog.pg_namespace", lambda x: None)

        # Should have called adapter.execute with duckdb_schemas
        assert any("duckdb_schemas" in sql for sql in adapter_calls)


class TestServerCatalogAdapterIntegration:
    """Tests for server catalog registration using adapter."""

    def test_start_server_uses_adapter_for_catalog_queries(self, monkeypatch):
        """Test that start_server uses layer.adapter.execute for catalog queries."""
        pytest.importorskip("riffq")

        from sidemantic.server.server import start_server

        adapter_calls = []

        class TrackingResult:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class TrackingAdapter:
            dialect = "duckdb"

            def execute(self, sql):
                adapter_calls.append(sql)
                if "information_schema.tables" in sql:
                    return TrackingResult([("main", "test_table")])
                if "information_schema.columns" in sql:
                    return TrackingResult([("id", "INTEGER", "NO")])
                return TrackingResult([])

            @property
            def raw_connection(self):
                return MagicMock()

        class FakeInnerServer:
            def register_database(self, name):
                pass

            def register_schema(self, db, schema):
                pass

            def register_table(self, db, schema, table, columns):
                pass

        class FakeServer:
            def __init__(self, *args, **kwargs):
                self._server = FakeInnerServer()

            def start(self, *args, **kwargs):
                pass

        monkeypatch.setattr("riffq.RiffqServer", FakeServer)

        layer = SemanticLayer(connection="duckdb:///:memory:")
        layer._adapter = TrackingAdapter()
        layer.add_model(
            Model(
                name="orders",
                table="test_table",
                dimensions=[Dimension(name="id", sql="id", type="categorical")],
                metrics=[Metric(name="count", agg="count")],
            )
        )

        start_server(layer, port=5445)

        # Verify adapter was used for catalog queries
        assert any("information_schema.tables" in sql for sql in adapter_calls)
        assert any("information_schema.columns" in sql for sql in adapter_calls)


class TestMCPServerAdapterIntegration:
    """Tests for MCP server using adapter."""

    def test_mcp_run_query_uses_adapter(self):
        """Test that run_query goes through adapter for execution."""
        pytest.importorskip("mcp")
        import tempfile
        from pathlib import Path

        from sidemantic.mcp_server import initialize_layer, run_query

        tmpdir = tempfile.mkdtemp()
        tmpdir_path = Path(tmpdir)

        model_yaml = """
models:
  - name: orders
    table: orders_table
    dimensions:
      - name: status
        sql: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
"""
        (tmpdir_path / "orders.yml").write_text(model_yaml)

        layer = initialize_layer(str(tmpdir_path), db_path=":memory:")

        # Create table using adapter
        layer.adapter.execute("CREATE TABLE orders_table (id INTEGER, status VARCHAR)")
        layer.adapter.execute("INSERT INTO orders_table VALUES (1, 'completed'), (2, 'pending')")

        # Run query through MCP
        result = run_query(
            dimensions=["orders.status"],
            metrics=["orders.order_count"],
        )

        assert result["row_count"] == 2
        assert len(result["rows"]) == 2

        # Cleanup
        import shutil

        shutil.rmtree(tmpdir)


class TestCLIQueryUsesAdapter:
    """Tests for CLI query execution through adapter."""

    def test_cli_query_command_uses_adapter(self, tmp_path):
        """Test that CLI query command works with adapter."""
        from typer.testing import CliRunner

        from sidemantic.cli import app

        # Create model file
        model_yaml = """
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
        sql: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
"""
        (tmp_path / "orders.yml").write_text(model_yaml)

        # Create a database with test data using a layer
        layer = SemanticLayer(connection=f"duckdb:///{tmp_path / 'test.db'}")
        layer.adapter.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
        layer.adapter.execute("INSERT INTO orders VALUES (1, 'completed'), (2, 'pending')")
        layer.adapter.raw_connection.close()

        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "query",
                "SELECT orders.order_count FROM orders",
                "--models",
                str(tmp_path),
                "--db",
                str(tmp_path / "test.db"),
            ],
        )

        # Query should succeed
        assert result.exit_code == 0, result.output
        # Should show the count (2)
        assert "2" in result.output


class TestWorkbenchAdapterIntegration:
    """Tests for workbench wiring (not full TUI)."""

    def test_workbench_command_wiring(self, tmp_path, monkeypatch):
        """Test that workbench command is wired correctly."""
        pytest.importorskip("textual")
        from typer.testing import CliRunner

        from sidemantic.cli import app

        # Create model file
        model_yaml = """
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
        sql: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
"""
        (tmp_path / "orders.yml").write_text(model_yaml)

        called = {}

        def fake_run_workbench(directory, demo_mode=False, connection=None):
            called["directory"] = directory
            called["demo_mode"] = demo_mode

        monkeypatch.setattr("sidemantic.workbench.run_workbench", fake_run_workbench)

        runner = CliRunner()
        result = runner.invoke(app, ["workbench", str(tmp_path)])

        assert result.exit_code == 0
        assert called["directory"] == tmp_path


class TestSemanticLayerExitClosesAdapter:
    """Verify SemanticLayer closes adapter on __exit__."""

    def test_exit_closes_adapter(self):
        layer = SemanticLayer(auto_register=False)
        mock_adapter = MagicMock()
        mock_adapter.close = MagicMock()
        layer.adapter = mock_adapter

        layer.__exit__(None, None, None)
        mock_adapter.close.assert_called_once()

    def test_exit_works_without_close_method(self):
        layer = SemanticLayer(auto_register=False)
        mock_adapter = MagicMock(spec=[])  # No close method
        layer.adapter = mock_adapter

        # Should not raise even if adapter has no close
        layer.__exit__(None, None, None)


class TestWidgetAdapterIntegration:
    """Tests for widget using adapter interface."""

    def test_widget_generator_uses_layer_dialect(self):
        """Widget should construct SQLGenerator with layer.dialect in SemanticLayer mode."""
        pytest.importorskip("pyarrow")
        pytest.importorskip("anywidget")

        import sqlglot

        from sidemantic.widget import MetricsExplorer

        # Use DuckDB backend but override dialect to ensure generator follows layer.dialect.
        layer = SemanticLayer(connection="duckdb:///:memory:", dialect="postgres")
        layer.add_model(
            Model(
                name="orders",
                table="orders_table",
                dimensions=[
                    Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
                    Dimension(name="status", sql="status", type="categorical"),
                ],
                metrics=[Metric(name="order_count", agg="count")],
            )
        )
        layer.adapter.execute("CREATE TABLE orders_table (id INT, order_date DATE, status VARCHAR)")
        layer.adapter.execute(
            "INSERT INTO orders_table VALUES (1, '2024-01-01', 'completed'), (2, '2024-01-02', 'pending')"
        )

        widget = MetricsExplorer(
            layer,
            metrics=["orders.order_count"],
            dimensions=["orders.status"],
            time_dimension="order_date",
        )

        assert widget._generator.dialect == "postgres"

        # Generated SQL should be parseable under the chosen dialect.
        sql = widget._generator.generate(
            metrics=["orders.order_count"],
            dimensions=["orders.order_date__day"],
            filters=[],
            order_by=["orders.order_date__day"],
            limit=10,
            skip_default_time_dimensions=True,
            use_preaggregations=getattr(layer, "use_preaggregations", False),
        )
        sqlglot.parse_one(sql, dialect="postgres")

    def test_widget_date_range_query_handles_qualified_table(self):
        """Date range query should handle qualified table names like schema.table."""
        pytest.importorskip("pyarrow")
        pytest.importorskip("anywidget")

        from sidemantic.widget import MetricsExplorer

        layer = SemanticLayer(connection="duckdb:///:memory:")
        layer.add_model(
            Model(
                name="orders",
                table="s.orders_table",
                dimensions=[
                    Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
                    Dimension(name="status", sql="status", type="categorical"),
                ],
                metrics=[Metric(name="order_count", agg="count")],
            )
        )
        layer.adapter.execute("CREATE SCHEMA s")
        layer.adapter.execute("CREATE TABLE s.orders_table (id INT, order_date DATE, status VARCHAR)")
        layer.adapter.execute(
            "INSERT INTO s.orders_table VALUES (1, '2024-01-01', 'completed'), (2, '2024-01-02', 'pending')"
        )

        widget = MetricsExplorer(
            layer,
            metrics=["orders.order_count"],
            dimensions=["orders.status"],
            time_dimension="order_date",
        )

        assert len(widget.date_range) == 2
        assert widget.date_range[0].startswith("2024-01-01")
        assert widget.date_range[1].startswith("2024-01-02")

    def test_widget_sets_time_series_column_in_config(self):
        """Widget should set config.time_series_column based on Arrow schema."""
        pytest.importorskip("pyarrow")
        pytest.importorskip("anywidget")

        from sidemantic.widget import MetricsExplorer

        layer = SemanticLayer(connection="duckdb:///:memory:")
        layer.add_model(
            Model(
                name="orders",
                table="orders_table",
                dimensions=[
                    Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
                    Dimension(name="status", sql="status", type="categorical"),
                ],
                metrics=[Metric(name="order_count", agg="count")],
            )
        )
        layer.adapter.execute("CREATE TABLE orders_table (id INT, order_date DATE, status VARCHAR)")
        layer.adapter.execute(
            "INSERT INTO orders_table VALUES (1, '2024-01-01', 'completed'), (2, '2024-01-02', 'pending')"
        )

        widget = MetricsExplorer(
            layer,
            metrics=["orders.order_count"],
            dimensions=["orders.status"],
            time_dimension="order_date",
        )

        assert widget.config.get("time_series_column")

    def test_widget_binary_transport_populates_binary_traits(self):
        """Binary transport should populate Bytes/Dict binary traits and clear base64 traits."""
        pytest.importorskip("pyarrow")
        pytest.importorskip("anywidget")

        from sidemantic.widget import MetricsExplorer

        layer = SemanticLayer(connection="duckdb:///:memory:")
        layer.add_model(
            Model(
                name="orders",
                table="orders_table",
                dimensions=[
                    Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
                    Dimension(name="status", sql="status", type="categorical"),
                ],
                metrics=[Metric(name="order_count", agg="count")],
            )
        )
        layer.adapter.execute("CREATE TABLE orders_table (id INT, order_date DATE, status VARCHAR)")
        layer.adapter.execute(
            "INSERT INTO orders_table VALUES (1, '2024-01-01', 'completed'), (2, '2024-01-02', 'pending')"
        )

        widget = MetricsExplorer(
            layer,
            metrics=["orders.order_count"],
            dimensions=["orders.status"],
            time_dimension="order_date",
            transport="binary",
        )

        assert widget.transport == "binary"
        assert widget.metric_series_data == ""
        assert isinstance(widget.metric_series_data_binary, (bytes, bytearray))
        assert widget.metric_series_data_binary

    def test_widget_uses_adapter_in_layer_mode(self):
        """Test that widget uses layer.adapter.execute in Semantic Layer mode."""
        pytest.importorskip("pyarrow")
        pytest.importorskip("anywidget")

        from sidemantic.widget import MetricsExplorer

        # Create layer with test data
        layer = SemanticLayer(connection="duckdb:///:memory:")
        layer.add_model(
            Model(
                name="orders",
                table="orders_table",
                dimensions=[
                    Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
                    Dimension(name="status", sql="status", type="categorical"),
                ],
                metrics=[Metric(name="order_count", agg="count")],
            )
        )
        layer.adapter.execute("CREATE TABLE orders_table (id INT, order_date DATE, status VARCHAR)")
        layer.adapter.execute(
            "INSERT INTO orders_table VALUES (1, '2024-01-01', 'completed'), (2, '2024-01-02', 'pending')"
        )

        # Create widget - this should use adapter internally
        widget = MetricsExplorer(
            layer,
            metrics=["orders.order_count"],
            dimensions=["orders.status"],
            time_dimension="order_date",
        )

        # Widget should have initialized successfully
        assert widget._layer is layer
        assert widget._conn is layer.adapter.raw_connection
        # Date range should have been computed using _execute (through adapter)
        assert len(widget.date_range) == 2

    def test_widget_execute_routes_through_adapter(self):
        """Test that widget._execute() routes through adapter when layer is set."""
        pytest.importorskip("pyarrow")
        pytest.importorskip("anywidget")

        from sidemantic.widget import MetricsExplorer

        adapter_calls = []

        # Create layer with tracking
        layer = SemanticLayer(connection="duckdb:///:memory:")
        layer.add_model(
            Model(
                name="orders",
                table="orders_table",
                dimensions=[
                    Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
                    Dimension(name="status", sql="status", type="categorical"),
                ],
                metrics=[Metric(name="order_count", agg="count")],
            )
        )
        layer.adapter.execute("CREATE TABLE orders_table (id INT, order_date DATE, status VARCHAR)")
        layer.adapter.execute(
            "INSERT INTO orders_table VALUES (1, '2024-01-01', 'completed'), (2, '2024-01-02', 'pending')"
        )

        # Wrap the adapter to track calls
        original_execute = layer.adapter.execute

        def tracking_execute(sql):
            adapter_calls.append(sql)
            return original_execute(sql)

        layer.adapter.execute = tracking_execute

        # Create widget
        widget = MetricsExplorer(
            layer,
            metrics=["orders.order_count"],
            dimensions=["orders.status"],
            time_dimension="order_date",
        )

        # Clear tracking from init
        adapter_calls.clear()

        # Call _execute directly
        result = widget._execute("SELECT COUNT(*) FROM orders_table")
        count = result.fetchone()[0]

        # Should have routed through adapter
        assert len(adapter_calls) == 1
        assert "SELECT COUNT(*)" in adapter_calls[0]
        assert count == 2

    def test_widget_dataframe_mode_uses_raw_connection(self):
        """Test that widget in DataFrame mode uses raw connection (no layer)."""
        pytest.importorskip("pyarrow")
        pytest.importorskip("anywidget")
        pytest.importorskip("polars")

        import polars as pl

        from sidemantic.widget import MetricsExplorer

        # Create a simple DataFrame
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "amount": [100.0, 200.0, 150.0],
                "category": ["A", "B", "A"],
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            }
        )

        # Create widget in DataFrame mode
        widget = MetricsExplorer(df, time_dimension="date")

        # Should not have a layer
        assert widget._layer is None
        # Should have a raw connection
        assert widget._conn is not None
        # _execute should use raw connection
        result = widget._execute("SELECT COUNT(*) FROM widget_data")
        count = result.fetchone()[0]
        assert count == 3

    def test_widget_execute_arrow_uses_fetch_record_batch(self):
        """Test that _execute_arrow() uses fetch_record_batch() for cross-adapter compatibility."""
        pytest.importorskip("pyarrow")
        pytest.importorskip("anywidget")

        import pyarrow as pa

        from sidemantic.widget import MetricsExplorer

        fetch_record_batch_called = []

        # Create layer
        layer = SemanticLayer(connection="duckdb:///:memory:")
        layer.add_model(
            Model(
                name="orders",
                table="orders_table",
                dimensions=[
                    Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
                    Dimension(name="status", sql="status", type="categorical"),
                ],
                metrics=[Metric(name="order_count", agg="count")],
            )
        )
        layer.adapter.execute("CREATE TABLE orders_table (id INT, order_date DATE, status VARCHAR)")
        layer.adapter.execute(
            "INSERT INTO orders_table VALUES (1, '2024-01-01', 'completed'), (2, '2024-01-02', 'pending')"
        )

        # Wrap adapter.execute to track fetch_record_batch calls
        original_execute = layer.adapter.execute

        class TrackingResult:
            def __init__(self, inner):
                self._inner = inner

            def fetchone(self):
                return self._inner.fetchone()

            def fetchall(self):
                return self._inner.fetchall()

            def fetch_record_batch(self):
                fetch_record_batch_called.append(True)
                return self._inner.fetch_record_batch()

        def tracking_execute(sql):
            result = original_execute(sql)
            return TrackingResult(result)

        layer.adapter.execute = tracking_execute

        # Create widget
        widget = MetricsExplorer(
            layer,
            metrics=["orders.order_count"],
            dimensions=["orders.status"],
            time_dimension="order_date",
        )

        # Clear tracking from init
        fetch_record_batch_called.clear()

        # Call _execute_arrow directly
        table = widget._execute_arrow("SELECT * FROM orders_table")

        # Should have called fetch_record_batch
        assert len(fetch_record_batch_called) == 1
        # Should return a PyArrow Table
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
