"""Interactive workbench application."""

from pathlib import Path

from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.reactive import reactive
from textual.widgets import Button, DataTable, Footer, Header, Label, Select, TextArea
from textual.widgets import Tree as TreeWidget
from textual_plotext import PlotextPlot

from sidemantic import SemanticLayer, load_from_directory

# Example queries
EXAMPLE_QUERIES = {
    "Timeseries": "-- Timeseries revenue by month and region\nSELECT \n  orders.created_month,\n  customers.region,\n  orders.total_revenue,\n  orders.order_count\nFROM orders\nORDER BY created_month DESC, region",
    "Top Customers": "-- Top customers by revenue\nSELECT \n  customers.name,\n  customers.region,\n  orders.total_revenue,\n  orders.order_count\nFROM orders\nORDER BY orders.total_revenue DESC\nLIMIT 10",
    "Aggregates": "-- Revenue metrics by region\nSELECT \n  customers.region,\n  orders.total_revenue,\n  orders.avg_order_value,\n  orders.order_count\nFROM orders\nGROUP BY customers.region\nORDER BY orders.total_revenue DESC",
    "Custom": "-- Write your custom query here\nSELECT \n  \nFROM ",
}


class SidequeryWorkbench(App):
    """Sidequery Workbench - Interactive semantic layer workbench."""

    CSS = """
    Screen {
        background: $surface;
    }

    #main {
        height: 100%;
    }

    #sidebar {
        border-right: solid $primary;
    }

    #query-panel {
        width: 1fr;
    }

    #query-buttons {
        height: auto;
        padding: 0 1;
        background: $panel;
    }

    #query-buttons Button {
        border: none;
        background: transparent;
        min-width: 15;
    }

    #query-buttons Button.active {
        text-style: bold;
        color: $primary;
    }

    #query-editors {
        height: 40%;
    }

    .query-editor {
        height: 100%;
        display: none;
    }

    .sql-editor {
        height: 100%;
        border: none;
    }

    .sql-editor:focus {
        border: none;
    }

    #results-panel {
        height: 60%;
    }

    #view-buttons {
        height: auto;
        padding: 0 1;
        background: $panel;
    }

    #view-buttons Button {
        border: none;
        background: transparent;
        min-width: 10;
    }

    #view-buttons Button.active {
        text-style: bold;
        color: $primary;
    }

    #table-view {
        height: 1fr;
    }

    #chart-view {
        height: 1fr;
        display: none;
    }

    #sql-view {
        height: 1fr;
        display: none;
    }

    #sql-display {
        height: 100%;
        border: none;
    }

    #sql-display:focus {
        border: none;
    }

    #results-table {
        height: 100%;
    }

    #chart-container {
        height: 100%;
    }

    #chart-plot {
        height: 1fr;
    }

    #chart-config {
        height: auto;
        padding: 1;
        background: $panel;
    }

    .config-row {
        height: auto;
        padding: 0 1;
    }
    """

    TITLE = "Sidemantic Workbench"

    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit"),
        Binding("ctrl+r", "run_query", "Run Query"),
    ]

    sidebar_width = reactive(38)

    def __init__(self, directory: Path, show_sql: bool = False, demo_mode: bool = False, connection: str | None = None):
        super().__init__()
        self.directory = directory
        self.layer = None
        self.last_result = None
        self.last_rendered_sql = None
        self.demo_mode = demo_mode
        self.connection = connection
        self.dragging_sidebar = False
        self.drag_start_x = 0

    def watch_sidebar_width(self, width: int) -> None:
        """Update sidebar width when reactive value changes."""
        sidebar = self.query_one("#sidebar")
        sidebar.styles.width = width

    def compose(self) -> ComposeResult:
        """Create child widgets."""
        yield Header()
        with Horizontal(id="main"):
            with VerticalScroll(id="sidebar"):
                yield TreeWidget("Models", id="tree")
            with Vertical(id="query-panel"):
                with Horizontal(id="query-buttons"):
                    yield Button("Timeseries", id="btn-timeseries", classes="active")
                    yield Button("Top Customers", id="btn-top-customers")
                    yield Button("Aggregates", id="btn-aggregates")
                    yield Button("Custom", id="btn-custom")
                with Vertical(id="query-editors"):
                    with Vertical(id="editor-timeseries", classes="query-editor"):
                        yield TextArea(
                            EXAMPLE_QUERIES["Timeseries"],
                            language="sql",
                            show_line_numbers=True,
                            classes="sql-editor",
                        )
                    with Vertical(id="editor-top-customers", classes="query-editor"):
                        yield TextArea(
                            EXAMPLE_QUERIES["Top Customers"],
                            language="sql",
                            show_line_numbers=True,
                            classes="sql-editor",
                        )
                    with Vertical(id="editor-aggregates", classes="query-editor"):
                        yield TextArea(
                            EXAMPLE_QUERIES["Aggregates"],
                            language="sql",
                            show_line_numbers=True,
                            classes="sql-editor",
                        )
                    with Vertical(id="editor-custom", classes="query-editor"):
                        yield TextArea(
                            EXAMPLE_QUERIES["Custom"],
                            language="sql",
                            show_line_numbers=True,
                            classes="sql-editor",
                        )
                with Vertical(id="results-panel"):
                    with Horizontal(id="view-buttons"):
                        yield Button("Table", id="btn-table", classes="active")
                        yield Button("Chart", id="btn-chart")
                        yield Button("SQL", id="btn-sql")
                    with Vertical(id="table-view"):
                        yield DataTable(id="results-table")
                    with Vertical(id="sql-view"):
                        yield TextArea("", language="sql", show_line_numbers=True, read_only=True, id="sql-display")
                    with Vertical(id="chart-view"):
                        with Horizontal(id="chart-config", classes="config-row"):
                            yield Label("X:", classes="config-label")
                            yield Select([], id="x-axis-select", allow_blank=True)
                            yield Label("Y:", classes="config-label")
                            yield Select([], id="y-axis-select", allow_blank=True)
                            yield Label("Type:", classes="config-label")
                            yield Select(
                                [("Bar", "bar"), ("Line", "line"), ("Scatter", "scatter")],
                                id="plot-type-select",
                                value="bar",
                            )
                        yield PlotextPlot(id="chart-plot")
        yield Footer()

    def watch_theme(self, theme_name: str) -> None:
        """Update TextArea themes when app theme changes."""
        # Map app themes to TextArea themes (css, dracula, github_light, monokai, vscode_dark)
        theme_map = {
            "textual-dark": "vscode_dark",
            "textual-light": "github_light",
            "nord": "dracula",
            "gruvbox": "monokai",
            "tokyo-night": "vscode_dark",
            "solarized-light": "github_light",
            "catppuccin-mocha": "monokai",
            "catppuccin-latte": "github_light",
        }

        # Default to vscode_dark for dark themes, github_light for light themes
        editor_theme = theme_map.get(
            theme_name,
            "vscode_dark" if "dark" in theme_name.lower() or "mocha" in theme_name.lower() else "github_light",
        )

        # Update all SQL editors
        for editor in self.query(".sql-editor").results(TextArea):
            editor.theme = editor_theme

        # Update SQL display
        try:
            sql_display = self.query_one("#sql-display", TextArea)
            sql_display.theme = editor_theme
        except Exception:
            pass  # SQL display may not exist yet

    def on_mount(self) -> None:
        """Load semantic layer and populate tree."""
        # Show first query editor
        self.query_one("#editor-timeseries").styles.display = "block"

        try:
            # Setup database connection
            if self.demo_mode:
                # Create in-memory demo database
                try:
                    # Try packaged import first
                    from sidemantic.examples.multi_format_demo.demo_data import create_demo_database
                except ModuleNotFoundError:
                    # Fall back to dev environment import
                    import sys

                    demo_data_path = self.directory / "demo_data.py"
                    if demo_data_path.exists():
                        import importlib.util

                        spec = importlib.util.spec_from_file_location("demo_data", demo_data_path)
                        demo_data_module = importlib.util.module_from_spec(spec)
                        sys.modules["demo_data"] = demo_data_module
                        spec.loader.exec_module(demo_data_module)
                        create_demo_database = demo_data_module.create_demo_database
                    else:
                        raise ImportError(f"Could not find demo_data.py at {demo_data_path}")

                # Create layer with in-memory DB
                self.layer = SemanticLayer(connection="duckdb:///:memory:")
                # Populate with demo data
                demo_conn = create_demo_database()
                # Copy data from demo connection to layer's connection
                for table in ["customers", "products", "orders"]:
                    # Get table data as regular Python objects (no pandas)
                    rows = demo_conn.execute(f"SELECT * FROM {table}").fetchall()
                    columns = [desc[0] for desc in demo_conn.execute(f"SELECT * FROM {table} LIMIT 0").description]

                    # Create table in target connection
                    create_sql = demo_conn.execute(
                        f"SELECT sql FROM duckdb_tables() WHERE table_name = '{table}'"
                    ).fetchone()[0]
                    self.layer.conn.execute(create_sql)

                    # Insert data if there are rows
                    if rows:
                        placeholders = ", ".join(["?" for _ in columns])
                        self.layer.conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
            else:
                # Use explicit connection if provided, otherwise try to find database file
                connection = None
                if self.connection:
                    connection = self.connection
                else:
                    # Auto-discover DuckDB file in data/ directory
                    data_dir = self.directory / "data"
                    if data_dir.exists():
                        db_files = list(data_dir.glob("*.db"))
                        if db_files:
                            connection = f"duckdb:///{db_files[0].absolute()}"

                # Only pass connection if it's not None (to use default parameter)
                if connection:
                    self.layer = SemanticLayer(connection=connection)
                else:
                    self.layer = SemanticLayer()

            # Load semantic layer models
            load_from_directory(self.layer, str(self.directory))

            tree = self.query_one("#tree", TreeWidget)
            tree.show_root = True
            tree.guide_depth = 4

            # Add summary as root label
            total_dims = sum(len(m.dimensions) for m in self.layer.graph.models.values())
            total_metrics = sum(len(m.metrics) for m in self.layer.graph.models.values())
            total_rels = sum(len(m.relationships) for m in self.layer.graph.models.values())

            tree.label = f"Models ({len(self.layer.graph.models)})"

            # Set root tooltip
            root_tooltip = f"[bold]Sidequery Workbench[/bold]\n\nLoaded from: {self.directory}\n\nModels: {len(self.layer.graph.models)}\nDimensions: {total_dims}\nMetrics: {total_metrics}\nRelationships: {total_rels}"
            tree.root.data = {"type": "root", "tooltip": root_tooltip}

            # Add models
            for model_name, model in sorted(self.layer.graph.models.items()):
                # Build detailed model tooltip
                tooltip_parts = [f"[bold cyan]Model: {model_name}[/bold cyan]"]

                # Show source format if available
                if hasattr(model, "_source_format"):
                    tooltip_parts.append(f"Format: {model._source_format}")
                if hasattr(model, "_source_file"):
                    tooltip_parts.append(f"File: {model._source_file}")

                if model.table:
                    tooltip_parts.append(f"Table: {model.table}")

                if model.primary_key:
                    tooltip_parts.append(f"Primary Key: {model.primary_key}")

                # Add counts
                tooltip_parts.append(f"Dimensions: {len(model.dimensions)}")
                tooltip_parts.append(f"Metrics: {len(model.metrics)}")
                tooltip_parts.append(f"Relationships: {len(model.relationships)}")

                if model.description:
                    tooltip_parts.append(f"\n{model.description}")

                model_tooltip = "\n".join(tooltip_parts)

                model_node = tree.root.add(
                    f"[bold cyan]{model_name}[/bold cyan]",
                    data={"type": "model", "name": model_name, "tooltip": model_tooltip},
                    expand=True,
                )

                # Add dimensions
                if model.dimensions:
                    dims_tooltip = f"[bold]{model_name} Dimensions[/bold]\n{len(model.dimensions)} dimension(s) for grouping and filtering"
                    dims_node = model_node.add(
                        f"[blue]Dimensions[/blue] ({len(model.dimensions)})",
                        data={"type": "dimensions", "model": model_name, "tooltip": dims_tooltip},
                        expand=True,
                    )
                    for dim in model.dimensions:
                        # Build detailed dimension tooltip
                        dim_tooltip_parts = [f"[bold]{model_name}.{dim.name}[/bold]"]
                        dim_tooltip_parts.append(f"Type: {dim.type}")

                        if dim.sql:
                            dim_tooltip_parts.append(f"SQL: {dim.sql}")

                        if dim.type == "time" and dim.granularity:
                            dim_tooltip_parts.append(f"Granularity: {dim.granularity}")
                            if dim.supported_granularities:
                                dim_tooltip_parts.append(f"Supported: {', '.join(dim.supported_granularities)}")

                        if dim.label:
                            dim_tooltip_parts.append(f"Label: {dim.label}")

                        if dim.format or dim.value_format_name:
                            fmt = dim.value_format_name or dim.format
                            dim_tooltip_parts.append(f"Format: {fmt}")

                        if dim.parent:
                            dim_tooltip_parts.append(f"Parent: {dim.parent}")

                        if dim.description:
                            dim_tooltip_parts.append(f"\n{dim.description}")

                        dims_node.add_leaf(
                            dim.name, data={"type": "dimension", "tooltip": "\n".join(dim_tooltip_parts)}
                        )

                # Add metrics
                if model.metrics:
                    metrics_tooltip = f"[bold]{model_name} Metrics[/bold]\n{len(model.metrics)} metric(s) for aggregations and calculations"
                    metrics_node = model_node.add(
                        f"[magenta]Metrics[/magenta] ({len(model.metrics)})",
                        data={"type": "metrics", "model": model_name, "tooltip": metrics_tooltip},
                        expand=True,
                    )
                    for metric in model.metrics:
                        # Build detailed metric tooltip
                        metric_tooltip_parts = [f"[bold]{model_name}.{metric.name}[/bold]"]

                        if metric.type:
                            metric_tooltip_parts.append(f"Metric Type: {metric.type}")

                        if metric.agg:
                            metric_tooltip_parts.append(f"Aggregation: {metric.agg.upper()}")

                        if metric.sql:
                            sql_preview = metric.sql if len(metric.sql) <= 60 else metric.sql[:57] + "..."
                            metric_tooltip_parts.append(f"SQL: {sql_preview}")

                        if metric.numerator or metric.denominator:
                            metric_tooltip_parts.append(f"Ratio: {metric.numerator} / {metric.denominator}")

                        if metric.filters:
                            filters_str = ", ".join(metric.filters[:2])
                            if len(metric.filters) > 2:
                                filters_str += f" +{len(metric.filters) - 2} more"
                            metric_tooltip_parts.append(f"Filters: {filters_str}")

                        if metric.label:
                            metric_tooltip_parts.append(f"Label: {metric.label}")

                        if metric.format or metric.value_format_name:
                            fmt = metric.value_format_name or metric.format
                            metric_tooltip_parts.append(f"Format: {fmt}")

                        if metric.description:
                            desc_preview = (
                                metric.description if len(metric.description) <= 80 else metric.description[:77] + "..."
                            )
                            metric_tooltip_parts.append(f"\n{desc_preview}")

                        metrics_node.add_leaf(
                            metric.name, data={"type": "metric", "tooltip": "\n".join(metric_tooltip_parts)}
                        )

                # Add relationships
                if model.relationships:
                    rels_tooltip = f"[bold]{model_name} Relationships[/bold]\n{len(model.relationships)} relationship(s) to other models"
                    rels_node = model_node.add(
                        f"[green]Relationships[/green] ({len(model.relationships)})",
                        data={"type": "relationships", "model": model_name, "tooltip": rels_tooltip},
                        expand=True,
                    )
                    for rel in model.relationships:
                        # Build detailed relationship tooltip
                        rel_tooltip_parts = [f"[bold]{model_name} → {rel.name}[/bold]"]
                        rel_tooltip_parts.append(f"Type: {rel.type}")

                        if rel.foreign_key and rel.primary_key:
                            rel_tooltip_parts.append(
                                f"Join: {model_name}.{rel.foreign_key} = {rel.name}.{rel.primary_key}"
                            )
                        elif rel.foreign_key:
                            rel_tooltip_parts.append(f"Foreign Key: {rel.foreign_key}")
                        elif rel.primary_key:
                            rel_tooltip_parts.append(f"Primary Key: {rel.primary_key}")

                        rels_node.add_leaf(
                            rel.name, data={"type": "relationship", "tooltip": "\n".join(rel_tooltip_parts)}
                        )

            # Expand root
            tree.root.expand()

        except Exception as e:
            self.exit(message=f"Error loading semantic layer: {e}")

    def on_tree_node_selected(self, event: TreeWidget.NodeSelected) -> None:
        """Show tooltip info when node is selected."""
        node = event.node
        if node.data and "tooltip" in node.data:
            # Update the tree widget's tooltip or show in a status area
            tree = self.query_one("#tree", TreeWidget)
            tree.tooltip = node.data["tooltip"]

    def on_mouse_move(self, event: events.MouseMove) -> None:
        """Handle mouse move for sidebar resize and tree tooltips."""
        # Handle sidebar dragging
        if self.dragging_sidebar:
            # Calculate new width based on mouse position
            new_width = event.screen_x + 1
            # Clamp between reasonable values
            new_width = max(20, min(100, new_width))
            self.sidebar_width = new_width
            event.stop()
            return

        # Update tree tooltip on hover
        tree = self.query_one("#tree", TreeWidget)

        # Get the widget under the mouse
        widget, _ = self.get_widget_at(*event.screen_offset)

        # Check if we're hovering over the tree
        if widget is tree or (hasattr(widget, "parent") and widget.parent is tree):
            # Get the hovered node if available
            if hasattr(tree, "hover_node") and tree.hover_node:
                node = tree.hover_node
                if node.data and "tooltip" in node.data:
                    tree.tooltip = node.data["tooltip"]
                else:
                    tree.tooltip = None
            elif tree.hover_line >= 0:
                # Alternative: try to get node from hover_line
                try:
                    lines = list(tree._tree_lines_cached)
                    if 0 <= tree.hover_line < len(lines):
                        node = lines[tree.hover_line].node
                        if node.data and "tooltip" in node.data:
                            tree.tooltip = node.data["tooltip"]
                except Exception:
                    pass

    def on_mouse_down(self, event: events.MouseDown) -> None:
        """Start sidebar resize on border click."""
        sidebar = self.query_one("#sidebar")
        # Check if clicking near right edge of sidebar
        sidebar_region = sidebar.region
        if sidebar_region.x <= event.screen_x <= sidebar_region.x + sidebar_region.width + 1:
            # Check if near right edge (within 2 columns)
            if abs(event.screen_x - (sidebar_region.x + sidebar_region.width)) <= 1:
                self.dragging_sidebar = True
                self.drag_start_x = event.screen_x
                event.stop()

    def on_mouse_up(self, event: events.MouseUp) -> None:
        """Stop sidebar resize."""
        if self.dragging_sidebar:
            self.dragging_sidebar = False
            event.stop()

    def action_run_query(self) -> None:
        """Execute the SQL query and display results."""
        try:
            # Get the visible editor
            editor = None
            for editor_id in ["editor-timeseries", "editor-top-customers", "editor-aggregates", "editor-custom"]:
                container = self.query_one(f"#{editor_id}")
                if container.styles.display == "block":
                    editor = container.query_one(TextArea)
                    break

            if not editor:
                return

            table = self.query_one("#results-table", DataTable)

            sql = editor.text.strip()
            if not sql:
                return

            # Execute query and get rendered SQL
            from sidemantic.sql.query_rewriter import QueryRewriter

            rewriter = QueryRewriter(self.layer.graph, dialect=self.layer.dialect)
            rendered_sql = rewriter.rewrite(sql)

            # Store rendered SQL
            self.last_rendered_sql = rendered_sql

            # Execute the query
            result = self.layer.adapter.execute(rendered_sql)

            # Get column names and rows
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            # Store for chart rendering
            self.last_result = {"columns": columns, "rows": rows}

            # Update SQL display
            if rendered_sql:
                sql_display = self.query_one("#sql-display", TextArea)
                sql_display.text = rendered_sql

            # Update table
            table.clear(columns=True)
            for col in columns:
                table.add_column(str(col), key=str(col))
            for row in rows:
                table.add_row(*[str(v) for v in row])

            # Update chart config dropdowns
            try:
                x_select = self.query_one("#x-axis-select", Select)
                y_select = self.query_one("#y-axis-select", Select)

                col_options = [(col, col) for col in columns]
                x_select.set_options(col_options)
                y_select.set_options(col_options)

                # Smart default selection
                x_default = None
                y_default = None

                # Look for time dimension for X axis (date, time, month, year, etc.)
                time_keywords = ["date", "time", "month", "year", "week", "day", "quarter", "created", "updated"]
                for col in columns:
                    col_lower = col.lower()
                    if any(keyword in col_lower for keyword in time_keywords):
                        x_default = col
                        break

                # If no time dimension, use first column
                if not x_default and columns:
                    x_default = columns[0]

                # Look for numeric/metric column for Y axis
                # Heuristic: check if column contains metric-like keywords or appears to be numeric
                metric_keywords = [
                    "revenue",
                    "total",
                    "count",
                    "sum",
                    "avg",
                    "average",
                    "amount",
                    "value",
                    "price",
                    "cost",
                    "metric",
                    "measure",
                ]

                for col in columns:
                    if col == x_default:
                        continue
                    col_lower = col.lower()
                    if any(keyword in col_lower for keyword in metric_keywords):
                        y_default = col
                        break

                # If no metric-like column found, check if we can find a numeric column by sampling data
                if not y_default and rows:
                    for i, col in enumerate(columns):
                        if col == x_default:
                            continue
                        # Check first row to see if this looks numeric
                        try:
                            val = rows[0][i]
                            if isinstance(val, (int, float)):
                                y_default = col
                                break
                            elif isinstance(val, str):
                                float(val)  # Try to parse as number
                                y_default = col
                                break
                        except (ValueError, TypeError, IndexError):
                            continue

                # Final fallback: use second column if available
                if not y_default and len(columns) >= 2:
                    y_default = columns[1] if columns[1] != x_default else columns[0]

                # Set defaults
                if x_default:
                    x_select.value = x_default
                if y_default:
                    y_select.value = y_default

                # Render chart if selections are made
                self._render_chart()
            except Exception:
                pass  # Chart updates are optional

        except Exception as e:
            # Show error in table
            import traceback

            table = self.query_one("#results-table", DataTable)
            table.clear(columns=True)
            table.add_column("Error", key="error")
            table.add_row(str(e))
            table.add_row(traceback.format_exc())
            self.last_result = None

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle view switching buttons."""
        button_id = event.button.id

        # Handle query tab switching
        if button_id.startswith("btn-") and button_id not in ("btn-table", "btn-chart", "btn-sql"):
            # Map button id to editor id
            editor_mapping = {
                "btn-timeseries": "editor-timeseries",
                "btn-top-customers": "editor-top-customers",
                "btn-aggregates": "editor-aggregates",
                "btn-custom": "editor-custom",
            }

            if button_id in editor_mapping:
                # Hide all editors
                for editor_id in editor_mapping.values():
                    self.query_one(f"#{editor_id}").styles.display = "none"

                # Show selected editor
                self.query_one(f"#{editor_mapping[button_id]}").styles.display = "block"

                # Update button states
                for btn_id in editor_mapping.keys():
                    btn = self.query_one(f"#{btn_id}", Button)
                    if btn_id == button_id:
                        btn.add_class("active")
                    else:
                        btn.remove_class("active")

                # Clear results when switching query tabs
                table = self.query_one("#results-table", DataTable)
                table.clear(columns=True)

                # Clear SQL display
                sql_display = self.query_one("#sql-display", TextArea)
                sql_display.text = ""

                # Clear chart
                try:
                    chart = self.query_one("#chart-plot", PlotextPlot)
                    chart.plt.clear_data()
                    chart.plt.clear_figure()
                    chart.refresh()
                except Exception:
                    pass

                self.last_result = None
                self.last_rendered_sql = None

        # Handle table/chart/sql view switching
        table_view = self.query_one("#table-view")
        chart_view = self.query_one("#chart-view")
        sql_view = self.query_one("#sql-view")

        if event.button.id == "btn-table":
            table_view.styles.display = "block"
            chart_view.styles.display = "none"
            sql_view.styles.display = "none"
            self.query_one("#btn-table", Button).add_class("active")
            self.query_one("#btn-chart", Button).remove_class("active")
            self.query_one("#btn-sql", Button).remove_class("active")
        elif event.button.id == "btn-chart":
            table_view.styles.display = "none"
            chart_view.styles.display = "block"
            sql_view.styles.display = "none"
            self.query_one("#btn-table", Button).remove_class("active")
            self.query_one("#btn-chart", Button).add_class("active")
            self.query_one("#btn-sql", Button).remove_class("active")
        elif event.button.id == "btn-sql":
            table_view.styles.display = "none"
            chart_view.styles.display = "none"
            sql_view.styles.display = "block"
            self.query_one("#btn-table", Button).remove_class("active")
            self.query_one("#btn-chart", Button).remove_class("active")
            self.query_one("#btn-sql", Button).add_class("active")

    def on_select_changed(self, event: Select.Changed) -> None:
        """Handle chart axis selection changes."""
        if event.select.id in ("x-axis-select", "y-axis-select", "plot-type-select"):
            self._render_chart()

    def _render_chart(self) -> None:
        """Render chart based on current selections."""
        if not self.last_result:
            return

        x_select = self.query_one("#x-axis-select", Select)
        y_select = self.query_one("#y-axis-select", Select)
        plot_type_select = self.query_one("#plot-type-select", Select)

        x_col = x_select.value
        y_col = y_select.value
        plot_type = plot_type_select.value

        if not x_col or not y_col:
            return

        try:
            columns = self.last_result["columns"]
            rows = self.last_result["rows"]

            x_idx = columns.index(x_col)
            y_idx = columns.index(y_col)

            # Extract data with original x values for sorting
            data_points = []
            for row in rows:
                try:
                    x_val = row[x_idx]
                    y_val = row[y_idx]

                    # Convert to appropriate types
                    if isinstance(y_val, str):
                        try:
                            y_val = float(y_val)
                        except ValueError:
                            continue

                    data_points.append((x_val, float(y_val)))
                except (ValueError, TypeError):
                    continue

            # Sort by x values ascending
            data_points.sort(key=lambda p: p[0])

            # Limit to reasonable number of points for display
            max_points = 50
            if len(data_points) > max_points:
                step = len(data_points) // max_points
                data_points = data_points[::step]

            # Format data based on plot type
            x_data = []
            y_data = []
            x_labels = []

            for x_val, y_val in data_points:
                y_data.append(y_val)

                # For bar charts, use string labels
                if plot_type == "bar":
                    x_label = str(x_val)
                    if len(x_label) > 20:
                        x_label = x_label[:17] + "..."
                    x_data.append(x_label)
                else:
                    # For line/scatter, try to use numeric x or indices
                    try:
                        # Try to use numeric value if possible
                        if isinstance(x_val, (int, float)):
                            x_data.append(x_val)
                        else:
                            # Use index for non-numeric x values
                            x_data.append(len(x_data))
                            x_labels.append(str(x_val))
                    except Exception:
                        x_data.append(len(x_data))
                        x_labels.append(str(x_val))

            # Update plot
            chart = self.query_one("#chart-plot", PlotextPlot)
            if x_data and y_data:
                chart.plt.clear_data()
                chart.plt.clear_figure()

                # Use appropriate plot type
                if plot_type == "bar":
                    chart.plt.bar(x_data, y_data)
                elif plot_type == "line":
                    chart.plt.plot(x_data, y_data)
                elif plot_type == "scatter":
                    chart.plt.scatter(x_data, y_data)

                chart.plt.title(f"{y_col} by {x_col}")
                chart.plt.xlabel(x_col)
                chart.plt.ylabel(y_col)
                chart.refresh()

        except Exception:
            pass
