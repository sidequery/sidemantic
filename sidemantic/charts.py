"""Beautiful chart generation for semantic layer queries using Altair.

Provides smart chart type selection and carefully designed aesthetic defaults
for professional, publication-quality data visualizations.
"""

import base64
from typing import Any, Literal

try:
    import altair as alt
    import vl_convert
    from altair import Chart
except ImportError:
    alt = None  # type: ignore
    vl_convert = None  # type: ignore
    Chart = None  # type: ignore


# Professional color palette - carefully selected for accessibility and aesthetics
# Based on modern data visualization best practices
COLORS = {
    "primary": "#2E5EAA",  # Deep blue - professional, trustworthy
    "secondary": "#E8702A",  # Warm orange - contrasts well with blue
    "success": "#4C9A2A",  # Forest green - growth, positive
    "warning": "#F39C12",  # Amber - attention without alarm
    "danger": "#C0392B",  # Deep red - clear but not garish
    "neutral": "#7F8C8D",  # Slate gray - balanced, neutral
    # Categorical palette (for multiple series)
    "categorical": [
        "#2E5EAA",  # Deep blue
        "#E8702A",  # Warm orange
        "#4C9A2A",  # Forest green
        "#9B59B6",  # Purple
        "#1ABC9C",  # Teal
        "#E74C3C",  # Red
        "#F39C12",  # Amber
        "#34495E",  # Dark slate
    ],
}


def check_altair_available() -> None:
    """Check if Altair is available."""
    if alt is None or vl_convert is None:
        raise ImportError(
            "Altair and vl-convert-python are required for chart generation. "
            "Install with: uv add altair vl-convert-python --optional serve"
        )


def create_chart(
    data: list[dict[str, Any]],
    x: str | None = None,
    y: str | list[str] | None = None,
    chart_type: Literal["auto", "bar", "line", "area", "scatter", "point"] = "auto",
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    width: int = 600,
    height: int = 400,
    color_scheme: str | None = None,
) -> Chart:
    """Create a beautiful chart with smart defaults.

    Args:
        data: List of dictionaries containing the data
        x: Column name for x-axis (or None for auto-detection)
        y: Column name(s) for y-axis (or None for auto-detection)
        chart_type: Type of chart ('auto' for smart selection, or specific type)
        title: Chart title
        x_label: X-axis label (defaults to column name)
        y_label: Y-axis label (defaults to column name)
        width: Chart width in pixels
        height: Chart height in pixels
        color_scheme: Color scheme to use (None for default)

    Returns:
        Altair Chart object ready to render

    Examples:
        >>> data = [{"month": "Jan", "revenue": 1000}, {"month": "Feb", "revenue": 1200}]
        >>> chart = create_chart(data, x="month", y="revenue", title="Monthly Revenue")
        >>> vega_json = chart.to_json()  # For MCP transport
        >>> png_bytes = vl_convert.vegalite_to_png(vega_json)  # For image
    """
    check_altair_available()

    if not data:
        raise ValueError("No data provided")

    # Auto-detect columns if not specified
    if x is None or y is None:
        x, y = _auto_detect_columns(data)

    # Ensure y is a list
    y_cols = [y] if isinstance(y, str) else y

    # Auto-select chart type if needed
    if chart_type == "auto":
        chart_type = _select_chart_type(data, x, y_cols)

    # Create base chart
    base = alt.Chart(alt.Data(values=data))

    # Build encoding based on chart type
    if chart_type in ("bar", "line", "area", "point"):
        chart = _create_standard_chart(base, x, y_cols, chart_type, x_label, y_label)
    elif chart_type == "scatter":
        chart = _create_scatter(base, x, y_cols[0], x_label, y_label)
    else:
        raise ValueError(f"Unsupported chart type: {chart_type}")

    # Apply beautiful defaults
    chart = _apply_theme(chart, title, width, height, color_scheme)

    return chart


def _auto_detect_columns(data: list[dict[str, Any]]) -> tuple[str, list[str]]:
    """Auto-detect which columns to use for x and y axes.

    Logic:
    - First column is typically the dimension (x-axis)
    - Remaining numeric columns are metrics (y-axis)
    """
    if not data:
        raise ValueError("Cannot auto-detect columns from empty data")

    columns = list(data[0].keys())

    if len(columns) == 1:
        # Single column - use index for x
        return "index", columns

    # First column is x, rest are y (filter to numeric)
    x_col = columns[0]
    y_cols = []

    for col in columns[1:]:
        # Check if numeric by sampling first value
        value = data[0][col]
        if isinstance(value, (int, float)) and value is not None:
            y_cols.append(col)

    if not y_cols:
        # No numeric columns found, use all remaining columns
        y_cols = columns[1:]

    return x_col, y_cols


def _select_chart_type(
    data: list[dict[str, Any]], x: str, y_cols: list[str]
) -> Literal["bar", "line", "area", "scatter"]:
    """Smart chart type selection based on data characteristics.

    Logic:
    - Time-like x-axis → line/area
    - Categorical x-axis → bar
    - Multiple numeric columns → scatter (if 2 cols) or line (if 3+ cols)
    """
    if not data:
        return "bar"

    x_value = data[0][x]

    # Check if x looks like a time dimension
    if isinstance(x_value, str):
        # Common time indicators
        time_indicators = ["date", "time", "month", "year", "day", "week", "quarter", "created", "updated"]
        if any(indicator in x.lower() for indicator in time_indicators):
            # Line for multiple metrics, area for single metric
            return "line" if len(y_cols) > 1 else "area"

    # Check if x is numeric (scatter plot territory)
    if isinstance(x_value, (int, float)):
        return "scatter"

    # Default to bar for categorical
    return "bar"


def _create_standard_chart(
    base: Chart,
    x: str,
    y_cols: list[str],
    chart_type: Literal["bar", "line", "area", "point"],
    x_label: str | None,
    y_label: str | None,
) -> Chart:
    """Create bar, line, area, or point chart."""
    if len(y_cols) == 1:
        # Single metric - simple encoding
        y_col = y_cols[0]

        # Determine data types
        x_type = _get_encoding_type(x)
        y_type = "quantitative"

        # Create encoding
        encoding = {
            "x": alt.X(f"{x}:{x_type}", title=x_label or _format_label(x)),
            "y": alt.Y(f"{y_col}:{y_type}", title=y_label or _format_label(y_col)),
            "tooltip": [
                alt.Tooltip(f"{x}:{x_type}", title=_format_label(x)),
                alt.Tooltip(f"{y_col}:{y_type}", title=_format_label(y_col), format=",.2f"),
            ],
        }

        # Add color for visual interest in bar charts
        if chart_type == "bar":
            encoding["color"] = alt.Color(f"{x}:{x_type}", legend=None, scale=alt.Scale(range=COLORS["categorical"]))

        # Create mark
        if chart_type == "bar":
            chart = base.mark_bar().encode(**encoding)
        elif chart_type == "line":
            chart = base.mark_line(point=True, strokeWidth=3).encode(**encoding)
        elif chart_type == "area":
            chart = base.mark_area(opacity=0.7, line=True).encode(**encoding)
        else:  # point
            chart = base.mark_point(size=100, filled=True).encode(**encoding)

    else:
        # Multiple metrics - need to transform to long format
        chart = _create_multi_metric_chart(base, x, y_cols, chart_type, x_label, y_label)

    return chart


def _create_multi_metric_chart(
    base: Chart,
    x: str,
    y_cols: list[str],
    chart_type: Literal["bar", "line", "area", "point"],
    x_label: str | None,
    y_label: str | None,
) -> Chart:
    """Create chart with multiple metrics (requires transform to long format)."""
    x_type = _get_encoding_type(x)

    # Transform to long format
    chart = base.transform_fold(y_cols, as_=["metric", "value"]).encode(
        x=alt.X(f"{x}:{x_type}", title=x_label or _format_label(x)),
        y=alt.Y("value:Q", title=y_label or "Value"),
        color=alt.Color("metric:N", title="Metric", scale=alt.Scale(range=COLORS["categorical"])),
        tooltip=[
            alt.Tooltip(f"{x}:{x_type}", title=_format_label(x)),
            alt.Tooltip("metric:N", title="Metric"),
            alt.Tooltip("value:Q", title="Value", format=",.2f"),
        ],
    )

    # Apply mark
    if chart_type == "bar":
        chart = chart.mark_bar()
    elif chart_type == "line":
        chart = chart.mark_line(point=True, strokeWidth=2.5)
    elif chart_type == "area":
        chart = chart.mark_area(opacity=0.6, line=True)
    else:  # point
        chart = chart.mark_point(size=80, filled=True)

    return chart


def _create_scatter(base: Chart, x: str, y: str, x_label: str | None, y_label: str | None) -> Chart:
    """Create scatter plot."""
    return base.mark_circle(size=80, opacity=0.7).encode(
        x=alt.X(f"{x}:Q", title=x_label or _format_label(x)),
        y=alt.Y(f"{y}:Q", title=y_label or _format_label(y)),
        color=alt.value(COLORS["primary"]),
        tooltip=[
            alt.Tooltip(f"{x}:Q", title=_format_label(x), format=",.2f"),
            alt.Tooltip(f"{y}:Q", title=_format_label(y), format=",.2f"),
        ],
    )


def _get_encoding_type(column: str) -> Literal["nominal", "quantitative", "temporal", "ordinal"]:
    """Determine Altair encoding type from column name."""
    # Check for time indicators
    time_indicators = ["date", "time", "month", "year", "day", "week", "quarter", "created", "updated", "timestamp"]
    if any(indicator in column.lower() for indicator in time_indicators):
        return "temporal"

    # Default to nominal for dimensions
    return "nominal"


def _format_label(column: str) -> str:
    """Format column name into readable label.

    Examples:
        order_count → Order Count
        total_revenue → Total Revenue
        created_at__month → Created At (Month)
    """
    # Handle granularity suffix
    if "__" in column:
        base, granularity = column.rsplit("__", 1)
        return f"{_format_label(base)} ({granularity.title()})"

    # Handle model.field format
    if "." in column:
        _, field = column.rsplit(".", 1)
        column = field

    # Convert snake_case to Title Case
    words = column.replace("_", " ").split()
    return " ".join(word.capitalize() for word in words)


def _apply_theme(chart: Chart, title: str | None, width: int, height: int, color_scheme: str | None) -> Chart:
    """Apply beautiful theme and configuration to chart."""
    config = {
        # Font configuration
        "font": "Inter, system-ui, -apple-system, sans-serif",
        # Title styling
        "title": {
            "fontSize": 18,
            "fontWeight": 600,
            "anchor": "start",
            "color": "#1a1a1a",
            "offset": 20,
        },
        # Axis styling
        "axis": {
            "labelFontSize": 12,
            "titleFontSize": 13,
            "titleFontWeight": 500,
            "titleColor": "#4a4a4a",
            "labelColor": "#6a6a6a",
            "gridColor": "#e8e8e8",
            "gridOpacity": 0.6,
            "domainColor": "#cccccc",
            "tickColor": "#cccccc",
            "titlePadding": 12,
            "labelPadding": 8,
        },
        # Legend styling
        "legend": {
            "titleFontSize": 13,
            "titleFontWeight": 500,
            "labelFontSize": 12,
            "titleColor": "#4a4a4a",
            "labelColor": "#6a6a6a",
            "symbolSize": 100,
            "orient": "right",
            "offset": 10,
        },
        # View configuration
        "view": {"strokeWidth": 0},
        # Mark defaults
        "bar": {"cornerRadiusEnd": 2},
        "line": {"strokeCap": "round"},
        "point": {"filled": True},
    }

    chart = chart.configure(**config).properties(width=width, height=height)

    if title:
        chart = chart.properties(title=title)

    return chart


def chart_to_png(chart: Chart) -> bytes:
    """Convert Altair chart to PNG bytes.

    Args:
        chart: Altair Chart object

    Returns:
        PNG image as bytes
    """
    check_altair_available()

    vega_spec = chart.to_json()
    png_data = vl_convert.vegalite_to_png(vega_spec, scale=2)  # 2x for retina displays
    return png_data


def chart_to_base64_png(chart: Chart) -> str:
    """Convert Altair chart to base64-encoded PNG string.

    Useful for embedding in HTML or sending over JSON APIs.

    Args:
        chart: Altair Chart object

    Returns:
        Base64-encoded PNG string (data URL format)
    """
    png_bytes = chart_to_png(chart)
    b64_str = base64.b64encode(png_bytes).decode("utf-8")
    return f"data:image/png;base64,{b64_str}"


def chart_to_vega(chart: Chart) -> dict:
    """Convert Altair chart to Vega-Lite JSON spec.

    This is the preferred format for MCP transport as clients can render it natively.

    Args:
        chart: Altair Chart object

    Returns:
        Vega-Lite specification as dictionary
    """
    check_altair_available()
    return chart.to_dict()
