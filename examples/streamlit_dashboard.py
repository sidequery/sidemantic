#!/usr/bin/env python3
# /// script
# dependencies = [
#   "sidemantic",
#   "duckdb",
#   "streamlit",
#   "plotly",
#   "pandas",
# ]
# ///
"""Interactive Streamlit dashboard with parameterized queries.

Run with: streamlit run examples/streamlit_dashboard.py
"""

from datetime import datetime

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator

# Set page config
st.set_page_config(page_title="Sidemantic Dashboard", layout="wide", initial_sidebar_state="expanded")


@st.cache_resource
def setup_database():
    """Create and populate DuckDB database."""
    conn = duckdb.connect(":memory:")

    # Create sample data
    conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 1, '2024-01-05'::DATE, 150, 'completed'),
            (2, 2, '2024-01-08'::DATE, 230, 'completed'),
            (3, 1, '2024-01-12'::DATE, 180, 'pending'),
            (4, 3, '2024-01-15'::DATE, 95, 'completed'),
            (5, 2, '2024-01-18'::DATE, 310, 'completed'),
            (6, 4, '2024-01-22'::DATE, 420, 'completed'),
            (7, 3, '2024-01-25'::DATE, 175, 'pending'),
            (8, 1, '2024-02-02'::DATE, 280, 'completed'),
            (9, 5, '2024-02-05'::DATE, 195, 'completed'),
            (10, 2, '2024-02-08'::DATE, 350, 'completed'),
            (11, 4, '2024-02-12'::DATE, 125, 'pending'),
            (12, 3, '2024-02-15'::DATE, 450, 'completed'),
            (13, 5, '2024-02-18'::DATE, 210, 'cancelled'),
            (14, 1, '2024-02-22'::DATE, 380, 'completed'),
            (15, 4, '2024-02-25'::DATE, 290, 'completed'),
            (16, 2, '2024-03-01'::DATE, 165, 'completed'),
            (17, 3, '2024-03-05'::DATE, 520, 'completed'),
            (18, 5, '2024-03-08'::DATE, 240, 'pending'),
            (19, 1, '2024-03-12'::DATE, 395, 'completed'),
            (20, 4, '2024-03-15'::DATE, 275, 'completed')
        ) AS t(id, customer_id, order_date, amount, status)
    """)

    conn.execute("""
        CREATE TABLE customers AS
        SELECT * FROM (VALUES
            (1, 'Alice Johnson', 'US', 'premium'),
            (2, 'Bob Smith', 'EU', 'premium'),
            (3, 'Carol White', 'US', 'basic'),
            (4, 'David Brown', 'APAC', 'premium'),
            (5, 'Eve Davis', 'EU', 'basic')
        ) AS t(id, name, region, tier)
    """)

    conn.execute("""
        CREATE TABLE order_items AS
        SELECT * FROM (VALUES
            (1, 1, 101, 2, 50.0),
            (2, 1, 102, 1, 50.0),
            (3, 2, 103, 3, 70.0),
            (4, 2, 101, 1, 20.0),
            (5, 4, 104, 5, 19.0),
            (6, 5, 102, 4, 77.5),
            (7, 6, 101, 6, 70.0),
            (8, 8, 103, 4, 70.0),
            (9, 9, 104, 5, 39.0),
            (10, 10, 101, 7, 50.0),
            (11, 12, 102, 9, 50.0),
            (12, 14, 103, 5, 76.0),
            (13, 16, 104, 5, 33.0),
            (14, 17, 101, 10, 52.0),
            (15, 19, 102, 5, 79.0)
        ) AS t(id, order_id, product_id, quantity, price)
    """)

    return conn


@st.cache_resource
def setup_semantic_layer():
    """Create semantic layer with models and parameters."""
    graph = SemanticGraph()

    # Define parameters
    graph.add_parameter(
        Parameter(
            name="order_status",
            type="string",
            default_value="all",
            description="Filter by order status",
        )
    )

    graph.add_parameter(
        Parameter(
            name="customer_region",
            type="string",
            default_value="all",
            description="Filter by customer region",
        )
    )

    graph.add_parameter(
        Parameter(
            name="customer_tier",
            type="string",
            default_value="all",
            description="Filter by customer tier",
        )
    )

    graph.add_parameter(
        Parameter(
            name="start_date",
            type="date",
            default_value="2024-01-01",
            description="Start date for analysis",
        )
    )

    graph.add_parameter(
        Parameter(
            name="end_date",
            type="date",
            default_value="2024-12-31",
            description="End date for analysis",
        )
    )

    # Orders model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="order_date", type="time", sql="order_date"),
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count", sql="*"),
            Metric(name="avg_order_value", agg="avg", sql="amount"),
        ],
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
            Relationship(name="order_items", type="one_to_many", foreign_key="order_id"),
        ],
    )

    # Customers model
    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[
            Dimension(name="name", type="categorical", sql="name"),
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="tier", type="categorical", sql="tier"),
        ],
        metrics=[Metric(name="customer_count", agg="count", sql="*")],
    )

    # Order items model
    order_items = Model(
        name="order_items",
        table="order_items",
        primary_key="id",
        dimensions=[Dimension(name="product_id", type="numeric", sql="product_id")],
        metrics=[
            Metric(name="total_quantity", agg="sum", sql="quantity"),
            Metric(name="item_count", agg="count", sql="*"),
        ],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
    )

    graph.add_model(orders)
    graph.add_model(customers)
    graph.add_model(order_items)

    # Add metrics
    graph.add_metric(Metric(name="total_revenue", sql="orders.revenue", description="Total revenue from all orders"))

    return graph


def build_filters(status, region, tier, start_date, end_date, include_customer_filters=True):
    """Build filter list based on user selections."""
    filters = []

    # Date range filter (always applied)
    filters.append("orders.order_date >= {{ start_date }}")
    filters.append("orders.order_date <= {{ end_date }}")

    # Status filter
    if status != "all":
        filters.append("orders.status = {{ order_status }}")

    if include_customer_filters:
        # Region filter
        if region != "all":
            filters.append("customers.region = {{ customer_region }}")

        # Tier filter
        if tier != "all":
            filters.append("customers.tier = {{ customer_tier }}")

    return filters


def query_data(conn, generator, metrics, dimensions, filters, parameters, order_by=None):
    """Execute query and return results as DataFrame."""
    sql = generator.generate(
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        parameters=parameters,
        order_by=order_by,
    )

    return conn.execute(sql).fetchdf(), sql


def main():
    """Main Streamlit app."""
    st.title("Sidemantic Interactive Dashboard")
    st.markdown("**Real-time parameterized queries with DuckDB**")

    # Setup
    conn = setup_database()
    graph = setup_semantic_layer()
    generator = SQLGenerator(graph)

    # Sidebar - Filters
    st.sidebar.header("Filters")

    # Date range
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime(2024, 1, 1),
            min_value=datetime(2024, 1, 1),
            max_value=datetime(2024, 12, 31),
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime(2024, 3, 31),
            min_value=datetime(2024, 1, 1),
            max_value=datetime(2024, 12, 31),
        )

    # Status filter
    status = st.sidebar.selectbox("Order Status", ["all", "completed", "pending", "cancelled"], index=0)

    # Region filter
    region = st.sidebar.selectbox("Customer Region", ["all", "US", "EU", "APAC"], index=0)

    # Tier filter
    tier = st.sidebar.selectbox("Customer Tier", ["all", "premium", "basic"], index=0)

    # Build parameters
    parameters = {
        "order_status": status,
        "customer_region": region,
        "customer_tier": tier,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    # Build filters - include customer filters for queries with customer dimensions
    filters_with_customers = build_filters(status, region, tier, start_date, end_date, include_customer_filters=True)
    filters_orders_only = build_filters(status, region, tier, start_date, end_date, include_customer_filters=False)

    # Show active filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("Active Filters")
    if status != "all":
        st.sidebar.markdown(f"Status: **{status}**")
    if region != "all":
        st.sidebar.markdown(f"Region: **{region}**")
    if tier != "all":
        st.sidebar.markdown(f"Tier: **{tier}**")
    st.sidebar.markdown(f"Date: **{start_date}** to **{end_date}**")

    # Main content

    # KPIs
    st.header("Key Metrics")

    kpi_df, kpi_sql = query_data(
        conn,
        generator,
        metrics=["orders.revenue", "orders.order_count", "orders.avg_order_value"],
        dimensions=[],
        filters=filters_orders_only,  # No customer filters for orders-only query
        parameters=parameters,
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Revenue", f"${kpi_df['revenue'].iloc[0]:,.2f}" if not kpi_df.empty else "$0.00")

    with col2:
        st.metric("Total Orders", f"{int(kpi_df['order_count'].iloc[0]):,}" if not kpi_df.empty else "0")

    with col3:
        st.metric(
            "Avg Order Value",
            f"${kpi_df['avg_order_value'].iloc[0]:,.2f}" if not kpi_df.empty else "$0.00",
        )

    # Revenue over time
    st.header("Revenue Trend")

    time_df, time_sql = query_data(
        conn,
        generator,
        metrics=["orders.revenue", "orders.order_count"],
        dimensions=["orders.order_date"],
        filters=filters_orders_only,
        parameters=parameters,
        order_by=["orders.order_date"],
    )

    if not time_df.empty:
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=time_df["order_date"],
                y=time_df["revenue"],
                mode="lines+markers",
                name="Revenue",
                line=dict(color="#3498db", width=3),
                marker=dict(size=8),
            )
        )

        fig.update_layout(
            title="Daily Revenue",
            xaxis_title="Date",
            yaxis_title="Revenue ($)",
            hovermode="x unified",
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for the selected filters.")

    # Two columns for charts
    col1, col2 = st.columns(2)

    # Revenue by region
    with col1:
        st.subheader("Revenue by Region")

        region_df, region_sql = query_data(
            conn,
            generator,
            metrics=["orders.revenue", "orders.order_count"],
            dimensions=["customers.region"],
            filters=filters_with_customers,  # Needs customers join
            parameters=parameters,
        )

        if not region_df.empty:
            fig = px.pie(
                region_df,
                values="revenue",
                names="region",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available.")

    # Revenue by tier
    with col2:
        st.subheader("Revenue by Tier")

        tier_df, tier_sql = query_data(
            conn,
            generator,
            metrics=["orders.revenue", "customers.customer_count"],
            dimensions=["customers.tier"],
            filters=filters_with_customers,  # Needs customers join
            parameters=parameters,
        )

        if not tier_df.empty:
            fig = px.bar(
                tier_df,
                x="tier",
                y="revenue",
                color="tier",
                color_discrete_sequence=["#3498db", "#e74c3c"],
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available.")

    # Monthly breakdown
    st.header("Monthly Breakdown")

    monthly_df, monthly_sql = query_data(
        conn,
        generator,
        metrics=["orders.revenue", "orders.order_count", "orders.avg_order_value"],
        dimensions=["orders.order_date__month"],
        filters=filters_orders_only,
        parameters=parameters,
        order_by=["orders.order_date__month"],
    )

    if not monthly_df.empty:
        # Format the dataframe
        display_df = monthly_df.copy()
        display_df["order_date__month"] = pd.to_datetime(display_df["order_date__month"]).dt.strftime("%B %Y")
        display_df["revenue"] = display_df["revenue"].apply(lambda x: f"${x:,.2f}")
        display_df["avg_order_value"] = display_df["avg_order_value"].apply(lambda x: f"${x:,.2f}")
        display_df.columns = ["Month", "Revenue", "Orders", "Avg Order Value"]

        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No data available.")

    # Show SQL (expandable)
    with st.expander("View Generated SQL"):
        st.code(kpi_sql, language="sql")
        st.caption("This SQL was generated from your semantic layer and parameter selections.")

    # Symmetric aggregates demo
    st.header("Symmetric Aggregates Demo")
    st.markdown("Querying across **orders + order_items** (fan-out scenario)")

    fanout_df, fanout_sql = query_data(
        conn,
        generator,
        metrics=["orders.revenue", "order_items.total_quantity", "order_items.item_count"],
        dimensions=["orders.order_date__month"],
        filters=filters_orders_only,
        parameters=parameters,
        order_by=["orders.order_date__month"],
    )

    col1, col2 = st.columns([2, 1])

    with col1:
        if not fanout_df.empty:
            st.dataframe(fanout_df, use_container_width=True, hide_index=True)
            st.success("Revenue is correctly calculated using symmetric aggregates!")
        else:
            st.info("No data available.")

    with col2:
        st.info("""
        **Why Symmetric Aggregates?**

        When joining orders to items (one-to-many),
        naive aggregation would multiply revenue by
        the number of items.

        Sidemantic automatically uses symmetric
        aggregates to prevent double-counting!
        """)

    with st.expander("View Symmetric Aggregate SQL"):
        st.code(fanout_sql, language="sql")
        if "HASH" in fanout_sql:
            st.success("Symmetric aggregates detected! Look for HASH() function.")
        else:
            st.info("No symmetric aggregates needed for this query.")


if __name__ == "__main__":
    main()
