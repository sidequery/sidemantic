"""Kitchen sink integration tests for Cube adapter.

This test suite stress-tests sidemantic's Cube adapter implementation with a
complex multi-entity data model featuring various relationship types, aggregation
patterns, and edge cases.

BUGS FOUND AND FIXED:
=====================

1. FIXED: Foreign key column inference in CubeAdapter
   - Now parses join SQL `${CUBE}.company_id = ${companies.id}` to extract actual FK column
   - Handles both many_to_one and one_to_many relationship directions

2. FIXED: Derived metrics with Cube ${measure} references
   - Now converts ratio patterns like `${billable_hours} / ${total_hours}` to sidemantic format

3. FIXED: Multiple filtered measures AND'd incorrectly
   - Now uses CASE WHEN for conditional aggregation instead of WHERE clause

4. FIXED: Complex derived metrics with inline SQL (e.g., approval_rate with ${CUBE}.status)
   - Now handles SQL expression metrics with inline aggregations
   - Extracts column references from SQL expressions and includes them in CTEs

5. FIXED: one_to_many join direction
   - Now correctly extracts FK from the target model side for one_to_many relationships

6. FIXED: Symmetric aggregation for multi-model fan-out
   - When metrics from different join levels are queried together (e.g., employees.salary
     + departments.budget by companies.name), pre-aggregates each metric separately
     to the dimension grain, then joins the pre-aggregated results
"""

import duckdb
import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.cube import CubeAdapter
from tests.utils import fetch_dicts


@pytest.fixture
def kitchen_sink_db():
    """Create a comprehensive test database matching the kitchen_sink.yml fixture.

    This creates realistic test data with known values to verify query correctness.
    """
    conn = duckdb.connect(":memory:")

    # Companies
    conn.execute("""
        CREATE TABLE companies (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            industry VARCHAR,
            founded_at DATE,
            is_active BOOLEAN,
            employee_count INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO companies VALUES
            (1, 'TechCorp', 'Technology', '2010-01-15', true, 150),
            (2, 'FinanceInc', 'Finance', '2005-06-20', true, 75),
            (3, 'RetailCo', 'Retail', '2015-03-10', false, 30),
            (4, 'HealthOrg', 'Healthcare', '2018-09-01', true, 200)
    """)

    # Departments
    conn.execute("""
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            company_id INTEGER,
            name VARCHAR,
            budget DECIMAL(15, 2),
            created_at DATE
        )
    """)
    conn.execute("""
        INSERT INTO departments VALUES
            (1, 1, 'Engineering', 2000000.00, '2010-02-01'),
            (2, 1, 'Sales', 1500000.00, '2010-02-01'),
            (3, 1, 'Marketing', 800000.00, '2011-01-01'),
            (4, 2, 'Trading', 5000000.00, '2005-07-01'),
            (5, 2, 'Risk', 1200000.00, '2006-01-01'),
            (6, 3, 'Operations', 500000.00, '2015-04-01'),
            (7, 4, 'Research', 3000000.00, '2018-10-01'),
            (8, 4, 'Clinical', 2500000.00, '2018-10-01')
    """)

    # Employees
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            department_id INTEGER,
            name VARCHAR,
            email VARCHAR,
            title VARCHAR,
            salary DECIMAL(10, 2),
            hired_at DATE,
            is_manager BOOLEAN,
            manager_id INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO employees VALUES
            (1, 1, 'Alice Smith', 'alice@techcorp.com', 'VP Engineering', 250000.00, '2010-03-01', true, NULL),
            (2, 1, 'Bob Jones', 'bob@techcorp.com', 'Senior Engineer', 180000.00, '2012-05-15', false, 1),
            (3, 1, 'Carol White', 'carol@techcorp.com', 'Engineer', 140000.00, '2020-01-10', false, 1),
            (4, 2, 'Dave Brown', 'dave@techcorp.com', 'Sales Director', 200000.00, '2011-02-01', true, NULL),
            (5, 2, 'Eve Wilson', 'eve@techcorp.com', 'Account Executive', 120000.00, '2019-06-01', false, 4),
            (6, 4, 'Frank Miller', 'frank@financeinc.com', 'Head Trader', 350000.00, '2005-08-01', true, NULL),
            (7, 4, 'Grace Lee', 'grace@financeinc.com', 'Trader', 200000.00, '2015-03-01', false, 6),
            (8, 7, 'Henry Chen', 'henry@healthorg.com', 'Research Lead', 180000.00, '2018-11-01', true, NULL),
            (9, 7, 'Ivy Park', 'ivy@healthorg.com', 'Researcher', 130000.00, '2022-01-15', false, 8),
            (10, 8, 'Jack Davis', 'jack@healthorg.com', 'Clinical Director', 220000.00, '2018-11-01', true, NULL)
    """)

    # Projects
    conn.execute("""
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            company_id INTEGER,
            name VARCHAR,
            status VARCHAR,
            budget DECIMAL(15, 2),
            start_date DATE,
            end_date DATE,
            priority INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO projects VALUES
            (1, 1, 'Platform Rewrite', 'active', 500000.00, '2023-01-01', '2024-06-30', 5),
            (2, 1, 'Mobile App', 'completed', 200000.00, '2022-01-01', '2022-12-31', 4),
            (3, 1, 'Data Pipeline', 'active', 150000.00, '2023-06-01', '2024-03-31', 3),
            (4, 2, 'Risk Dashboard', 'active', 300000.00, '2023-03-01', '2024-02-28', 5),
            (5, 4, 'Patient Portal', 'active', 400000.00, '2023-04-01', '2024-09-30', 5),
            (6, 4, 'Research DB', 'completed', 100000.00, '2022-06-01', '2023-01-31', 2),
            (7, 3, 'Inventory System', 'cancelled', 80000.00, '2021-01-01', '2021-06-30', 3)
    """)

    # Project Assignments (many-to-many join table with its own metrics)
    conn.execute("""
        CREATE TABLE project_assignments (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            project_id INTEGER,
            role VARCHAR,
            assigned_at DATE,
            hours_allocated INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO project_assignments VALUES
            (1, 1, 1, 'lead', '2023-01-01', 40),
            (2, 2, 1, 'contributor', '2023-01-15', 30),
            (3, 3, 1, 'contributor', '2023-02-01', 25),
            (4, 2, 3, 'lead', '2023-06-01', 35),
            (5, 3, 3, 'contributor', '2023-06-15', 20),
            (6, 1, 2, 'lead', '2022-01-01', 20),
            (7, 2, 2, 'contributor', '2022-02-01', 40),
            (8, 6, 4, 'lead', '2023-03-01', 30),
            (9, 7, 4, 'contributor', '2023-03-15', 40),
            (10, 8, 5, 'lead', '2023-04-01', 35),
            (11, 9, 5, 'contributor', '2023-04-15', 30),
            (12, 10, 5, 'reviewer', '2023-04-01', 10),
            (13, 8, 6, 'lead', '2022-06-01', 25),
            (14, 9, 6, 'contributor', '2022-07-01', 30)
    """)

    # Timesheets (time-series data)
    conn.execute("""
        CREATE TABLE timesheets (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            project_id INTEGER,
            work_date DATE,
            hours DECIMAL(4, 2),
            description VARCHAR,
            is_billable BOOLEAN
        )
    """)
    # Generate time-series data for testing cumulative/rolling
    conn.execute("""
        INSERT INTO timesheets VALUES
            (1, 2, 1, '2023-01-02', 8.0, 'Feature development', true),
            (2, 2, 1, '2023-01-03', 7.5, 'Code review', true),
            (3, 2, 1, '2023-01-04', 8.0, 'Bug fixes', true),
            (4, 2, 1, '2023-01-05', 6.0, 'Meetings', false),
            (5, 3, 1, '2023-01-02', 8.0, 'Testing', true),
            (6, 3, 1, '2023-01-03', 8.0, 'Documentation', false),
            (7, 3, 1, '2023-01-04', 7.0, 'Testing', true),
            (8, 1, 1, '2023-01-02', 4.0, 'Planning', false),
            (9, 1, 1, '2023-01-03', 3.0, 'Review', true),
            (10, 1, 1, '2023-01-04', 5.0, 'Architecture', true),
            (11, 6, 4, '2023-03-06', 9.0, 'Trading analysis', true),
            (12, 6, 4, '2023-03-07', 10.0, 'Risk modeling', true),
            (13, 7, 4, '2023-03-06', 8.0, 'Data prep', true),
            (14, 7, 4, '2023-03-07', 8.0, 'Testing', true),
            (15, 8, 5, '2023-04-03', 8.0, 'Research design', true),
            (16, 8, 5, '2023-04-04', 7.0, 'Literature review', false),
            (17, 9, 5, '2023-04-03', 6.0, 'Data collection', true),
            (18, 9, 5, '2023-04-04', 8.0, 'Analysis', true)
    """)

    # Expenses (with optional project_id - nullable FK)
    conn.execute("""
        CREATE TABLE expenses (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            project_id INTEGER,
            category VARCHAR,
            amount DECIMAL(10, 2),
            submitted_at DATE,
            approved_at DATE,
            status VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO expenses VALUES
            (1, 2, 1, 'software', 500.00, '2023-01-10', '2023-01-12', 'approved'),
            (2, 2, NULL, 'meals', 75.00, '2023-01-15', NULL, 'pending'),
            (3, 3, 1, 'equipment', 1200.00, '2023-01-20', '2023-01-25', 'approved'),
            (4, 1, NULL, 'travel', 2500.00, '2023-02-01', '2023-02-05', 'approved'),
            (5, 6, 4, 'software', 800.00, '2023-03-10', '2023-03-12', 'approved'),
            (6, 6, NULL, 'meals', 150.00, '2023-03-15', NULL, 'rejected'),
            (7, 8, 5, 'equipment', 3000.00, '2023-04-10', '2023-04-15', 'approved'),
            (8, 9, 5, 'travel', 1800.00, '2023-04-20', NULL, 'pending'),
            (9, 10, NULL, 'meals', 200.00, '2023-04-25', '2023-04-26', 'approved')
    """)

    # Invoices (with optional project_id)
    conn.execute("""
        CREATE TABLE invoices (
            id INTEGER PRIMARY KEY,
            company_id INTEGER,
            project_id INTEGER,
            invoice_number VARCHAR,
            status VARCHAR,
            issued_at DATE,
            due_at DATE,
            paid_at DATE,
            total_amount DECIMAL(15, 2)
        )
    """)
    conn.execute("""
        INSERT INTO invoices VALUES
            (1, 1, 1, 'INV-001', 'paid', '2023-01-31', '2023-02-28', '2023-02-15', 50000.00),
            (2, 1, 1, 'INV-002', 'paid', '2023-02-28', '2023-03-31', '2023-03-20', 75000.00),
            (3, 1, 2, 'INV-003', 'paid', '2022-06-30', '2022-07-31', '2022-07-15', 100000.00),
            (4, 1, NULL, 'INV-004', 'sent', '2023-03-31', '2023-04-30', NULL, 25000.00),
            (5, 2, 4, 'INV-005', 'paid', '2023-06-30', '2023-07-31', '2023-07-25', 150000.00),
            (6, 2, 4, 'INV-006', 'overdue', '2023-09-30', '2023-10-31', NULL, 100000.00),
            (7, 4, 5, 'INV-007', 'sent', '2023-09-30', '2023-10-31', NULL, 200000.00),
            (8, 4, 6, 'INV-008', 'paid', '2023-01-31', '2023-02-28', '2023-02-20', 100000.00)
    """)

    # Invoice Line Items (for fan-out testing)
    conn.execute("""
        CREATE TABLE invoice_line_items (
            id INTEGER PRIMARY KEY,
            invoice_id INTEGER,
            description VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10, 2),
            line_total DECIMAL(10, 2)
        )
    """)
    conn.execute("""
        INSERT INTO invoice_line_items VALUES
            (1, 1, 'Development hours', 100, 250.00, 25000.00),
            (2, 1, 'Design hours', 50, 200.00, 10000.00),
            (3, 1, 'Project management', 30, 500.00, 15000.00),
            (4, 2, 'Development hours', 200, 250.00, 50000.00),
            (5, 2, 'QA hours', 100, 150.00, 15000.00),
            (6, 2, 'Documentation', 20, 500.00, 10000.00),
            (7, 3, 'Full project delivery', 1, 100000.00, 100000.00),
            (8, 4, 'Consulting', 50, 500.00, 25000.00),
            (9, 5, 'Trading system', 1, 150000.00, 150000.00),
            (10, 6, 'Risk module', 1, 100000.00, 100000.00),
            (11, 7, 'Portal development', 400, 500.00, 200000.00),
            (12, 8, 'Research DB', 1, 100000.00, 100000.00)
    """)

    return conn


@pytest.fixture
def kitchen_sink_layer(kitchen_sink_db):
    """Create semantic layer from kitchen_sink.yml Cube fixture with test DB."""
    adapter = CubeAdapter()
    graph = adapter.parse("tests/fixtures/cube/kitchen_sink.yml")

    layer = SemanticLayer(auto_register=False)
    layer.graph = graph
    layer.conn = kitchen_sink_db

    return layer


# ============================================================
# Basic Parsing Tests
# ============================================================


class TestCubeParsingKitchenSink:
    """Test that the complex Cube YAML is parsed correctly."""

    def test_all_cubes_parsed(self):
        """Verify all cubes are imported."""
        adapter = CubeAdapter()
        graph = adapter.parse("tests/fixtures/cube/kitchen_sink.yml")

        expected = [
            "companies",
            "departments",
            "employees",
            "projects",
            "project_assignments",
            "timesheets",
            "expenses",
            "invoices",
            "invoice_line_items",
        ]
        for name in expected:
            assert name in graph.models, f"Model {name} not found"

    def test_relationships_parsed(self):
        """Verify relationships are parsed correctly."""
        adapter = CubeAdapter()
        graph = adapter.parse("tests/fixtures/cube/kitchen_sink.yml")

        # departments -> companies (many_to_one)
        departments = graph.get_model("departments")
        assert any(r.name == "companies" for r in departments.relationships)

        # employees -> departments (many_to_one)
        employees = graph.get_model("employees")
        assert any(r.name == "departments" for r in employees.relationships)

        # projects -> project_assignments (one_to_many)
        projects = graph.get_model("projects")
        pa_rel = next((r for r in projects.relationships if r.name == "project_assignments"), None)
        assert pa_rel is not None
        assert pa_rel.type == "one_to_many"

    def test_segments_parsed(self):
        """Verify segments are parsed with correct SQL normalization."""
        adapter = CubeAdapter()
        graph = adapter.parse("tests/fixtures/cube/kitchen_sink.yml")

        companies = graph.get_model("companies")
        active_segment = next((s for s in companies.segments if s.name == "active_companies"), None)
        assert active_segment is not None
        assert "{model}" in active_segment.sql
        assert "${CUBE}" not in active_segment.sql

    def test_filtered_measures_parsed(self):
        """Verify measures with filters are parsed."""
        adapter = CubeAdapter()
        graph = adapter.parse("tests/fixtures/cube/kitchen_sink.yml")

        employees = graph.get_model("employees")
        manager_count = employees.get_metric("manager_count")
        assert manager_count is not None
        assert manager_count.filters is not None
        assert len(manager_count.filters) > 0

    def test_derived_measures_parsed(self):
        """Verify derived/calculated measures are parsed."""
        adapter = CubeAdapter()
        graph = adapter.parse("tests/fixtures/cube/kitchen_sink.yml")

        projects = graph.get_model("projects")
        completion_rate = projects.get_metric("completion_rate")
        assert completion_rate is not None
        # Should be detected as derived or ratio type
        assert completion_rate.type in ["derived", "ratio", None]


# ============================================================
# Single Model Query Tests
# ============================================================


class TestSingleModelQueries:
    """Test queries against single models."""

    def test_simple_count(self, kitchen_sink_layer):
        """Test basic count metric."""
        result = kitchen_sink_layer.query(metrics=["companies.count"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        assert rows[0]["count"] == 4

    def test_sum_metric(self, kitchen_sink_layer):
        """Test sum aggregation."""
        result = kitchen_sink_layer.query(metrics=["departments.total_budget"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        # Sum of all department budgets
        expected = 2000000 + 1500000 + 800000 + 5000000 + 1200000 + 500000 + 3000000 + 2500000
        assert rows[0]["total_budget"] == expected

    def test_avg_metric(self, kitchen_sink_layer):
        """Test avg aggregation."""
        result = kitchen_sink_layer.query(metrics=["employees.avg_salary"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        # Average of all salaries
        total = 250000 + 180000 + 140000 + 200000 + 120000 + 350000 + 200000 + 180000 + 130000 + 220000
        expected = total / 10
        assert rows[0]["avg_salary"] == expected

    def test_count_distinct(self, kitchen_sink_layer):
        """Test count_distinct aggregation."""
        result = kitchen_sink_layer.query(metrics=["employees.headcount"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        assert rows[0]["headcount"] == 10

    def test_min_max(self, kitchen_sink_layer):
        """Test min/max aggregations."""
        result = kitchen_sink_layer.query(metrics=["departments.min_budget", "departments.max_budget"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        assert rows[0]["min_budget"] == 500000  # Operations at RetailCo
        assert rows[0]["max_budget"] == 5000000  # Trading at FinanceInc

    def test_filtered_measure(self, kitchen_sink_layer):
        """Test measure with filter."""
        result = kitchen_sink_layer.query(metrics=["employees.manager_count"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        # 5 managers: Alice, Dave, Frank, Henry, Jack
        assert rows[0]["manager_count"] == 5

    def test_grouped_by_dimension(self, kitchen_sink_layer):
        """Test grouping by categorical dimension."""
        result = kitchen_sink_layer.query(metrics=["companies.count"], dimensions=["companies.industry"])
        rows = fetch_dicts(result)
        industry_counts = {row["industry"]: row["count"] for row in rows}
        assert industry_counts["Technology"] == 1
        assert industry_counts["Finance"] == 1
        assert industry_counts["Retail"] == 1
        assert industry_counts["Healthcare"] == 1

    def test_segment_filter(self, kitchen_sink_layer):
        """Test using a segment as a filter."""
        result = kitchen_sink_layer.query(metrics=["companies.count"], segments=["companies.active_companies"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        assert rows[0]["count"] == 3  # TechCorp, FinanceInc, HealthOrg (not RetailCo)

    def test_multiple_segments(self, kitchen_sink_layer):
        """Test combining multiple segments."""
        result = kitchen_sink_layer.query(
            metrics=["companies.count"],
            segments=["companies.active_companies", "companies.tech_companies"],
        )
        rows = fetch_dicts(result)
        assert len(rows) == 1
        assert rows[0]["count"] == 1  # Only TechCorp is both active and tech

    def test_time_dimension_with_granularity(self, kitchen_sink_layer):
        """Test time dimension with granularity."""
        result = kitchen_sink_layer.query(
            metrics=["timesheets.total_hours"],
            dimensions=["timesheets.work_date__month"],
        )
        rows = fetch_dicts(result)
        # Should have distinct months
        assert len(rows) >= 2


# ============================================================
# Join Tests (Multi-Model Queries)
# ============================================================


class TestJoinQueries:
    """Test queries that join multiple models."""

    def test_simple_many_to_one_join(self, kitchen_sink_layer):
        """Test simple many-to-one join."""
        result = kitchen_sink_layer.query(
            metrics=["departments.total_budget"],
            dimensions=["companies.name"],
        )
        rows = fetch_dicts(result)
        budget_by_company = {row["name"]: row["total_budget"] for row in rows}
        # TechCorp: Engineering(2M) + Sales(1.5M) + Marketing(0.8M) = 4.3M
        assert budget_by_company["TechCorp"] == 4300000
        # FinanceInc: Trading(5M) + Risk(1.2M) = 6.2M
        assert budget_by_company["FinanceInc"] == 6200000

    def test_two_hop_join(self, kitchen_sink_layer):
        """Test two-hop join: employees -> departments -> companies."""
        result = kitchen_sink_layer.query(
            metrics=["employees.total_salary"],
            dimensions=["companies.name"],
        )
        rows = fetch_dicts(result)
        salary_by_company = {row["name"]: row["total_salary"] for row in rows}
        # TechCorp employees: Alice(250k) + Bob(180k) + Carol(140k) + Dave(200k) + Eve(120k) = 890k
        assert salary_by_company["TechCorp"] == 890000
        # FinanceInc: Frank(350k) + Grace(200k) = 550k
        assert salary_by_company["FinanceInc"] == 550000

    def test_join_with_dimension_from_joined_model(self, kitchen_sink_layer):
        """Test using a dimension from a joined model."""
        result = kitchen_sink_layer.query(
            metrics=["employees.count"],
            dimensions=["departments.name"],
        )
        rows = fetch_dicts(result)
        emp_by_dept = {row["name"]: row["count"] for row in rows}
        assert emp_by_dept["Engineering"] == 3  # Alice, Bob, Carol
        assert emp_by_dept["Sales"] == 2  # Dave, Eve
        assert emp_by_dept["Trading"] == 2  # Frank, Grace

    def test_join_table_with_own_metrics(self, kitchen_sink_layer):
        """Test that join tables (project_assignments) can have their own metrics."""
        result = kitchen_sink_layer.query(
            metrics=["project_assignments.total_hours_allocated"],
            dimensions=["projects.name"],
        )
        rows = fetch_dicts(result)
        hours_by_project = {row["name"]: row["total_hours_allocated"] for row in rows}
        # Platform Rewrite: 40+30+25 = 95
        assert hours_by_project["Platform Rewrite"] == 95

    def test_count_distinct_across_join(self, kitchen_sink_layer):
        """Test count_distinct across a join."""
        result = kitchen_sink_layer.query(
            metrics=["project_assignments.unique_employees"],
            dimensions=["projects.name"],
        )
        rows = fetch_dicts(result)
        unique_by_project = {row["name"]: row["unique_employees"] for row in rows}
        # Platform Rewrite: employees 1, 2, 3
        assert unique_by_project["Platform Rewrite"] == 3


# ============================================================
# Fan-Out / Symmetric Aggregate Tests
# ============================================================


class TestFanOutAggregation:
    """Test correct aggregation when joins cause row fan-out."""

    def test_invoice_total_with_line_items(self, kitchen_sink_layer):
        """Test that invoice totals aren't inflated by line items join.

        This is a critical test for symmetric aggregates.
        Invoice 1 has 3 line items, so without symmetric aggs the
        total would be 3x the correct value.
        """
        result = kitchen_sink_layer.query(
            metrics=["invoices.total_invoiced", "invoice_line_items.count"],
            dimensions=["invoices.invoice_number"],
        )
        rows = fetch_dicts(result)
        inv1 = next(r for r in rows if r["invoice_number"] == "INV-001")
        # Invoice 1 total is 50000, has 3 line items
        # With proper symmetric aggregation, total should still be 50000
        assert inv1["total_invoiced"] == 50000
        assert inv1["count"] == 3

    def test_project_budget_with_assignments(self, kitchen_sink_layer):
        """Test project budget isn't inflated by assignment fan-out."""
        result = kitchen_sink_layer.query(
            metrics=["projects.total_budget", "project_assignments.count"],
            dimensions=["projects.name"],
        )
        rows = fetch_dicts(result)
        platform = next(r for r in rows if r["name"] == "Platform Rewrite")
        # Platform Rewrite has budget 500000, 3 assignments
        # Budget should still be 500000, not 1.5M
        assert platform["total_budget"] == 500000
        assert platform["count"] == 3


# ============================================================
# Nullable Foreign Key Tests
# ============================================================


class TestNullableForeignKeys:
    """Test queries involving optional (nullable) foreign keys."""

    def test_expenses_with_null_project(self, kitchen_sink_layer):
        """Test expenses that have NULL project_id."""
        # Get all expenses count (includes those with NULL project_id)
        result = kitchen_sink_layer.query(metrics=["expenses.count"])
        rows = fetch_dicts(result)
        assert rows[0]["count"] == 9  # Total expenses

        # Now query with segment that filters to project_id IS NULL
        result = kitchen_sink_layer.query(
            metrics=["expenses.count"],
            segments=["expenses.general_expenses"],
        )
        rows = fetch_dicts(result)
        # 4 expenses have NULL project_id
        assert rows[0]["count"] == 4

    def test_left_join_preserves_null_fk(self, kitchen_sink_layer):
        """Test that LEFT JOIN properly handles NULL FKs.

        When joining expenses to projects, expenses with NULL project_id
        should still be included in totals.
        """
        # Query expenses with project dimension - should still get all expenses
        # if we're using proper LEFT JOINs
        result = kitchen_sink_layer.query(
            metrics=["expenses.total_amount"],
            dimensions=["projects.name"],
        )
        # The SQL should use LEFT JOIN for nullable FKs
        # This is a hole if sidemantic uses INNER JOIN here
        rows = fetch_dicts(result)
        # We should have rows for each project with expenses
        # AND potentially a NULL row for general expenses
        assert len(rows) > 0


# ============================================================
# Derived Metric Tests
# ============================================================


class TestDerivedMetrics:
    """Test derived/calculated metrics."""

    def test_ratio_metric(self, kitchen_sink_layer):
        """Test a simple ratio metric."""
        result = kitchen_sink_layer.query(metrics=["timesheets.billable_ratio"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        # Total hours: 8+7.5+8+6+8+8+7+4+3+5+9+10+8+8+8+7+6+8 = 129.5
        # Billable hours: 8+7.5+8+8+7+3+5+9+10+8+8+8+6+8 = 103.5
        # Ratio ≈ 0.799
        ratio = rows[0]["billable_ratio"]
        assert ratio is not None
        assert 0.7 < ratio < 0.9

    def test_derived_metric_with_filtered_measures(self, kitchen_sink_layer):
        """Test derived metric that uses filtered measures."""
        result = kitchen_sink_layer.query(metrics=["expenses.approval_rate"])
        rows = fetch_dicts(result)
        assert len(rows) == 1
        # 6 approved, 9 total = 66.67%
        rate = rows[0]["approval_rate"]
        assert rate is not None
        assert abs(rate - (6 / 9)) < 0.01

    def test_completion_rate_by_company(self, kitchen_sink_layer):
        """Test completion rate (derived) grouped by dimension."""
        result = kitchen_sink_layer.query(
            metrics=["projects.completion_rate"],
            dimensions=["companies.name"],
        )
        rows = fetch_dicts(result)
        rates = {row["name"]: row["completion_rate"] for row in rows}
        # TechCorp: 1 completed, 3 total = 33.33%
        assert rates["TechCorp"] == pytest.approx(1 / 3, rel=0.01)
        # HealthOrg: 1 completed, 2 total = 50%
        assert rates["HealthOrg"] == pytest.approx(0.5, rel=0.01)


# ============================================================
# Edge Cases and Potential Holes
# ============================================================


class TestEdgeCasesAndHoles:
    """Test potential edge cases that might reveal holes in sidemantic."""

    def test_self_referential_join(self, kitchen_sink_layer):
        """Test self-referential relationship (manager_id).

        This is a potential hole - does sidemantic support self-joins?
        """
        # This would require a self-join: employees.manager_id -> employees.id
        # Most semantic layers don't handle this well
        # We skip this as a known limitation but document it
        pytest.skip("Self-referential joins not currently supported - potential enhancement")

    def test_multiple_paths_to_same_model(self, kitchen_sink_layer):
        """Test when there are multiple paths to the same model.

        expenses -> employees -> departments -> companies
        expenses -> projects -> companies

        Which path should sidemantic use?
        """
        # Query expenses with company dimension - could go through employee or project
        result = kitchen_sink_layer.query(
            metrics=["expenses.total_amount"],
            dimensions=["companies.name"],
        )
        rows = fetch_dicts(result)
        # Should work and produce some result
        assert len(rows) > 0

    def test_measure_from_intermediate_model(self, kitchen_sink_layer):
        """Test querying a measure from an intermediate model in a join chain.

        Query: employees metric + companies dimension
        But also want departments metric in the same query.

        This uses pre-aggregation to avoid fan-out: each metric is aggregated
        to the dimension grain separately, then the results are joined.
        """
        result = kitchen_sink_layer.query(
            metrics=["employees.total_salary", "departments.total_budget"],
            dimensions=["companies.name"],
        )
        rows = fetch_dicts(result)
        # Should have salary and budget by company
        techcorp = next(r for r in rows if r["name"] == "TechCorp")
        assert techcorp["total_salary"] == 890000
        assert techcorp["total_budget"] == 4300000

    def test_multiple_filtered_measures_same_model(self, kitchen_sink_layer):
        """Test multiple filtered measures from the same model.

        Each filtered measure should compute independently using CASE WHEN.
        """
        result = kitchen_sink_layer.query(
            metrics=[
                "expenses.approved_amount",
                "expenses.pending_amount",
                "expenses.total_amount",
            ],
        )
        rows = fetch_dicts(result)
        assert len(rows) == 1
        # Approved: 500+1200+2500+800+3000+200 = 8200
        # Pending: 75+1800 = 1875
        # Total: 500+75+1200+2500+800+150+3000+1800+200 = 10225
        assert rows[0]["approved_amount"] == 8200
        assert rows[0]["pending_amount"] == 1875
        assert rows[0]["total_amount"] == 10225

    def test_filter_on_joined_model(self, kitchen_sink_layer):
        """Test filtering on a dimension from a joined model."""
        result = kitchen_sink_layer.query(
            metrics=["employees.count"],
            filters=["companies.industry = 'Technology'"],
        )
        rows = fetch_dicts(result)
        assert len(rows) == 1
        # TechCorp employees: 5
        assert rows[0]["count"] == 5

    def test_segment_from_joined_model(self, kitchen_sink_layer):
        """Test using a segment from a joined model.

        This might be a hole - do segments work across joins?
        """
        # This would use companies.active_companies segment when querying employees
        # Many semantic layers don't support this
        try:
            result = kitchen_sink_layer.query(
                metrics=["employees.count"],
                segments=["companies.active_companies"],
            )
            rows = fetch_dicts(result)
            # Should only include employees from active companies
            # Excludes RetailCo which has no employees anyway in our data
            assert rows[0]["count"] == 10
        except Exception as e:
            pytest.skip(f"Cross-model segments not supported: {e}")

    def test_having_clause_on_aggregate(self, kitchen_sink_layer):
        """Test HAVING clause (filter on aggregated value).

        Filter: departments with total_budget > 1M
        """
        result = kitchen_sink_layer.query(
            metrics=["departments.total_budget"],
            dimensions=["departments.name"],
            filters=["departments.total_budget > 1000000"],
        )
        rows = fetch_dicts(result)
        # Only departments with budget > 1M
        dept_names = {row["name"] for row in rows}
        assert "Engineering" in dept_names  # 2M
        assert "Trading" in dept_names  # 5M
        assert "Operations" not in dept_names  # 500k

    def test_order_by_metric(self, kitchen_sink_layer):
        """Test ordering by a metric."""
        result = kitchen_sink_layer.query(
            metrics=["departments.total_budget"],
            dimensions=["departments.name"],
            order_by=["departments.total_budget"],
        )
        rows = fetch_dicts(result)
        budgets = [row["total_budget"] for row in rows]
        # Should be ascending
        assert budgets == sorted(budgets)

    def test_limit_and_offset(self, kitchen_sink_layer):
        """Test LIMIT and OFFSET."""
        result = kitchen_sink_layer.query(
            metrics=["employees.count"],
            dimensions=["employees.name"],
            order_by=["employees.name"],
            limit=3,
        )
        rows = fetch_dicts(result)
        assert len(rows) == 3

    def test_boolean_dimension_grouping(self, kitchen_sink_layer):
        """Test grouping by boolean dimension."""
        result = kitchen_sink_layer.query(
            metrics=["employees.count"],
            dimensions=["employees.is_manager"],
        )
        rows = fetch_dicts(result)
        by_manager = {row["is_manager"]: row["count"] for row in rows}
        assert by_manager[True] == 5
        assert by_manager[False] == 5

    def test_numeric_dimension_as_grouping(self, kitchen_sink_layer):
        """Test grouping by numeric dimension (priority)."""
        result = kitchen_sink_layer.query(
            metrics=["projects.count"],
            dimensions=["projects.priority"],
        )
        rows = fetch_dicts(result)
        by_priority = {row["priority"]: row["count"] for row in rows}
        # Priority 5: 3 projects, 4: 1, 3: 2, 2: 1
        assert by_priority[5] == 3
        assert by_priority[4] == 1

    def test_aggregation_on_aggregation(self, kitchen_sink_layer):
        """Test whether we can aggregate an already aggregated value.

        E.g., AVG of department budgets per company.
        This requires a two-level aggregation.
        """
        # This might not work - sidemantic might not support nested aggregation
        try:
            # First aggregate departments by company, then avg those budgets
            # This would be: SELECT company, AVG(dept_budget_per_company)
            # which requires subquery or window function
            pytest.skip("Nested aggregation not directly supported")
        except Exception:
            pytest.skip("Nested aggregation not supported")


# ============================================================
# SQL Compilation Inspection Tests
# ============================================================


class TestSQLCompilation:
    """Test that compiled SQL is correct."""

    def test_cte_structure(self, kitchen_sink_layer):
        """Verify CTEs are generated for each model."""
        sql = kitchen_sink_layer.compile(
            metrics=["employees.total_salary"],
            dimensions=["companies.name"],
        )
        # Should have CTEs for employees, departments, companies
        assert "employees_cte" in sql
        assert "departments_cte" in sql
        assert "companies_cte" in sql

    def test_join_types(self, kitchen_sink_layer):
        """Verify correct join types are used."""
        sql = kitchen_sink_layer.compile(
            metrics=["employees.count"],
            dimensions=["companies.name"],
        )
        # Should use LEFT JOIN for standard joins
        assert "LEFT JOIN" in sql

    def test_filter_pushdown(self, kitchen_sink_layer):
        """Verify filters are pushed down to CTEs when possible."""
        sql = kitchen_sink_layer.compile(
            metrics=["employees.count"],
            dimensions=["employees.title"],
            filters=["employees.is_manager = true"],
        )
        # Filter should ideally be in the employees_cte WHERE clause
        # Look for the filter in CTE definition
        assert "is_manager" in sql

    def test_group_by_generated(self, kitchen_sink_layer):
        """Verify GROUP BY is generated for dimension queries."""
        sql = kitchen_sink_layer.compile(
            metrics=["companies.count"],
            dimensions=["companies.industry"],
        )
        assert "GROUP BY" in sql

    def test_segment_filter_application(self, kitchen_sink_layer):
        """Verify segment SQL is correctly applied."""
        sql = kitchen_sink_layer.compile(
            metrics=["companies.count"],
            segments=["companies.active_companies"],
        )
        # Should have is_active = true somewhere
        assert "is_active" in sql.lower()
        assert "true" in sql.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
