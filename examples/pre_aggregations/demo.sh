#!/bin/bash
# Pre-Aggregations Demo
#
# This demonstrates the complete workflow:
# 1. Setup sample data
# 2. Discover pre-aggregation opportunities from query patterns
# 3. Apply recommendations to model files
# 4. Materialize pre-aggregations
# 5. Query with automatic routing

set -e

echo "====================================================================="
echo "Pre-Aggregations: From Query Patterns to Performance"
echo "====================================================================="

# Step 1: Setup data
echo ""
echo "[Step 1] Creating sample data..."
echo "---------------------------------------------------------------------"
uv run setup_data.py

# Step 2: Discover opportunities
echo ""
echo "[Step 2] Analyzing query history to find pre-aggregation opportunities..."
echo "---------------------------------------------------------------------"
uvx sidemantic preagg recommend --queries query_history.sql --min-count 5

echo ""
read -p "Press Enter to apply recommendations..."

# Step 3: Apply recommendations
echo ""
echo "[Step 3] Applying recommended pre-aggregations to model file..."
echo "---------------------------------------------------------------------"
uvx sidemantic preagg apply models/ --queries query_history.sql --min-count 5

echo ""
read -p "Press Enter to materialize pre-aggregations..."

# Step 4: Refresh pre-aggregations
echo ""
echo "[Step 4] Materializing pre-aggregation tables..."
echo "---------------------------------------------------------------------"
uvx sidemantic preagg refresh

echo ""
echo "====================================================================="
echo "✓ Pre-aggregations are ready!"
echo "====================================================================="
echo ""
echo "Try querying with pre-aggregations enabled:"
echo ""
echo "  uvx sidemantic workbench"
echo ""
echo "Or test from command line:"
echo ""
echo "  # This query will automatically use pre-aggregations when available"
echo "  uvx sidemantic query \"SELECT status, revenue FROM orders\""
echo ""
