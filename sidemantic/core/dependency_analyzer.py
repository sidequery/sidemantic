"""Automatic dependency detection from SQL expressions."""

import sqlglot
from sqlglot import expressions as exp


def extract_column_references(sql_expr: str) -> set[str]:
    """Extract all column references from a SQL expression.

    Returns set of column names (without table prefixes).
    """
    try:
        parsed = sqlglot.parse_one(sql_expr, read="duckdb")
    except Exception:
        return set()

    columns = set()
    for col in parsed.find_all(exp.Column):
        # Get column name without table prefix
        columns.add(col.name)

    return columns


def extract_metric_dependencies(metric_obj, graph=None, model_context=None) -> set[str]:
    """Auto-detect which measures/metrics a metric depends on.

    Parses SQL expressions to find referenced columns/metrics.
    Uses semantic graph to resolve ambiguous references when available.

    Args:
        metric_obj: Metric object
        graph: Optional SemanticGraph for resolving references
        model_context: Optional model name to prefer when resolving ambiguous references

    Returns:
        Set of dependency names (measures or metrics in model.measure format).
    """
    deps = set()

    # Ratio metric - depends on numerator and denominator
    if metric_obj.type == "ratio":
        if metric_obj.numerator:
            deps.add(metric_obj.numerator)
        if metric_obj.denominator:
            deps.add(metric_obj.denominator)

    # Derived metric (or untyped metric with sql expression) - parse sql to find references
    elif (metric_obj.type == "derived" or (not metric_obj.type and not metric_obj.agg)) and metric_obj.sql:
        # Special case: if sql is a simple qualified reference (model.metric), treat it as a direct dependency
        if (
            "." in metric_obj.sql
            and " " not in metric_obj.sql.strip()
            and not any(op in metric_obj.sql for op in ["+", "-", "*", "/", "(", ")"])
        ):
            deps.add(metric_obj.sql)
            return deps

        # Check if this is an expression metric with inline aggregations
        # (e.g., SUM(x) / SUM(y), COUNT(DISTINCT col) * 1.0)
        # These don't have measure dependencies - the aggregations are inline
        try:
            parsed = sqlglot.parse_one(metric_obj.sql)
            # Check if the expression contains any aggregation functions
            agg_types = (exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max, exp.Median)
            has_inline_agg = any(parsed.find_all(*agg_types))
            if has_inline_agg and not metric_obj.type:
                # Expression metric with inline aggregations - no measure dependencies
                return deps
        except Exception:
            pass

        # Extract column references from expression
        refs = extract_column_references(metric_obj.sql)

        # Use graph to resolve references if available
        if graph:
            for ref in refs:
                # Check if it's already qualified (model.measure)
                if "." in ref:
                    deps.add(ref)
                else:
                    # Try to resolve as metric first
                    resolved = False
                    try:
                        if graph.get_metric(ref):
                            deps.add(ref)
                            resolved = True
                            continue
                    except KeyError:
                        pass

                    # If we have model context, check that model first
                    if not resolved and model_context:
                        try:
                            model = graph.get_model(model_context)
                            if model and model.get_metric(ref):
                                deps.add(f"{model_context}.{ref}")
                                resolved = True
                        except (KeyError, AttributeError):
                            pass

                    # Search all models for this measure name (fallback)
                    if not resolved:
                        for model_name, model in graph.models.items():
                            try:
                                if model.get_metric(ref):
                                    deps.add(f"{model_name}.{ref}")
                                    resolved = True
                                    break
                            except (KeyError, AttributeError):
                                pass

                    # If not resolved, keep as-is (might be a metric not yet added)
                    if not resolved:
                        deps.add(ref)
        else:
            # Without graph, just return raw column names
            deps.update(refs)

    # Cumulative metric - depends on its base measure (stored in expr)
    elif metric_obj.type == "cumulative":
        if metric_obj.sql:
            deps.add(metric_obj.sql)
        elif metric_obj.base_metric:
            deps.add(metric_obj.base_metric)

    # Time comparison - depends on base metric
    elif metric_obj.type == "time_comparison" and metric_obj.base_metric:
        deps.add(metric_obj.base_metric)

    return deps


def extract_dimension_dependencies(dimension_obj) -> set[str]:
    """Extract dimension dependencies from SQL expression.

    Returns set of column names referenced in dimension's sql field.
    """
    if not dimension_obj.sql:
        # If no SQL expr, just the dimension name itself
        return {dimension_obj.name}

    return extract_column_references(dimension_obj.sql)
