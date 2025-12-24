"""Shared assertion helpers for adapter tests.

These helpers verify semantic equivalence between models, dimensions, metrics,
relationships, and segments. Use them in roundtrip tests to ensure data survives
export/import cycles.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sidemantic.core.dimension import Dimension
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.relationship import Relationship
    from sidemantic.core.segment import Segment


def assert_dimension_equivalent(
    dim1: Dimension,
    dim2: Dimension,
    check_metadata: bool = True,
    msg_prefix: str = "",
) -> None:
    """Verify two dimensions are semantically equivalent.

    Args:
        dim1: First dimension
        dim2: Second dimension
        check_metadata: Whether to check description/label/format fields
        msg_prefix: Prefix for assertion error messages
    """
    prefix = f"{msg_prefix}dimension '{dim1.name}': " if msg_prefix else f"dimension '{dim1.name}': "

    assert dim1.name == dim2.name, f"{prefix}name mismatch"
    assert dim1.type == dim2.type, f"{prefix}type mismatch ({dim1.type} vs {dim2.type})"

    # SQL can be None for simple column references
    if dim1.sql is not None or dim2.sql is not None:
        assert dim1.sql == dim2.sql, f"{prefix}sql mismatch ({dim1.sql!r} vs {dim2.sql!r})"

    # Granularity for time dimensions
    if dim1.type == "time" or dim2.type == "time":
        assert dim1.granularity == dim2.granularity, f"{prefix}granularity mismatch"

    if check_metadata:
        assert dim1.description == dim2.description, f"{prefix}description mismatch"
        assert dim1.label == dim2.label, f"{prefix}label mismatch"
        if hasattr(dim1, "format") and hasattr(dim2, "format"):
            assert dim1.format == dim2.format, f"{prefix}format mismatch"


def assert_metric_equivalent(
    m1: Metric,
    m2: Metric,
    check_metadata: bool = True,
    msg_prefix: str = "",
) -> None:
    """Verify two metrics are semantically equivalent.

    Args:
        m1: First metric
        m2: Second metric
        check_metadata: Whether to check description/label/format fields
        msg_prefix: Prefix for assertion error messages
    """
    prefix = f"{msg_prefix}metric '{m1.name}': " if msg_prefix else f"metric '{m1.name}': "

    assert m1.name == m2.name, f"{prefix}name mismatch"
    assert m1.type == m2.type, f"{prefix}type mismatch ({m1.type} vs {m2.type})"
    assert m1.agg == m2.agg, f"{prefix}agg mismatch ({m1.agg} vs {m2.agg})"

    # SQL can be None for count metrics
    if m1.sql is not None or m2.sql is not None:
        assert m1.sql == m2.sql, f"{prefix}sql mismatch ({m1.sql!r} vs {m2.sql!r})"

    # For ratio metrics
    if m1.type == "ratio" or m2.type == "ratio":
        assert m1.numerator == m2.numerator, f"{prefix}numerator mismatch"
        assert m1.denominator == m2.denominator, f"{prefix}denominator mismatch"

    # For derived metrics
    if m1.type == "derived" or m2.type == "derived":
        # SQL expression should match
        pass  # Already checked above

    # Check filters if present
    if m1.filters or m2.filters:
        assert_filters_equivalent(m1.filters, m2.filters, prefix)

    if check_metadata:
        assert m1.description == m2.description, f"{prefix}description mismatch"
        if hasattr(m1, "label") and hasattr(m2, "label"):
            assert m1.label == m2.label, f"{prefix}label mismatch"
        if hasattr(m1, "format") and hasattr(m2, "format"):
            assert m1.format == m2.format, f"{prefix}format mismatch"
        if hasattr(m1, "drill_fields") and hasattr(m2, "drill_fields"):
            assert m1.drill_fields == m2.drill_fields, f"{prefix}drill_fields mismatch"


def assert_filters_equivalent(
    filters1: list | None,
    filters2: list | None,
    msg_prefix: str = "",
) -> None:
    """Verify two filter lists are equivalent.

    Args:
        filters1: First filter list
        filters2: Second filter list
        msg_prefix: Prefix for assertion error messages
    """
    if filters1 is None and filters2 is None:
        return

    assert filters1 is not None and filters2 is not None, f"{msg_prefix}filter presence mismatch"
    assert len(filters1) == len(filters2), f"{msg_prefix}filter count mismatch"

    for i, (f1, f2) in enumerate(zip(filters1, filters2)):
        # Filters can be strings or Filter objects
        if isinstance(f1, str) and isinstance(f2, str):
            assert f1 == f2, f"{msg_prefix}filter[{i}] mismatch"
        elif hasattr(f1, "field") and hasattr(f2, "field"):
            assert f1.field == f2.field, f"{msg_prefix}filter[{i}].field mismatch"
            assert f1.operator == f2.operator, f"{msg_prefix}filter[{i}].operator mismatch"
            assert f1.value == f2.value, f"{msg_prefix}filter[{i}].value mismatch"


def assert_relationship_equivalent(
    rel1: Relationship,
    rel2: Relationship,
    msg_prefix: str = "",
) -> None:
    """Verify two relationships are semantically equivalent.

    Args:
        rel1: First relationship
        rel2: Second relationship
        msg_prefix: Prefix for assertion error messages
    """
    prefix = f"{msg_prefix}relationship '{rel1.name}': " if msg_prefix else f"relationship '{rel1.name}': "

    assert rel1.name == rel2.name, f"{prefix}name mismatch"
    assert rel1.type == rel2.type, f"{prefix}type mismatch ({rel1.type} vs {rel2.type})"

    if rel1.foreign_key is not None or rel2.foreign_key is not None:
        assert rel1.foreign_key == rel2.foreign_key, f"{prefix}foreign_key mismatch"

    if rel1.primary_key is not None or rel2.primary_key is not None:
        assert rel1.primary_key == rel2.primary_key, f"{prefix}primary_key mismatch"


def assert_segment_equivalent(
    seg1: Segment,
    seg2: Segment,
    check_metadata: bool = True,
    msg_prefix: str = "",
) -> None:
    """Verify two segments are semantically equivalent.

    Args:
        seg1: First segment
        seg2: Second segment
        check_metadata: Whether to check description field
        msg_prefix: Prefix for assertion error messages
    """
    prefix = f"{msg_prefix}segment '{seg1.name}': " if msg_prefix else f"segment '{seg1.name}': "

    assert seg1.name == seg2.name, f"{prefix}name mismatch"
    assert seg1.sql == seg2.sql, f"{prefix}sql mismatch ({seg1.sql!r} vs {seg2.sql!r})"

    if check_metadata:
        assert seg1.description == seg2.description, f"{prefix}description mismatch"


def assert_model_equivalent(
    model1: Model,
    model2: Model,
    check_metadata: bool = True,
    check_relationships: bool = True,
    check_segments: bool = True,
    check_table_schema: bool = True,
) -> None:
    """Verify two models are semantically equivalent.

    Args:
        model1: First model
        model2: Second model
        check_metadata: Whether to check description/label fields
        check_relationships: Whether to check relationships
        check_segments: Whether to check segments
        check_table_schema: Whether to check full table name including schema
    """
    prefix = f"model '{model1.name}': "

    # Basic model properties
    assert model1.name == model2.name, f"{prefix}name mismatch"

    # Table comparison - optionally normalize by stripping schema
    if check_table_schema:
        assert model1.table == model2.table, f"{prefix}table mismatch ({model1.table} vs {model2.table})"
    else:
        # Compare just table names, ignoring schema prefix
        table1 = model1.table.split(".")[-1] if model1.table else None
        table2 = model2.table.split(".")[-1] if model2.table else None
        assert table1 == table2, f"{prefix}table name mismatch ({table1} vs {table2})"

    if check_metadata:
        assert model1.description == model2.description, f"{prefix}description mismatch"

    # Dimensions
    assert len(model1.dimensions) == len(model2.dimensions), (
        f"{prefix}dimension count mismatch ({len(model1.dimensions)} vs {len(model2.dimensions)})"
    )

    for dim1 in model1.dimensions:
        dim2 = model2.get_dimension(dim1.name)
        assert dim2 is not None, f"{prefix}missing dimension '{dim1.name}'"
        assert_dimension_equivalent(dim1, dim2, check_metadata=check_metadata, msg_prefix=prefix)

    # Metrics
    assert len(model1.metrics) == len(model2.metrics), (
        f"{prefix}metric count mismatch ({len(model1.metrics)} vs {len(model2.metrics)})"
    )

    for m1 in model1.metrics:
        m2 = model2.get_metric(m1.name)
        assert m2 is not None, f"{prefix}missing metric '{m1.name}'"
        assert_metric_equivalent(m1, m2, check_metadata=check_metadata, msg_prefix=prefix)

    # Relationships
    if check_relationships:
        assert len(model1.relationships) == len(model2.relationships), f"{prefix}relationship count mismatch"
        for rel1 in model1.relationships:
            rel2 = next((r for r in model2.relationships if r.name == rel1.name), None)
            assert rel2 is not None, f"{prefix}missing relationship '{rel1.name}'"
            assert_relationship_equivalent(rel1, rel2, msg_prefix=prefix)

    # Segments
    if check_segments and (model1.segments or model2.segments):
        seg1_list = model1.segments or []
        seg2_list = model2.segments or []
        assert len(seg1_list) == len(seg2_list), f"{prefix}segment count mismatch"

        for seg1 in seg1_list:
            seg2 = model2.get_segment(seg1.name)
            assert seg2 is not None, f"{prefix}missing segment '{seg1.name}'"
            assert_segment_equivalent(seg1, seg2, check_metadata=check_metadata, msg_prefix=prefix)


def assert_graph_equivalent(
    graph1,
    graph2,
    check_metadata: bool = True,
    check_relationships: bool = True,
    check_segments: bool = True,
    check_table_schema: bool = True,
) -> None:
    """Verify two semantic graphs are semantically equivalent.

    Args:
        graph1: First SemanticGraph
        graph2: Second SemanticGraph
        check_metadata: Whether to check description/label fields
        check_relationships: Whether to check relationships
        check_segments: Whether to check segments
        check_table_schema: Whether to check full table name including schema
    """
    assert set(graph1.models.keys()) == set(graph2.models.keys()), (
        f"model set mismatch: {set(graph1.models.keys())} vs {set(graph2.models.keys())}"
    )

    for name, model1 in graph1.models.items():
        model2 = graph2.models[name]
        assert_model_equivalent(
            model1,
            model2,
            check_metadata=check_metadata,
            check_relationships=check_relationships,
            check_segments=check_segments,
            check_table_schema=check_table_schema,
        )
