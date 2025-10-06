#!/usr/bin/env python3
"""Fix test files to use the layer fixture instead of creating SemanticLayer() instances."""

import re
from pathlib import Path

# Files to fix
test_files = [
    "tests/metrics/test_derived.py",
    "tests/test_with_data.py",
    "tests/test_preaggregation_bugs.py",
    "tests/adapters/test_metricflow_roundtrip.py",
    "tests/test_sql_generation_security.py",
    "tests/test_catalog.py",
    "tests/adapters/test_rill.py",
    "tests/adapters/test_lookml_roundtrip.py",
    "tests/adapters/test_hex.py",
    "tests/adapters/test_cube_roundtrip.py",
    "tests/test_hierarchies.py",
    "tests/templates/test_jinja_integration.py",
    "tests/queries/test_ungrouped_queries.py",
    "tests/test_segments.py",
    "tests/metrics/test_filters.py",
    "tests/test_metadata_fields.py",
    "tests/queries/test_basic.py",
    "tests/optimizations/test_pre_aggregations.py",
    "tests/optimizations/test_preagg_recommender.py",
    "tests/optimizations/test_predicate_pushdown.py",
]


def fix_file(file_path: Path):
    """Fix a single test file."""
    content = file_path.read_text()
    original_content = content

    # Pattern 1: def test_name(): followed by layer = SemanticLayer()
    # Replace with: def test_name(layer):
    pattern1 = r"def (test_\w+)\(\):\n(.*?)    layer = SemanticLayer\(\)"

    def replace_func(match):
        func_name = match.group(1)
        docstring_etc = match.group(2)
        return f"def {func_name}(layer):\n{docstring_etc}"

    content = re.sub(pattern1, replace_func, content, flags=re.DOTALL)

    # Pattern 2: def test_name(fixture): followed by layer = SemanticLayer()
    # Replace with: def test_name(layer, fixture):
    # This is trickier - we need to add layer as first param
    pattern2 = r"def (test_\w+)\(([^)]+)\):\n(.*?)    layer = SemanticLayer\(\)"

    def replace_func2(match):
        func_name = match.group(1)
        existing_params = match.group(2)
        docstring_etc = match.group(3)
        # Add layer as first parameter
        new_params = f"layer, {existing_params}"
        return f"def {func_name}({new_params}):\n{docstring_etc}"

    content = re.sub(pattern2, replace_func2, content, flags=re.DOTALL)

    if content != original_content:
        file_path.write_text(content)
        print(f"Fixed: {file_path}")
        return True
    else:
        print(f"No changes: {file_path}")
        return False


def main():
    """Fix all test files."""
    root = Path(__file__).parent.parent
    fixed_count = 0

    for file_path in test_files:
        full_path = root / file_path
        if full_path.exists():
            if fix_file(full_path):
                fixed_count += 1
        else:
            print(f"Not found: {full_path}")

    print(f"\nFixed {fixed_count} files")


if __name__ == "__main__":
    main()
