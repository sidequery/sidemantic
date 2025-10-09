#!/usr/bin/env python
"""Example demonstrating export functionality for Cube and MetricFlow."""

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.core.semantic_layer import SemanticLayer

# Load from native Sidemantic YAML
print("Loading native Sidemantic YAML...")
sl = SemanticLayer.from_yaml("tests/fixtures/sidemantic/orders.yml")
print(f"Loaded {len(sl.list_models())} models and {len(sl.list_metrics())} metrics")

# Export to Cube format
print("\nExporting to Cube format...")
cube_adapter = CubeAdapter()
cube_adapter.export(sl.graph, "/tmp/orders_exported.yml")
print("Exported to /tmp/orders_exported.yml")

# Export to MetricFlow format
print("\nExporting to MetricFlow format...")
mf_adapter = MetricFlowAdapter()
mf_adapter.export(sl.graph, "/tmp/semantic_models_exported.yml")
print("Exported to /tmp/semantic_models_exported.yml")

# Demonstrate round-trip: Cube -> Sidemantic -> Cube
print("\nDemonstrating Cube round-trip...")
cube_graph = cube_adapter.parse("/tmp/orders_exported.yml")
print(f"Re-imported {len(cube_graph.models)} models from Cube")

# Demonstrate round-trip: MetricFlow -> Sidemantic -> MetricFlow
print("\nDemonstrating MetricFlow round-trip...")
mf_graph = mf_adapter.parse("/tmp/semantic_models_exported.yml")
print(f"Re-imported {len(mf_graph.models)} models and {len(mf_graph.metrics)} metrics from MetricFlow")

print("\nAll exports successful!")
