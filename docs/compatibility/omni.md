# Omni native export and graph edits

Omni imports retain view paths, model configuration and topic definitions for native export. Unchanged topics retain their original definitions. If a graph edit removes a topic's base view, joined view, or active relationship, export rejects the stale topic before writing. Update or remove the corresponding `graph.metadata['omni']['topics']` entry in an advanced Python workflow before exporting the changed graph.

Time dimensions retain their original `timeframes` while `granularity` and `supported_granularities` remain unchanged. Editing those properties rebuilds the exported timeframes; clearing both removes them. When both are set, the base granularity must appear in the supported list. Conflicting values and unsupported granularities are rejected instead of replaying the original timeframes.
