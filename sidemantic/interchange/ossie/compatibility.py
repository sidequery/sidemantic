"""Portable export subset for the pinned official converter interoperability corpus.

The reference converters at 011ea9f9a1e2c941f351e18e071622339ccae881
share flat Ossie 0.2.0.dev0, but several read only model-scoped metrics.
This transformation changes placement, not metric names or aggregate expressions.
It does not claim support for every feature of every downstream vendor.
"""

from __future__ import annotations

from dataclasses import replace

import sqlglot
from sqlglot import exp

from sidemantic.fidelity import record_export_feature
from sidemantic.interchange.ossie.documents import OssieLogicalDocument, thaw_json
from sidemantic.interchange.ossie.identifier import normalize_identifier
from sidemantic.interchange.ossie.profiles import CURRENT_OSSIE_SCHEMA_COMMIT, OSSIE_CORE_0_2_0_CURRENT
from sidemantic.interchange.ossie.semantic_validation import validate_ossie_semantics
from sidemantic.interchange.ossie.validation import validate_ossie_schema

_DIALECTS = {"ANSI_SQL": None, "SNOWFLAKE": "snowflake", "BIGQUERY": "bigquery", "DATABRICKS": "databricks"}


def for_official_converters(document: OssieLogicalDocument) -> OssieLogicalDocument:
    """Hoist dataset metrics and validate the conservative shared document shape.

    Name collisions and row-count aggregates without a field binding are rejected:
    model-scoped COUNT(*) would otherwise lose its owning dataset.
    """
    data = thaw_json(document.canonical_data)
    metrics = data.setdefault("metrics", [])
    names = {normalize_identifier(metric["name"]) for metric in metrics}
    for dataset_index, dataset in enumerate(data["datasets"]):
        for field_index, field in enumerate(dataset.get("fields", [])):
            if "datatype" not in field:
                record_export_feature(
                    "ossie.datatype_unspecified",
                    "partial",
                    detail=(
                        f"Field {dataset['name']}.{field['name']} has no logical datatype. "
                        "Official converters may default it to string; set logical_data_type "
                        "on the source dimension to preserve its intended type."
                    ),
                    source="ossie",
                    location=f"/datasets/{dataset_index}/fields/{field_index}/datatype",
                )
        for metric in dataset.pop("metrics", []):
            name = normalize_identifier(metric["name"])
            if name in names:
                raise ValueError(
                    f"Official converter compatibility requires unique model metric names: {metric['name']!r}"
                )
            names.add(name)
            for variant in metric["expression"]["dialects"]:
                dialect = _DIALECTS[variant["dialect"]]
                expression = sqlglot.parse_one(variant["expression"], read=dialect)
                columns = list(expression.find_all(exp.Column))
                unbound_aggregates = [
                    aggregate
                    for aggregate in expression.find_all(exp.AggFunc)
                    if not any(aggregate.find_all(exp.Column))
                ]
                if not columns or unbound_aggregates:
                    raise ValueError(
                        f"Cannot move dataset metric {dataset['name']}.{metric['name']} to model scope without a field binding"
                    )
                owner = sqlglot.parse_one(dataset["name"], into=exp.Column).this
                for column in columns:
                    if column.table or column.db or column.catalog:
                        raise ValueError(
                            "Dataset metric requires an unqualified declared field for compatibility export"
                        )
                    column.set("table", owner.copy())
                variant["expression"] = expression.sql(dialect=dialect)
            metrics.append(metric)
    if not metrics:
        data.pop("metrics", None)
    profile = OSSIE_CORE_0_2_0_CURRENT
    diagnostics = (
        *validate_ossie_schema(data, profile=profile).diagnostics,
        *validate_ossie_semantics(data, profile=profile).diagnostics,
    )
    errors = [item.message for item in diagnostics if item.severity.value == "error"]
    if errors:
        raise ValueError("Official converter compatibility validation failed: " + "; ".join(errors))
    return replace(document, canonical_data=data, schema_revision=CURRENT_OSSIE_SCHEMA_COMMIT)
