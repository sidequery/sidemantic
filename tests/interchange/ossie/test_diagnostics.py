from dataclasses import FrozenInstanceError

import pytest

from sidemantic.interchange.ossie import (
    OSSIE_CORE_0_2_0_DEV0,
    OssieDiagnostic,
    OssieDiagnosticSeverity,
    OssieSchemaProvenance,
    OssieSourceLocation,
    sort_diagnostics,
)


def test_diagnostic_preserves_structured_context_and_is_immutable():
    diagnostic = OssieDiagnostic(
        severity="error",
        code="ossie.schema.required",
        message="'datasets' is required",
        json_pointer="/semantic_model/0",
        source=OssieSourceLocation("models/commerce.yaml", line=7, column=3),
        scope="commerce",
        profile=OSSIE_CORE_0_2_0_DEV0,
        schema=OssieSchemaProvenance(
            schema_id="https://ossie.apache.org/schema/core",
            version="0.2.0.dev0",
            commit="88e0011",
            sha256="abc123",
        ),
    )

    assert diagnostic.severity is OssieDiagnosticSeverity.ERROR
    assert diagnostic.source.line == 7
    assert diagnostic.profile.identifier == "ossie-core:0.2.0.dev0"
    assert diagnostic.schema.commit == "88e0011"

    with pytest.raises(FrozenInstanceError):
        diagnostic.message = "changed"


def test_diagnostics_sort_deterministically_by_source_path_and_identity():
    source = OssieSourceLocation("models/commerce.yaml", line=4, column=2)
    diagnostics = [
        OssieDiagnostic(
            severity=OssieDiagnosticSeverity.WARNING,
            code="ossie.semantic.reference",
            message="unknown dataset",
            json_pointer="/semantic_model/0/relationships/0",
            source=source,
        ),
        OssieDiagnostic(
            severity=OssieDiagnosticSeverity.ERROR,
            code="ossie.schema.type",
            message="expected an array",
            json_pointer="/semantic_model/0",
            source=source,
        ),
        OssieDiagnostic(
            severity=OssieDiagnosticSeverity.INFO,
            code="ossie.profile.alias",
            message="using compatibility alias",
            source=OssieSourceLocation("models/compat.json", line=1, column=1),
        ),
    ]

    expected_codes = ["ossie.schema.type", "ossie.semantic.reference", "ossie.profile.alias"]
    assert [item.code for item in sort_diagnostics(diagnostics)] == expected_codes
    assert [item.code for item in sort_diagnostics(reversed(diagnostics))] == expected_codes


@pytest.mark.parametrize(
    "kwargs",
    [
        {"identifier": "source", "line": 0},
        {"identifier": "source", "column": 1},
        {"identifier": "source", "end_column": 1},
    ],
)
def test_source_locations_require_one_based_consistent_coordinates(kwargs):
    with pytest.raises(ValueError):
        OssieSourceLocation(**kwargs)


def test_diagnostic_validates_stable_code_message_and_json_pointer():
    with pytest.raises(ValueError, match="contain no whitespace"):
        OssieDiagnostic(severity="error", code="not stable", message="invalid")
    with pytest.raises(ValueError, match="message must not be empty"):
        OssieDiagnostic(severity="error", code="ossie.invalid", message="")
    with pytest.raises(ValueError, match="start with"):
        OssieDiagnostic(
            severity="error",
            code="ossie.invalid",
            message="invalid",
            json_pointer="semantic_model/0",
        )
