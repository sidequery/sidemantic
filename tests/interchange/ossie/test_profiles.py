from dataclasses import FrozenInstanceError

import pytest

from sidemantic.interchange.ossie import (
    DBT_1_12_0_1_0_ALIAS,
    OSSIE_CORE_0_1_1,
    OssieConsumerProfile,
    OssieImportPolicy,
    OssieOptions,
    OssiePreservationPolicy,
    OssieProfileError,
    OssieSerialization,
    resolve_ossie_profile,
)


@pytest.mark.parametrize("schema_version", ["0.1.1", "0.2.0.dev0"])
@pytest.mark.parametrize("serialization", [OssieSerialization.YAML, OssieSerialization.JSON])
def test_core_schema_version_and_serialization_are_independent(schema_version, serialization):
    options = OssieOptions(schema_version=schema_version, serialization=serialization)

    assert options.schema_version == schema_version
    assert options.serialization is serialization
    assert options.profile.consumer_profile is OssieConsumerProfile.OSSIE_CORE


def test_0_1_0_is_only_a_dbt_1_12_compatibility_alias():
    with pytest.raises(OssieProfileError, match="only a dbt-1.12 compatibility alias"):
        resolve_ossie_profile("0.1.0", OssieConsumerProfile.OSSIE_CORE)

    options = OssieOptions(
        schema_version="0.1.0",
        serialization=OssieSerialization.JSON,
        consumer_profile=OssieConsumerProfile.DBT_1_12,
    )

    assert options.profile is DBT_1_12_0_1_0_ALIAS
    assert options.profile.is_compatibility_alias
    assert options.profile.compatibility_alias_for == "0.1.1"
    assert options.profile.validation_schema_version == "0.1.1"
    assert options.profile.upstream_schema_version is None


def test_ordinary_profile_validation_versions_are_unchanged():
    assert OSSIE_CORE_0_1_1.validation_schema_version == "0.1.1"


def test_dbt_1_12_rejects_draft_0_2_profile():
    with pytest.raises(OssieProfileError, match="Unsupported schema version"):
        OssieOptions(
            schema_version="0.2.0.dev0",
            serialization=OssieSerialization.JSON,
            consumer_profile=OssieConsumerProfile.DBT_1_12,
        )


def test_invalid_enum_options_use_the_central_profile_error():
    with pytest.raises(OssieProfileError, match="Invalid Ossie option"):
        OssieOptions(schema_version="0.1.1", serialization="toml")


def test_options_keep_policy_dialects_and_preservation_independent():
    options = OssieOptions(
        schema_version="0.1.1",
        serialization="yaml",
        consumer_profile="ossie-core",
        import_policy="permissive",
        source_dialect="ANSI_SQL",
        target_dialect="postgres",
        preservation_policy="source-bytes",
    )

    assert options.profile is OSSIE_CORE_0_1_1
    assert options.serialization is OssieSerialization.YAML
    assert options.import_policy is OssieImportPolicy.PERMISSIVE
    assert options.source_dialect == "ANSI_SQL"
    assert options.target_dialect == "postgres"
    assert options.preservation_policy is OssiePreservationPolicy.SOURCE_BYTES

    with pytest.raises(FrozenInstanceError):
        options.target_dialect = "bigquery"


@pytest.mark.parametrize(
    ("field", "value"),
    [("schema_version", " 0.1.1"), ("source_dialect", ""), ("target_dialect", "   ")],
)
def test_options_reject_ambiguous_empty_or_whitespace_values(field, value):
    values = {"schema_version": "0.1.1", "serialization": OssieSerialization.JSON, field: value}
    with pytest.raises(OssieProfileError):
        OssieOptions(**values)
