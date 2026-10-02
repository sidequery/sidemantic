"""Apache Ossie profile and import/export option contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

CURRENT_OSSIE_SCHEMA_COMMIT = "b6c702ed1c07e91382a69e870c875cbd19570828"
LEGACY_OSSIE_SCHEMA_COMMIT = "831f48e582731cf1ee2e65380ca5abf8157869c7"


class OssieProfileError(ValueError):
    """Raised when an Ossie option combination does not identify a supported profile."""


class OssieSerialization(str, Enum):
    """Source or output serialization, independent from the Ossie schema version."""

    YAML = "yaml"
    JSON = "json"


class OssieConsumerProfile(str, Enum):
    """The contract against which an Ossie document is interpreted."""

    OSSIE_CORE = "ossie-core"
    DBT_1_12 = "dbt-1.12"


class OssieImportPolicy(str, Enum):
    """How validation diagnostics affect later lowering."""

    STRICT = "strict"
    PERMISSIVE = "permissive"


class OssiePreservationPolicy(str, Enum):
    """The round-trip material retained in addition to typed projections."""

    CANONICAL_DATA = "canonical-data"
    SOURCE_BYTES = "source-bytes"


@dataclass(frozen=True, slots=True)
class OssieProfile:
    """One supported schema-version and consumer-profile contract."""

    schema_version: str
    consumer_profile: OssieConsumerProfile
    upstream_schema_version: str | None
    compatibility_alias_for: str | None = None
    schema_revision: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "consumer_profile", OssieConsumerProfile(self.consumer_profile))
        if not self.schema_version or self.schema_version != self.schema_version.strip():
            raise OssieProfileError("profile schema_version must be a non-empty value without surrounding whitespace")
        if self.compatibility_alias_for is not None and self.upstream_schema_version is not None:
            raise OssieProfileError("a compatibility alias cannot also claim an upstream schema version")
        if self.compatibility_alias_for is None and self.upstream_schema_version is None:
            raise OssieProfileError("a non-alias profile must identify its upstream schema version")

    @property
    def identifier(self) -> str:
        revision = f"@{self.schema_revision}" if self.schema_revision else ""
        return f"{self.consumer_profile.value}:{self.schema_version}{revision}"

    @property
    def is_compatibility_alias(self) -> bool:
        return self.compatibility_alias_for is not None

    @property
    def validation_schema_version(self) -> str:
        """Return the pinned logical schema version used to validate this profile.

        Compatibility aliases retain their declared version in source and output;
        this value is only the schema version used during validation.
        """

        return self.compatibility_alias_for or self.schema_version


OSSIE_CORE_0_1_1 = OssieProfile(
    schema_version="0.1.1",
    consumer_profile=OssieConsumerProfile.OSSIE_CORE,
    upstream_schema_version="0.1.1",
)
OSSIE_CORE_0_2_0_DEV0 = OssieProfile(
    schema_version="0.2.0.dev0",
    consumer_profile=OssieConsumerProfile.OSSIE_CORE,
    upstream_schema_version="0.2.0.dev0",
)
OSSIE_CORE_0_2_0_CURRENT = OssieProfile(
    schema_version="0.2.0.dev0",
    consumer_profile=OssieConsumerProfile.OSSIE_CORE,
    upstream_schema_version="0.2.0.dev0",
    schema_revision=CURRENT_OSSIE_SCHEMA_COMMIT,
)
DBT_1_12_0_1_0_ALIAS = OssieProfile(
    schema_version="0.1.0",
    consumer_profile=OssieConsumerProfile.DBT_1_12,
    upstream_schema_version=None,
    compatibility_alias_for="0.1.1",
)
DBT_1_12_0_1_1 = OssieProfile(
    schema_version="0.1.1",
    consumer_profile=OssieConsumerProfile.DBT_1_12,
    upstream_schema_version="0.1.1",
)

OSSIE_PROFILES = (
    OSSIE_CORE_0_1_1,
    OSSIE_CORE_0_2_0_DEV0,
    DBT_1_12_0_1_0_ALIAS,
    DBT_1_12_0_1_1,
    OSSIE_CORE_0_2_0_CURRENT,
)
_PROFILE_INDEX = {
    (profile.schema_version, profile.consumer_profile): profile
    for profile in OSSIE_PROFILES
    if profile.schema_revision is None
}


def resolve_ossie_profile(
    schema_version: str,
    consumer_profile: OssieConsumerProfile | str,
    schema_revision: str | None = None,
) -> OssieProfile:
    """Resolve a supported profile without treating serialization as a version selector."""

    try:
        normalized_consumer = OssieConsumerProfile(consumer_profile)
    except ValueError as exc:
        raise OssieProfileError(f"Unsupported Ossie consumer profile: {consumer_profile!r}") from exc

    if schema_revision is not None:
        if not isinstance(schema_revision, str):
            raise OssieProfileError("schema_revision must be a pinned commit string")
        if schema_version != "0.2.0.dev0" or normalized_consumer is not OssieConsumerProfile.OSSIE_CORE:
            raise OssieProfileError("A schema revision is supported only for ossie-core 0.2.0.dev0")
        if schema_revision in {CURRENT_OSSIE_SCHEMA_COMMIT, CURRENT_OSSIE_SCHEMA_COMMIT[:7]}:
            return OSSIE_CORE_0_2_0_CURRENT
        if schema_revision not in {LEGACY_OSSIE_SCHEMA_COMMIT, LEGACY_OSSIE_SCHEMA_COMMIT[:7]}:
            raise OssieProfileError(f"Unsupported Ossie schema revision: {schema_revision!r}")

    profile = _PROFILE_INDEX.get((schema_version, normalized_consumer))
    if profile is not None:
        return profile

    if schema_version == "0.1.0":
        raise OssieProfileError(
            "Ossie version 0.1.0 is only a dbt-1.12 compatibility alias; it is not an upstream Ossie schema"
        )

    supported = ", ".join(
        profile.schema_version for profile in OSSIE_PROFILES if profile.consumer_profile is normalized_consumer
    )
    raise OssieProfileError(
        f"Unsupported schema version {schema_version!r} for {normalized_consumer.value}; supported: {supported}"
    )


@dataclass(frozen=True, slots=True)
class OssieOptions:
    """Validated options for parsing, preserving, lowering, or exporting Ossie data."""

    schema_version: str
    serialization: OssieSerialization
    consumer_profile: OssieConsumerProfile = OssieConsumerProfile.OSSIE_CORE
    import_policy: OssieImportPolicy = OssieImportPolicy.STRICT
    source_dialect: str | None = None
    target_dialect: str | None = None
    preservation_policy: OssiePreservationPolicy = OssiePreservationPolicy.CANONICAL_DATA
    schema_revision: str | None = None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "serialization", OssieSerialization(self.serialization))
            object.__setattr__(self, "consumer_profile", OssieConsumerProfile(self.consumer_profile))
            object.__setattr__(self, "import_policy", OssieImportPolicy(self.import_policy))
            object.__setattr__(self, "preservation_policy", OssiePreservationPolicy(self.preservation_policy))
        except ValueError as exc:
            raise OssieProfileError(f"Invalid Ossie option: {exc}") from exc

        if not self.schema_version or self.schema_version != self.schema_version.strip():
            raise OssieProfileError("schema_version must be a non-empty value without surrounding whitespace")

        for field_name in ("source_dialect", "target_dialect"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise OssieProfileError(f"{field_name} must be a non-empty string when provided")

        resolve_ossie_profile(self.schema_version, self.consumer_profile, self.schema_revision)

    @property
    def profile(self) -> OssieProfile:
        return resolve_ossie_profile(self.schema_version, self.consumer_profile, self.schema_revision)
