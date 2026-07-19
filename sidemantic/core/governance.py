"""Shared governance metadata for semantic objects."""

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from sidemantic.core.freshness import Freshness


class Deprecation(BaseModel):
    """Lifecycle details for a deprecated semantic object."""

    model_config = ConfigDict(extra="forbid")

    message: str | None = Field(None, description="Human-readable migration guidance")
    deprecated_at: date | None = Field(None, description="Date the object became deprecated")
    sunset_at: date | None = Field(None, description="Date after which the object may be removed")
    replaced_by: str | None = Field(None, description="Replacement semantic object reference")


class GovernedObject(BaseModel):
    """Reusable, non-enforcing governance fields.

    These fields describe trust and lifecycle. They intentionally do not implement
    authorization; model security policies remain the access-control boundary.
    """

    owner: str | None = Field(None, description="Accountable person or team")
    domain: str | None = Field(None, description="Business domain")
    category: str | None = Field(None, description="Catalog category")
    tags: list[str] = Field(default_factory=list, description="Searchable catalog tags")
    status: Literal["draft", "active", "deprecated"] | None = Field(
        None, description="Semantic object lifecycle status"
    )
    certification: Literal["certified", "verified", "uncertified"] | None = Field(
        None, description="Trust or review state"
    )
    deprecation: Deprecation | None = Field(None, description="Deprecation lifecycle and migration guidance")
    freshness: Freshness | None = Field(None, description="Expected data freshness policy")
    visibility: Literal["public", "internal", "private"] = Field(
        "public", description="Catalog visibility; not an authorization policy"
    )


GOVERNANCE_FIELDS = {
    "owner",
    "domain",
    "category",
    "tags",
    "status",
    "certification",
    "deprecation",
    "freshness",
    "visibility",
}


def governance_dict(value: GovernedObject) -> dict:
    """Return non-default governance fields for metadata/catalog payloads."""
    return value.model_dump(
        include=GOVERNANCE_FIELDS,
        exclude_none=True,
        exclude_defaults=True,
        mode="json",
    )
