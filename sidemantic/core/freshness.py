"""Freshness policy definitions."""

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


class Freshness(BaseModel):
    """Model-level freshness policy for live chart/runtime responses.

    Prefer ``watermark`` for normal semantic-model usage. ``sql`` is retained as
    an advanced escape hatch when the source freshness marker cannot be expressed
    as a model dimension or physical column.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    watermark: str | None = Field(
        None,
        description="Dimension or source column whose MAX value represents source freshness",
    )
    sql: str | None = Field(
        None,
        description="Advanced SQL query returning one scalar freshness marker",
    )
    ttl_seconds: int | None = Field(
        None,
        gt=0,
        validation_alias=AliasChoices("ttl_seconds", "ttlSeconds"),
        serialization_alias="ttl_seconds",
        description="Maximum allowed age in seconds before data is considered stale",
    )

    @model_validator(mode="after")
    def validate_policy(self) -> "Freshness":
        if self.watermark and self.sql:
            raise ValueError("Freshness cannot define both watermark and sql")
        return self
