"""Database-schema exposure controls for auto-discovered dimensions."""

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SchemaExposure(BaseModel):
    """Controls which physical columns become auto-discovered dimensions.

    This is intentionally opt-in. Models without ``schema_exposure`` retain the
    historical ``auto_dimensions`` behavior, including excluding primary keys
    and recovering from unavailable schema introspection.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    strict: bool = Field(
        default=False,
        description="Raise when backing-source schema introspection fails or returns no columns",
    )
    include_primary_key: bool = Field(
        default=False,
        description="Expose declared primary-key columns as dimensions when otherwise visible",
    )
    accept: list[str] | None = Field(
        default=None,
        description="Optional allowlist of physical columns eligible for auto-discovery",
    )
    except_fields: list[str] = Field(
        default_factory=list,
        alias="except",
        serialization_alias="except",
        description="Physical columns excluded from auto-discovery",
    )
    private: list[str] = Field(
        default_factory=list,
        description="Private physical columns excluded before dimensions become queryable",
    )

    @model_validator(mode="after")
    def validate_visibility_rules(self) -> "SchemaExposure":
        """Reject ambiguous visibility declarations instead of guessing precedence."""
        if self.accept is not None and self.except_fields:
            raise ValueError("schema_exposure.accept and schema_exposure.except are mutually exclusive")

        accepted = set(self.accept or [])
        private = set(self.private)
        accepted_private = accepted & private
        if accepted_private:
            names = ", ".join(sorted(accepted_private))
            raise ValueError(f"schema_exposure columns cannot be both accepted and private: {names}")

        excluded_private = set(self.except_fields) & private
        if excluded_private:
            names = ", ".join(sorted(excluded_private))
            raise ValueError(f"schema_exposure columns cannot be both excepted and private: {names}")

        return self

    def allows(self, column_name: str) -> bool:
        """Return whether a physical column may become an auto dimension."""
        if self.accept is not None and column_name not in self.accept:
            return False
        return column_name not in self.except_fields and column_name not in self.private
