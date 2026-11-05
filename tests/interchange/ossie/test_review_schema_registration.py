"""Every exposed schema revision can emit validation diagnostics."""

import pytest

from sidemantic.interchange.ossie import validation


@pytest.mark.parametrize("family", ["logical", "ontology", "mapping"])
@pytest.mark.parametrize("missing_validator", [False, True])
def test_latest_explicit_schema_profiles_return_diagnostics(family, missing_validator, monkeypatch):
    if missing_validator:

        def unavailable():
            raise ModuleNotFoundError("test: optional validator unavailable")

        monkeypatch.setattr(validation, "_load_validator_runtime", unavailable)
    profile = f"{family}-0.2.0.dev0-172a3e5"
    result = validation.validate_ossie_schema({}, profile=profile)
    assert not result.valid
    assert result.profile == profile
    assert result.failure_stage == ("availability" if missing_validator else "schema")
    assert result.diagnostics
    assert all(
        item.profile.schema_revision == "172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1" for item in result.diagnostics
    )
