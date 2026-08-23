from __future__ import annotations

import json

from sidemantic.interchange.ossie import (
    OssieParseOptions,
    lower_ossie_document,
    parse_ossie_document,
    serialize_ossie_document,
    synthesize_ossie_document,
)

DATA_TYPES = ("String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time", "DateTime", "DateTimeTz", "Opaque")
TEMPORAL_TYPES = {"Date", "Time", "DateTime", "DateTimeTz"}
DECLARATIONS = (("omitted", None), ("true", True), ("false", False))


def test_complete_datatype_and_time_role_matrix_survives_lowering_and_synthesis() -> None:
    fields: list[dict[str, object]] = []
    expected: dict[str, tuple[str, str, bool | None]] = {}
    for data_type in DATA_TYPES:
        for declaration_name, declared_is_time in DECLARATIONS:
            name = f"{data_type.lower()}_{declaration_name}"
            field: dict[str, object] = {
                "name": name,
                "datatype": data_type,
                "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": name}]},
            }
            if declared_is_time is not None:
                field["dimension"] = {"is_time": declared_is_time}
            effective_is_time = declared_is_time if declared_is_time is not None else data_type in TEMPORAL_TYPES
            if effective_is_time:
                runtime_type = "time"
            elif data_type == "Boolean":
                runtime_type = "boolean"
            elif data_type in {"Integer", "Decimal", "Float"}:
                runtime_type = "numeric"
            else:
                runtime_type = "categorical"
            expected[name] = (runtime_type, data_type, declared_is_time)
            fields.append(field)

    source = {
        "version": "0.2.0.dev0",
        "semantic_model": [
            {
                "name": "types",
                "datasets": [{"name": "values", "source": "analytics.type_values", "fields": fields}],
            }
        ],
    }
    parsed = parse_ossie_document(
        json.dumps(source).encode(),
        options=OssieParseOptions(validate_schema=True),
    )
    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert lowered.valid
    model = lowered.catalog["types"].graph.get_model("values")
    for dimension in model.dimensions:
        assert (dimension.type, dimension.logical_data_type, dimension.declared_is_time) == expected[dimension.name]

    synthesized = synthesize_ossie_document(
        lowered.catalog["types"].graph,
        scope_name="types",
        expression_dialect="ANSI_SQL",
    )

    assert synthesized.valid
    synthesized_fields = {
        field["name"]: field
        for field in synthesized.document.to_parsed_data()["semantic_model"][0]["datasets"][0]["fields"]
    }
    for name, (_, data_type, declared_is_time) in expected.items():
        assert synthesized_fields[name]["datatype"] == data_type
        if declared_is_time is None:
            assert "dimension" not in synthesized_fields[name]
        else:
            assert synthesized_fields[name]["dimension"] == {"is_time": declared_is_time}

    canonical = serialize_ossie_document(synthesized.document, "json")
    reparsed = parse_ossie_document(canonical.data, options=OssieParseOptions(validate_schema=True))
    assert reparsed.valid
    assert reparsed.document.to_parsed_data() == synthesized.document.to_parsed_data()
