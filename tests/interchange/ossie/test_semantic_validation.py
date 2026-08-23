from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest

from sidemantic.interchange.ossie import (
    OssieDocumentSource,
    OssieLogicalDocument,
    OssieOntologyDocument,
    SemanticValidationResult,
    validate_ossie_semantics,
)
from sidemantic.interchange.ossie.profiles import DBT_1_12_0_1_0_ALIAS
from sidemantic.interchange.ossie.validation import validate_ossie_schema


def _expression(
    text: str = "id",
    dialect: str = "ANSI_SQL",
) -> dict[str, object]:
    return {
        "dialects": [
            {
                "dialect": dialect,
                "expression": text,
            }
        ]
    }


def _dataset(
    name: str,
    *,
    field_names: tuple[str, ...] = ("id",),
    primary_key: tuple[str, ...] | None = ("id",),
    unique_keys: tuple[tuple[str, ...], ...] = (),
) -> dict[str, object]:
    dataset: dict[str, object] = {
        "name": name,
        "source": name,
        "fields": [
            {
                "name": field_name,
                "expression": _expression(field_name),
            }
            for field_name in field_names
        ],
    }
    if primary_key is not None:
        dataset["primary_key"] = list(primary_key)
    if unique_keys:
        dataset["unique_keys"] = [list(key) for key in unique_keys]
    return dataset


def _logical_document(*semantic_models: dict[str, object]) -> dict[str, object]:
    return {
        "version": "0.2.0.dev0",
        "semantic_model": list(semantic_models),
    }


def _semantic_model(
    name: str,
    *,
    datasets: list[dict[str, object]] | None = None,
    relationships: list[dict[str, object]] | None = None,
    metrics: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    model: dict[str, object] = {
        "name": name,
        "datasets": datasets or [_dataset("orders")],
    }
    if relationships is not None:
        model["relationships"] = relationships
    if metrics is not None:
        model["metrics"] = metrics
    return model


def _contracts(result: SemanticValidationResult) -> list[tuple[str, str, str | None]]:
    return [(diagnostic.code, diagnostic.json_pointer, diagnostic.scope) for diagnostic in result.diagnostics]


@pytest.mark.parametrize(
    ("version", "schema_profile"),
    [
        ("0.1.1", "logical-0.1.1"),
        ("0.2.0.dev0", "logical-0.2.0.dev0"),
    ],
)
def test_schema_valid_document_passes_the_semantic_stage(
    version: str,
    schema_profile: str,
) -> None:
    document = _logical_document(
        _semantic_model(
            "commerce",
            datasets=[
                _dataset("orders", field_names=("id", "customer_id")),
                _dataset("customers"),
            ],
            relationships=[
                {
                    "name": "order_customer",
                    "from": "orders",
                    "to": "customers",
                    "from_columns": ["customer_id"],
                    "to_columns": ["id"],
                }
            ],
            metrics=[{"name": "order_count", "expression": _expression("COUNT(*)")}],
        )
    )
    document["version"] = version

    schema_result = validate_ossie_schema(document, profile=schema_profile)
    semantic_result = validate_ossie_semantics(document)

    assert schema_result.valid
    assert semantic_result.valid
    assert semantic_result.stage == "semantic"
    assert semantic_result.failure_stage is None
    assert semantic_result.document_kind == "logical"
    assert semantic_result.checked_scopes == ("commerce",)
    assert semantic_result.diagnostics == ()


def test_duplicate_names_are_checked_in_their_own_namespaces() -> None:
    orders = _dataset("orders", field_names=("id",))
    orders["fields"].append({"name": "id", "expression": _expression("other_id")})
    model = _semantic_model(
        "commerce",
        datasets=[orders, _dataset("orders")],
        relationships=[
            {
                "name": "same_edge",
                "from": "orders",
                "to": "orders",
                "from_columns": ["id"],
                "to_columns": ["id"],
            },
            {
                "name": "same_edge",
                "from": "orders",
                "to": "orders",
                "from_columns": ["id"],
                "to_columns": ["id"],
            },
        ],
        metrics=[
            {"name": "revenue", "expression": _expression()},
            {"name": "revenue", "expression": _expression()},
        ],
    )
    result = validate_ossie_semantics(_logical_document(model, model.copy()))

    assert not result.valid
    assert result.failure_stage == "semantic"
    assert {code for code, _, _ in _contracts(result)} >= {
        "ossie.semantic.semantic_model.name_duplicate",
        "ossie.semantic.dataset.name_duplicate",
        "ossie.semantic.field.name_duplicate",
        "ossie.semantic.metric.name_duplicate",
        "ossie.semantic.relationship.name_duplicate",
    }
    assert (
        "ossie.semantic.semantic_model.name_duplicate",
        "/semantic_model/1/name",
        None,
    ) in _contracts(result)
    assert (
        "ossie.semantic.field.name_duplicate",
        "/semantic_model/0/datasets/0/fields/1/name",
        "commerce@0",
    ) in _contracts(result)


def test_duplicate_dataset_names_are_allowed_across_scopes() -> None:
    result = validate_ossie_semantics(
        _logical_document(
            _semantic_model("commerce", datasets=[_dataset("orders")]),
            _semantic_model("operations", datasets=[_dataset("orders")]),
        )
    )

    assert result.valid
    assert result.checked_scopes == ("commerce", "operations")


def test_names_are_independent_across_dataset_metric_relationship_and_field_namespaces() -> None:
    result = validate_ossie_semantics(
        _logical_document(
            _semantic_model(
                "commerce",
                datasets=[
                    _dataset("orders", field_names=("id", "customers")),
                    _dataset("customers", field_names=("id",)),
                ],
                metrics=[{"name": "customers", "expression": _expression("COUNT(*)")}],
                relationships=[
                    {
                        "name": "customers",
                        "from": "orders",
                        "to": "customers",
                        "from_columns": ["customers"],
                        "to_columns": ["id"],
                    }
                ],
            )
        )
    )

    assert result.valid


def test_relationship_references_do_not_cross_scope_boundaries() -> None:
    result = validate_ossie_semantics(
        _logical_document(
            _semantic_model(
                "commerce",
                datasets=[_dataset("orders", field_names=("id", "customer_id"))],
                relationships=[
                    {
                        "name": "order_customer",
                        "from": "orders",
                        "to": "customers",
                        "from_columns": ["customer_id"],
                        "to_columns": ["id"],
                    }
                ],
            ),
            _semantic_model("crm", datasets=[_dataset("customers")]),
        )
    )

    assert _contracts(result) == [
        (
            "ossie.semantic.relationship.to_dataset_unknown",
            "/semantic_model/0/relationships/0/to",
            "commerce",
        )
    ]


def test_regular_identifier_duplicates_are_detected_after_case_normalization() -> None:
    result = validate_ossie_semantics(
        _logical_document(
            _semantic_model(
                "commerce",
                datasets=[_dataset("orders"), _dataset("Orders")],
                metrics=[
                    {"name": "revenue", "expression": _expression("1")},
                    {"name": "Revenue", "expression": _expression("2")},
                ],
            )
        )
    )

    assert _contracts(result) == [
        (
            "ossie.semantic.dataset.name_duplicate",
            "/semantic_model/0/datasets/1/name",
            "commerce",
        ),
        (
            "ossie.semantic.metric.name_duplicate",
            "/semantic_model/0/metrics/1/name",
            "commerce",
        ),
    ]


def test_quoted_identifiers_use_exact_case_when_resolving_relationships() -> None:
    model = _semantic_model(
        "commerce",
        datasets=[
            _dataset("orders", field_names=("id", "customer_id")),
            _dataset("customers", field_names=("id",)),
        ],
        relationships=[
            {
                "name": "exact",
                "from": '"ORDERS"',
                "to": '"CUSTOMERS"',
                "from_columns": ['"CUSTOMER_ID"'],
                "to_columns": ['"ID"'],
            },
            {
                "name": "case_mismatch",
                "from": "orders",
                "to": '"customers"',
                "from_columns": ["customer_id"],
                "to_columns": ['"id"'],
            },
        ],
    )

    result = validate_ossie_semantics(_logical_document(model))

    assert _contracts(result) == [
        (
            "ossie.semantic.relationship.to_dataset_unknown",
            "/semantic_model/0/relationships/1/to",
            "commerce",
        )
    ]


def test_identifier_over_128_characters_has_structured_refusal() -> None:
    too_long = "x" * 129
    result = validate_ossie_semantics(_logical_document(_semantic_model("commerce", datasets=[_dataset(too_long)])))

    assert _contracts(result) == [
        (
            "ossie.semantic.identifier.length_exceeded",
            "/semantic_model/0/datasets/0/name",
            "commerce",
        )
    ]
    assert "129 characters" in result.diagnostics[0].message


@pytest.mark.parametrize(
    ("relationship", "expected"),
    [
        (
            {
                "name": "missing_to_keys",
                "from": "orders",
                "to": "customers",
                "from_columns": ["customer_id"],
            },
            [
                (
                    "ossie.semantic.relationship.keys_incomplete",
                    "/semantic_model/0/relationships/0",
                    "commerce",
                )
            ],
        ),
        (
            {
                "name": "empty_keys",
                "from": "orders",
                "to": "customers",
                "from_columns": [],
                "to_columns": [],
            },
            [
                (
                    "ossie.semantic.relationship.keys_empty",
                    "/semantic_model/0/relationships/0/from_columns",
                    "commerce",
                ),
                (
                    "ossie.semantic.relationship.keys_empty",
                    "/semantic_model/0/relationships/0/to_columns",
                    "commerce",
                ),
            ],
        ),
        (
            {
                "name": "different_arity",
                "from": "orders",
                "to": "customers",
                "from_columns": ["customer_id", "region_id"],
                "to_columns": ["id"],
            },
            [
                (
                    "ossie.semantic.relationship.key_arity_mismatch",
                    "/semantic_model/0/relationships/0",
                    "commerce",
                )
            ],
        ),
    ],
)
def test_relationship_key_arrays_are_executable_together(
    relationship: dict[str, object],
    expected: list[tuple[str, str, str | None]],
) -> None:
    result = validate_ossie_semantics(
        _logical_document(
            _semantic_model(
                "commerce",
                datasets=[
                    _dataset("orders", field_names=("id", "customer_id", "region_id")),
                    _dataset("customers"),
                ],
                relationships=[relationship],
            )
        )
    )

    assert _contracts(result) == expected


def test_relationship_keys_must_resolve_to_fields_and_a_unique_target_tuple() -> None:
    result = validate_ossie_semantics(
        _logical_document(
            _semantic_model(
                "commerce",
                datasets=[
                    _dataset("orders", field_names=("id", "customer_id")),
                    _dataset(
                        "customers",
                        field_names=("id", "email"),
                        primary_key=("id",),
                    ),
                ],
                relationships=[
                    {
                        "name": "unknown_fields",
                        "from": "orders",
                        "to": "customers",
                        "from_columns": ["missing_customer_id"],
                        "to_columns": ["missing_id"],
                    },
                    {
                        "name": "non_unique_target",
                        "from": "orders",
                        "to": "customers",
                        "from_columns": ["customer_id"],
                        "to_columns": ["email"],
                    },
                ],
            )
        )
    )

    assert _contracts(result) == [
        (
            "ossie.semantic.relationship.from_key_field_unknown",
            "/semantic_model/0/relationships/0/from_columns/0",
            "commerce",
        ),
        (
            "ossie.semantic.relationship.to_key_field_unknown",
            "/semantic_model/0/relationships/0/to_columns/0",
            "commerce",
        ),
        (
            "ossie.semantic.relationship.target_key_not_unique",
            "/semantic_model/0/relationships/1/to_columns",
            "commerce",
        ),
    ]


def test_declared_dataset_keys_resolve_and_are_unique_after_normalization() -> None:
    result = validate_ossie_semantics(
        _logical_document(
            _semantic_model(
                "commerce",
                datasets=[
                    _dataset(
                        "orders",
                        field_names=("id", "region_id"),
                        primary_key=["ID", "missing"],
                        unique_keys=[["id", "ID"], ["region_id"], ["REGION_ID"]],
                    )
                ],
            )
        )
    )

    assert _contracts(result) == [
        (
            "ossie.semantic.dataset.key_field_unknown",
            "/semantic_model/0/datasets/0/primary_key/1",
            "commerce",
        ),
        (
            "ossie.semantic.dataset.key_column_duplicate",
            "/semantic_model/0/datasets/0/unique_keys/0/1",
            "commerce",
        ),
        (
            "ossie.semantic.dataset.key_duplicate",
            "/semantic_model/0/datasets/0/unique_keys/2",
            "commerce",
        ),
    ]


def test_primary_and_unique_composite_target_keys_are_safe() -> None:
    customers = _dataset(
        "customers",
        field_names=("id", "tenant_id", "external_id"),
        primary_key=("id",),
        unique_keys=(("tenant_id", "external_id"),),
    )
    orders = _dataset(
        "orders",
        field_names=("id", "tenant_id", "customer_external_id"),
    )
    result = validate_ossie_semantics(
        _logical_document(
            _semantic_model(
                "commerce",
                datasets=[orders, customers],
                relationships=[
                    {
                        "name": "order_customer",
                        "from": "orders",
                        "to": "customers",
                        "from_columns": ["tenant_id", "customer_external_id"],
                        "to_columns": ["tenant_id", "external_id"],
                    }
                ],
            )
        )
    )

    assert result.valid


def test_expression_variants_are_non_empty_and_unique_per_expression() -> None:
    fields = [
        {
            "name": "empty_variants",
            "expression": {"dialects": []},
        },
        {
            "name": "bad_variants",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": " "},
                    {"dialect": "ansi_sql", "expression": "id"},
                    {"dialect": " ", "expression": "id"},
                ]
            },
        },
    ]
    dataset = _dataset("orders", primary_key=None)
    dataset["fields"] = fields

    result = validate_ossie_semantics(_logical_document(_semantic_model("commerce", datasets=[dataset])))

    assert _contracts(result) == [
        (
            "ossie.semantic.expression.dialects_empty",
            "/semantic_model/0/datasets/0/fields/0/expression/dialects",
            "commerce",
        ),
        (
            "ossie.semantic.expression.text_empty",
            "/semantic_model/0/datasets/0/fields/1/expression/dialects/0/expression",
            "commerce",
        ),
        (
            "ossie.semantic.expression.dialect_duplicate",
            "/semantic_model/0/datasets/0/fields/1/expression/dialects/1/dialect",
            "commerce",
        ),
        (
            "ossie.semantic.expression.dialect_empty",
            "/semantic_model/0/datasets/0/fields/1/expression/dialects/2/dialect",
            "commerce",
        ),
    ]


def test_logical_document_input_carries_source_and_profile_into_diagnostics() -> None:
    document = OssieLogicalDocument(
        canonical_data=_logical_document(
            _semantic_model(
                "commerce",
                datasets=[_dataset("orders")],
                relationships=[
                    {
                        "name": "missing_customer",
                        "from": "orders",
                        "to": "customers",
                        "from_columns": ["id"],
                        "to_columns": ["id"],
                    }
                ],
            )
        ),
        serialization="yaml",
        source=OssieDocumentSource(identifier="models/commerce.ossie.yaml"),
    )

    result = validate_ossie_semantics(document)
    diagnostic = result.diagnostics[0]

    assert diagnostic.source is not None
    assert diagnostic.source.identifier == "models/commerce.ossie.yaml"
    assert diagnostic.profile is not None
    assert diagnostic.profile.identifier == "ossie-core:0.2.0.dev0"


def test_explicit_consumer_profile_carries_into_semantic_diagnostics() -> None:
    document = _logical_document(
        _semantic_model(
            "commerce",
            relationships=[
                {
                    "name": "missing_customer",
                    "from": "orders",
                    "to": "customers",
                    "from_columns": ["id"],
                    "to_columns": ["id"],
                }
            ],
        )
    )
    document["version"] = "0.1.0"

    result = validate_ossie_semantics(document, profile=DBT_1_12_0_1_0_ALIAS)

    assert result.diagnostics[0].profile is DBT_1_12_0_1_0_ALIAS


def test_ontology_validation_checks_explicit_mapping_references_only() -> None:
    ontology = OssieOntologyDocument(
        canonical_data={
            "version": "0.2.0.dev0",
            "name": "commerce-ontology",
            "ontology": [
                {
                    "concept": "Order",
                    "type": "EntityType",
                }
            ],
            "ontology_mappings": [
                {
                    "name": "commerce-map",
                    "semantic_model": _semantic_model("commerce"),
                    "concept_mappings": [
                        {
                            "concept": "MissingConcept",
                            "object_mappings": [{"concept": "Order"}],
                            "link_mappings": [
                                {
                                    "object_mapping": {"concept": "AlsoMissing"},
                                    "children": [{"object_mapping": {"concept": "Order"}}],
                                }
                            ],
                        }
                    ],
                }
            ],
        },
        serialization="json",
    )

    result = validate_ossie_semantics(ontology)

    assert result.document_kind == "ontology"
    assert result.checked_scopes == ("commerce",)
    assert _contracts(result) == [
        (
            "ossie.semantic.ontology.concept_unknown",
            "/ontology_mappings/0/concept_mappings/0/concept",
            None,
        ),
        (
            "ossie.semantic.ontology.concept_unknown",
            "/ontology_mappings/0/concept_mappings/0/link_mappings/0/object_mapping/concept",
            None,
        ),
    ]
    assert all("reason" not in diagnostic.message.lower() for diagnostic in result.diagnostics)


def test_ontology_embedded_models_are_isolated_logical_scopes() -> None:
    ontology = {
        "version": "0.2.0.dev0",
        "name": "commerce-ontology",
        "ontology": [{"concept": "Order", "type": "EntityType"}],
        "ontology_mappings": [
            {
                "semantic_model": _semantic_model(
                    "orders-scope",
                    datasets=[_dataset("orders")],
                    relationships=[
                        {
                            "name": "cross_scope",
                            "from": "orders",
                            "to": "customers",
                            "from_columns": ["id"],
                            "to_columns": ["id"],
                        }
                    ],
                ),
                "concept_mappings": [],
            },
            {
                "semantic_model": _semantic_model(
                    "customers-scope",
                    datasets=[_dataset("customers")],
                ),
                "concept_mappings": [],
            },
        ],
    }

    result = validate_ossie_semantics(ontology)

    assert result.checked_scopes == ("orders-scope", "customers-scope")
    assert _contracts(result) == [
        (
            "ossie.semantic.relationship.to_dataset_unknown",
            "/ontology_mappings/0/semantic_model/relationships/0/to",
            "orders-scope",
        )
    ]


def test_result_is_deterministic_immutable_and_serialization_friendly() -> None:
    document = _logical_document(
        _semantic_model(
            "commerce",
            datasets=[_dataset("orders")],
            relationships=[
                {
                    "name": "broken",
                    "from": "missing",
                    "to": "also_missing",
                    "from_columns": [],
                    "to_columns": [],
                }
            ],
        )
    )

    first = validate_ossie_semantics(document)
    second = validate_ossie_semantics(document)

    assert first == second
    assert json.loads(json.dumps(first.to_dict()))["stage"] == "semantic"
    with pytest.raises(FrozenInstanceError):
        first.valid = True


def test_unsupported_mapping_has_no_semantic_claims() -> None:
    result = validate_ossie_semantics({"version": "0.2.0.dev0"})

    assert result.valid
    assert result.document_kind == "unsupported"
    assert result.checked_scopes == ()
    assert result.diagnostics == ()


def test_non_mapping_input_is_rejected() -> None:
    with pytest.raises(TypeError, match="parsed mapping"):
        validate_ossie_semantics(["not", "a", "mapping"])
