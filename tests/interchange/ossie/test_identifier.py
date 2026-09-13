from __future__ import annotations

from sidemantic.interchange.ossie.identifier import (
    OSSIE_IDENTIFIER_MAX_LENGTH,
    identifier_length,
    identifier_within_limit,
    normalize_identifier,
)


def test_regular_and_quoted_identifier_normalization() -> None:
    assert normalize_identifier("customer_id") == "CUSTOMER_ID"
    assert normalize_identifier("Customer_Id") == "CUSTOMER_ID"
    assert normalize_identifier('"CUSTOMER_ID"') == "CUSTOMER_ID"
    assert normalize_identifier('"customer_id"') == "customer_id"
    assert normalize_identifier('"a""b"') == 'a"b'


def test_identifier_limit_counts_the_decoded_identifier_body() -> None:
    assert identifier_length('"a""b"') == 3
    assert identifier_within_limit("x" * OSSIE_IDENTIFIER_MAX_LENGTH)
    assert not identifier_within_limit("x" * (OSSIE_IDENTIFIER_MAX_LENGTH + 1))
