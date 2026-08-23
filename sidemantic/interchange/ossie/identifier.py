"""Identifier comparison rules from the Apache Ossie expression proposal."""

from __future__ import annotations

OSSIE_IDENTIFIER_MAX_LENGTH = 128


def is_quoted_identifier(identifier: str) -> bool:
    """Return whether *identifier* uses Ossie's ANSI double-quote form."""

    return len(identifier) >= 2 and identifier.startswith('"') and identifier.endswith('"')


def normalize_identifier(identifier: str) -> str:
    """Return the proposal's exact comparison key without changing source text.

    Regular identifiers compare after upper-casing. Double-quoted identifiers
    compare after removing their outer quotes and unescaping doubled quotes.
    """

    if is_quoted_identifier(identifier):
        return identifier[1:-1].replace('""', '"')
    return identifier.upper()


def identifier_length(identifier: str) -> int:
    """Return the identifier-body length used by the proposal's 128-char limit."""

    return len(normalize_identifier(identifier)) if is_quoted_identifier(identifier) else len(identifier)


def identifier_within_limit(identifier: str) -> bool:
    """Return whether *identifier* satisfies Ossie's proposed size limit."""

    return identifier_length(identifier) <= OSSIE_IDENTIFIER_MAX_LENGTH
