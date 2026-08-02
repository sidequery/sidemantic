"""Shared SQLGlot parsing helpers and failure policy.

Only ``SqlglotError`` represents malformed SQL that callers may safely treat as
a best-effort parse miss. Configuration errors (for example an unknown dialect)
and implementation bugs deliberately propagate instead of being hidden by SQL
fallback paths.
"""

from __future__ import annotations

from functools import lru_cache

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError


@lru_cache(maxsize=4096)
def _parse_fragment_cached(sql: str, dialect: str) -> exp.Expression:
    """Parse and cache an immutable-by-convention SQL fragment."""
    return sqlglot.parse_one(sql, dialect=dialect)


def parse_fragment(sql: str, dialect: str) -> exp.Expression:
    """Return a fresh, mutable expression parsed from ``sql``.

    SQLGlot expressions carry parent pointers, so callers must never mutate the
    cached tree directly. Copying the cached expression preserves the existing
    generator behavior without repeating the expensive parse.
    """
    return _parse_fragment_cached(sql, dialect).copy()


def try_parse_fragment(sql: str, dialect: str) -> exp.Expression | None:
    """Best-effort parse for optional analysis.

    ``None`` means the input is syntactically invalid. Unknown dialects and
    unexpected runtime failures are not input errors and therefore propagate.
    """
    try:
        return parse_fragment(sql, dialect)
    except SqlglotError:
        return None
