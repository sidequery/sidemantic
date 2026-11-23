"""Separate a declared result field from its optional SQL ordering suffix."""

import re

_SUFFIX = re.compile(r"(?:(?:ASC|DESC)(?: NULLS (?:FIRST|LAST))?|NULLS (?:FIRST|LAST))?")


def split_order_field(item: str, known_fields=()) -> tuple[str, str]:
    item = item.strip()
    for field in sorted(set(known_fields), key=len, reverse=True):
        if not item.startswith(field):
            continue
        remainder = item[len(field) :]
        if remainder and not remainder[0].isspace():
            continue
        suffix = " ".join(remainder.split()).upper()
        if _SUFFIX.fullmatch(suffix):
            return field, suffix

    # Undeclared expressions keep their full text except for a valid suffix.
    boundaries = [match.start() for match in re.finditer(r"\s+", item)]
    for boundary in boundaries:
        suffix = " ".join(item[boundary:].split()).upper()
        if suffix and _SUFFIX.fullmatch(suffix):
            return item[:boundary], suffix
    return item, ""
