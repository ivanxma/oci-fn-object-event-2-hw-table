"""Safe SQL identifier and object-name helpers."""

from __future__ import annotations

import re

IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
CSV_NAME_RE = re.compile(r"^(?P<table>[A-Za-z][A-Za-z0-9_]*)_(?P<sequence>\d+)\.csv$", re.IGNORECASE)


def validate_identifier(value: str, label: str = "identifier") -> str:
    if not IDENTIFIER_RE.fullmatch(value or ""):
        raise ValueError(f"Invalid {label}: use letters, numbers, and underscores; start with a letter.")
    return value


def quote_identifier(value: str, label: str = "identifier") -> str:
    return f"`{validate_identifier(value, label)}`"


def table_name_from_filename(filename: str) -> str:
    match = CSV_NAME_RE.fullmatch(filename.rsplit("/", 1)[-1])
    if match:
        return match.group("table").lower()
    stem = filename.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    normalized = re.sub(r"[^A-Za-z0-9_]+", "_", stem).strip("_").lower()
    if not normalized or normalized[0].isdigit():
        normalized = f"table_{normalized}" if normalized else "table"
    return validate_identifier(normalized, "table name")
