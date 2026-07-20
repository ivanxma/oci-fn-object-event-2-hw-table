"""Streaming CSV inspection and conservative MySQL type inference."""

from __future__ import annotations

import csv
import re
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .naming import validate_identifier

MAX_PREVIEW_ROWS = 25


def sanitize_header(header: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_]+", "_", header.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized or normalized[0].isdigit():
        normalized = f"column_{normalized}" if normalized else "column"
    return validate_identifier(normalized, "CSV column name")


def _dialect(path: Path) -> csv.Dialect:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        sample = source.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return csv.excel


def iter_rows(path: Path) -> tuple[list[str], Iterator[dict[str, str]]]:
    dialect = _dialect(path)
    source = path.open("r", encoding="utf-8-sig", newline="")
    reader = csv.DictReader(source, dialect=dialect)
    if not reader.fieldnames:
        source.close()
        raise ValueError("CSV must contain a header row.")
    headers = [sanitize_header(header) for header in reader.fieldnames]
    if len(set(headers)) != len(headers):
        source.close()
        raise ValueError("CSV headers become duplicates after normalization.")

    def rows() -> Iterator[dict[str, str]]:
        try:
            for raw in reader:
                if None in raw:
                    raise ValueError("CSV row has more values than its header.")
                yield {headers[index]: raw[field] or "" for index, field in enumerate(reader.fieldnames or [])}
        finally:
            source.close()

    return headers, rows()


def infer_mysql_type(values: list[str]) -> str:
    non_empty = [value.strip() for value in values if value.strip()]
    if not non_empty:
        return "TEXT"
    if all(re.fullmatch(r"[-+]?\d+", value) for value in non_empty):
        return "BIGINT"
    try:
        for value in non_empty:
            Decimal(value)
        return "DECIMAL(38, 10)"
    except InvalidOperation:
        pass
    for parser, sql_type in ((datetime.fromisoformat, "DATETIME"), (date.fromisoformat, "DATE")):
        try:
            for value in non_empty:
                parser(value)
            return sql_type
        except ValueError:
            continue
    max_length = max(len(value) for value in non_empty)
    return f"VARCHAR({min(max(max_length, 1), 1024)})" if max_length <= 1024 else "TEXT"


def inspect_csv(path: Path) -> dict:
    dialect = _dialect(path)
    headers, rows = iter_rows(path)
    preview = []
    inference = {
        header: {
            "has_value": False,
            "all_integer": True,
            "all_decimal": True,
            "all_datetime": True,
            "all_date": True,
            "max_length": 0,
        }
        for header in headers
    }
    for row in rows:
        if len(preview) < MAX_PREVIEW_ROWS:
            preview.append(row)
        for header in headers:
            value = row[header].strip()
            if not value:
                continue
            stats = inference[header]
            stats["has_value"] = True
            stats["max_length"] = max(stats["max_length"], len(value))
            stats["all_integer"] = stats["all_integer"] and bool(re.fullmatch(r"[-+]?\d+", value))
            try:
                Decimal(value)
            except InvalidOperation:
                stats["all_decimal"] = False
            try:
                datetime.fromisoformat(value)
            except ValueError:
                stats["all_datetime"] = False
            try:
                date.fromisoformat(value)
            except ValueError:
                stats["all_date"] = False

    def inferred_type(stats: dict) -> str:
        if not stats["has_value"]:
            return "TEXT"
        if stats["all_integer"]:
            return "BIGINT"
        if stats["all_decimal"]:
            return "DECIMAL(38, 10)"
        if stats["all_datetime"]:
            return "DATETIME"
        if stats["all_date"]:
            return "DATE"
        maximum = stats["max_length"]
        return f"VARCHAR({maximum})" if maximum <= 1024 else "TEXT"

    return {
        "headers": headers,
        "preview": preview,
        "types": {header: inferred_type(inference[header]) for header in headers},
        "delimiter": dialect.delimiter,
    }
