import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from ch_tools.common.utils import escape_for_file_name

ROW_EXISTS_COLUMN = "_row_exists"


@dataclass(frozen=True)
class PartColumn:
    name: str
    type: str
    definition: str


@dataclass
class RecoveryAnalysis:
    part_type: str
    columns: List[PartColumn]
    recovered_columns: List[PartColumn]
    lost_files_by_column: Dict[str, List[str]]
    files_to_copy: Set[str]
    rows: int
    columns_substreams: Optional[Dict[str, List[str]]]
    serialization: Optional[Dict[str, Any]]

    @property
    def recovered_user_columns(self) -> List[PartColumn]:
        return [
            column
            for column in self.recovered_columns
            if column.name != ROW_EXISTS_COLUMN
        ]

    @property
    def has_row_exists(self) -> bool:
        return any(
            column.name == ROW_EXISTS_COLUMN for column in self.recovered_columns
        )


def parse_columns_text(value: str) -> List[PartColumn]:
    lines = value.splitlines()
    if len(lines) < 2 or lines[0] != "columns format version: 1":
        raise ValueError("Unsupported columns.txt format")

    count_match = re.fullmatch(r"(\d+) columns:", lines[1])
    if not count_match:
        raise ValueError("Invalid columns.txt header")
    count = int(count_match.group(1))
    definitions = lines[2:]
    while definitions and not definitions[-1]:
        definitions.pop()
    if len(definitions) != count:
        raise ValueError(
            f"columns.txt declares {count} columns but contains {len(definitions)}"
        )

    result: List[PartColumn] = []
    for definition in definitions:
        name, type_name = _parse_column_definition(definition)
        result.append(PartColumn(name, type_name, definition))
    return result


def _parse_backquoted(value: str) -> Tuple[str, int]:
    quote = chr(96)
    if not value.startswith(quote):
        raise ValueError(f"Expected backquoted value: {value}")

    result: List[str] = []
    index = 1
    escapes = {
        "0": "\0",
        "a": "\a",
        "b": "\b",
        "f": "\f",
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "v": "\v",
    }
    while index < len(value):
        char = value[index]
        if char == quote:
            if index + 1 < len(value) and value[index + 1] == quote:
                result.append(quote)
                index += 2
                continue
            return "".join(result), index + 1
        if char == "\\":
            if index + 1 >= len(value):
                raise ValueError(f"Invalid escape in backquoted value: {value}")
            escaped = value[index + 1]
            result.append(escapes.get(escaped, escaped))
            index += 2
            continue
        result.append(char)
        index += 1

    raise ValueError(f"Unterminated backquoted value: {value}")


def _parse_column_definition(definition: str) -> Tuple[str, str]:
    name, end = _parse_backquoted(definition)
    if end >= len(definition) or definition[end] != " ":
        raise ValueError(f"Invalid column definition: {definition}")
    type_name = definition[end + 1 :]
    if not type_name:
        raise ValueError(f"Missing type in column definition: {definition}")
    return name, type_name


def render_columns_text(columns: Iterable[PartColumn]) -> bytes:
    column_list = list(columns)
    lines = [
        "columns format version: 1",
        f"{len(column_list)} columns:",
        *(column.definition for column in column_list),
    ]
    return ("\n".join(lines) + "\n").encode()


def parse_columns_substreams(value: str) -> Dict[str, List[str]]:
    lines = value.splitlines()
    while lines and not lines[-1]:
        lines.pop()
    if len(lines) < 2 or lines[0] != "columns substreams version: 1":
        raise ValueError("Unsupported columns_substreams.txt format")
    count_match = re.fullmatch(r"(\d+) columns:", lines[1])
    if not count_match:
        raise ValueError("Invalid columns_substreams.txt header")

    result: Dict[str, List[str]] = {}
    index = 2
    for _ in range(int(count_match.group(1))):
        if index >= len(lines):
            raise ValueError("Unexpected end of columns_substreams.txt")
        header = re.fullmatch(r"(\d+) substreams for column (.+):", lines[index])
        if not header:
            raise ValueError(f"Invalid substreams header: {lines[index]}")
        index += 1
        quoted_name = header.group(2)
        name, name_end = _parse_backquoted(quoted_name)
        if name_end != len(quoted_name):
            raise ValueError(f"Invalid substreams header: {lines[index - 1]}")
        if name in result:
            raise ValueError(f"Duplicate substreams metadata for column {name}")

        streams: List[str] = []
        for _ in range(int(header.group(1))):
            if index >= len(lines) or not lines[index].startswith("\t"):
                raise ValueError("Invalid substream entry")
            streams.append(lines[index][1:])
            index += 1
        result[name] = streams

    if index != len(lines):
        raise ValueError("Unexpected trailing data in columns_substreams.txt")
    return result


def _has_valid_stream_prefix(stream: str, prefix: str) -> bool:
    return (
        stream == prefix
        or stream.startswith(prefix + ".")
        or stream.startswith(prefix + "%2E")
    )


def _validate_columns_substreams(
    columns: List[PartColumn], substreams: Dict[str, List[str]]
) -> None:
    expected = [column.name for column in columns]
    actual = list(substreams)
    if actual != expected:
        raise ValueError(
            "columns_substreams.txt columns differ from columns.txt: "
            f"expected {expected}, got {actual}"
        )

    for column in columns:
        escaped = escape_for_file_name(column.name)
        nested = escape_for_file_name(column.name.split(".", 1)[0])
        for stream in substreams[column.name]:
            if _has_valid_stream_prefix(stream, escaped) or _has_valid_stream_prefix(
                stream, nested
            ):
                continue
            raise ValueError(f"Invalid substream {stream} for column {column.name}")


def render_columns_substreams(
    substreams: Dict[str, List[str]], columns: Iterable[PartColumn]
) -> bytes:
    column_list = list(columns)
    lines = [
        "columns substreams version: 1",
        f"{len(column_list)} columns:",
    ]
    for column in column_list:
        column_streams = substreams.get(column.name)
        if not column_streams:
            raise ValueError(f"No substreams found for column {column.name}")
        escaped_name = column.name.replace("`", "``")
        lines.append(f"{len(column_streams)} substreams for column `{escaped_name}`:")
        lines.extend(f"\t{stream}" for stream in column_streams)
    return ("\n".join(lines) + "\n").encode()


def filter_serialization(
    serialization: Dict[str, Any], columns: Iterable[PartColumn]
) -> bytes:
    names = {column.name for column in columns}
    result = dict(serialization)
    if "columns" in result:
        result["columns"] = [
            item for item in result["columns"] if item.get("name") in names
        ]
    return json.dumps(result, separators=(",", ":")).encode()
