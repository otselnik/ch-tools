from __future__ import annotations

from typing import List, Sequence, Tuple


def _back_quote(name: str) -> str:
    """Quote a column identifier the same way ClickHouse does."""
    escaped = name.replace("\\", "\\\\").replace("`", "\\`")
    return f"`{escaped}`"


def generate_columns_txt(columns: Sequence[Tuple[str, str]]) -> bytes:
    if not columns:
        raise ValueError("columns.txt generation requires a non-empty column list")

    lines: List[str] = [
        "columns format version: 1",
        f"{len(columns)} columns:",
    ]
    for name, type_ in columns:
        lines.append(f"{_back_quote(name)} {type_}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def generate_count_txt(rows: int) -> bytes:
    if rows < 0:
        raise ValueError("count.txt: row count must be non-negative")
    return f"{rows}\n".encode("ascii")


def generate_default_compression_codec(codec: str = "CODEC(LZ4)") -> bytes:
    codec = codec.strip()
    if not codec:
        raise ValueError("default_compression_codec.txt: codec must not be empty")
    return (codec + "\n").encode("utf-8")


def generate_metadata_version(metadata_version: int) -> bytes:
    if metadata_version < 0:
        raise ValueError("metadata_version.txt: version must be non-negative")
    return f"{metadata_version}\n".encode("ascii")
