"""
Classify a missing file inside a MergeTree part as one of:

* ``recoverable``        — structural file CH can/we can regenerate
                           (checksums.txt, columns.txt, count.txt, ...).
* ``partially-recoverable`` — column data / skip-index / projection file;
                           part can be kept by replacing the column with
                           NULL or by dropping the index/projection.
* ``unrecoverable``      — losing the file kills the whole part
                           (primary.idx, Compact data.bin, *.bin of
                           PARTITION/ORDER BY columns).

Used by `chadmin data-store detect-broken-partitions` to decide what to
do with each broken part.
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

RECOVERABLE = "recoverable"
PARTIALLY_RECOVERABLE = "partially-recoverable"
UNRECOVERABLE = "unrecoverable"

_STRUCTURAL_FILES = {
    "checksums.txt",
    "columns.txt",
    "count.txt",
    "partition.dat",
    "default_compression_codec.txt",
    "metadata_version.txt",
    "ttl.txt",
    "serialization.json",
    "columns_substreams.txt",
}
_UNRECOVERABLE_FILES = {"primary.idx", "primary.cidx"}
_COMPACT_DATA_FILES = {"data.bin", "data.cmrk2", "data.cmrk3", "data.mrk2", "data.mrk3"}

_MINMAX_RE = re.compile(r"^minmax_.+\.idx$")
_SKIP_INDEX_RE = re.compile(r"^skp_idx_.+\.(idx|idx2|cidx|mrk2|mrk3|cmrk2|cmrk3)$")
_COLUMN_DATA_RE = re.compile(r"^(?P<col>.+)\.(?:bin|cmrk2|cmrk3|mrk2|mrk3|mrk|cmrk)$")
_PROJECTION_RE = re.compile(r"(^|/)[^/]+\.proj/")

_RANK = {RECOVERABLE: 0, PARTIALLY_RECOVERABLE: 1, UNRECOVERABLE: 2}


def classify_file(
    filename: str,
    *,
    part_type: Optional[str] = None,
    critical_columns: Iterable[str] = (),
) -> str:
    """Return one of RECOVERABLE / PARTIALLY_RECOVERABLE / UNRECOVERABLE.

    ``filename`` is the file path relative to the part directory
    (forward slashes; projection sub-files look like ``foo.proj/bar``).
    ``part_type`` is ``"Wide"`` / ``"Compact"`` / ``None``.
    ``critical_columns`` — columns participating in PARTITION BY / ORDER BY;
    losing their ``.bin`` is unrecoverable.
    """
    if _PROJECTION_RE.search("/" + filename):
        # losing anything in a projection means dropping the projection
        return PARTIALLY_RECOVERABLE

    if filename in _STRUCTURAL_FILES or _MINMAX_RE.match(filename):
        return RECOVERABLE

    if filename in _UNRECOVERABLE_FILES:
        return UNRECOVERABLE

    if part_type == "Compact" and filename in _COMPACT_DATA_FILES:
        return UNRECOVERABLE

    if _SKIP_INDEX_RE.match(filename):
        return PARTIALLY_RECOVERABLE

    col_match = _COLUMN_DATA_RE.match(filename)
    if col_match:
        if col_match.group("col") in set(critical_columns):
            return UNRECOVERABLE
        return PARTIALLY_RECOVERABLE

    # Unknown file kind — be conservative.
    return UNRECOVERABLE


def classify_part(
    missing_files: Iterable[str],
    *,
    part_type: Optional[str] = None,
    critical_columns: Iterable[str] = (),
) -> str:
    """Aggregate per-file statuses into a single part status (worst wins)."""
    worst = RECOVERABLE
    seen = False
    for f in missing_files:
        seen = True
        status = classify_file(
            f, part_type=part_type, critical_columns=critical_columns
        )
        if _RANK[status] > _RANK[worst]:
            worst = status
    return worst if seen else RECOVERABLE
