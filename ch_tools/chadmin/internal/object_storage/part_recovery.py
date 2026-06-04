from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from boto3 import client as Boto3Client
from click import Context

from ch_tools.chadmin.internal.object_storage.checksums_recovery_via_local import (
    build_recovery_ddl,
    get_show_create_table,
    is_projection_checksums,
    recover_checksums_via_local,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    S3ObjectLocalMetaData,
)
from ch_tools.chadmin.internal.object_storage.structural_files import (
    generate_columns_txt,
    generate_count_txt,
    generate_default_compression_codec,
    generate_metadata_version,
)
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.client import OutputFormat

# Names of files we can rebuild from cheap ClickHouse metadata only.
SIMPLE_RECOVERABLE_FILES = {
    "columns.txt",
    "count.txt",
    "default_compression_codec.txt",
    "metadata_version.txt",
}

# Files whose recovery is recoverable in principle but is *not* part of
# this PR — checksums.txt for Replicated is handled via clickhouse-local
# (see checksums_recovery_via_local.py).  The binary structural files below
# still need format-aware generators and are deferred.
UNSUPPORTED_FILES = {
    "partition.dat",
    "ttl.txt",
    "serialization.json",
    "columns_substreams.txt",
}


@dataclass
class PartRecoveryContext:
    """Everything we need to regenerate the simple files of one part."""

    database: str
    table: str
    part_name: str
    part_type: Optional[str]
    rows: int
    metadata_version: int
    default_codec: str
    is_replicated: bool
    columns: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def quoted_table(self) -> str:
        return f"`{self.database}`.`{self.table}`"


# ---------------------------------------------------------------------------
# ClickHouse metadata queries
# ---------------------------------------------------------------------------


def get_part_recovery_context(
    ctx: Context, part_path: str
) -> Optional[PartRecoveryContext]:
    """Collect table + part metadata required for structural recovery.

    Returns ``None`` if the part is not found in ``system.parts`` (e.g.
    it has already been detached or merged away).
    """
    parts_query = (
        "SELECT database, table, name, part_type, rows, default_compression_codec "
        f"FROM system.parts WHERE path = '{part_path}/' AND active LIMIT 1"
    )
    parts_res = execute_query(ctx, parts_query, format_=OutputFormat.JSONCompact)
    if not parts_res.get("data"):
        logging.warning("Part not found in system.parts for path {}", part_path)
        return None
    (
        database,
        table,
        part_name,
        part_type,
        rows,
        default_codec,
    ) = parts_res[
        "data"
    ][0]

    tables_query = (
        "SELECT engine, metadata_version FROM system.tables "
        f"WHERE database = '{database}' AND name = '{table}' LIMIT 1"
    )
    tables_res = execute_query(ctx, tables_query, format_=OutputFormat.JSONCompact)
    if not tables_res.get("data"):
        logging.warning("Table {}.{} not found in system.tables", database, table)
        return None
    engine, metadata_version = tables_res["data"][0]

    columns_query = (
        "SELECT name, type FROM system.columns "
        f"WHERE database = '{database}' AND table = '{table}' "
        "ORDER BY position"
    )
    columns_res = execute_query(ctx, columns_query, format_=OutputFormat.JSONCompact)
    columns: List[Tuple[str, str]] = [
        (row[0], row[1]) for row in columns_res.get("data", [])
    ]

    return PartRecoveryContext(
        database=database,
        table=table,
        part_name=part_name,
        part_type=part_type,
        rows=int(rows),
        metadata_version=int(metadata_version) if metadata_version is not None else 0,
        default_codec=default_codec or "CODEC(LZ4)",
        is_replicated="Replicated" in (engine or ""),
        columns=columns,
    )


# ---------------------------------------------------------------------------
# File content generation
# ---------------------------------------------------------------------------


def generate_simple_recoverable_file(
    filename: str, recovery_ctx: PartRecoveryContext
) -> bytes:
    """Build the textual payload for one of :data:`SIMPLE_RECOVERABLE_FILES`."""
    if filename == "columns.txt":
        return generate_columns_txt(recovery_ctx.columns)
    if filename == "count.txt":
        return generate_count_txt(recovery_ctx.rows)
    if filename == "default_compression_codec.txt":
        return generate_default_compression_codec(recovery_ctx.default_codec)
    if filename == "metadata_version.txt":
        return generate_metadata_version(recovery_ctx.metadata_version)
    raise ValueError(f"No simple generator for {filename!r}")


# ---------------------------------------------------------------------------
# S3 upload — always under a brand-new key
# ---------------------------------------------------------------------------


def upload_recovered_file_to_s3(
    s3_client: Boto3Client,
    bucket: str,
    prefix: str,
    content: bytes,
) -> Tuple[str, int]:
    """
    Upload ``content`` to S3 under a freshly generated key and return
    ``(key, size)`` that the caller will record in the local disk-metadata
    file of the part.

    The key is *never* the old one — that blob may still be referenced by
    other replicas through ``remote_fs_zero_copy_path`` in ZooKeeper.
    """
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    # Match ClickHouse's pattern: <prefix>/<xx>/<random uuid4 hex>.
    rand = uuid.uuid4().hex
    key = f"{prefix}{rand[:3]}/{rand}"
    s3_client.put_object(Bucket=bucket, Key=key, Body=content)
    return key, len(content)


# ---------------------------------------------------------------------------
# Local disk-metadata file rewriting
# ---------------------------------------------------------------------------


def rewrite_disk_metadata(
    metadata_path: Path,
    new_key: str,
    new_size: int,
) -> None:
    """
    Rewrite the local disk-metadata file at ``metadata_path`` so that it
    points at exactly one new S3 object (``new_key``/``new_size``).

    Used after a fresh blob has been uploaded for a regenerated file —
    the old blob may still be referenced by zero-copy replicas, so we
    *replace* the local pointer without touching the old key in S3.
    """
    if metadata_path.exists():
        old = S3ObjectLocalMetaData.from_file(metadata_path)
        version = old.version
        key_is_full = old.has_full_object_key()
        ref_counter = old.ref_counter
        read_only = old.read_only
    else:
        # Pre-VERSION_FULL_OBJECT_KEY default — local keys (relative to the
        # disk prefix).  We use it only for brand-new files; for the
        # detect-broken-partitions flow the file always pre-exists.
        version = 3
        key_is_full = False
        ref_counter = 0
        read_only = False

    new_meta = S3ObjectLocalMetaData(
        version=version,
        total_size=new_size,
        objects=[
            S3ObjectLocalInfo(key=new_key, size=new_size, key_is_full=key_is_full)
        ],
        ref_counter=ref_counter,
        read_only=read_only,
    )
    new_meta.to_file(metadata_path)


# ---------------------------------------------------------------------------
# Recovery flow
# ---------------------------------------------------------------------------


@dataclass
class FileRecoveryResult:
    filename: str
    status: str  # "regenerated" / "unlinked" / "skipped" / "failed"
    detail: str = ""


@dataclass
class PartRecoveryResult:
    part_path: str
    part_name: Optional[str]
    table: Optional[str]
    files: List[FileRecoveryResult] = field(default_factory=list)
    detached: bool = False
    reattached: bool = False
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "part_path": self.part_path,
            "part_name": self.part_name,
            "table": self.table,
            "files": [
                {"filename": f.filename, "status": f.status, "detail": f.detail}
                for f in self.files
            ],
            "detached": self.detached,
            "reattached": self.reattached,
            "error": self.error,
        }


def recover_part_files_locally(
    *,
    part_path: str,
    missing_files: List[str],
    recovery_ctx: PartRecoveryContext,
    s3_client: Boto3Client,
    bucket: str,
    s3_prefix: str,
    ctx: Optional[Context] = None,
    disk_name: Optional[str] = None,
    disk_config: Optional[dict] = None,
    ch_version: Optional[str] = None,
) -> List[FileRecoveryResult]:
    """
    Walk ``missing_files`` and perform the per-file recovery action.

    The part is expected to be *already detached* at this point — the
    function only touches files inside ``part_path`` (which by then lives
    under ``…/detached/…``) and S3 objects.
    """
    results: List[FileRecoveryResult] = []
    restored_checksums: List[str] = []
    if recovery_ctx.is_replicated and "checksums.txt" in missing_files:
        result, restored_checksums = _recover_checksums_replicated(
            filename="checksums.txt",
            part_path=part_path,
            missing_files=missing_files,
            recovery_ctx=recovery_ctx,
            ctx=ctx,
            disk_name=disk_name,
            disk_config=disk_config,
            ch_version=ch_version,
        )
        results.append(result)

    restored_checksums_set = set(restored_checksums)
    for filename in missing_files:
        if filename == "checksums.txt" and recovery_ctx.is_replicated:
            continue
        try:
            if filename == "checksums.txt" and not recovery_ctx.is_replicated:
                # Plain MergeTree: unlink local metadata so CH regenerates
                # the file on ATTACH via checkDataPart + writeChecksums.
                target = Path(part_path) / filename
                if target.exists():
                    target.unlink()
                results.append(
                    FileRecoveryResult(filename, "unlinked", "non-replicated MergeTree")
                )
                continue

            if filename in restored_checksums_set and is_projection_checksums(filename):
                results.append(
                    FileRecoveryResult(
                        filename,
                        "regenerated",
                        "checksums.txt regenerated via clickhouse-local",
                    )
                )
                continue

            if filename in UNSUPPORTED_FILES:
                results.append(
                    FileRecoveryResult(filename, "skipped", "not yet implemented")
                )
                continue

            if filename in SIMPLE_RECOVERABLE_FILES:
                payload = generate_simple_recoverable_file(filename, recovery_ctx)
                new_key, new_size = upload_recovered_file_to_s3(
                    s3_client, bucket, s3_prefix, payload
                )
                rewrite_disk_metadata(Path(part_path) / filename, new_key, new_size)
                results.append(
                    FileRecoveryResult(
                        filename, "regenerated", f"new s3 key {new_key} ({new_size} B)"
                    )
                )
                continue

            results.append(
                FileRecoveryResult(filename, "skipped", "no recovery action available")
            )
        except Exception as exc:  # pragma: no cover - defensive
            logging.warning(
                "Recovery of {} for part {} failed: {!r}",
                filename,
                part_path,
                exc,
            )
            results.append(FileRecoveryResult(filename, "failed", repr(exc)))
    return results


def _recover_checksums_replicated(
    *,
    filename: str,
    part_path: str,
    missing_files: List[str],
    recovery_ctx: PartRecoveryContext,
    ctx: Optional[Context],
    disk_name: Optional[str],
    disk_config: Optional[dict],
    ch_version: Optional[str],
) -> Tuple[FileRecoveryResult, List[str]]:
    """Attempt to regenerate ``checksums.txt`` for a Replicated part via
    ``clickhouse-local``.  Returns a :class:`FileRecoveryResult`.
    """
    if ctx is None or disk_name is None or disk_config is None or ch_version is None:
        return (
            FileRecoveryResult(
                filename,
                "skipped",
                "clickhouse-local recovery not available (missing ctx/disk_name/disk_config/ch_version)",
            ),
            [],
        )

    raw_ddl = get_show_create_table(ctx, recovery_ctx.database, recovery_ctx.table)
    if raw_ddl is None:
        return (
            FileRecoveryResult(
                filename,
                "skipped",
                f"SHOW CREATE TABLE failed for {recovery_ctx.quoted_table}",
            ),
            [],
        )

    try:
        recovery_ddl = build_recovery_ddl(raw_ddl, disk_name)
    except Exception as exc:
        return (
            FileRecoveryResult(
                filename,
                "failed",
                f"build_recovery_ddl failed: {exc!r}",
            ),
            [],
        )

    try:
        restored_files = recover_checksums_via_local(
            detached_part_path=part_path,
            part_name=recovery_ctx.part_name,
            database=recovery_ctx.database,
            table=recovery_ctx.table,
            disk_name=disk_name,
            disk_config=disk_config,
            create_table_ddl=recovery_ddl,
            ch_version=ch_version,
            missing_files=missing_files,
        )
    except Exception as exc:
        return (
            FileRecoveryResult(
                filename,
                "failed",
                f"clickhouse-local run failed: {exc!r}",
            ),
            [],
        )

    return (
        FileRecoveryResult(
            filename,
            "regenerated",
            "checksums.txt regenerated via clickhouse-local",
        ),
        restored_files,
    )
