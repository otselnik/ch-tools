from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from boto3 import client as Boto3Client
from botocore.exceptions import ClientError
from click import Context

from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    S3ObjectLocalMetaData,
    get_object_storage_key,
)
from ch_tools.chadmin.internal.part import (
    list_detached_parts,
    list_parts,
)
from ch_tools.chadmin.internal.partition import (
    attach_partition as attach_clickhouse_partition,
)
from ch_tools.chadmin.internal.partition import (
    detach_partition as detach_clickhouse_partition,
)
from ch_tools.common import logging
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration

ATTACH_DETACH_QUERY_RETRY = 10


@dataclass(frozen=True)
class BrokenPartInfo:
    database: str
    table: str
    partition_id: str
    name: str
    part_type: str
    rows: int
    default_compression_codec: str

    @property
    def table_id(self) -> str:
        return f"`{self.database}`.`{self.table}`"


@dataclass(frozen=True)
class MissingObject:
    filename: str
    metadata_path: Path
    metadata: S3ObjectLocalMetaData
    object_info: S3ObjectLocalInfo
    object_key: str


@dataclass
class BrokenPart:
    path: str
    info: BrokenPartInfo
    missing_objects: List[MissingObject]


def check_key_in_object_storage(s3_client: Boto3Client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def restore_recoverable_broken_partitions(
    ctx: Context,
    root_path: str,
    s3_client: Boto3Client,
    disk_conf: S3DiskConfiguration,
    detach_unrecoverable: bool,
) -> List[Dict[str, Any]]:
    broken_parts = find_broken_parts(ctx, root_path, s3_client, disk_conf)
    result: List[Dict[str, Any]] = []

    for broken_part in broken_parts:
        result.extend(
            restore_recoverable_broken_part(
                ctx=ctx,
                broken_part=broken_part,
                s3_client=s3_client,
                bucket=disk_conf.bucket_name,
                detach_unrecoverable=detach_unrecoverable,
            )
        )

    return result


def find_broken_parts(
    ctx: Context,
    root_path: str,
    s3_client: Boto3Client,
    disk_conf: S3DiskConfiguration,
) -> List[BrokenPart]:
    active_parts_by_path = get_active_parts_by_path(ctx)
    broken_parts_by_path: Dict[str, BrokenPart] = {}

    for path, _, files in os.walk(root_path):
        part_info = active_parts_by_path.get(f"{path}/")

        for file in files:
            metadata_path = Path(path) / file
            try:
                metadata = S3ObjectLocalMetaData.from_file(metadata_path)
            except Exception as e:
                logging.error(
                    "Failed to read object metadata {}: {!r}", metadata_path, e
                )
                continue

            for object_info in metadata.objects:
                object_key = get_object_storage_key(disk_conf.prefix, object_info)
                if check_key_in_object_storage(
                    s3_client, disk_conf.bucket_name, object_key
                ):
                    continue

                if part_info is None:
                    logging.warning("Skip failed path {}.", path)
                    break

                broken_part = broken_parts_by_path.setdefault(
                    path,
                    BrokenPart(path=path, info=part_info, missing_objects=[]),
                )
                broken_part.missing_objects.append(
                    MissingObject(
                        filename=file,
                        metadata_path=metadata_path,
                        metadata=metadata,
                        object_info=object_info,
                        object_key=object_key,
                    )
                )

    return sorted(broken_parts_by_path.values(), key=lambda item: item.path)


def restore_recoverable_broken_part(
    ctx: Context,
    broken_part: BrokenPart,
    s3_client: Boto3Client,
    bucket: str,
    detach_unrecoverable: bool,
) -> List[Dict[str, Any]]:
    report: List[Dict[str, Any]] = []
    unrecoverable = False

    missing_by_file: Dict[str, List[MissingObject]] = {}
    for missing_object in broken_part.missing_objects:
        missing_by_file.setdefault(missing_object.filename, []).append(missing_object)

    supported_attach_files = {"checksums.txt", "columns.txt"}
    if set(missing_by_file).issubset(supported_attach_files):
        filenames = sorted(missing_by_file)
        if all(
            filename == "checksums.txt"
            or (filename == "columns.txt" and can_restore_columns_txt(broken_part.info))
            for filename in filenames
        ):
            return restore_files_via_attach(ctx, broken_part.info, filenames)

        unrecoverable = True
        for filename in filenames:
            report.append(
                make_restore_report(
                    broken_part.info,
                    filename,
                    "unrecoverable",
                    "columns.txt can be restored only for Wide parts",
                )
            )

        if detach_unrecoverable and unrecoverable:
            report.append(detach_unrecoverable_part(ctx, broken_part.info))
        return report

    for filename, missing_objects in sorted(missing_by_file.items()):
        if filename in supported_attach_files:
            unrecoverable = True
            report.append(
                make_restore_report(
                    broken_part.info,
                    filename,
                    "skipped",
                    "metadata restore via attach is skipped while other files are missing",
                )
            )
        elif all(item.object_info.size == 0 for item in missing_objects):
            report.extend(
                restore_empty_objects(
                    broken_part.info, missing_objects, s3_client, bucket
                )
            )
        elif filename == "count.txt":
            report.append(
                restore_generated_file(
                    broken_part.info,
                    filename,
                    str(broken_part.info.rows).encode("utf-8"),
                    missing_objects,
                    s3_client,
                    bucket,
                )
            )
        elif filename == "default_compression_codec.txt":
            report.append(
                restore_generated_file(
                    broken_part.info,
                    filename,
                    broken_part.info.default_compression_codec.encode("utf-8"),
                    missing_objects,
                    s3_client,
                    bucket,
                )
            )
        else:
            unrecoverable = True
            report.append(
                make_restore_report(
                    broken_part.info,
                    filename,
                    "unrecoverable",
                    "file is not safely recoverable in v1",
                )
            )

    if detach_unrecoverable and unrecoverable:
        report.append(detach_unrecoverable_part(ctx, broken_part.info))

    return report


def restore_empty_objects(
    part_info: BrokenPartInfo,
    missing_objects: List[MissingObject],
    s3_client: Boto3Client,
    bucket: str,
) -> List[Dict[str, Any]]:
    report: List[Dict[str, Any]] = []
    for missing_object in missing_objects:
        s3_client.put_object(Bucket=bucket, Key=missing_object.object_key, Body=b"")
        head = s3_client.head_object(Bucket=bucket, Key=missing_object.object_key)
        if head.get("ContentLength") == 0:
            report.append(
                make_restore_report(
                    part_info,
                    missing_object.filename,
                    "restored",
                    "uploaded empty object",
                )
            )
        else:
            report.append(
                make_restore_report(
                    part_info,
                    missing_object.filename,
                    "unrecoverable",
                    "empty object upload verification failed",
                )
            )
    return report


def restore_generated_file(
    part_info: BrokenPartInfo,
    filename: str,
    content: bytes,
    missing_objects: List[MissingObject],
    s3_client: Boto3Client,
    bucket: str,
) -> Dict[str, Any]:
    metadata = missing_objects[0].metadata
    total_object_size = sum(item.size for item in metadata.objects)
    if metadata.total_size != len(content) or total_object_size != len(content):
        return make_restore_report(
            part_info,
            filename,
            "unrecoverable",
            f"generated size {len(content)} does not match metadata size {metadata.total_size}",
        )

    missing_keys = {item.object_key for item in missing_objects}
    offset = 0
    chunks_to_upload: List[Tuple[str, bytes]] = []
    for object_info in metadata.objects:
        object_key = get_full_object_key_from_sample(missing_objects[0], object_info)
        chunk = content[offset : offset + object_info.size]
        offset += object_info.size
        if object_key in missing_keys:
            chunks_to_upload.append((object_key, chunk))
            continue

        current = s3_client.get_object(Bucket=bucket, Key=object_key)["Body"].read()
        if current != chunk:
            return make_restore_report(
                part_info,
                filename,
                "unrecoverable",
                "existing object content does not match generated content",
            )

    for object_key, chunk in chunks_to_upload:
        s3_client.put_object(Bucket=bucket, Key=object_key, Body=chunk)

    return make_restore_report(
        part_info,
        filename,
        "restored",
        "uploaded generated content with exact metadata size",
    )


def restore_files_via_attach(
    ctx: Context, part_info: BrokenPartInfo, filenames: List[str]
) -> List[Dict[str, Any]]:
    if not detach_partition_with_retry(ctx, part_info):
        return [
            make_restore_report(
                part_info,
                filename,
                "detach_failed",
                "failed to detach part before metadata regeneration",
            )
            for filename in filenames
        ]

    detached_part_path = get_detached_part_path(ctx, part_info)
    if detached_part_path is None:
        attach_partition_with_retry(ctx, part_info)
        return [
            make_restore_report(
                part_info,
                filename,
                "unrecoverable",
                "detached part path was not found",
            )
            for filename in filenames
        ]

    backups: List[Tuple[Path, Path]] = []
    try:
        for filename in filenames:
            pointer_path = detached_part_path / filename
            if pointer_path.exists():
                backup_path = make_backup_path(
                    detached_part_path, part_info.name, filename
                )
                pointer_path.rename(backup_path)
                backups.append((backup_path, pointer_path))

        if attach_partition_with_retry(ctx, part_info):
            for backup_path, _ in backups:
                cleanup_backup_path(backup_path)
            return [
                make_restore_report(
                    part_info,
                    filename,
                    "restored",
                    "regenerated by ClickHouse ATTACH PARTITION",
                )
                for filename in filenames
            ]

        restore_backup_paths(backups)
        return [
            make_restore_report(
                part_info,
                filename,
                "attach_failed",
                "ClickHouse failed to attach part after pointer removal",
            )
            for filename in filenames
        ]
    except OSError as e:
        restore_backup_paths(backups)
        return [
            make_restore_report(
                part_info,
                filename,
                "unrecoverable",
                f"failed to move metadata pointer: {e}",
            )
            for filename in filenames
        ]


def repair_broken_partition(
    ctx: Context, part_info: BrokenPartInfo, attach: bool = True
) -> bool:
    if not detach_partition_with_retry(ctx, part_info):
        return False
    if attach:
        return attach_partition_with_retry(ctx, part_info)
    return True


def detach_unrecoverable_part(
    ctx: Context, part_info: BrokenPartInfo
) -> Dict[str, Any]:
    if repair_broken_partition(ctx, part_info, attach=False):
        return make_restore_report(
            part_info,
            "*",
            "detached",
            "part has unrecoverable missing objects",
        )
    return make_restore_report(
        part_info,
        "*",
        "detach_failed",
        "failed to detach part with unrecoverable missing objects",
    )


def can_restore_columns_txt(part_info: BrokenPartInfo) -> bool:
    return part_info.part_type.lower() == "wide"


def make_backup_path(detached_part_path: Path, part_name: str, filename: str) -> Path:
    backup_dir = Path(
        tempfile.mkdtemp(prefix=f"{part_name}.", dir=detached_part_path.parent)
    )
    return backup_dir / filename


def cleanup_backup_path(backup_path: Optional[Path]) -> None:
    if backup_path is None:
        return
    if backup_path.exists():
        backup_path.unlink()
    backup_path.parent.rmdir()


def restore_backup_paths(backups: List[Tuple[Path, Path]]) -> None:
    for backup_path, pointer_path in reversed(backups):
        if backup_path.exists() and not pointer_path.exists():
            backup_path.rename(pointer_path)
        backup_path.parent.rmdir()


def make_restore_report(
    part_info: BrokenPartInfo,
    filename: str,
    status: str,
    detail: str,
) -> Dict[str, Any]:
    return {
        "table": part_info.table_id,
        "partition": part_info.partition_id,
        "part": part_info.name,
        "file": filename,
        "status": status,
        "detail": detail,
    }


def make_partition_report(part_info: BrokenPartInfo) -> Dict[str, Any]:
    return {"table": part_info.table_id, "partition": part_info.partition_id}


def get_full_object_key_from_sample(
    sample_missing_object: MissingObject, object_info: S3ObjectLocalInfo
) -> str:
    if object_info.key_is_full:
        return object_info.key
    sample_key = sample_missing_object.object_info.key
    sample_full_key = sample_missing_object.object_key
    prefix = sample_full_key[: -len(sample_key)] if sample_key else ""
    return os.path.join(prefix, object_info.key)


def get_active_parts_by_path(ctx: Context) -> Dict[str, BrokenPartInfo]:
    return {
        part["path"]: BrokenPartInfo(
            database=part["database"],
            table=part["table"],
            partition_id=part["partition_id"],
            name=part["name"],
            part_type=part["part_type"],
            rows=int(part["rows"]),
            default_compression_codec=part["default_compression_codec"],
        )
        for part in list_parts(ctx, active=True)
    }


def get_detached_part_path(ctx: Context, part_info: BrokenPartInfo) -> Optional[Path]:
    detached_parts = list_detached_parts(
        ctx,
        database=part_info.database,
        table=part_info.table,
        part_name=part_info.name,
        limit=1,
    )
    if not detached_parts:
        return None
    return Path(detached_parts[0]["path"].rstrip("/"))


def detach_partition_with_retry(ctx: Context, part_info: BrokenPartInfo) -> bool:
    return query_with_retry(
        lambda: detach_clickhouse_partition(
            ctx, part_info.database, part_info.table, part_info.partition_id
        ),
        f"DETACH PARTITION {part_info.table_id}.{part_info.partition_id}",
        retries=ATTACH_DETACH_QUERY_RETRY,
    )


def attach_partition_with_retry(ctx: Context, part_info: BrokenPartInfo) -> bool:
    return query_with_retry(
        lambda: attach_clickhouse_partition(
            ctx, part_info.database, part_info.table, part_info.partition_id
        ),
        f"ATTACH PARTITION {part_info.table_id}.{part_info.partition_id}",
        retries=2 * ATTACH_DETACH_QUERY_RETRY,
    )


def query_with_retry(
    action: Callable[[], None], description: str, retries: int
) -> bool:
    logging.debug("Execute query: {}", description)
    for retry in range(retries):
        try:
            action()
            logging.info("Query {} finished successfully", description)
            return True
        except Exception as e:
            if retry + 1 == retries:
                logging.warning("Query {} failed  with:  {!r}\n", description, e)
                return False

    return False
