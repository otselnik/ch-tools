import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple

import boto3
from boto3 import client as Boto3Client
from botocore.exceptions import ClientError
from click import Context, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.clickhouse_disks import (
    CLICKHOUSE_METADATA_PATH,
    CLICKHOUSE_PATH,
    CLICKHOUSE_STORE_PATH,
    S3_METADATA_STORE_PATH,
    make_ch_disks_config,
    remove_from_ch_disk,
)
from ch_tools.chadmin.internal.object_storage.part_file_classifier import (
    RECOVERABLE,
    UNRECOVERABLE,
    classify_part,
)
from ch_tools.chadmin.internal.object_storage.part_recovery import (
    PartRecoveryResult,
    get_part_recovery_context,
    recover_part_files_locally,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalMetaData,
)
from ch_tools.chadmin.internal.part import attach_part, detach_part
from ch_tools.chadmin.internal.system import get_version
from ch_tools.chadmin.internal.utils import execute_query, remove_from_disk
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.client import OutputFormat
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel

ATTACH_DETTACH_TIMEOUT = 5000
ATTACH_DETACH_QUERY_RETRY = 10


class TablePartition(NamedTuple):
    table: str
    partition: str


@group("data-store", cls=Chadmin)
def data_store_group() -> None:
    """
    Commands for manipulating data stored by ClickHouse.
    """
    pass


@data_store_group.command("clean-orphaned-tables")
@pass_context
@option(
    "--column",
    "column",
    default=None,
    help="Additional check: specified COLUMN name should exists in data to be removed. Example: `initial_query_start_time_microseconds.bin` for `query_log`-table.",
)
@option(
    "--remove",
    is_flag=True,
    default=False,
    help="Flag to REMOVE data from store subdirectories.",
)
@option(
    "--store-path",
    "store_path",
    default=CLICKHOUSE_STORE_PATH,
    help="Set the store subdirectory path.",
)
@option(
    "--show-all-metadata",
    "show_only_orphaned_metadata",
    is_flag=True,
    default=True,
    help="Flag to only orphaned metadata.",
)
def clean_orphaned_tables_command(
    ctx: Context,
    column: Optional[str],
    remove: bool,
    store_path: str,
    show_only_orphaned_metadata: bool,
) -> None:
    results: List[Dict[str, Any]] = []
    for prefix in os.listdir(store_path):
        path = store_path + "/" + prefix
        try:
            path_result = process_path(path, prefix, column, remove)
        except subprocess.CalledProcessError as e:
            if "No such file or directory" in e.stdout.decode("utf-8"):
                print("Skip directory {} because it is removed: {}", path, e.stdout)
                continue
            raise

        if show_only_orphaned_metadata and path_result["status"] != "not_used":
            continue
        results.append(path_result)

    print_response(ctx, results, default_format="table")


def process_path(
    path: str,
    prefix: str,
    column: Optional[str],
    remove: bool,
) -> Dict[str, Any]:
    logging.info("Processing path {} with prefix {}:", path, prefix)

    result: Dict[str, Any] = {
        "path": path,
        "status": "unknown",
        "size": 0,
        "removed": False,
    }

    size = du(path)
    logging.info("Size of path {}: {}", path, size)
    result["size"] = size

    file = prefix_exists_in_metadata(prefix)

    if file:
        logging.info('Prefix "{}" is used in metadata file "{}"', prefix, file)
        result["status"] = "used"
        return result

    if column and not additional_check_successed(column, path):
        logging.info("Additional check for column-parameter not passed")
        result["status"] = "not_passed_column_check"
        return result

    logging.info('Prefix "{}" is NOT used in any metadata file', prefix)
    result["status"] = "not_used"

    if remove:
        logging.info('Trying to remove path "{}"', path)

        remove_data(path)
        result["removed"] = True
    else:
        logging.info(
            'Path "{}" is not removed because of remove-parameter is not specified',
            path,
        )
        result["removed"] = False

    return result


def prefix_exists_in_metadata(prefix: str) -> Optional[str]:
    for w in os.walk(CLICKHOUSE_PATH):
        dir_name = w[0]
        filenames = w[2]

        for file in filenames:
            if not file.endswith(".sql"):
                continue

            with open(dir_name + "/" + file, encoding="utf-8") as f:
                if f"'{prefix}" in f.read():
                    return file

    return None


def additional_check_successed(column: str, path: str) -> bool:
    for w in os.walk(path):
        filenames = w[2]

        columns = [file for file in filenames if column in file]
        if columns:
            return True

    return False


def du(path: str) -> str:
    return (
        subprocess.check_output(["du", "-sh", path], stderr=subprocess.STDOUT)
        .split()[0]
        .decode("utf-8")
    )


def remove_data(path: str) -> None:
    def onerror(*args: Any) -> None:
        errors = "\n".join(list(args))
        logging.error("ERROR: {}", errors)

    shutil.rmtree(path=path, onerror=onerror)  # pylint: disable=deprecated-argument


@data_store_group.command("cleanup-data-dir")
@pass_context
@option(
    "--remove",
    is_flag=True,
    default=False,
    help="Flag to REMOVE data from store subdirectories.",
)
@option(
    "--disk",
    "disk",
    default="default",
    help="Set the data subdirectory path.",
)
@option(
    "--keep-going",
    is_flag=True,
    default=False,
    help="Flag to REMOVE data from store subdirectories.",
)
@option(
    "--max-sql-objects",
    default=1000000,
    help="Restriction for max count of sql objects.",
)
@option(
    "--max-workers",
    default=4,
    help="Max workers for removing.",
)
@option(
    "--remove-only-metadata",
    is_flag=True,
    default=False,
    help="Flag to remove only local metadata.",
)
def cleanup_data_dir(
    ctx: Context,
    remove: bool,
    disk: str,
    keep_going: bool,
    max_sql_objects: int,
    max_workers: int,
    remove_only_metadata: bool,
) -> None:
    lost_data: List[Dict[str, Any]] = []
    path_to_disk = CLICKHOUSE_PATH + (f"/disks/{disk}" if disk != "default" else "")
    data_path = path_to_disk + "/data"

    collect_orphaned_sql_objects_recursive(
        CLICKHOUSE_METADATA_PATH,
        data_path,
        lost_data,
        0,
        1,
        max_sql_objects,
    )

    if remove:
        disk_config_path = make_ch_disks_config(disk)

        tasks: List[WorkerTask] = []
        if remove_only_metadata:
            for data in lost_data:
                task = WorkerTask(
                    data["path"], remove_orphaned_sql_object_metadata, {"data": data}
                )
                tasks.append(task)
        else:
            for data in lost_data:
                task = WorkerTask(
                    data["path"],
                    remove_orphaned_sql_object_full,
                    {
                        "data": data,
                        "disk": disk,
                        "path_to_disk": path_to_disk,
                        "disks_config_path": disk_config_path,
                        "ch_version": get_version(ctx),
                    },
                )
                tasks.append(task)
        execute_tasks_in_parallel(tasks, max_workers=max_workers, keep_going=keep_going)
    print_response(ctx, lost_data, default_format="table")


def remove_orphaned_sql_object_metadata(data: Dict[str, Any]) -> None:
    path = data["path"]

    remove_from_disk(path)
    data["deleted"] = "Yes"


def remove_orphaned_sql_object_full(
    data: Dict[str, Any],
    disk: str,
    path_to_disk: str,
    ch_version: str,
    disks_config_path: str,
) -> None:

    path = data["path"]

    if not path.startswith(path_to_disk):
        raise RuntimeError(f"Path {path} on fs does not math with disk {disk}")
    relative_path_on_disk = path[len(path_to_disk) + 1 :]
    retcode, stderr = remove_from_ch_disk(
        disk=disk,
        path=relative_path_on_disk,
        ch_version=ch_version,
        disk_config_path=disks_config_path,
    )
    if retcode:
        raise RuntimeError(
            f"clickhouse-disks remove command has failed: retcode {retcode}, stderr: {stderr.decode()}"
        )

    data["deleted"] = "Yes"


def collect_orphaned_sql_objects_recursive(
    metadata_path: str,
    data_path: str,
    lost_data: List[Dict[str, Any]],
    depth: int,
    max_depth: int,
    max_sql_objects: int,
) -> None:
    sql_suff = ".sql"
    # Extract all active sql object from metadata dir
    if max_sql_objects == len(lost_data):
        return

    list_sql_objects = [
        entry.name[: -len(sql_suff)]
        for entry in os.scandir(metadata_path)
        if entry.is_file() and entry.name.endswith(sql_suff)
    ]

    for entry in os.scandir(data_path):
        if max_sql_objects == len(lost_data):
            return

        if not entry.is_dir():
            continue
        if entry.name not in list_sql_objects:
            lost_data.append({"path": entry.path, "deleted": "No"})
            continue
        if max_depth >= depth + 1:
            collect_orphaned_sql_objects_recursive(
                metadata_path + "/" + entry.name,
                entry.path,
                lost_data,
                depth + 1,
                max_depth,
                max_sql_objects,
            )


@data_store_group.command("detect-broken-partitions")
@option(
    "--root-path",
    "root_path",
    default=S3_METADATA_STORE_PATH,
    help="Set the store subdirectory path.",
)
@option(
    "--restore-recoverable",
    "restore_recoverable",
    is_flag=True,
    default=False,
    help=(
        "For broken parts classified as recoverable, regenerate their "
        "missing structural files via DETACH PART + recovery + ATTACH "
        "PART. Compatible with --detach: unrecoverable parts are detached, "
        "recoverable ones are restored."
    ),
)
@option(
    "--detach",
    is_flag=True,
    default=False,
    help="Detach unrecoverable broken parts (whole partitions).",
)
@pass_context
def detect_broken_partitions(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ctx: Context,
    root_path: str,
    restore_recoverable: bool,
    detach: bool,
) -> None:
    ch_config = get_clickhouse_config(ctx)

    disk_conf = S3DiskConfiguration.from_config(
        ch_config.storage_configuration,
        "object_storage",
        ctx.obj["config"]["object_storage"]["bucket_name_prefix"],
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
    )

    # Pass 1: walk the metadata tree, collect (part_path -> [(filename, key)])
    parts: List[Tuple[str, List[Tuple[str, str]]]] = []
    keys_to_check: Set[str] = set()
    for path, _, files in os.walk(root_path):
        if not files:
            continue
        logging.debug("Checking files from: {}", path)
        part_files: List[Tuple[str, str]] = []
        for file in files:
            file_full_path = Path(os.path.join(path, file))
            try:
                meta = S3ObjectLocalMetaData.from_file(file_full_path)
            except Exception as e:
                logging.error(
                    "Failed to read S3 metadata for {}: {!r}", file_full_path, e
                )
                continue
            for obj in meta.objects:
                key = (
                    obj.key
                    if obj.key_is_full
                    else os.path.join(disk_conf.prefix, obj.key)
                )
                rel = os.path.relpath(str(file_full_path), path)
                part_files.append((rel, key))
                keys_to_check.add(key)
        if part_files:
            parts.append((path, part_files))

    # Pass 2: batch existence check for all unique keys (head_object + cache).
    existing_keys = _check_keys_exist(s3_client, disk_conf.bucket_name, keys_to_check)

    # Pass 3: per-part classification.
    broken_parts: List[Dict[str, Any]] = []
    for path, part_files in parts:
        missing = [rel for rel, key in part_files if key not in existing_keys]
        if not missing:
            continue
        table_partition = get_partition_by_path(ctx, path)
        part_type = _get_part_type_by_path(ctx, path)
        critical = _get_critical_columns(ctx, table_partition)
        status = classify_part(missing, part_type=part_type, critical_columns=critical)
        broken_parts.append(
            {
                "path": path,
                "table": table_partition.table if table_partition else None,
                "partition": table_partition.partition if table_partition else None,
                "part_type": part_type,
                "missing_files": missing,
                "status": status,
            }
        )

    # Pass 4: apply the requested action.
    #   --restore-recoverable -> per-recoverable-part: DETACH PART, regenerate
    #                            missing structural files, ATTACH PART.
    #   --detach              -> DETACH (whole partition) for parts that did
    #                            not get restored. Compatible with the flag
    #                            above: restore is attempted first.
    #   no flag               -> just report.
    repaired_partitions: Set[TablePartition] = set()
    recovery_results: List[PartRecoveryResult] = []
    for part in broken_parts:
        if part["table"] is None:
            logging.warning("Skip failed path {}.", part["path"])
            continue
        table_partition = TablePartition(part["table"], part["partition"])
        logging.debug(
            "Broken part path={} table={} partition={} status={}",
            part["path"],
            table_partition.table,
            table_partition.partition,
            part["status"],
        )

        restored = False
        if restore_recoverable and part["status"] == RECOVERABLE:
            result = _restore_recoverable_part(
                ctx,
                s3_client=s3_client,
                bucket=disk_conf.bucket_name,
                s3_prefix=disk_conf.prefix,
                part_path=part["path"],
                missing_files=part["missing_files"],
                disk_name="object_storage",
                disk_config=ch_config.storage_configuration.get_disk_config(
                    "object_storage"
                ),
                ch_version=get_version(ctx),
            )
            recovery_results.append(result)
            restored = result.reattached

        if not restored:
            if part["status"] == UNRECOVERABLE and detach:
                if table_partition not in repaired_partitions:
                    repaired_partitions.add(table_partition)
                    # Whole-partition DETACH as the last-resort fallback.
                    # Reattach is intentionally not attempted — the part
                    # would just land in detached/broken-on-start_* again.
                    detach_partition(ctx, table_partition)
            else:
                # Still surface the partition so the operator sees it.
                repaired_partitions.add(table_partition)

    print_partitions(ctx, repaired_partitions)

    if recovery_results:
        for r in recovery_results:
            logging.info("Recovery result: {}", r.to_dict())

    logging.debug(
        "Found {} broken part(s); affected partitions: {}",
        len(broken_parts),
        repaired_partitions,
    )


def _restore_recoverable_part(
    ctx: Context,
    *,
    s3_client: "Boto3Client",
    bucket: str,
    s3_prefix: str,
    part_path: str,
    missing_files: List[str],
    disk_name: Optional[str] = None,
    disk_config: Optional[dict] = None,
    ch_version: Optional[str] = None,
) -> PartRecoveryResult:
    """DETACH PART → regenerate missing files → ATTACH PART.

    ``disk_name``, ``disk_config``, and ``ch_version`` are forwarded to
    :func:`recover_part_files_locally` for the ``checksums.txt`` recovery
    path on ``ReplicatedMergeTree`` tables (via ``clickhouse-local``).
    """
    recovery_ctx = get_part_recovery_context(ctx, part_path)
    result = PartRecoveryResult(
        part_path=part_path,
        part_name=recovery_ctx.part_name if recovery_ctx else None,
        table=recovery_ctx.quoted_table if recovery_ctx else None,
    )
    if recovery_ctx is None:
        result.error = "no recovery context (part missing in system.parts)"
        return result

    try:
        detach_part(
            ctx, recovery_ctx.database, recovery_ctx.table, recovery_ctx.part_name
        )
        result.detached = True
    except Exception as exc:
        result.error = f"DETACH PART failed: {exc!r}"
        logging.warning(
            "DETACH PART '{}' on {} failed: {!r}",
            recovery_ctx.part_name,
            recovery_ctx.quoted_table,
            exc,
        )
        return result

    detached_part_path = _find_detached_part_path(part_path, recovery_ctx.part_name)
    if detached_part_path is None:
        result.error = "detached part directory not found after DETACH PART"
        return result

    result.files = recover_part_files_locally(
        part_path=detached_part_path,
        missing_files=missing_files,
        recovery_ctx=recovery_ctx,
        s3_client=s3_client,
        bucket=bucket,
        s3_prefix=s3_prefix,
        ctx=ctx,
        disk_name=disk_name,
        disk_config=disk_config,
        ch_version=ch_version,
    )

    try:
        attach_part(
            ctx, recovery_ctx.database, recovery_ctx.table, recovery_ctx.part_name
        )
        result.reattached = True
    except Exception as exc:
        result.error = f"ATTACH PART failed: {exc!r}"
        logging.warning(
            "ATTACH PART '{}' on {} failed: {!r}",
            recovery_ctx.part_name,
            recovery_ctx.quoted_table,
            exc,
        )
    return result


def _find_detached_part_path(active_path: str, part_name: str) -> Optional[str]:
    """
    Locate the directory of a part that has just been DETACHed.

    The active part path looks like ``…/<table>/<part_name>``; its
    detached counterpart sits next to it as ``…/<table>/detached/<part_name>``.
    """
    table_dir = os.path.dirname(active_path.rstrip("/"))
    candidate = os.path.join(table_dir, "detached", part_name)
    if os.path.isdir(candidate):
        return candidate
    logging.warning(
        "Could not find detached part path next to {} (looked for {})",
        active_path,
        candidate,
    )
    return None


def _check_keys_exist(s3_client: Boto3Client, bucket: str, keys: Set[str]) -> Set[str]:
    """Return the subset of ``keys`` that actually exists in S3."""
    existing: Set[str] = set()
    for key in keys:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            existing.add(key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("404", "NoSuchKey", "NotFound") or status == 404:
                continue
            logging.warning(
                "head_object failed for key={} bucket={}: {!r}", key, bucket, exc
            )
            # Treat ambiguous failures as "exists" so we don't trigger
            # destructive actions on transient errors.
            existing.add(key)
    return existing


def _get_part_type_by_path(ctx: Context, path: str) -> Optional[str]:
    query = (
        "SELECT part_type FROM system.parts "
        f"WHERE path = '{path}/' AND active LIMIT 1"
    )
    res = execute_query(ctx, query, format_=OutputFormat.JSONCompact)
    if not res.get("data"):
        return None
    return res["data"][0][0]


def _get_critical_columns(
    ctx: Context, table_partition: Optional["TablePartition"]
) -> List[str]:
    """Return columns participating in PARTITION BY / ORDER BY."""
    if table_partition is None:
        return []
    # table_partition.table is already quoted as `db`.`name` — split it back.
    try:
        db, name = table_partition.table.strip("`").split("`.`")
    except ValueError:
        return []
    query = (
        "SELECT sorting_key, partition_key FROM system.tables "
        f"WHERE database = '{db}' AND name = '{name}' LIMIT 1"
    )
    res = execute_query(ctx, query, format_=OutputFormat.JSONCompact)
    if not res.get("data"):
        return []
    sorting_key, partition_key = res["data"][0]
    return _extract_columns(sorting_key) + _extract_columns(partition_key)


def _extract_columns(key_expression: str) -> List[str]:
    """Extract bare column identifiers from a CH key expression.

    Drops function names so e.g. ``cityHash64(a), b`` yields ``[a, b]``.
    """
    if not key_expression:
        return []

    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", key_expression)
    result: List[str] = []
    for tok in tokens:
        if re.search(r"\b" + re.escape(tok) + r"\s*\(", key_expression):
            continue
        result.append(tok)
    return result


def get_partition_by_path(ctx: Context, path: str) -> Optional[TablePartition]:
    """
    Get partition from path
    """
    query_string = (
        f"SELECT database, table, partition FROM system.parts WHERE path='{path}/'"
    )
    res = execute_query(ctx, query_string, format_=OutputFormat.JSONCompact)
    if "data" not in res:
        logging.warning("Not found data for part with path {}", path)
        return None

    if len(res["data"]) != 1 or len(res["data"][0]) != 3:
        return None

    table = f"`{res['data'][0][0]}`.`{res['data'][0][1]}`"
    partition = res["data"][0][2]
    return TablePartition(table, partition)


def print_partitions(ctx: Context, repaired_partitions: Set[TablePartition]) -> None:
    """
    For each path of part match corresponding table and partition.
    """

    # It's not really necessary, just to make output stable for tests.
    partitions_list: List[TablePartition] = list(repaired_partitions)
    partitions_list.sort()

    result = [
        {"table": partition[0], "partition": partition[1]}
        for partition in partitions_list
    ]

    print_response(ctx, result, default_format="table")


def query_with_retry(ctx: Context, query: str, timeout: int, retries: int) -> bool:
    """
    Execute clickhouse query with given number of retries.
    """
    logging.debug("Execute query: {}", query)
    for retry in range(retries):
        try:
            res = execute_query(ctx, query, timeout=timeout)
            if res == "":
                break
        except Exception as e:
            if retry + 1 == retries:
                logging.warning("Query {} failed  with:  {!r}\n", query, e)
                return False
            continue

    logging.info("Query {} finished successfully", query)
    return True


def detach_partition(ctx: Context, table_partition: TablePartition) -> bool:
    """
    Run DETACH the partition.
    """

    detach_query = f"ALTER TABLE {table_partition.table} DETACH PARTITION '{table_partition.partition}'"
    return query_with_retry(
        ctx,
        detach_query,
        timeout=ATTACH_DETTACH_TIMEOUT,
        retries=ATTACH_DETACH_QUERY_RETRY,
    )
