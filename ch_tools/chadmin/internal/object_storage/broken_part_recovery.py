from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import boto3
from boto3 import client as Boto3Client
from botocore.client import Config
from click import Context

from ch_tools.chadmin.internal.clickhouse_disks import ClickHouseDiskClient
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalMetaData,
    get_object_storage_key,
    object_exists,
)
from ch_tools.chadmin.internal.part import attach_part, get_disks
from ch_tools.chadmin.internal.system import get_version
from ch_tools.chadmin.internal.table import check_table, delete_table
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.utils import escape_for_file_name, version_ge

# pylint: disable=too-many-lines


COLUMNS_FILE = "columns.txt"
COLUMNS_SUBSTREAMS_FILE = "columns_substreams.txt"
COUNT_FILE = "count.txt"
SERIALIZATION_FILE = "serialization.json"
ROW_EXISTS_COLUMN = "_row_exists"
RECOVERY_ROW_EXISTS_COLUMN = "_recovery_row_exists"
STAGING_PART_NAME = "all_0_0_0"
MIN_CLICKHOUSE_VERSION = "25.8"
MARK_SUFFIX_RE = re.compile(r"\.(?:c?mrk\d*)$")


@dataclass(frozen=True)
class TableRef:
    database: str
    table: str

    @property
    def sql(self) -> str:
        return f"{quote_identifier(self.database)}.{quote_identifier(self.table)}"

    @property
    def display(self) -> str:
        return f"{self.database}.{self.table}"


@dataclass(frozen=True)
class SourceTable:
    ref: TableRef
    storage_policy: str
    data_paths: List[str]


@dataclass(frozen=True)
class DiskInfo:
    name: str
    root: Path


@dataclass(frozen=True)
class PartColumn:
    name: str
    type: str
    definition: str


@dataclass(frozen=True)
class RecoverySource:
    table: SourceTable
    disk: DiskInfo
    path: Path
    relative_path: str
    part_name: str


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


def quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def quote_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def parse_qualified_table(value: str) -> TableRef:
    if "." not in value:
        raise ValueError("Target table must be specified as DATABASE.TABLE")
    database, table = value.split(".", 1)
    if not database or not table:
        raise ValueError("Target table must be specified as DATABASE.TABLE")
    return TableRef(database, table)


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


def resolve_recovery_source(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    part_name: Optional[str],
    part_path: Optional[str],
) -> RecoverySource:
    if bool(part_name) == bool(part_path):
        raise ValueError("Specify exactly one of --name and --path")
    if bool(database) != bool(table):
        raise ValueError("--database and --table must be specified together")
    if part_name and not database:
        raise ValueError("--name requires --database and --table")

    tables = _list_merge_tree_tables(ctx)
    explicit_ref = TableRef(database, table) if database and table else None

    if part_name:
        assert explicit_ref is not None
        detached = _find_detached_part(ctx, explicit_ref, part_name)
        path = Path(detached["path"]).resolve()
        inferred_ref: Optional[TableRef] = explicit_ref
        display_part_name = detached["name"]
    else:
        assert part_path is not None
        path = Path(part_path).resolve()
        inferred_ref = _infer_table_from_path(ctx, path, tables)
        display_part_name = path.name

    if explicit_ref and inferred_ref and explicit_ref != inferred_ref:
        raise ValueError(
            f"Path belongs to {inferred_ref.display}, not {explicit_ref.display}"
        )
    source_ref = explicit_ref or inferred_ref
    if source_ref is None:
        raise ValueError(
            "Cannot infer source table from --path; specify --database and --table"
        )

    source_table = next((item for item in tables if item.ref == source_ref), None)
    if source_table is None:
        raise ValueError(f"Source table {source_ref.display} does not exist")
    if not path.is_dir():
        raise ValueError(f"Part path is not a directory: {path}")

    disk = _find_disk_for_path(ctx, path)
    _validate_policy_disk(ctx, source_table.storage_policy, disk.name)
    relative_path = str(path.relative_to(disk.root))
    return RecoverySource(
        source_table,
        disk,
        path,
        relative_path,
        display_part_name,
    )


def _list_merge_tree_tables(ctx: Context) -> List[SourceTable]:
    rows = execute_query(
        ctx,
        """
        SELECT database, name, storage_policy, data_paths
        FROM system.tables
        WHERE engine LIKE '%MergeTree%'
        """,
        format_="JSON",
    )["data"]
    return [
        SourceTable(
            TableRef(row["database"], row["name"]),
            row["storage_policy"],
            list(row["data_paths"]),
        )
        for row in rows
    ]


def _find_detached_part(
    ctx: Context, table: TableRef, part_name: str
) -> Dict[str, Any]:
    rows = execute_query(
        ctx,
        f"""
        SELECT database, table, name, path, disk
        FROM system.detached_parts
        WHERE database = {quote_string(table.database)}
          AND table = {quote_string(table.table)}
          AND name = {quote_string(part_name)}
        """,
        format_="JSON",
    )["data"]
    if len(rows) != 1:
        raise ValueError(
            f"Expected one detached part {table.display}.{part_name}, found {len(rows)}"
        )
    return rows[0]


def _infer_table_from_path(
    ctx: Context, path: Path, tables: List[SourceTable]
) -> Optional[TableRef]:
    normalized = str(path).rstrip("/") + "/"
    rows = execute_query(
        ctx,
        f"""
        SELECT database, table
        FROM system.detached_parts
        WHERE path = {quote_string(normalized)}
           OR path = {quote_string(str(path))}
        """,
        format_="JSON",
    )["data"]
    refs = {TableRef(row["database"], row["table"]) for row in rows}
    if len(refs) > 1:
        raise ValueError(f"Part path matches multiple tables: {path}")
    if refs:
        return next(iter(refs))

    matches = []
    for source_table in tables:
        for data_path in source_table.data_paths:
            detached_root = Path(data_path).resolve() / "detached"
            if path.is_relative_to(detached_root):
                matches.append(source_table.ref)
                break
    unique_matches = set(matches)
    if len(unique_matches) > 1:
        raise ValueError(f"Part path matches multiple table data paths: {path}")
    return next(iter(unique_matches)) if unique_matches else None


def _find_disk_for_path(ctx: Context, path: Path) -> DiskInfo:
    disks = [
        DiskInfo(name, Path(info["path"]).resolve())
        for name, info in get_disks(ctx).items()
        if path.is_relative_to(Path(info["path"]).resolve())
    ]
    if not disks:
        raise ValueError(f"Part path is outside configured ClickHouse disks: {path}")
    return max(disks, key=lambda disk: len(str(disk.root)))


def _validate_policy_disk(ctx: Context, policy: str, disk: str) -> None:
    rows = execute_query(
        ctx,
        f"""
        SELECT 1
        FROM system.storage_policies
        WHERE policy_name = {quote_string(policy)}
          AND has(disks, {quote_string(disk)})
        LIMIT 1
        """,
        format_="JSONCompact",
    )["data"]
    if not rows:
        raise ValueError(f"Disk {disk} is not part of storage policy {policy}")


def inspect_logical_files(
    source: RecoverySource,
    s3_client: Boto3Client,
    disk_conf: S3DiskConfiguration,
) -> Dict[str, bool]:
    result: Dict[str, bool] = {}
    for path in sorted(source.path.iterdir()):
        if not path.is_file():
            continue
        try:
            metadata = S3ObjectLocalMetaData.from_file(path)
            result[path.name] = all(
                object_exists(
                    s3_client,
                    disk_conf.bucket_name,
                    get_object_storage_key(disk_conf.prefix, object_info),
                )
                for object_info in metadata.objects
            )
        except (OSError, ValueError) as e:
            logging.warning("Cannot parse metadata file {}: {!r}", path, e)
            result[path.name] = False
    return result


def analyze_part(
    ctx: Context,
    disk_client: ClickHouseDiskClient,
    source: RecoverySource,
    files: Dict[str, bool],
) -> RecoveryAnalysis:
    _require_intact(files, COLUMNS_FILE)
    _require_intact(files, COUNT_FILE)
    columns_text = _read_source_file(disk_client, source, COLUMNS_FILE).decode()
    columns = parse_columns_text(columns_text)
    rows = int(_read_source_file(disk_client, source, COUNT_FILE).decode().strip())

    substreams = None
    if COLUMNS_SUBSTREAMS_FILE in files:
        _require_intact(files, COLUMNS_SUBSTREAMS_FILE)
        substreams = parse_columns_substreams(
            _read_source_file(disk_client, source, COLUMNS_SUBSTREAMS_FILE).decode()
        )
        _validate_columns_substreams(columns, substreams)

    serialization = None
    if SERIALIZATION_FILE in files:
        _require_intact(files, SERIALIZATION_FILE)
        serialization = json.loads(
            _read_source_file(disk_client, source, SERIALIZATION_FILE)
        )

    if "data.bin" in files and any(
        name.startswith("data.mrk") or name.startswith("data.cmrk") for name in files
    ):
        analysis = _analyze_compact(columns, files, rows, substreams, serialization)
    else:
        analysis = _analyze_wide(ctx, columns, files, rows, substreams, serialization)

    if not analysis.recovered_user_columns:
        raise ValueError("No fully recoverable user columns found")
    if any(column.name == ROW_EXISTS_COLUMN for column in columns):
        if not analysis.has_row_exists:
            raise ValueError("The lightweight-delete mask is not fully recoverable")
    return analysis


def _analyze_compact(
    columns: List[PartColumn],
    files: Dict[str, bool],
    rows: int,
    substreams: Optional[Dict[str, List[str]]],
    serialization: Optional[Dict[str, Any]],
) -> RecoveryAnalysis:
    mark_files = sorted(
        name
        for name in files
        if name.startswith("data.mrk") or name.startswith("data.cmrk")
    )
    required = {"data.bin", *mark_files}
    missing = sorted(name for name in required if not files.get(name, False))
    recovered = columns if not missing and mark_files else []
    lost = (
        {}
        if recovered
        else {column.name: missing or ["data marks"] for column in columns}
    )
    copy_files = set(required) if recovered else set()
    copy_files.add(COUNT_FILE)
    return RecoveryAnalysis(
        "Compact",
        columns,
        list(recovered),
        lost,
        copy_files,
        rows,
        substreams,
        serialization,
    )


def _analyze_wide(
    ctx: Context,
    columns: List[PartColumn],
    files: Dict[str, bool],
    rows: int,
    substreams: Optional[Dict[str, List[str]]],
    serialization: Optional[Dict[str, Any]],
) -> RecoveryAnalysis:
    if substreams is not None:
        return _analyze_wide_with_substreams(
            ctx, columns, files, rows, substreams, serialization
        )
    return _analyze_legacy_wide(columns, files, rows, serialization)


def _analyze_wide_with_substreams(
    ctx: Context,
    columns: List[PartColumn],
    files: Dict[str, bool],
    rows: int,
    substreams: Dict[str, List[str]],
    serialization: Optional[Dict[str, Any]],
) -> RecoveryAnalysis:
    all_streams = [
        stream for column in columns for stream in substreams.get(column.name, [])
    ]
    missing_stream_columns = [
        column.name for column in columns if not substreams.get(column.name)
    ]
    if missing_stream_columns:
        raise ValueError(
            "No stream metadata for columns: " + ", ".join(missing_stream_columns)
        )
    hashes = _hash_stream_names(ctx, all_streams)
    hash_by_stream = dict(zip(all_streams, hashes))

    recovered: List[PartColumn] = []
    lost: Dict[str, List[str]] = {}
    copy_files: Set[str] = {COUNT_FILE}
    for column in columns:
        required: Set[str] = set()
        missing: List[str] = []
        for stream in substreams[column.name]:
            base = _resolve_stream_base(stream, hash_by_stream[stream], files)
            if base is None:
                missing.append(stream)
                continue
            data_file = base + ".bin"
            mark_files = _marks_for_base(base, files)
            if data_file not in files or not files[data_file]:
                missing.append(data_file)
            if not mark_files:
                missing.append(base + ".mrk*")
            else:
                missing.extend(mark for mark in mark_files if not files[mark])
            required.add(data_file)
            required.update(mark_files)
        if missing:
            lost[column.name] = sorted(set(missing))
        else:
            recovered.append(column)
            copy_files.update(required)

    return RecoveryAnalysis(
        "Wide",
        columns,
        recovered,
        lost,
        copy_files,
        rows,
        substreams,
        serialization,
    )


def _analyze_legacy_wide(
    columns: List[PartColumn],
    files: Dict[str, bool],
    rows: int,
    serialization: Optional[Dict[str, Any]],
) -> RecoveryAnalysis:
    data_files = [name for name in files if name.endswith(".bin")]
    bases_by_column: Dict[str, Set[str]] = {}
    owner_groups_by_base: Dict[str, Set[str]] = {}

    for column in columns:
        escaped = escape_for_file_name(column.name)
        owner_group = column.name.split(".", 1)[0]
        nested = escape_for_file_name(owner_group)
        bases = {
            name[:-4]
            for name in data_files
            if name[:-4] == escaped
            or name[:-4].startswith(escaped + ".")
            or name[:-4].startswith(escaped + "%2E")
            or (
                "." in column.name
                and (name[:-4] == nested or name[:-4].startswith(nested + "."))
            )
        }
        if not bases:
            raise ValueError(
                f"Cannot unambiguously map legacy streams for column {column.name}"
            )
        bases_by_column[column.name] = bases
        for base in bases:
            owner_groups_by_base.setdefault(base, set()).add(owner_group)

    ambiguous = sorted(
        base for base, owners in owner_groups_by_base.items() if len(owners) > 1
    )
    if ambiguous:
        raise ValueError(
            "Legacy streams match unrelated columns: " + ", ".join(ambiguous)
        )

    recovered: List[PartColumn] = []
    lost: Dict[str, List[str]] = {}
    copy_files: Set[str] = {COUNT_FILE}
    for column in columns:
        bases = bases_by_column[column.name]
        required = {base + ".bin" for base in bases}
        for base in bases:
            required.update(_marks_for_base(base, files))
        missing = sorted(
            name for name in required if name not in files or not files.get(name, False)
        )
        if any(not _marks_for_base(base, files) for base in bases):
            missing.append("marks")
        if missing:
            lost[column.name] = sorted(set(missing))
        else:
            recovered.append(column)
            copy_files.update(required)

    return RecoveryAnalysis(
        "Wide",
        columns,
        recovered,
        lost,
        copy_files,
        rows,
        None,
        serialization,
    )


def _hash_stream_names(ctx: Context, streams: List[str]) -> List[str]:
    if not streams:
        return []
    values = ", ".join(quote_string(stream) for stream in streams)
    rows = execute_query(
        ctx,
        f"""
        SELECT arrayMap(
            x -> lower(hex(reverse(CAST(sipHash128(x), 'FixedString(16)')))),
            [{values}]
        )
        """,
        format_="JSONCompact",
    )["data"]
    return list(rows[0][0])


def _resolve_stream_base(
    stream: str, stream_hash: str, files: Dict[str, bool]
) -> Optional[str]:
    if stream + ".bin" in files or _marks_for_base(stream, files):
        return stream
    if stream_hash + ".bin" in files or _marks_for_base(stream_hash, files):
        return stream_hash
    return None


def _marks_for_base(base: str, files: Dict[str, bool]) -> List[str]:
    return sorted(
        name
        for name in files
        if name.startswith(base + ".") and MARK_SUFFIX_RE.search(name)
    )


def _require_intact(files: Dict[str, bool], filename: str) -> None:
    if filename not in files or not files[filename]:
        raise ValueError(f"Required structural file is not recoverable: {filename}")


def _read_source_file(
    disk_client: ClickHouseDiskClient, source: RecoverySource, filename: str
) -> bytes:
    return disk_client.read(os.path.join(source.relative_path, filename))


def recover_part(
    ctx: Context,
    database: Optional[str],
    table: Optional[str],
    part_name: Optional[str],
    part_path: Optional[str],
    target_table: str,
) -> List[Dict[str, Any]]:
    ch_version = get_version(ctx)
    if not version_ge(ch_version, MIN_CLICKHOUSE_VERSION):
        raise RuntimeError(
            f"Part recovery requires ClickHouse version {MIN_CLICKHOUSE_VERSION} or above"
        )

    target = parse_qualified_table(target_table)
    source = resolve_recovery_source(ctx, database, table, part_name, part_path)
    _assert_target_absent(ctx, target)

    ch_config = get_clickhouse_config(ctx)
    disk_conf = S3DiskConfiguration.from_config(
        ch_config.storage_configuration,
        source.disk.name,
        ctx.obj["config"]["object_storage"]["bucket_name_prefix"],
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
        config=Config(
            s3={"addressing_style": "auto"},
            retries=ctx.obj["config"]["object_storage"].get("retries"),
        ),
    )
    disk_client = ClickHouseDiskClient(source.disk.name)
    files = inspect_logical_files(source, s3_client, disk_conf)
    analysis = analyze_part(ctx, disk_client, source, files)

    stage = TableRef(
        source.table.ref.database,
        f"_chadmin_recover_{uuid.uuid4().hex}",
    )
    target_created = False
    stage_created = False
    try:
        _create_recovery_table(
            ctx,
            stage,
            [column for column in analysis.columns if column.name != ROW_EXISTS_COLUMN],
            source.table.storage_policy,
        )
        stage_created = True
        stage_part_path = _prepare_staging_part(
            ctx, disk_client, source, stage, analysis, files
        )
        logging.info("Prepared recovery staging part at {}", stage_part_path)
        attach_part(
            ctx,
            stage.database,
            stage.table,
            STAGING_PART_NAME,
            echo=False,
        )
        active_part = _get_only_active_part(ctx, stage)
        if not check_table(ctx, stage.database, stage.table, part=active_part):
            raise RuntimeError("CHECK TABLE failed for the staging part")

        mask_settings = (
            " SETTINGS apply_deleted_mask = 0" if analysis.has_row_exists else ""
        )
        physical_rows = int(
            execute_query(
                ctx,
                f"SELECT count() FROM {stage.sql}{mask_settings}",
                format_=None,
            )
        )
        if physical_rows != analysis.rows:
            raise RuntimeError(
                f"Staging row count differs: expected {analysis.rows}, got {physical_rows}"
            )

        execute_query(
            ctx,
            f"CREATE DATABASE IF NOT EXISTS {quote_identifier(target.database)}",
            format_=None,
        )
        target_columns = list(analysis.recovered_user_columns)
        if analysis.has_row_exists:
            target_columns.append(
                PartColumn(
                    RECOVERY_ROW_EXISTS_COLUMN,
                    "UInt8",
                    f"{quote_identifier(RECOVERY_ROW_EXISTS_COLUMN)} UInt8",
                )
            )
        _create_recovery_table(
            ctx,
            target,
            target_columns,
            source.table.storage_policy,
        )
        target_created = True
        _insert_recovered_data(ctx, stage, target, analysis)

        target_rows = int(
            execute_query(ctx, f"SELECT count() FROM {target.sql}", format_=None)
        )
        if target_rows != analysis.rows:
            raise RuntimeError(
                f"Target row count differs: expected {analysis.rows}, got {target_rows}"
            )
        if not check_table(ctx, target.database, target.table):
            raise RuntimeError("CHECK TABLE failed for the recovery target")
        return _make_report(source, target, analysis)
    except Exception:
        if target_created:
            delete_table(ctx, target.database, target.table)
        raise
    finally:
        if stage_created:
            delete_table(ctx, stage.database, stage.table)


def _assert_target_absent(ctx: Context, target: TableRef) -> None:
    rows = execute_query(
        ctx,
        f"""
        SELECT 1
        FROM system.tables
        WHERE database = {quote_string(target.database)}
          AND name = {quote_string(target.table)}
        LIMIT 1
        """,
        format_="JSONCompact",
    )["data"]
    if rows:
        raise ValueError(f"Target table already exists: {target.display}")


def _create_recovery_table(
    ctx: Context,
    table: TableRef,
    columns: List[PartColumn],
    storage_policy: str,
) -> None:
    if not columns:
        raise ValueError("Cannot create a recovery table without columns")
    definitions = ",\n".join(column.definition for column in columns)
    policy = (
        f" SETTINGS storage_policy = {quote_string(storage_policy)}"
        if storage_policy
        else ""
    )
    execute_query(
        ctx,
        f"""
        CREATE TABLE {table.sql}
        (
            {definitions}
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        {policy}
        """,
        format_=None,
    )


def _prepare_staging_part(
    ctx: Context,
    disk_client: ClickHouseDiskClient,
    source: RecoverySource,
    stage: TableRef,
    analysis: RecoveryAnalysis,
    files: Dict[str, bool],
) -> str:
    stage_root = _get_table_path_on_disk(ctx, stage, source.disk)
    relative_stage_root = str(stage_root.relative_to(source.disk.root))
    detached_root = os.path.join(relative_stage_root, "detached")
    stage_part = os.path.join(detached_root, STAGING_PART_NAME)
    disk_client.mkdir(stage_part, parents=True)

    for filename in sorted(analysis.files_to_copy):
        _require_intact(files, filename)
        disk_client.copy(
            os.path.join(source.relative_path, filename),
            os.path.join(stage_part, filename),
        )

    disk_client.write(
        os.path.join(stage_part, COLUMNS_FILE),
        render_columns_text(analysis.recovered_columns),
    )
    if analysis.serialization is not None:
        disk_client.write(
            os.path.join(stage_part, SERIALIZATION_FILE),
            filter_serialization(analysis.serialization, analysis.recovered_columns),
        )
    if analysis.columns_substreams is not None:
        disk_client.write(
            os.path.join(stage_part, COLUMNS_SUBSTREAMS_FILE),
            render_columns_substreams(
                analysis.columns_substreams, analysis.recovered_columns
            ),
        )
    return stage_part


def _get_table_path_on_disk(ctx: Context, table: TableRef, disk: DiskInfo) -> Path:
    rows = execute_query(
        ctx,
        f"""
        SELECT data_paths
        FROM system.tables
        WHERE database = {quote_string(table.database)}
          AND name = {quote_string(table.table)}
        """,
        format_="JSONCompact",
    )["data"]
    if len(rows) != 1:
        raise RuntimeError(f"Cannot get data paths for {table.display}")
    paths = [
        Path(item).resolve()
        for item in rows[0][0]
        if Path(item).resolve().is_relative_to(disk.root)
    ]
    if len(paths) != 1:
        raise RuntimeError(
            f"Cannot find a unique path for {table.display} on disk {disk.name}"
        )
    return paths[0]


def _get_only_active_part(ctx: Context, table: TableRef) -> str:
    rows = execute_query(
        ctx,
        f"""
        SELECT name
        FROM system.parts
        WHERE database = {quote_string(table.database)}
          AND table = {quote_string(table.table)}
          AND active
        """,
        format_="JSONCompact",
    )["data"]
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected one active staging part for {table.display}, found {len(rows)}"
        )
    return str(rows[0][0])


def _insert_recovered_data(
    ctx: Context,
    stage: TableRef,
    target: TableRef,
    analysis: RecoveryAnalysis,
) -> None:
    target_names = [
        quote_identifier(column.name) for column in analysis.recovered_user_columns
    ]
    select_names = list(target_names)
    if analysis.has_row_exists:
        target_names.append(quote_identifier(RECOVERY_ROW_EXISTS_COLUMN))
        select_names.append(
            f"{quote_identifier(ROW_EXISTS_COLUMN)} AS "
            f"{quote_identifier(RECOVERY_ROW_EXISTS_COLUMN)}"
        )
    mask_settings = (
        " SETTINGS apply_deleted_mask = 0" if analysis.has_row_exists else ""
    )
    execute_query(
        ctx,
        f"""
        INSERT INTO {target.sql} ({", ".join(target_names)})
        SELECT {", ".join(select_names)}
        FROM {stage.sql}
        {mask_settings}
        """,
        format_=None,
    )


def _make_report(
    source: RecoverySource,
    target: TableRef,
    analysis: RecoveryAnalysis,
) -> List[Dict[str, Any]]:
    recovered_names = {column.name for column in analysis.recovered_columns}
    result: List[Dict[str, Any]] = []
    for column in analysis.columns:
        recovered = column.name in recovered_names
        target_column: Optional[str] = column.name if recovered else None
        if column.name == ROW_EXISTS_COLUMN and recovered:
            target_column = RECOVERY_ROW_EXISTS_COLUMN
        result.append(
            {
                "source_table": source.table.ref.display,
                "source_part": source.part_name,
                "source_path": str(source.path),
                "target_table": target.display,
                "part_type": analysis.part_type,
                "source_column": column.name,
                "target_column": target_column,
                "type": column.type,
                "status": "recovered" if recovered else "lost",
                "missing_files": analysis.lost_files_by_column.get(column.name, []),
                "physical_rows": analysis.rows,
            }
        )
    return result
