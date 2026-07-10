# pylint: disable=protected-access

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from ch_tools.chadmin.internal.clickhouse_disks import ClickHouseDiskClient
from ch_tools.chadmin.internal.object_storage import broken_part_recovery as recovery
from ch_tools.chadmin.internal.object_storage.broken_part_recovery import (
    COUNT_FILE,
    DiskInfo,
    LogicalFile,
    PartColumn,
    SourceTable,
    TableRef,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    get_object_storage_key,
    object_exists,
)


def logical_file(name: str, intact: bool = True) -> LogicalFile:
    return LogicalFile(name, Path(name), intact, [], 1)


def test_columns_round_trip() -> None:
    value = (
        "columns format version: 1\n"
        "2 columns:\n"
        "`a``b` UInt64\n"
        "`payload` Nullable(String)\n"
    )

    columns = recovery.parse_columns_text(value)

    assert [(column.name, column.type) for column in columns] == [
        ("a`b", "UInt64"),
        ("payload", "Nullable(String)"),
    ]
    assert recovery.render_columns_text(columns) == value.encode()
    assert recovery.parse_columns_text(value + "\n\n") == columns


def test_columns_substreams_round_trip_and_filter_serialization() -> None:
    value = (
        "columns substreams version: 1\n"
        "2 columns:\n"
        "1 substreams for column `a`:\n"
        "\ta\n"
        "2 substreams for column `payload`:\n"
        "\tpayload.null\n"
        "\tpayload\n"
    )
    columns = [
        PartColumn("a", "UInt64", "`a` UInt64"),
        PartColumn("payload", "Nullable(String)", "`payload` Nullable(String)"),
    ]

    substreams = recovery.parse_columns_substreams(value)

    assert recovery.render_columns_substreams(substreams, columns) == value.encode()
    assert recovery.parse_columns_substreams(value + "\n") == substreams
    serialized = recovery.filter_serialization(
        {
            "columns": [
                {"name": "a", "kind": "Default"},
                {"name": "payload", "kind": "Default"},
            ],
            "version": 1,
        },
        columns[:1],
    )
    assert json.loads(serialized) == {
        "columns": [{"name": "a", "kind": "Default"}],
        "version": 1,
    }


def test_compact_part_is_only_recoverable_as_a_whole() -> None:
    columns = [
        PartColumn("a", "UInt64", "`a` UInt64"),
        PartColumn("b", "String", "`b` String"),
    ]
    files = {
        COUNT_FILE: logical_file(COUNT_FILE),
        "data.bin": logical_file("data.bin"),
        "data.mrk3": logical_file("data.mrk3"),
    }

    intact = recovery._analyze_compact(columns, files, 12, None, None)
    assert intact.recovered_columns == columns
    assert intact.files_to_copy == {COUNT_FILE, "data.bin", "data.mrk3"}

    files["data.bin"].intact = False
    broken = recovery._analyze_compact(columns, files, 12, None, None)
    assert not broken.recovered_columns
    assert set(broken.lost_files_by_column) == {"a", "b"}


@patch(
    "ch_tools.chadmin.internal.object_storage.broken_part_recovery._hash_stream_names"
)
def test_wide_part_recovers_only_columns_with_all_streams(
    hash_stream_names: MagicMock,
) -> None:
    hash_stream_names.return_value = ["hash_a", "hash_b"]
    columns = [
        PartColumn("a", "UInt64", "`a` UInt64"),
        PartColumn("b", "String", "`b` String"),
    ]
    files = {
        COUNT_FILE: logical_file(COUNT_FILE),
        "a.bin": logical_file("a.bin"),
        "a.mrk2": logical_file("a.mrk2"),
        "b.bin": logical_file("b.bin", intact=False),
        "b.mrk2": logical_file("b.mrk2"),
    }

    analysis = recovery._analyze_wide_with_substreams(
        MagicMock(),
        columns,
        files,
        7,
        {"a": ["a"], "b": ["b"]},
        None,
    )

    assert [column.name for column in analysis.recovered_columns] == ["a"]
    assert analysis.files_to_copy == {COUNT_FILE, "a.bin", "a.mrk2"}
    assert analysis.lost_files_by_column == {"b": ["b.bin"]}


@patch("ch_tools.chadmin.internal.object_storage.broken_part_recovery.execute_query")
def test_path_infers_table_from_detached_data_path(
    execute_query: MagicMock, tmp_path: Path
) -> None:
    execute_query.return_value = {"data": []}
    table_root = tmp_path / "store" / "table"
    part_path = table_root / "detached" / "broken_all_1_1_0"
    part_path.mkdir(parents=True)
    table = SourceTable(
        TableRef("db", "source"),
        "MergeTree",
        "s3",
        [str(table_root)],
    )

    assert recovery._infer_table_from_path(MagicMock(), part_path, [table]) == TableRef(
        "db", "source"
    )


@patch(
    "ch_tools.chadmin.internal.object_storage.broken_part_recovery._validate_policy_disk"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.broken_part_recovery._find_disk_for_path"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.broken_part_recovery._infer_table_from_path"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.broken_part_recovery._list_merge_tree_tables"
)
def test_explicit_table_is_fallback_when_path_cannot_be_inferred(
    list_tables: MagicMock,
    infer_table: MagicMock,
    find_disk: MagicMock,
    validate_policy: MagicMock,
    tmp_path: Path,
) -> None:
    part_path = tmp_path / "broken_all_1_1_0"
    part_path.mkdir()
    source_table = SourceTable(
        TableRef("db", "source"),
        "MergeTree",
        "s3",
        [str(tmp_path / "unrelated")],
    )
    list_tables.return_value = [source_table]
    infer_table.return_value = None
    find_disk.return_value = DiskInfo("s3", tmp_path, "s3")

    source = recovery.resolve_recovery_source(
        MagicMock(), "db", "source", None, str(part_path)
    )

    assert source.table == source_table
    assert source.relative_path == part_path.name
    validate_policy.assert_called_once()


@patch(
    "ch_tools.chadmin.internal.object_storage.broken_part_recovery._infer_table_from_path"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.broken_part_recovery._list_merge_tree_tables"
)
def test_unresolved_path_without_table_is_an_error(
    list_tables: MagicMock,
    infer_table: MagicMock,
    tmp_path: Path,
) -> None:
    part_path = tmp_path / "broken_all_1_1_0"
    part_path.mkdir()
    list_tables.return_value = []
    infer_table.return_value = None

    with pytest.raises(ValueError, match="Cannot infer source table"):
        recovery.resolve_recovery_source(MagicMock(), None, None, None, str(part_path))


@pytest.mark.parametrize(
    "version,expected_tail",
    [
        ("24.6.1.1", ["read", "path with spaces/file.bin"]),
        (
            "24.7.1.1",
            ["--query", "read 'path with spaces/file.bin'"],
        ),
    ],
)
@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_uses_argument_list_without_shell(
    run: MagicMock,
    _logging: MagicMock,
    version: str,
    expected_tail: list[str],
) -> None:
    run.return_value = MagicMock(returncode=0, stdout=b"data", stderr=b"")
    client = ClickHouseDiskClient("s3", version, "/tmp/disks.xml")

    assert client.read("path with spaces/file.bin") == b"data"

    arguments = run.call_args.args[0]
    assert arguments[:6] == [
        "sudo",
        "-u",
        "clickhouse",
        "env",
        "HOME=/tmp",
        "clickhouse-disks",
    ]
    assert arguments[-2:] == expected_tail
    assert run.call_args.kwargs["check"] is False


def test_object_storage_key_respects_metadata_version() -> None:
    relative = S3ObjectLocalInfo("object", 1, False)
    full = S3ObjectLocalInfo("other/object", 1, True)

    assert get_object_storage_key("prefix/", relative) == "prefix/object"
    assert get_object_storage_key("prefix/", full) == "other/object"


def test_object_exists_only_swallows_not_found() -> None:
    client = MagicMock()
    client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "missing"}},
        "HeadObject",
    )
    assert not object_exists(client, "bucket", "missing")

    client.head_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "HeadObject",
    )
    with pytest.raises(ClientError):
        object_exists(client, "bucket", "forbidden")
