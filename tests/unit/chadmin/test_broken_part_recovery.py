# pylint: disable=protected-access

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from ch_tools.chadmin.internal.object_storage import broken_part_recovery as recovery
from ch_tools.chadmin.internal.object_storage.broken_part_recovery import (
    COUNT_FILE,
    DiskInfo,
    PartColumn,
    SourceTable,
    TableRef,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    get_object_storage_key,
    object_exists,
)


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


def test_columns_parsers_accept_clickhouse_backslash_escapes() -> None:
    quote = chr(96)
    columns = recovery.parse_columns_text(
        "columns format version: 1\n"
        "1 columns:\n"
        f"{quote}a\\{quote}b{quote} String\n"
    )
    substreams = recovery.parse_columns_substreams(
        "columns substreams version: 1\n"
        "1 columns:\n"
        f"1 substreams for column {quote}a\\{quote}b{quote}:\n"
        "\ta%60b\n"
    )

    assert columns[0].name == f"a{quote}b"
    assert list(substreams) == [f"a{quote}b"]
    recovery._validate_columns_substreams(columns, substreams)


def test_columns_substreams_reject_duplicate_columns() -> None:
    quote = chr(96)
    value = (
        "columns substreams version: 1\n"
        "2 columns:\n"
        f"1 substreams for column {quote}a{quote}:\n"
        "\ta\n"
        f"1 substreams for column {quote}a{quote}:\n"
        "\ta\n"
    )

    with pytest.raises(ValueError, match="Duplicate substreams metadata"):
        recovery.parse_columns_substreams(value)


@pytest.mark.parametrize(
    "substreams,error",
    [
        ({"b": ["b"], "a": ["a"]}, "columns differ"),
        ({"a": ["other"], "b": ["b"]}, "Invalid substream"),
    ],
)
def test_columns_substreams_validate_order_and_prefix(
    substreams: dict[str, list[str]], error: str
) -> None:
    quote = chr(96)
    columns = [
        PartColumn("a", "UInt64", f"{quote}a{quote} UInt64"),
        PartColumn("b", "UInt64", f"{quote}b{quote} UInt64"),
    ]

    with pytest.raises(ValueError, match=error):
        recovery._validate_columns_substreams(columns, substreams)


def test_compact_part_is_only_recoverable_as_a_whole() -> None:
    columns = [
        PartColumn("a", "UInt64", "`a` UInt64"),
        PartColumn("b", "String", "`b` String"),
    ]
    files = {
        COUNT_FILE: True,
        "data.bin": True,
        "data.mrk3": True,
    }

    intact = recovery._analyze_compact(columns, files, 12, None, None)
    assert intact.recovered_columns == columns
    assert intact.files_to_copy == {COUNT_FILE, "data.bin", "data.mrk3"}

    files["data.bin"] = False
    broken = recovery._analyze_compact(columns, files, 12, None, None)
    assert not broken.recovered_columns
    assert set(broken.lost_files_by_column) == {"a", "b"}


def test_legacy_wide_part_uses_intact_column_streams() -> None:
    quote = chr(96)
    columns = [PartColumn("value", "UInt64", f"{quote}value{quote} UInt64")]
    files = {
        COUNT_FILE: True,
        "value.bin": True,
        "value.mrk2": True,
    }

    analysis = recovery._analyze_legacy_wide(columns, files, 3, None)

    assert analysis.recovered_columns == columns
    assert analysis.files_to_copy == {COUNT_FILE, "value.bin", "value.mrk2"}


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
        COUNT_FILE: True,
        "a.bin": True,
        "a.mrk2": True,
        "b.bin": False,
        "b.mrk2": True,
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
        "s3",
        [str(tmp_path / "unrelated")],
    )
    list_tables.return_value = [source_table]
    infer_table.return_value = None
    find_disk.return_value = DiskInfo("s3", tmp_path)

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


def test_recover_part_rejects_unsupported_clickhouse_before_source_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolve_source = MagicMock()
    monkeypatch.setattr(recovery, "get_version", MagicMock(return_value="25.7.9.1"))
    monkeypatch.setattr(recovery, "resolve_recovery_source", resolve_source)

    with pytest.raises(
        RuntimeError,
        match="Part recovery requires ClickHouse version 25.8 or above",
    ):
        recovery.recover_part(MagicMock(), None, None, None, "/part", "target.data")

    resolve_source.assert_not_called()


def patch_recover_part_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    source = recovery.RecoverySource(
        SourceTable(
            TableRef("source_db", "source"),
            "s3",
            ["/disk/source"],
        ),
        DiskInfo("s3", Path("/disk")),
        Path("/disk/source/detached/part"),
        "source/detached/part",
        "part",
    )
    column = PartColumn("value", "UInt64", "`value` UInt64")
    analysis = recovery.RecoveryAnalysis(
        "Wide",
        [column],
        [column],
        {},
        set(),
        1,
        None,
        None,
    )
    disk_conf = MagicMock(
        endpoint_url="http://minio",
        access_key_id="key",
        secret_access_key="secret",
    )
    monkeypatch.setattr(
        recovery, "resolve_recovery_source", MagicMock(return_value=source)
    )
    monkeypatch.setattr(recovery, "_assert_target_absent", MagicMock())
    monkeypatch.setattr(
        recovery,
        "_get_s3_disk_configuration",
        MagicMock(return_value=disk_conf),
    )
    monkeypatch.setattr(recovery.boto3, "client", MagicMock())
    monkeypatch.setattr(recovery, "ClickHouseDiskClient", MagicMock())
    monkeypatch.setattr(recovery, "get_version", MagicMock(return_value="25.8"))
    monkeypatch.setattr(recovery, "inspect_logical_files", MagicMock(return_value={}))
    monkeypatch.setattr(recovery, "analyze_part", MagicMock(return_value=analysis))
    monkeypatch.setattr(
        recovery.uuid,
        "uuid4",
        MagicMock(return_value=MagicMock(hex="stage")),
    )


def test_non_s3_disk_is_rejected_before_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_configuration = MagicMock()
    storage_configuration.get_disk_config.return_value = {"type": "azure_blob_storage"}
    monkeypatch.setattr(
        recovery,
        "get_clickhouse_config",
        MagicMock(return_value=MagicMock(storage_configuration=storage_configuration)),
    )
    ctx = MagicMock()
    ctx.obj = {"config": {"object_storage": {"bucket_name_prefix": ""}}}

    with pytest.raises(ValueError, match="supports only S3 disks"):
        recovery._get_s3_disk_configuration(ctx, "azure")


def test_stage_creation_failure_still_attempts_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_recover_part_dependencies(monkeypatch)
    create_table = MagicMock(side_effect=RuntimeError("stage creation failed"))
    delete_table = MagicMock()
    monkeypatch.setattr(recovery, "_create_recovery_table", create_table)
    monkeypatch.setattr(recovery, "delete_table", delete_table)
    ctx = MagicMock()
    ctx.obj = {"config": {"object_storage": {"bucket_name_prefix": "", "retries": {}}}}

    with pytest.raises(RuntimeError, match="stage creation failed"):
        recovery.recover_part(ctx, None, None, None, "/part", "target.data")

    create_table.assert_called_once()
    delete_table.assert_called_once_with(ctx, "source_db", "_chadmin_recover_stage")


def test_stage_cleanup_failure_preserves_creation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_recover_part_dependencies(monkeypatch)
    monkeypatch.setattr(
        recovery,
        "_create_recovery_table",
        MagicMock(side_effect=RuntimeError("stage creation failed")),
    )
    monkeypatch.setattr(
        recovery,
        "delete_table",
        MagicMock(side_effect=RuntimeError("cleanup failed")),
    )
    logging = MagicMock()
    monkeypatch.setattr(recovery, "logging", logging)
    ctx = MagicMock()
    ctx.obj = {"config": {"object_storage": {"bucket_name_prefix": "", "retries": {}}}}

    with pytest.raises(RuntimeError, match="stage creation failed"):
        recovery.recover_part(ctx, None, None, None, "/part", "target.data")

    logging.exception.assert_called_once()


def test_successfully_created_stage_is_cleaned_up_after_later_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_recover_part_dependencies(monkeypatch)
    delete_table = MagicMock()
    monkeypatch.setattr(recovery, "_create_recovery_table", MagicMock())
    monkeypatch.setattr(
        recovery,
        "_prepare_staging_part",
        MagicMock(side_effect=RuntimeError("staging failed")),
    )
    monkeypatch.setattr(recovery, "delete_table", delete_table)
    ctx = MagicMock()
    ctx.obj = {"config": {"object_storage": {"bucket_name_prefix": "", "retries": {}}}}

    with pytest.raises(RuntimeError, match="staging failed"):
        recovery.recover_part(ctx, None, None, None, "/part", "target.data")

    delete_table.assert_called_once_with(ctx, "source_db", "_chadmin_recover_stage")


def test_object_storage_key_respects_metadata_version() -> None:
    relative = S3ObjectLocalInfo("object", 1, False)
    full = S3ObjectLocalInfo("other/object", 1, True)

    assert get_object_storage_key("prefix/", relative) == "prefix/object"
    assert get_object_storage_key("", relative) == "object"
    assert (
        get_object_storage_key("", S3ObjectLocalInfo("/object", 1, False)) == "object"
    )
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
