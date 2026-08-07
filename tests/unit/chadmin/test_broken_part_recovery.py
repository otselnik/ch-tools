# pylint: disable=protected-access

import json
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

from ch_tools.chadmin.internal.object_storage import broken_part_recovery as recovery
from ch_tools.chadmin.internal.object_storage import (
    broken_partitions_recovery as partition_recovery,
)
from ch_tools.chadmin.internal.object_storage import part_recovery_metadata as metadata
from ch_tools.chadmin.internal.object_storage import (
    part_recovery_source as recovery_source,
)
from ch_tools.chadmin.internal.object_storage.broken_part_recovery import COUNT_FILE
from ch_tools.chadmin.internal.object_storage.part_recovery_metadata import (
    PartColumn,
    RecoveryAnalysis,
)
from ch_tools.chadmin.internal.object_storage.part_recovery_source import (
    DiskInfo,
    RecoverySource,
    SourceTable,
    TableRef,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    get_object_storage_key,
    object_exists,
    restore_empty_object,
)


def test_columns_round_trip() -> None:
    value = (
        "columns format version: 1\n"
        "2 columns:\n"
        "`a``b` UInt64\n"
        "`payload` Nullable(String)\n"
    )

    columns = metadata.parse_columns_text(value)

    assert [(column.name, column.type) for column in columns] == [
        ("a`b", "UInt64"),
        ("payload", "Nullable(String)"),
    ]
    assert metadata.render_columns_text(columns) == value.encode()
    assert metadata.parse_columns_text(value + "\n\n") == columns


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

    substreams = metadata.parse_columns_substreams(value)

    assert metadata.render_columns_substreams(substreams, columns) == value.encode()
    assert metadata.parse_columns_substreams(value + "\n") == substreams
    serialized = metadata.filter_serialization(
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
    columns = metadata.parse_columns_text(
        "columns format version: 1\n"
        "1 columns:\n"
        f"{quote}a\\{quote}b{quote} String\n"
    )
    substreams = metadata.parse_columns_substreams(
        "columns substreams version: 1\n"
        "1 columns:\n"
        f"1 substreams for column {quote}a\\{quote}b{quote}:\n"
        "\ta%60b\n"
    )

    assert columns[0].name == f"a{quote}b"
    assert list(substreams) == [f"a{quote}b"]
    metadata.validate_columns_substreams(columns, substreams)


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
        metadata.parse_columns_substreams(value)


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
        metadata.validate_columns_substreams(columns, substreams)


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


def test_restore_empty_object_uploads_and_verifies_content() -> None:
    client = MagicMock()
    client.head_object.return_value = {"ContentLength": 0}

    assert restore_empty_object(client, "bucket", "empty")
    client.put_object.assert_called_once_with(Bucket="bucket", Key="empty", Body=b"")

    client.head_object.return_value = {"ContentLength": 1}
    assert not restore_empty_object(client, "bucket", "not-empty")


def test_partition_recovery_keeps_empty_object_report_semantics() -> None:
    part_info = MagicMock()
    missing_object = MagicMock(object_key="empty")
    client = MagicMock()

    with patch.object(partition_recovery, "restore_empty_object", return_value=False):
        report = partition_recovery.restore_empty_objects(
            part_info, [missing_object], client, "bucket"
        )

    assert report[0]["status"] == "unrecoverable"
    assert report[0]["detail"] == "empty object upload verification failed"


def make_recovery_source(tmp_path: Path, metadata_text: str) -> RecoverySource:
    part_path = tmp_path / "source" / "detached" / "part"
    part_path.mkdir(parents=True)
    (part_path / "data.bin").write_text(metadata_text, encoding="latin-1")
    return RecoverySource(
        SourceTable(TableRef("db", "source"), "s3", [str(tmp_path / "source")]),
        DiskInfo("s3", tmp_path),
        part_path,
        "source/detached/part",
        "part",
    )


def missing_object_error(code: str = "404") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": "missing"}},
        "HeadObject",
    )


@patch("ch_tools.chadmin.internal.object_storage.part_recovery_source.logging")
def test_restore_missing_empty_objects_repairs_only_empty_missing_objects(
    logging: MagicMock,
    tmp_path: Path,
) -> None:
    source = make_recovery_source(
        tmp_path,
        "4\n2 0\n0 object-a\n0 /object-b\n0\n0\n",
    )
    client = MagicMock()
    client.head_object.side_effect = [
        missing_object_error(),
        missing_object_error(),
        {"ContentLength": 0},
        {"ContentLength": 0},
    ]
    disk_conf = MagicMock(prefix="prefix/", bucket_name="bucket")

    restored = recovery_source.restore_missing_empty_objects(source, client, disk_conf)

    assert restored == ["data.bin"]
    assert client.put_object.call_args_list == [
        call(Bucket="bucket", Key="prefix/object-a", Body=b""),
        call(Bucket="bucket", Key="prefix/object-b", Body=b""),
    ]


def test_restore_missing_empty_objects_leaves_mixed_file_untouched(
    tmp_path: Path,
) -> None:
    source = make_recovery_source(
        tmp_path,
        "4\n2 1\n0 empty\n1 non-empty\n0\n0\n",
    )
    client = MagicMock()
    client.head_object.side_effect = [
        missing_object_error(),
        missing_object_error(),
    ]

    restored = recovery_source.restore_missing_empty_objects(
        source,
        client,
        MagicMock(prefix="", bucket_name="bucket"),
    )

    assert restored == []
    client.put_object.assert_not_called()


def test_restore_missing_empty_objects_propagates_access_denied(
    tmp_path: Path,
) -> None:
    source = make_recovery_source(
        tmp_path,
        "4\n1 0\n0 empty\n0\n0\n",
    )
    client = MagicMock()
    client.head_object.side_effect = missing_object_error("AccessDenied")

    with pytest.raises(ClientError):
        recovery_source.restore_missing_empty_objects(
            source,
            client,
            MagicMock(prefix="", bucket_name="bucket"),
        )
    client.put_object.assert_not_called()


def test_restore_missing_empty_objects_rejects_failed_verification(
    tmp_path: Path,
) -> None:
    source = make_recovery_source(
        tmp_path,
        "5\n1 0\n0 full/path\n0\n0\n",
    )
    client = MagicMock()
    client.head_object.side_effect = [
        missing_object_error(),
        {"ContentLength": 1},
    ]

    with pytest.raises(RuntimeError, match="verification failed for data.bin"):
        recovery_source.restore_missing_empty_objects(
            source,
            client,
            MagicMock(prefix="ignored", bucket_name="bucket"),
        )
    client.put_object.assert_called_once_with(
        Bucket="bucket", Key="full/path", Body=b""
    )


@patch("ch_tools.chadmin.internal.object_storage.part_recovery_source.execute_query")
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

    assert recovery_source._infer_table_from_path(
        MagicMock(), part_path, [table]
    ) == TableRef("db", "source")


@patch(
    "ch_tools.chadmin.internal.object_storage.part_recovery_source._validate_policy_disk"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.part_recovery_source._find_disk_for_path"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.part_recovery_source._infer_table_from_path"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.part_recovery_source._list_merge_tree_tables"
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

    source = recovery_source.resolve_recovery_source(
        MagicMock(), "db", "source", None, str(part_path)
    )

    assert source.table == source_table
    assert source.relative_path == part_path.name
    validate_policy.assert_called_once()


@patch(
    "ch_tools.chadmin.internal.object_storage.part_recovery_source._infer_table_from_path"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.part_recovery_source._list_merge_tree_tables"
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
        recovery_source.resolve_recovery_source(
            MagicMock(), None, None, None, str(part_path)
        )


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
    analysis = RecoveryAnalysis(
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
    monkeypatch.setattr(recovery, "restore_missing_empty_objects", MagicMock())
    monkeypatch.setattr(
        recovery, "inspect_file_recoverability", MagicMock(return_value={})
    )
    monkeypatch.setattr(recovery, "_analyze_part", MagicMock(return_value=analysis))
    monkeypatch.setattr(
        recovery.uuid,
        "uuid4",
        MagicMock(return_value=MagicMock(hex="stage")),
    )


def test_missing_target_uses_generated_table_in_source_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_recover_part_dependencies(monkeypatch)
    ctx = MagicMock()
    ctx.obj = {"config": {"object_storage": {"bucket_name_prefix": "", "retries": {}}}}

    plan = recovery._prepare_recovery(ctx, None, None, None, "/part", None)

    assert plan.target == TableRef("source_db", "_chadmin_recovered_stage")


@pytest.mark.parametrize(
    "filename,error",
    [
        ("columns.txt", "original part schema cannot be inferred safely"),
        ("count.txt", "physical row count is unavailable after detach"),
    ],
)
def test_required_detached_metadata_has_specific_error(
    filename: str, error: str
) -> None:
    with pytest.raises(ValueError, match=error):
        recovery._require_recoverable_file({filename: False}, filename)


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


def test_incomplete_target_is_removed_before_staging_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_recover_part_dependencies(monkeypatch)
    create_table = MagicMock()
    delete_table = MagicMock()
    monkeypatch.setattr(recovery, "_create_recovery_table", create_table)
    monkeypatch.setattr(
        recovery,
        "_insert_recovered_data",
        MagicMock(side_effect=RuntimeError("insert failed")),
    )
    monkeypatch.setattr(recovery, "delete_table", delete_table)
    monkeypatch.setattr(recovery, "_prepare_staging_part", MagicMock())
    monkeypatch.setattr(recovery, "attach_part", MagicMock())
    monkeypatch.setattr(
        recovery,
        "_get_only_active_part",
        MagicMock(return_value="all_1_1_0"),
    )
    monkeypatch.setattr(recovery, "check_table", MagicMock(return_value=True))
    monkeypatch.setattr(recovery, "execute_query", MagicMock(return_value="1"))
    monkeypatch.setattr(recovery, "logging", MagicMock())
    ctx = MagicMock()
    ctx.obj = {"config": {"object_storage": {"bucket_name_prefix": "", "retries": {}}}}

    with pytest.raises(RuntimeError, match="insert failed"):
        recovery.recover_part(ctx, None, None, None, "/part", "target.data")

    assert create_table.call_count == 2
    assert delete_table.call_args_list == [
        call(ctx, "target", "data"),
        call(ctx, "source_db", "_chadmin_recover_stage"),
    ]


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
