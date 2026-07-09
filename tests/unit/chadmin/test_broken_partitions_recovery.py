from __future__ import annotations

from io import BytesIO
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from ch_tools.chadmin.internal.object_storage import (
    broken_partitions_recovery as module,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    S3ObjectLocalMetaData,
    get_object_storage_key,
)
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration


def _part_info(part_type: str = "Wide") -> module.BrokenPartInfo:
    return module.BrokenPartInfo(
        database="db",
        table="tbl",
        partition_id="0",
        name="0_1_1_0",
        part_type=part_type,
        rows=42,
        default_compression_codec="CODEC(LZ4)",
    )


def _missing_object(
    filename: str, metadata: S3ObjectLocalMetaData, object_info: S3ObjectLocalInfo
) -> module.MissingObject:
    return module.MissingObject(
        filename=filename,
        metadata_path=Path("/var/lib/clickhouse/store/part") / filename,
        metadata=metadata,
        object_info=object_info,
        object_key=object_info.key,
    )


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code}}, "HeadObject")


def test_check_key_in_object_storage_uses_head_object() -> None:
    s3_client = MagicMock()

    assert module.check_key_in_object_storage(s3_client, "bucket", "key")

    s3_client.head_object.assert_called_once_with(Bucket="bucket", Key="key")


def test_check_key_in_object_storage_returns_false_for_missing_key() -> None:
    s3_client = MagicMock()
    s3_client.head_object.side_effect = _client_error("NoSuchKey")

    assert not module.check_key_in_object_storage(s3_client, "bucket", "key")


def test_check_key_in_object_storage_raises_for_non_404_client_error() -> None:
    s3_client = MagicMock()
    s3_client.head_object.side_effect = _client_error("AccessDenied")

    with pytest.raises(ClientError) as excinfo:
        module.check_key_in_object_storage(s3_client, "bucket", "key")

    assert excinfo.value.response["Error"]["Code"] == "AccessDenied"


def test_get_object_storage_key_preserves_legacy_join_semantics() -> None:
    assert (
        get_object_storage_key(
            "prefix", S3ObjectLocalInfo(key="object", size=1, key_is_full=False)
        )
        == "prefix/object"
    )
    assert (
        get_object_storage_key(
            "prefix", S3ObjectLocalInfo(key="/object", size=1, key_is_full=False)
        )
        == "/object"
    )
    assert (
        get_object_storage_key(
            "", S3ObjectLocalInfo(key="object", size=1, key_is_full=False)
        )
        == "object"
    )
    assert (
        get_object_storage_key(
            "prefix", S3ObjectLocalInfo(key="full/object", size=1, key_is_full=True)
        )
        == "full/object"
    )


def test_find_broken_parts_ignores_empty_metadata_file(tmp_path: Path) -> None:
    (tmp_path / "empty.txt").write_text("5\n0 0\n1\n0\n")
    disk_conf = S3DiskConfiguration(
        name="object_storage",
        endpoint_url="http://localhost:9000",
        access_key_id="access",
        secret_access_key="secret",
        bucket_name="bucket",
        prefix="prefix",
    )
    s3_client = MagicMock()

    assert (
        module.find_broken_parts(MagicMock(), str(tmp_path), s3_client, disk_conf) == []
    )
    s3_client.head_object.assert_not_called()


@patch(
    "ch_tools.chadmin.internal.object_storage.broken_partitions_recovery.get_active_parts_by_path"
)
def test_find_broken_parts_detects_missing_object(
    get_active_parts_by_path_mock: MagicMock, tmp_path: Path
) -> None:
    part_path = tmp_path / "fake_part"
    part_path.mkdir()
    metadata_path = part_path / "count.txt"
    metadata_path.write_text("5\n1 2\n2 prefix/count\n1\n0\n")
    part_info = _part_info()
    get_active_parts_by_path_mock.return_value = {f"{part_path}/": part_info}
    disk_conf = S3DiskConfiguration(
        name="object_storage",
        endpoint_url="http://localhost:9000",
        access_key_id="access",
        secret_access_key="secret",
        bucket_name="bucket",
        prefix="prefix",
    )
    s3_client = MagicMock()
    s3_client.head_object.side_effect = _client_error("NoSuchKey")

    broken_parts = module.find_broken_parts(
        MagicMock(), str(tmp_path), s3_client, disk_conf
    )

    assert len(broken_parts) == 1
    broken_part = broken_parts[0]
    assert broken_part.path == str(part_path)
    assert broken_part.info == part_info
    assert len(broken_part.missing_objects) == 1
    assert broken_part.missing_objects[0].filename == "count.txt"
    assert broken_part.missing_objects[0].object_key == "prefix/count"


def test_restore_generated_file_uploads_only_missing_exact_chunk() -> None:
    object_a = S3ObjectLocalInfo(key="prefix/a", size=1, key_is_full=True)
    object_b = S3ObjectLocalInfo(key="prefix/b", size=1, key_is_full=True)
    metadata = S3ObjectLocalMetaData(
        version=5,
        total_size=2,
        objects=[object_a, object_b],
        ref_counter=1,
        read_only=False,
    )
    s3_client = MagicMock()
    s3_client.get_object.return_value = {"Body": BytesIO(b"4")}

    report = module.restore_generated_file(
        _part_info(),
        "count.txt",
        b"42",
        [_missing_object("count.txt", metadata, object_b)],
        s3_client,
        "bucket",
    )

    assert report["status"] == "restored"
    s3_client.get_object.assert_called_once_with(Bucket="bucket", Key="prefix/a")
    s3_client.put_object.assert_called_once_with(
        Bucket="bucket", Key="prefix/b", Body=b"2"
    )


def test_restore_generated_file_fails_on_size_mismatch() -> None:
    object_info = S3ObjectLocalInfo(key="prefix/count", size=3, key_is_full=True)
    metadata = S3ObjectLocalMetaData(
        version=5, total_size=3, objects=[object_info], ref_counter=1, read_only=False
    )
    s3_client = MagicMock()

    report = module.restore_generated_file(
        _part_info(),
        "count.txt",
        b"42",
        [_missing_object("count.txt", metadata, object_info)],
        s3_client,
        "bucket",
    )

    assert report["status"] == "unrecoverable"
    s3_client.put_object.assert_not_called()


def test_restore_generated_file_fails_on_existing_chunk_content_mismatch() -> None:
    object_a = S3ObjectLocalInfo(key="prefix/a", size=1, key_is_full=True)
    object_b = S3ObjectLocalInfo(key="prefix/b", size=1, key_is_full=True)
    metadata = S3ObjectLocalMetaData(
        version=5,
        total_size=2,
        objects=[object_a, object_b],
        ref_counter=1,
        read_only=False,
    )
    s3_client = MagicMock()
    s3_client.get_object.return_value = {"Body": BytesIO(b"x")}

    report = module.restore_generated_file(
        _part_info(),
        "count.txt",
        b"42",
        [_missing_object("count.txt", metadata, object_b)],
        s3_client,
        "bucket",
    )

    assert report["status"] == "unrecoverable"
    s3_client.put_object.assert_not_called()


@patch(
    "ch_tools.chadmin.internal.object_storage.broken_partitions_recovery.detach_partition_with_retry"
)
def test_compact_columns_txt_is_not_restored(detach_part_mock: MagicMock) -> None:
    object_info = S3ObjectLocalInfo(key="prefix/columns", size=10, key_is_full=True)
    metadata = S3ObjectLocalMetaData(
        version=5, total_size=10, objects=[object_info], ref_counter=1, read_only=False
    )
    broken_part = module.BrokenPart(
        path="/var/lib/clickhouse/store/part",
        info=_part_info("Compact"),
        missing_objects=[_missing_object("columns.txt", metadata, object_info)],
    )

    report = module.restore_recoverable_broken_part(
        MagicMock(), broken_part, MagicMock(), "bucket", detach_unrecoverable=False
    )

    assert report[0]["status"] == "unrecoverable"
    detach_part_mock.assert_not_called()


@patch(
    "ch_tools.chadmin.internal.object_storage.broken_partitions_recovery.attach_partition_with_retry"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.broken_partitions_recovery.detach_partition_with_retry"
)
@patch(
    "ch_tools.chadmin.internal.object_storage.broken_partitions_recovery.get_detached_part_path"
)
def test_wide_columns_and_checksums_are_restored(
    get_detached_part_path_mock: MagicMock,
    detach_part_mock: MagicMock,
    attach_part_mock: MagicMock,
    tmp_path: Path,
) -> None:
    detached_part_path = tmp_path / "detached" / "0_1_1_0"
    detached_part_path.mkdir(parents=True)
    (detached_part_path / "columns.txt").write_text("columns")
    (detached_part_path / "checksums.txt").write_text("checksums")
    get_detached_part_path_mock.return_value = detached_part_path
    detach_part_mock.return_value = True
    attach_part_mock.return_value = True

    object_info_columns = S3ObjectLocalInfo(
        key="prefix/columns.txt", size=10, key_is_full=True
    )
    object_info_checksums = S3ObjectLocalInfo(
        key="prefix/checksums.txt", size=20, key_is_full=True
    )
    metadata_columns = S3ObjectLocalMetaData(
        version=5,
        total_size=10,
        objects=[object_info_columns],
        ref_counter=1,
        read_only=False,
    )
    metadata_checksums = S3ObjectLocalMetaData(
        version=5,
        total_size=20,
        objects=[object_info_checksums],
        ref_counter=1,
        read_only=False,
    )
    part_info = _part_info("Wide")
    broken_part = module.BrokenPart(
        path="/var/lib/clickhouse/store/wide_part",
        info=part_info,
        missing_objects=[
            _missing_object("columns.txt", metadata_columns, object_info_columns),
            _missing_object("checksums.txt", metadata_checksums, object_info_checksums),
        ],
    )

    report = module.restore_recoverable_broken_part(
        MagicMock(), broken_part, MagicMock(), "bucket", detach_unrecoverable=False
    )

    assert [item["status"] for item in report] == ["restored", "restored"]
    detach_part_mock.assert_called_once_with(ANY, part_info)
    attach_part_mock.assert_called_once_with(ANY, part_info)
    assert not (detached_part_path / "columns.txt").exists()
    assert not (detached_part_path / "checksums.txt").exists()
