from __future__ import annotations

from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from ch_tools.chadmin.internal.object_storage import (
    broken_partitions_recovery as module,
)
from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    S3ObjectLocalInfo,
    S3ObjectLocalMetaData,
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
