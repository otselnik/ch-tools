import subprocess
from unittest.mock import MagicMock, patch

import pytest

from ch_tools.chadmin.internal.clickhouse_disks import ClickHouseDiskClient


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_uses_argument_list_without_shell(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.return_value = MagicMock(returncode=0, stdout=b"data", stderr=b"")
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

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
    assert arguments[-2:] == ["--query", "read 'path with spaces/file.bin'"]
    assert run.call_args.kwargs["check"] is False
    assert "shell" not in run.call_args.kwargs


@pytest.mark.parametrize(
    "parents,expected_query",
    [
        (True, "mkdir 'nested/path' --parents"),
        (False, "mkdir 'nested/path'"),
    ],
)
@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_mkdir_uses_query_interface(
    run: MagicMock,
    _logging: MagicMock,
    parents: bool,
    expected_query: str,
) -> None:
    run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

    client.mkdir("nested/path", parents=parents)

    arguments = run.call_args.args[0]
    assert arguments[-2:] == ["--query", expected_query]


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_copy_and_recursive_remove_use_query_interface(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

    client.copy("source/file.bin", "target/file.bin")
    client.remove("target", recursive=True)

    assert run.call_args_list[0].args[0][-2:] == [
        "--query",
        "copy 'source/file.bin' 'target/file.bin'",
    ]
    assert run.call_args_list[1].args[0][-2:] == [
        "--query",
        "remove 'target' --recursive",
    ]


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_remove_supports_legacy_interface(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.return_value = MagicMock(returncode=7, stdout=b"", stderr=b"failed")
    client = ClickHouseDiskClient("s3", run_as_clickhouse=False)

    result = client.remove(
        "legacy/path",
        recursive=True,
        ch_version="24.6",
        check=False,
    )

    assert result == (7, b"failed")
    assert run.call_args.args[0] == [
        "clickhouse-disks",
        "--disk",
        "s3",
        "remove",
        "legacy/path",
    ]


@patch("ch_tools.chadmin.internal.clickhouse_disks.logging")
@patch("ch_tools.chadmin.internal.clickhouse_disks.subprocess.run")
def test_clickhouse_disks_query_error_is_not_hidden_by_zero_exit_code(
    run: MagicMock,
    _logging: MagicMock,
) -> None:
    run.return_value = subprocess.CompletedProcess(
        [], 0, b"", b"Error: Code: 76. Cannot open file"
    )
    client = ClickHouseDiskClient("s3", "/tmp/disks.xml")

    with pytest.raises(RuntimeError, match="Cannot open file"):
        client.mkdir("recovery/stage", parents=True)
