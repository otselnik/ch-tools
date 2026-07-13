import subprocess
from dataclasses import dataclass
from typing import Optional, Tuple

import xmltodict

from ch_tools.common import logging
from ch_tools.common.clickhouse.config import ClickhouseConfig
from ch_tools.common.utils import version_ge

CLICKHOUSE_PATH = "/var/lib/clickhouse"
CLICKHOUSE_STORE_PATH = CLICKHOUSE_PATH + "/store"
CLICKHOUSE_DATA_PATH = CLICKHOUSE_PATH + "/data"
CLICKHOUSE_METADATA_PATH = CLICKHOUSE_PATH + "/metadata"
S3_PATH = CLICKHOUSE_PATH + "/disks/object_storage"
S3_METADATA_STORE_PATH = S3_PATH + "/store"

OBJECT_STORAGE_DISK_TYPES = ["s3", "object_storage", "ObjectStorage"]


def _quote_path(path: str) -> str:
    if "'" in path or "\n" in path or "\r" in path:
        raise ValueError(f"Unsupported clickhouse-disks path: {path!r}")
    return f"'{path}'"


@dataclass(frozen=True)
class ClickHouseDiskResult:
    stdout: bytes
    stderr: bytes


class ClickHouseDiskClient:
    """Small non-interactive wrapper around clickhouse-disks."""

    def __init__(
        self,
        disk: str,
        disk_config_path: Optional[str] = None,
    ) -> None:
        self.disk = disk
        self.disk_config_path = disk_config_path or make_ch_disks_config(disk)

    def _run(
        self,
        command: str,
        stdin: Optional[bytes] = None,
    ) -> ClickHouseDiskResult:
        args = [
            "sudo",
            "-u",
            "clickhouse",
            "env",
            "HOME=/tmp",
            "clickhouse-disks",
            "-C",
            self.disk_config_path,
            "--disk",
            self.disk,
            "--query",
            command,
        ]

        logging.info("Run clickhouse-disks command: {}", command)
        proc = subprocess.run(
            args,
            input=stdin,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if proc.returncode:
            raise RuntimeError(
                f"clickhouse-disks command failed with code {proc.returncode}: "
                f"{proc.stderr.decode(errors='replace')}"
            )
        return ClickHouseDiskResult(proc.stdout, proc.stderr)

    def read(self, path: str) -> bytes:
        return self._run(f"read {_quote_path(path)}").stdout

    def write(self, path: str, content: bytes) -> None:
        self._run(f"write {_quote_path(path)}", stdin=content)

    def copy(self, source: str, target: str) -> None:
        self._run(f"copy {_quote_path(source)} {_quote_path(target)}")

    def mkdir(self, path: str, parents: bool = False) -> None:
        suffix = " --parents" if parents else ""
        self._run(f"mkdir {_quote_path(path)}{suffix}")

    def remove(self, path: str, recursive: bool = False) -> None:
        suffix = " --recursive" if recursive else ""
        self._run(f"remove {_quote_path(path)}{suffix}")


def make_ch_disks_config(disk: str) -> str:
    disk_config = ClickhouseConfig.load().storage_configuration.get_disk_config(disk)
    disk_config_path = f"/tmp/chadmin-ch-disks-{disk}.xml"
    logging.info("Create a conf for {} disk: {}", disk, disk_config_path)
    with open(disk_config_path, "w", encoding="utf-8") as f:
        xmltodict.unparse(
            {
                "clickhouse": {
                    "storage_configuration": {"disks": {disk: disk_config}},
                }
            },
            f,
            pretty=True,
        )
    return disk_config_path


def remove_from_ch_disk(
    disk: str,
    path: str,
    ch_version: str,
    disk_config_path: Optional[str] = None,
    dry_run: bool = False,
) -> Tuple[int, bytes]:
    args = ["clickhouse-disks"]
    if disk_config_path:
        args.extend(["-C", disk_config_path])
    args.extend(["--disk", disk])
    if version_ge(ch_version, "24.7"):
        args.extend(
            [
                "--query",
                f"remove {_quote_path(path)} --recursive",
            ]
        )
    else:
        args.extend(["remove", path])

    logging.info("Run: {}", args)

    if dry_run:
        return (0, b"")

    proc = subprocess.run(
        args,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    logging.info(
        "clickhouse-disks remove command has finished: retcode {}, stderr: {}",
        proc.returncode,
        proc.stderr.decode(),
    )
    return (proc.returncode, proc.stderr)
