import subprocess
from dataclasses import dataclass
from typing import List, Optional, Tuple

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


@dataclass(frozen=True)
class ClickHouseDiskResult:
    stdout: bytes
    stderr: bytes


class ClickHouseDiskClient:
    """Small non-interactive wrapper around clickhouse-disks."""

    def __init__(
        self,
        disk: str,
        ch_version: str,
        disk_config_path: Optional[str] = None,
    ) -> None:
        self.disk = disk
        self.ch_version = ch_version
        self.disk_config_path = disk_config_path or make_ch_disks_config(disk)

    def _run(
        self,
        command: str,
        legacy_arguments: List[str],
        stdin: Optional[bytes] = None,
    ) -> ClickHouseDiskResult:
        base = [
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
        ]
        if version_ge(self.ch_version, "24.7"):
            args = base + ["--query", command]
        else:
            args = base + legacy_arguments

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

    @staticmethod
    def _quote(path: str) -> str:
        if "'" in path or "\n" in path or "\r" in path:
            raise ValueError(f"Unsupported clickhouse-disks path: {path!r}")
        return f"'{path}'"

    def read(self, path: str) -> bytes:
        return self._run(f"read {self._quote(path)}", ["read", path]).stdout

    def write(self, path: str, content: bytes) -> None:
        self._run(f"write {self._quote(path)}", ["write", path], stdin=content)

    def copy(self, source: str, target: str) -> None:
        self._run(
            f"copy {self._quote(source)} {self._quote(target)}",
            ["copy", source, target],
        )

    def mkdir(self, path: str, parents: bool = False) -> None:
        suffix = " --parents" if parents else ""
        legacy_arguments = ["mkdir", *(["--parents"] if parents else []), path]
        self._run(f"mkdir {self._quote(path)}{suffix}", legacy_arguments)

    def remove(self, path: str, recursive: bool = False) -> None:
        suffix = " --recursive" if recursive else ""
        self._run(f"remove {self._quote(path)}{suffix}", ["remove", path])


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
    cmd = f"clickhouse-disks {'-C ' + disk_config_path if disk_config_path else ''} --disk {disk}"
    if version_ge(ch_version, "24.7"):
        cmd += f' --query "remove {path} --recursive"'
    else:
        cmd += f" remove {path}"

    logging.info("Run : {}", cmd)

    if dry_run:
        return (0, b"")

    proc = subprocess.run(
        cmd,
        shell=True,
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
