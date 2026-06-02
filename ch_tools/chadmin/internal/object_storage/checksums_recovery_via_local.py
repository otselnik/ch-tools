from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

import xmltodict

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging

if TYPE_CHECKING:
    from click import Context

CLICKHOUSE_LOCAL_BIN = "clickhouse-local"
_SUBPROCESS_TIMEOUT = 600


def recover_checksums_via_local(
    *,
    detached_part_path: str,
    part_name: str,
    database: str,
    table: str,
    disk_name: str,
    disk_config: dict,
    create_table_ddl: str,
    ch_version: str,
) -> None:
    logging.debug(
        "recover_checksums_via_local: part_path={} part_name={} ch_version={}",
        detached_part_path,
        part_name,
        ch_version,
    )

    with tempfile.TemporaryDirectory(prefix="chadmin-recovery-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        disk_config_path = _write_disk_config(tmp_path, disk_name, disk_config)
        sql = _build_recovery_sql(
            create_table_ddl=create_table_ddl,
            part_name=part_name,
        )

        _run_clickhouse_local(
            sql=sql,
            local_path=str(tmp_path),
            disk_config_path=str(disk_config_path),
            ch_version=ch_version,
        )

    logging.info(
        "checksums_recovery_via_local: successfully regenerated checksums.txt "
        "for part '{}' (table {}.{})",
        part_name,
        database,
        table,
    )


def build_recovery_ddl(
    original_ddl: str,
    disk_name: str,
) -> str:
    ddl = original_ddl.strip()

    ddl = re.sub(
        r"CREATE TABLE\s+`[^`]+`\s*\.\s*`[^`]+`",
        "CREATE TABLE recovery",
        ddl,
        count=1,
    )
    ddl = re.sub(
        r"CREATE TABLE\s+\w+\s*\.\s*\w+",
        "CREATE TABLE recovery",
        ddl,
        count=1,
    )

    ddl = re.sub(
        r"\bReplicated(MergeTree|ReplacingMergeTree|AggregatingMergeTree"
        r"|CollapsingMergeTree|SummingMergeTree|VersionedCollapsingMergeTree"
        r"|GraphiteMergeTree)\s*\([^)]*\)",
        r"\1()",
        ddl,
    )

    if re.search(r"\bSETTINGS\b", ddl, re.IGNORECASE):
        if re.search(r"\bdisk\s*=", ddl, re.IGNORECASE):
            ddl = re.sub(
                r"\bdisk\s*=\s*'[^']*'",
                f"disk = '{disk_name}'",
                ddl,
            )
        else:
            ddl = re.sub(
                r"(SETTINGS\b)",
                rf"\1 disk = '{disk_name}',",
                ddl,
                count=1,
                flags=re.IGNORECASE,
            )
    else:
        ddl = ddl.rstrip(";").rstrip()
        ddl += f"\nSETTINGS disk = '{disk_name}'"

    return ddl


def _write_disk_config(tmp_path: Path, disk_name: str, disk_config: dict) -> Path:
    config_path = tmp_path / f"disk-{disk_name}.xml"
    with config_path.open("w", encoding="utf-8") as fh:
        xmltodict.unparse(
            {
                "clickhouse": {
                    "storage_configuration": {
                        "disks": {disk_name: disk_config},
                    }
                }
            },
            fh,
            pretty=True,
        )
    return config_path


def _build_recovery_sql(
    *,
    create_table_ddl: str,
    part_name: str,
) -> str:
    ddl = create_table_ddl.strip().rstrip(";") + ";"
    attach = f"ALTER TABLE recovery ATTACH PART '{part_name}';"
    detach = f"ALTER TABLE recovery DETACH PART '{part_name}';"
    return "\n".join([ddl, attach, detach])


def _run_clickhouse_local(
    *,
    sql: str,
    local_path: str,
    disk_config_path: str,
    ch_version: str,
) -> None:
    logging.debug(
        "_run_clickhouse_local: ch_version={} local_path={}", ch_version, local_path
    )
    cmd: List[str] = [
        CLICKHOUSE_LOCAL_BIN,
        "--path",
        local_path,
        "--config-file",
        disk_config_path,
        "--multiquery",
        "--query",
        sql,
    ]

    logging.info("Running clickhouse-local for checksums recovery: {}", " ".join(cmd))

    result = subprocess.run(
        cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=_SUBPROCESS_TIMEOUT,
    )

    stdout = result.stdout.decode(errors="replace").strip()
    stderr = result.stderr.decode(errors="replace").strip()

    if stdout:
        logging.debug("clickhouse-local stdout: {}", stdout)
    if stderr:
        logging.debug("clickhouse-local stderr: {}", stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"clickhouse-local exited with code {result.returncode}.\n"
            f"stderr: {stderr}\n"
            f"sql: {sql}"
        )


def get_show_create_table(
    ctx: "Context",
    database: str,
    table: str,
) -> Optional[str]:
    query = f"SHOW CREATE TABLE `{database}`.`{table}`"
    try:
        result = execute_query(ctx, query, format_="TSVRaw")
        if isinstance(result, str):
            return result.strip() or None
        return None
    except Exception as exc:
        logging.warning(
            "SHOW CREATE TABLE `{}`.`{}` failed: {!r}", database, table, exc
        )
        return None
