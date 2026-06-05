from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List, Sequence

from ch_tools.chadmin.internal.clickhouse_disks import make_ch_disks_config
from ch_tools.common import logging
from ch_tools.common.utils import atomic_write_file

CLICKHOUSE_LOCAL_BIN = "clickhouse-local"
_SUBPROCESS_TIMEOUT = 600
_RECOVERY_DATABASE = "default"
_RECOVERY_TABLE = "recovery"
_CHECKSUMS_FILENAME = "checksums.txt"
_ENGINE_MAP = {
    "ReplicatedMergeTree": "MergeTree",
    "ReplicatedReplacingMergeTree": "ReplacingMergeTree",
    "ReplicatedSummingMergeTree": "SummingMergeTree",
    "ReplicatedAggregatingMergeTree": "AggregatingMergeTree",
    "ReplicatedCollapsingMergeTree": "CollapsingMergeTree",
    "ReplicatedVersionedCollapsingMergeTree": "VersionedCollapsingMergeTree",
    "ReplicatedGraphiteMergeTree": "GraphiteMergeTree",
}


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
    missing_files: Sequence[str],
) -> List[str]:
    logging.debug(
        "recover_checksums_via_local: part_path={} part_name={} ch_version={}",
        detached_part_path,
        part_name,
        ch_version,
    )

    requested_checksums = [
        filename
        for filename in missing_files
        if filename == _CHECKSUMS_FILENAME or is_projection_checksums(filename)
    ]
    if _CHECKSUMS_FILENAME not in requested_checksums:
        requested_checksums.insert(0, _CHECKSUMS_FILENAME)

    with tempfile.TemporaryDirectory(prefix="chadmin-recovery-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        local_part_path = prepare_local_part_layout(
            tmp_path=tmp_path,
            detached_part_path=Path(detached_part_path),
            part_name=part_name,
            missing_checksums=requested_checksums,
        )
        disk_config_path = make_ch_disks_config(
            disk_name,
            output_path=str(tmp_path / f"disk-{disk_name}.xml"),
            disk_config=disk_config,
        )
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

        restored_files = copy_recovered_checksums_back(
            recovered_part_path=local_part_path,
            detached_part_path=Path(detached_part_path),
            requested_checksums=requested_checksums,
        )

    logging.info(
        "checksums_recovery_via_local: regenerated {} checksums file(s) "
        "for part '{}' (table {}.{})",
        len(restored_files),
        part_name,
        database,
        table,
    )
    return restored_files


def build_recovery_ddl(
    original_ddl: str,
    disk_name: str,
) -> str:
    ddl = original_ddl.strip().rstrip(";")
    ddl = _replace_create_table_name(ddl, _RECOVERY_TABLE)
    ddl = _rewrite_replicated_engine(ddl)
    ddl = _set_disk_setting(ddl, disk_name)
    return ddl


def prepare_local_part_layout(
    *,
    tmp_path: Path,
    detached_part_path: Path,
    part_name: str,
    missing_checksums: Sequence[str],
) -> Path:
    detached_dir = tmp_path / "data" / _RECOVERY_DATABASE / _RECOVERY_TABLE / "detached"
    detached_dir.mkdir(parents=True)
    local_part_path = detached_dir / part_name
    shutil.copytree(detached_part_path, local_part_path)

    for filename in missing_checksums:
        target = local_part_path / filename
        if target.exists():
            target.unlink()
    return local_part_path


def copy_recovered_checksums_back(
    *,
    recovered_part_path: Path,
    detached_part_path: Path,
    requested_checksums: Sequence[str],
) -> List[str]:
    restored: List[str] = []
    seen = set()
    for filename in requested_checksums:
        if filename in seen:
            continue
        seen.add(filename)
        source = recovered_part_path / filename
        if not source.exists():
            raise FileNotFoundError(f"Recovered checksums file not found: {source}")
        destination = detached_part_path / filename
        _atomic_copy(source, destination)
        restored.append(filename)
    return restored


def is_projection_checksums(filename: str) -> bool:
    parts = Path(filename).parts
    return (
        len(parts) >= 2
        and parts[-1] == _CHECKSUMS_FILENAME
        and any(part.endswith(".proj") for part in parts[:-1])
    )


def _atomic_copy(source: Path, destination: Path) -> None:
    atomic_write_file(destination, source.read_bytes())


def _replace_create_table_name(ddl: str, table_name: str) -> str:
    return re.sub(
        r"^(CREATE\s+TABLE\s+)(?:\`?\w+\`?\.)?\`?\w+\`?",
        rf"\1{table_name}",
        ddl,
        count=1,
        flags=re.IGNORECASE,
    )


def _rewrite_replicated_engine(ddl: str) -> str:
    m = re.search(
        r"ENGINE\s*=\s*(Replicated\w*MergeTree)\s*"
        r"\(\s*'[^']*'\s*,\s*'[^']*'\s*(?:,\s*(.*?))?\s*\)",
        ddl,
        re.IGNORECASE,
    )
    if not m:
        raise ValueError("ENGINE clause with Replicated*MergeTree not found")

    engine_name = m.group(1)
    if engine_name not in _ENGINE_MAP:
        raise ValueError(f"Unsupported replicated engine: {engine_name!r}")

    plain_engine = _ENGINE_MAP[engine_name]
    rest = m.group(2) or ""
    if rest:
        replacement = f"ENGINE = {plain_engine}({rest})"
    else:
        replacement = f"ENGINE = {plain_engine}"
    return ddl[: m.start()] + replacement + ddl[m.end() :]


def _set_disk_setting(ddl: str, disk_name: str) -> str:
    si = ddl.lower().rfind("settings")
    if si < 0:
        return f"{ddl.rstrip()}\nSETTINGS disk = '{disk_name}'"

    prefix = ddl[:si]
    settings_text = ddl[si:]
    m = re.search(r"\bdisk\s*=\s*'[^']*'", settings_text, re.IGNORECASE)
    if m:
        settings_text = (
            settings_text[: m.start()]
            + f"disk = '{disk_name}'"
            + settings_text[m.end() :]
        )
    else:
        settings_text = (
            f"SETTINGS disk = '{disk_name}'," + settings_text[len("SETTINGS") :]
        )

    return prefix + settings_text


def _build_recovery_sql(
    *,
    create_table_ddl: str,
    part_name: str,
) -> str:
    ddl = create_table_ddl.strip().rstrip(";") + ";"
    attach = f"ALTER TABLE {_RECOVERY_TABLE} ATTACH PART '{part_name}';"
    detach = f"ALTER TABLE {_RECOVERY_TABLE} DETACH PART '{part_name}';"
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
