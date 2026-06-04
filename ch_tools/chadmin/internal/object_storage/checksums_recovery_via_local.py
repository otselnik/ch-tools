from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

import xmltodict

from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging

if TYPE_CHECKING:
    from click import Context

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
    return len(parts) >= 2 and parts[-1] == _CHECKSUMS_FILENAME and any(
        part.endswith(".proj") for part in parts[:-1]
    )


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp = destination.with_name(destination.name + ".chadmin-tmp")
    tmp.write_bytes(source.read_bytes())
    os.replace(tmp, destination)


def _replace_create_table_name(ddl: str, table_name: str) -> str:
    idx = _find_keyword(ddl, "CREATE", 0)
    if idx < 0:
        raise ValueError("CREATE keyword not found")
    idx += len("CREATE")
    idx = _skip_ws(ddl, idx)
    if _keyword_at(ddl, idx, "OR"):
        idx = _skip_ws(ddl, idx + len("OR"))
        if not _keyword_at(ddl, idx, "REPLACE"):
            raise ValueError("Expected REPLACE after CREATE OR")
        idx = _skip_ws(ddl, idx + len("REPLACE"))
    if not _keyword_at(ddl, idx, "TABLE"):
        raise ValueError("TABLE keyword not found after CREATE")
    idx = _skip_ws(ddl, idx + len("TABLE"))
    if _keyword_at(ddl, idx, "IF"):
        idx = _skip_ws(ddl, idx + len("IF"))
        if not _keyword_at(ddl, idx, "NOT"):
            raise ValueError("Expected NOT in IF NOT EXISTS")
        idx = _skip_ws(ddl, idx + len("NOT"))
        if not _keyword_at(ddl, idx, "EXISTS"):
            raise ValueError("Expected EXISTS in IF NOT EXISTS")
        idx = _skip_ws(ddl, idx + len("EXISTS"))

    start, end = _read_table_identifier(ddl, idx)
    return ddl[:start] + table_name + ddl[end:]


def _rewrite_replicated_engine(ddl: str) -> str:
    engine_idx = _find_keyword(ddl, "ENGINE", 0)
    if engine_idx < 0:
        raise ValueError("ENGINE clause not found")
    idx = _skip_ws(ddl, engine_idx + len("ENGINE"))
    if idx < len(ddl) and ddl[idx] == "=":
        idx = _skip_ws(ddl, idx + 1)

    name_start, name_end = _read_bare_identifier(ddl, idx)
    engine_name = ddl[name_start:name_end]
    if engine_name not in _ENGINE_MAP:
        raise ValueError(f"Unsupported replicated engine: {engine_name!r}")

    idx = _skip_ws(ddl, name_end)
    args: List[str] = []
    call_end = name_end
    if idx < len(ddl) and ddl[idx] == "(":
        args_text, call_end = _read_parenthesized(ddl, idx)
        args = _split_top_level_args(args_text)

    if len(args) < 2:
        raise ValueError(f"Replicated engine {engine_name} must have at least 2 args")
    plain_engine = _ENGINE_MAP[engine_name]
    plain_args = args[2:]
    replacement = f"{plain_engine}({', '.join(plain_args)})"
    return ddl[:name_start] + replacement + ddl[call_end:]


def _set_disk_setting(ddl: str, disk_name: str) -> str:
    settings_idx = _find_keyword(ddl, "SETTINGS", 0)
    if settings_idx < 0:
        return f"{ddl.rstrip()}\nSETTINGS disk = '{disk_name}'"

    disk_idx = _find_keyword(ddl, "disk", settings_idx + len("SETTINGS"))
    if disk_idx >= 0:
        idx = _skip_ws(ddl, disk_idx + len("disk"))
        if idx < len(ddl) and ddl[idx] == "=":
            value_start = _skip_ws(ddl, idx + 1)
            value_end = _read_setting_value_end(ddl, value_start)
            return ddl[:disk_idx] + f"disk = '{disk_name}'" + ddl[value_end:]

    insert_at = settings_idx + len("SETTINGS")
    return ddl[:insert_at] + f" disk = '{disk_name}'," + ddl[insert_at:]


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


def _find_keyword(sql: str, keyword: str, start: int) -> int:
    keyword_upper = keyword.upper()
    idx = start
    while idx < len(sql):
        char = sql[idx]
        if char == "'":
            idx = _skip_single_quoted(sql, idx)
            continue
        if char == "`":
            idx = _skip_back_quoted(sql, idx)
            continue
        if sql.startswith("--", idx):
            newline = sql.find("\n", idx + 2)
            idx = len(sql) if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", idx):
            end = sql.find("*/", idx + 2)
            idx = len(sql) if end < 0 else end + 2
            continue
        if sql[idx : idx + len(keyword)].upper() == keyword_upper and _is_boundary(
            sql, idx - 1
        ) and _is_boundary(sql, idx + len(keyword)):
            return idx
        idx += 1
    return -1


def _keyword_at(sql: str, idx: int, keyword: str) -> bool:
    return (
        sql[idx : idx + len(keyword)].upper() == keyword.upper()
        and _is_boundary(sql, idx - 1)
        and _is_boundary(sql, idx + len(keyword))
    )


def _is_boundary(sql: str, idx: int) -> bool:
    if idx < 0 or idx >= len(sql):
        return True
    return not (sql[idx].isalnum() or sql[idx] == "_")


def _skip_ws(sql: str, idx: int) -> int:
    while idx < len(sql) and sql[idx].isspace():
        idx += 1
    return idx


def _read_table_identifier(sql: str, idx: int) -> Tuple[int, int]:
    start = idx
    _, idx = _read_identifier(sql, idx)
    idx = _skip_ws(sql, idx)
    if idx < len(sql) and sql[idx] == ".":
        idx = _skip_ws(sql, idx + 1)
        _, idx = _read_identifier(sql, idx)
    return start, idx


def _read_identifier(sql: str, idx: int) -> Tuple[int, int]:
    if idx < len(sql) and sql[idx] == "`":
        end = _skip_back_quoted(sql, idx)
        return idx, end
    return _read_bare_identifier(sql, idx)


def _read_bare_identifier(sql: str, idx: int) -> Tuple[int, int]:
    if idx >= len(sql) or not (sql[idx].isalpha() or sql[idx] == "_"):
        raise ValueError(f"Expected identifier at position {idx}")
    start = idx
    idx += 1
    while idx < len(sql) and (sql[idx].isalnum() or sql[idx] == "_"):
        idx += 1
    return start, idx


def _read_parenthesized(sql: str, idx: int) -> Tuple[str, int]:
    if idx >= len(sql) or sql[idx] != "(":
        raise ValueError(f"Expected '(' at position {idx}")
    start = idx + 1
    depth = 1
    idx += 1
    while idx < len(sql):
        char = sql[idx]
        if char == "'":
            idx = _skip_single_quoted(sql, idx)
            continue
        if char == "`":
            idx = _skip_back_quoted(sql, idx)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return sql[start:idx], idx + 1
        idx += 1
    raise ValueError("Unclosed parenthesized expression")


def _split_top_level_args(args_text: str) -> List[str]:
    args: List[str] = []
    start = 0
    idx = 0
    depth = 0
    while idx < len(args_text):
        char = args_text[idx]
        if char == "'":
            idx = _skip_single_quoted(args_text, idx)
            continue
        if char == "`":
            idx = _skip_back_quoted(args_text, idx)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            args.append(args_text[start:idx].strip())
            start = idx + 1
        idx += 1
    tail = args_text[start:].strip()
    if tail:
        args.append(tail)
    return args


def _read_setting_value_end(sql: str, idx: int) -> int:
    depth = 0
    while idx < len(sql):
        char = sql[idx]
        if char == "'":
            idx = _skip_single_quoted(sql, idx)
            continue
        if char == "`":
            idx = _skip_back_quoted(sql, idx)
            continue
        if char == "(":
            depth += 1
        elif char == ")" and depth > 0:
            depth -= 1
        elif char == "," and depth == 0:
            return idx
        idx += 1
    return idx


def _skip_single_quoted(sql: str, idx: int) -> int:
    idx += 1
    while idx < len(sql):
        if sql[idx] == "\\":
            idx += 2
            continue
        if sql[idx] == "'":
            if idx + 1 < len(sql) and sql[idx + 1] == "'":
                idx += 2
                continue
            return idx + 1
        idx += 1
    raise ValueError("Unclosed string literal")


def _skip_back_quoted(sql: str, idx: int) -> int:
    idx += 1
    while idx < len(sql):
        if sql[idx] == "\\":
            idx += 2
            continue
        if sql[idx] == "`":
            return idx + 1
        idx += 1
    raise ValueError("Unclosed quoted identifier")


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
