"""
Steps for interacting with ClickHouse DBMS.
"""

import os

from behave import when
from hamcrest import assert_that, equal_to
from modules import s3
from modules.clickhouse import execute_query
from modules.docker import get_container
from modules.steps import get_step_data
from modules.typing import ContextT


@when("we remove key from s3 for partitions database {database} on {node:w}")
def step_remove_keys_from_s3_for_partition(
    context: ContextT, database: str, node: str
) -> None:
    data = get_step_data(context)
    keys_to_remove = []

    # Get the list of keys in s3 to broke specified partitions.
    for database, database_info in data.items():
        for table, table_info in database_info.items():
            for partition in table_info:
                keys_to_remove.append(
                    _get_part_file_s3_key(
                        context, node, database, table, partition, "columns.txt"
                    )
                )

    s3_client = s3.S3Client(context)
    for key in keys_to_remove:
        s3_client.delete_data(key)


@when("we remove s3 objects for part files on {node:w}")
def step_remove_s3_objects_for_part_files(context: ContextT, node: str) -> None:
    data = get_step_data(context)
    keys_to_remove = []

    for database, database_info in data.items():
        for table, table_info in database_info.items():
            for partition, files in table_info.items():
                for filename in files:
                    keys_to_remove.append(
                        _get_part_file_s3_key(
                            context, node, database, table, str(partition), filename
                        )
                    )

    s3_client = s3.S3Client(context)
    for key in keys_to_remove:
        s3_client.delete_data(key)


def _get_part_file_s3_key(
    context: ContextT,
    node: str,
    database: str,
    table: str,
    partition: str,
    filename: str,
) -> str:
    get_parts_info_query = (
        "SELECT name, partition, path FROM system.parts "
        f"WHERE database='{database}' AND table='{table}' "
        f"AND partition='{partition}' AND active LIMIT 1"
    )
    part_local_path = execute_query(
        context, node, get_parts_info_query, format_="JSONCompact"
    )["data"][0][2]

    file_local_path = os.path.join(part_local_path, filename)
    container = get_container(context, node)
    result = container.exec_run(
        ["bash", "-c", f"sed -n '1p;3p' {file_local_path}"],
        user="root",
    )
    assert_that(result.exit_code, equal_to(0))
    lines = result.output.decode().splitlines()
    assert len(lines) == 2, f"S3 object metadata for {file_local_path} was not found"

    version = int(lines[0])
    key = lines[1].split(maxsplit=1)[1]
    if version < 5:
        key = f"data/cluster_id/shard_1/{key}"
    return key


@when("we move parts as broken_on_start for table {database}.{table} on {node:w}")
def step_mark_parts_as_broken_on_start(
    context: ContextT, database: str, table: str, node: str
) -> None:
    part_list_query = f"SELECT name FROM system.parts WHERE database='{database}' and table='{table}' and active"

    for resp in execute_query(context, node, part_list_query, format_="JSONCompact")[
        "data"
    ]:
        detach_part = f"ALTER TABLE {database}.{table} DETACH PART '{resp[0]}'"
        execute_query(context, node, detach_part)

    broken_prefix = "broken-on-start_"
    detached_part_list_query = f"SELECT path FROM system.detached_parts WHERE database='{database}' and table='{table}'"
    container = get_container(context, node)

    for resp in execute_query(
        context, node, detached_part_list_query, format_="JSONCompact"
    )["data"]:
        path = resp[0]
        broken_path = path.split("/")
        broken_path[-1] = broken_prefix + broken_path[-1]
        broken_path = os.path.join("/", *broken_path)
        result = container.exec_run(
            ["bash", "-c", f"mv {path} {broken_path}"], user="root"
        )
        assert_that(result.exit_code, equal_to(0))
