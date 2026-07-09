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
                get_parts_info_query = f"SELECT name, partition, path  FROM system.parts where database='{database}' and table='{table}' and partition='{partition}'"
                # Get local path on disk to the single data part.
                part_local_path = execute_query(
                    context, node, get_parts_info_query, format_="JSONCompact"
                )["data"][0][2]
                keys_to_remove.append(
                    get_s3_object_key_for_part_file(
                        context, node, part_local_path, "columns.txt"
                    )
                )

    s3_client = s3.S3Client(context)
    for key in keys_to_remove:
        s3_client.delete_data(key)


@when(
    "we remove s3 object for active part file {filename} from table {database}.{table} on {node:w}"
)
def step_remove_s3_object_for_active_part_file(
    context: ContextT, filename: str, database: str, table: str, node: str
) -> None:
    part_path_query = (
        "SELECT path FROM system.parts "
        f"WHERE database='{database}' AND table='{table}' AND active "
        "ORDER BY name LIMIT 1"
    )
    part_data = execute_query(context, node, part_path_query, format_="JSONCompact")[
        "data"
    ]
    if not part_data:
        raise AssertionError(
            f"No active part found for table {database}.{table} on node {node} "
            f"when trying to remove S3 object for file {filename}"
        )
    part_path = part_data[0][0]

    object_key = get_s3_object_key_for_part_file(context, node, part_path, filename)

    s3_client = s3.S3Client(context)
    s3_client.delete_data(object_key)
    assert not s3_client.path_exists(object_key)


def get_s3_object_key_for_part_file(
    context: ContextT, node: str, part_path: str, filename: str
) -> str:
    object_key_query = (
        "SELECT remote_path FROM system.remote_data_paths "
        "WHERE disk_name='object_storage' "
        f"AND startsWith(concat(path, local_path), '{os.path.join(part_path, filename)}') "
        "LIMIT 1"
    )
    object_key_data = execute_query(
        context, node, object_key_query, format_="JSONCompact"
    )["data"]
    if not object_key_data:
        raise AssertionError(
            f"No remote object path found for active part file {filename} "
            f"at {part_path} on node {node}"
        )
    return object_key_data[0][0]


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
