from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

TESTS_DIR = Path(__file__).parents[2]
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from steps import failure_mockers  # noqa: E402  pylint: disable=wrong-import-position


def test_remove_s3_object_for_active_part_file_fails_without_active_part() -> None:
    with patch.object(failure_mockers, "execute_query", return_value={"data": []}):
        with pytest.raises(AssertionError, match="No active part found"):
            failure_mockers.step_remove_s3_object_for_active_part_file(
                SimpleNamespace(), "checksums.txt", "db", "tbl", "clickhouse01"
            )


def test_get_s3_object_key_for_part_file_fails_without_remote_path() -> None:
    with patch.object(failure_mockers, "execute_query", return_value={"data": []}):
        with pytest.raises(AssertionError, match="No remote object path found"):
            failure_mockers.get_s3_object_key_for_part_file(
                MagicMock(),
                "clickhouse01",
                "/var/lib/clickhouse/store/part",
                "count.txt",
            )
