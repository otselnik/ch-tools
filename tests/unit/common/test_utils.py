import pytest

from ch_tools.common.utils import (
    escape_for_file_name,
    unescape_for_file_name,
    version_ge,
    version_lt,
)


@pytest.mark.parametrize(
    "value",
    [
        "plain_name",
        "table-name.with spaces",
        "таблица",
        "日本語/данные",
    ],
)
def test_file_name_escape_round_trip(value: str) -> None:
    assert unescape_for_file_name(escape_for_file_name(value)) == value


@pytest.mark.parametrize("value", ["%", "%1", "%GG", "value%GGsuffix"])
def test_file_name_unescape_preserves_malformed_sequences(value: str) -> None:
    assert unescape_for_file_name(value) == value


@pytest.mark.parametrize(
    "version1,version2,expected",
    [
        ("22.8.21.38", "22.8.21.38", True),
        ("24.10.4.191", "22.8.21.38", True),
        ("24.10.4.191", "22.8", True),
        ("22.8.21.38", "24.10.4.191", False),
        ("24.10.4.191.dev", "22.8.21.38", True),
        ("24.10.4.191-dev.1", "22.8.21.38", True),
        ("22.8.21.38", "24.10.4.191-dev.1", False),
        ("24.10.4.191-dev.1", "24.10.4.191", True),
    ],
)
def test_version_ge(version1: str, version2: str, expected: bool) -> None:
    assert version_ge(version1, version2) == expected


@pytest.mark.parametrize(
    "version1,version2,expected",
    [
        ("22.8.21.38", "22.8.21.38", False),
        ("24.10.4.191", "22.8.21.38", False),
        ("22.8.21.38", "24.10.4.191", True),
        ("22.8.21.38", "24.10", True),
        ("22.8.21.38", "24.10.4.191.dev", True),
        ("22.8.21.38", "24.10.4.191-dev.1", True),
        ("24.10.4.191-dev.1", "24.10.4.191", False),
    ],
)
def test_version_lt(version1: str, version2: str, expected: bool) -> None:
    assert version_lt(version1, version2) == expected
