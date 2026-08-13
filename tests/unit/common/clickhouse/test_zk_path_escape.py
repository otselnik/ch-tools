import pytest

from ch_tools.chadmin.internal.zookeeper import (
    escape_for_zookeeper,
    unescape_from_zookeeper,
)

# type: ignore


@pytest.mark.parametrize(
    "hostname, result",
    [
        pytest.param(
            "zone-hostname.database.urs.net",
            "zone%2Dhostname%2Edatabase%2Eurs%2Enet",
        ),
        pytest.param("таблица", "%D1%82%D0%B0%D0%B1%D0%BB%D0%B8%D1%86%D0%B0"),
    ],
)
def test_config(hostname: str, result: str) -> None:
    assert escape_for_zookeeper(hostname) == result
    assert unescape_from_zookeeper(result) == hostname
