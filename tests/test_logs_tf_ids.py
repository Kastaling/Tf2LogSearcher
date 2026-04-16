"""Round-trip tests for logs.tf Steam ID helpers (pure functions; no HTTP)."""

import pytest

from app.logs_tf import steamid3_to_steamid64, steamid64_to_steamid3
from app.steamid_constants import STEAMID64_OFFSET


def test_steamid64_steamid3_roundtrip() -> None:
    sid64 = str(STEAMID64_OFFSET + 123456)
    s3 = steamid64_to_steamid3(sid64)
    assert s3 == "[U:1:123456]"
    assert steamid3_to_steamid64(s3) == sid64


@pytest.mark.parametrize("bad", ["", "invalid", "[U:2:1]", "[U:1:]", "[U:1:x]"])
def test_steamid3_to_steamid64_invalid(bad: str) -> None:
    assert steamid3_to_steamid64(bad) is None
