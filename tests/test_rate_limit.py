"""Tests for sliding-window per-IP rate limiting."""
from typing import Literal
from unittest.mock import patch

import pytest

from app.rate_limit import SlidingWindowLimiter, rate_limit_exceeded


def test_sliding_window_allows_then_blocks() -> None:
    lim = SlidingWindowLimiter(3, 60.0, max_keys=1000)
    t = [1000.0]

    def mono() -> float:
        return t[0]

    with patch("app.rate_limit.time.monotonic", mono):
        assert lim.check("a") == (True, None)
        t[0] += 1.0
        assert lim.check("a") == (True, None)
        t[0] += 1.0
        assert lim.check("a") == (True, None)
        t[0] += 1.0
        ok, retry = lim.check("a")
        assert ok is False
        assert retry is not None and retry >= 1


def test_sliding_window_slides() -> None:
    lim = SlidingWindowLimiter(2, 10.0, max_keys=1000)
    t = [0.0]

    def mono() -> float:
        return t[0]

    with patch("app.rate_limit.time.monotonic", mono):
        assert lim.check("b") == (True, None)
        assert lim.check("b") == (True, None)
        ok, retry = lim.check("b")
        assert ok is False
        # remaining=10; client must wait strictly >10s → floor(10)+1 == 11
        assert retry == 11
        t[0] = 11.0
        assert lim.check("b") == (True, None)


def test_independent_keys() -> None:
    lim = SlidingWindowLimiter(1, 60.0, max_keys=1000)
    with patch("app.rate_limit.time.monotonic", return_value=0.0):
        assert lim.check("x") == (True, None)
        assert lim.check("y") == (True, None)


def test_rate_limit_exceeded_returns_json_response() -> None:
    lim = SlidingWindowLimiter(1, 60.0, max_keys=1000)
    with patch("app.rate_limit._profile_limiter", lim):
        with patch("app.rate_limit.time.monotonic", return_value=0.0):
            assert rate_limit_exceeded(kind="profile", client_ip="1.2.3.4") is None
            resp = rate_limit_exceeded(kind="profile", client_ip="1.2.3.4")
            assert resp is not None
            assert resp.status_code == 429
            assert resp.headers.get("retry-after") is not None


@pytest.mark.parametrize("kind", ["profile", "leaderboard"])
def test_rate_limit_kind_dispatch(kind: Literal["profile", "leaderboard"]) -> None:
    lim = SlidingWindowLimiter(10, 60.0, max_keys=1000)
    patch_path = "app.rate_limit._profile_limiter" if kind == "profile" else "app.rate_limit._leaderboard_limiter"
    with patch(patch_path, lim):
        with patch("app.rate_limit.time.monotonic", return_value=0.0):
            assert rate_limit_exceeded(kind=kind, client_ip="10.0.0.1") is None
