"""Per-IP sliding-window rate limits for expensive API routes.

Uses in-process state (thread-safe). Multiple uvicorn workers each enforce limits
independently; use a single worker or a shared store (e.g. Redis) if you need a
global limit across processes.
"""
from __future__ import annotations

import logging
import math
import os
import threading
import time
from collections import deque
from typing import Literal

from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


def _int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return max(minimum, default)
    try:
        return max(minimum, int(raw.strip()))
    except ValueError:
        return max(minimum, default)


# Requests allowed per IP per window for each expensive endpoint (independent buckets).
PROFILE_REQUESTS_PER_WINDOW = _int_env("RATE_LIMIT_PROFILE_PER_MINUTE", 10)
LEADERBOARD_REQUESTS_PER_WINDOW = _int_env("RATE_LIMIT_LEADERBOARD_PER_MINUTE", 10)
WINDOW_SECONDS = float(_int_env("RATE_LIMIT_WINDOW_SECONDS", 60, minimum=1))
# Bound memory if many distinct IPs connect (LRU eviction of tracking entries).
MAX_TRACKED_IPS = _int_env("RATE_LIMIT_MAX_TRACKED_IPS", 50_000, minimum=1000)


class SlidingWindowLimiter:
    """Fixed-size sliding window: at most N timestamps in the last ``window_sec``."""

    def __init__(
        self,
        max_requests: int,
        window_sec: float,
        *,
        max_keys: int = MAX_TRACKED_IPS,
    ) -> None:
        self._max = max_requests
        self._window = window_sec
        self._max_keys = max_keys
        self._lock = threading.Lock()
        self._buckets: dict[str, deque[float]] = {}
        self._last_seen: dict[str, float] = {}

    def _evict_lru(self) -> None:
        if not self._last_seen:
            return
        victim = min(self._last_seen, key=lambda k: self._last_seen[k])
        self._buckets.pop(victim, None)
        self._last_seen.pop(victim, None)

    def check(self, key: str) -> tuple[bool, int | None]:
        """
        Register one request for ``key`` if allowed.

        Returns (allowed, retry_after_seconds). When not allowed, retry_after is
        the minimum whole seconds until a slot may open (for Retry-After).
        """
        with self._lock:
            # Sample time after taking the lock so timestamps append in acquisition
            # order; otherwise a stale ``now`` could be appended after newer entries.
            now = time.monotonic()
            cutoff = now - self._window

            if key not in self._buckets:
                if len(self._buckets) >= self._max_keys:
                    self._evict_lru()
                self._buckets[key] = deque()
            self._last_seen[key] = now

            dq = self._buckets[key]
            while dq and dq[0] < cutoff:
                dq.popleft()

            if len(dq) < self._max:
                dq.append(now)
                return True, None

            oldest = dq[0]
            # Slot is free only for now > oldest + window (trim uses dq[0] < cutoff).
            # So clients must wait strictly longer than (oldest + window - now); integer
            # Retry-After is floor(remaining) + 1, not ceil(remaining) (avoids boundary).
            remaining = oldest + self._window - now
            retry_after = max(1, int(math.floor(remaining)) + 1)
            return False, retry_after


_profile_limiter = SlidingWindowLimiter(PROFILE_REQUESTS_PER_WINDOW, WINDOW_SECONDS)
_leaderboard_limiter = SlidingWindowLimiter(LEADERBOARD_REQUESTS_PER_WINDOW, WINDOW_SECONDS)


def rate_limit_exceeded(*, kind: Literal["profile", "leaderboard"], client_ip: str) -> JSONResponse | None:
    """
    Enforce rate limit for ``kind`` (``profile`` or ``leaderboard``).

    Pass the same client IP string as for request logging (see ``_client_ip`` in routes).
    """
    limiter = _profile_limiter if kind == "profile" else _leaderboard_limiter

    key = client_ip.strip() if client_ip else "unknown"
    allowed, retry_after = limiter.check(key)
    if allowed:
        return None

    assert retry_after is not None
    logger.warning("Rate limit exceeded: kind=%s client_ip=%s retry_after=%ss", kind, key, retry_after)
    return JSONResponse(
        {
            "error": "Too many requests. Please try again in a moment.",
            "retry_after": retry_after,
        },
        status_code=429,
        headers={"Retry-After": str(retry_after)},
    )
