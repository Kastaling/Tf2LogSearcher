"""In-memory search result cache with log-based invalidation and TTL fallback."""
import logging
import threading
import time
from typing import Any, Callable, TypeVar

from app.chat_db import chat_log_fingerprint, chat_player_fingerprint
from app.config import CHAT_DB_PATH, LOGS_DIR, STATS_DB_PATH
from app.search.search import log_match_source_token
from app.stats_db import stats_db_fingerprint, stats_player_stats_cache_token, stats_player_stats_cache_token_parts

logger = logging.getLogger(__name__)

# Cache entry: { "payload": {...}, "log_ids": version token, "created_at": float }
CACHE_TTL_SEC = 86400 * 2  # 2 days fallback
CACHE_MAX_ENTRIES = 2000

# (endpoint, key_tuple) -> entry
_cache: dict[tuple[str, tuple[Any, ...]], dict[str, Any]] = {}
_cache_order: list[tuple[str, tuple[Any, ...]]] = []  # LRU order
# HTTP workers and background prefetch threads both touch the cache; guard all mutations.
_cache_lock = threading.RLock()


def _evict_lru() -> None:
    """Drop oldest entries until under CACHE_MAX_ENTRIES. Caller must hold ``_cache_lock``."""
    while len(_cache) >= CACHE_MAX_ENTRIES and _cache_order:
        k = _cache_order.pop(0)
        _cache.pop(k, None)


def _cache_key(mode: str, *parts: Any) -> tuple[str, tuple[Any, ...]]:
    return (mode, tuple(parts))


def _is_valid(entry: dict[str, Any], mode: str, key_tuple: tuple[Any, ...]) -> bool:
    """Return False if cache entry should be invalidated (new/removed logs or TTL)."""
    try:
        if (time.time() - entry["created_at"]) > CACHE_TTL_SEC:
            return False
        cached_ids = entry["log_ids"]
        if mode == "chat":
            steamid64 = key_tuple[0]
            current = chat_player_fingerprint(CHAT_DB_PATH, steamid64)
            if current != cached_ids:
                return False
        elif mode == "chatlb" or mode == "playername":
            current = chat_log_fingerprint(CHAT_DB_PATH)
            if current != cached_ids:
                return False
        elif mode == "leaderboard":
            current = stats_db_fingerprint(STATS_DB_PATH)
            if current != cached_ids:
                return False
        elif mode == "stats" or mode == "coplayers":
            steamid64 = key_tuple[0]
            stats_path = STATS_DB_PATH
            if stats_path.is_file():
                current = stats_player_stats_cache_token(STATS_DB_PATH, steamid64)
            else:
                return False
            if current != cached_ids:
                return False
        elif mode == "profile":
            steamid64 = key_tuple[0]
            stats_path = STATS_DB_PATH
            if stats_path.is_file():
                current = (*stats_player_stats_cache_token_parts(STATS_DB_PATH, steamid64),
                           hash(chat_player_fingerprint(CHAT_DB_PATH, steamid64)))
            else:
                return False
            if current != cached_ids:
                return False
        elif mode == "logmatch":
            steamids_tuple = key_tuple[0]
            current = log_match_source_token(list(steamids_tuple), LOGS_DIR)
            if current != cached_ids:
                return False
        return True
    except Exception as e:
        logger.warning("Search cache validation failed: %s", e)
        return False


def get(mode: str, key_tuple: tuple[Any, ...]) -> dict[str, Any] | None:
    """Return cached payload if present and still valid; else None.

    Lock discipline: acquire lock only for the in-memory dict read and for any
    subsequent mutation (evict or LRU promote). Validation (which may open DB
    connections) runs outside the lock so background prefetch threads do not
    serialize on I/O.
    """
    k = _cache_key(mode, *key_tuple)

    # 1. Read the entry under the lock (fast dict lookup only).
    with _cache_lock:
        entry = _cache.get(k)
    if entry is None:
        return None

    # 2. Validate outside the lock — may open SQLite connections.
    validation_started = time.perf_counter()
    valid = _is_valid(entry, mode, key_tuple)
    validation_ms = (time.perf_counter() - validation_started) * 1000
    if validation_ms >= 500:
        logger.warning("Slow cache validation: mode=%s duration_ms=%.0f", mode, validation_ms)
    if not valid:
        # Entry is stale: remove it under the lock.
        with _cache_lock:
            # Guard against a concurrent set_ that may have replaced the entry
            # with a fresh one between our validation and this removal.
            current = _cache.get(k)
            if current is entry:  # only evict if it's still the same object
                _cache.pop(k, None)
                if k in _cache_order:
                    _cache_order[:] = [x for x in _cache_order if x != k]
        return None

    # 3. Cache hit: promote to MRU under the lock.
    with _cache_lock:
        current = _cache.get(k)
        if current is not entry:
            # Evicted or replaced while validating — must not append a key to
            # _cache_order that is absent from _cache (LRU / _cache invariant).
            return entry["payload"]
        if k in _cache_order:
            _cache_order[:] = [x for x in _cache_order if x != k]
        _cache_order.append(k)
        return entry["payload"]


def set_(mode: str, key_tuple: tuple[Any, ...], payload: dict[str, Any], log_ids: Any) -> None:
    """Store result in cache."""
    with _cache_lock:
        _evict_lru()
        k = _cache_key(mode, *key_tuple)
        _cache[k] = {
            "payload": payload,
            "log_ids": log_ids,
            "created_at": time.time(),
        }
        if k in _cache_order:
            _cache_order[:] = [x for x in _cache_order if x != k]
        _cache_order.append(k)


def invalidate_profile_for_steamid(steamid64: str) -> None:
    """Remove all in-memory profile cache entries for one player (any filter scope)."""
    sid = (steamid64 or "").strip()
    if not sid:
        return
    with _cache_lock:
        to_drop = [k for k in _cache if k[0] == "profile" and k[1] and k[1][0] == sid]
        for k in to_drop:
            _cache.pop(k, None)
        if to_drop:
            drop_set = set(to_drop)
            _cache_order[:] = [x for x in _cache_order if x not in drop_set]


def invalidate_coplayers_for_steamid(steamid64: str) -> None:
    """Remove all in-memory co-players cache entries for one player (any filter scope)."""
    sid = (steamid64 or "").strip()
    if not sid:
        return
    with _cache_lock:
        to_drop = [k for k in _cache if k[0] == "coplayers" and k[1] and k[1][0] == sid]
        for k in to_drop:
            _cache.pop(k, None)
        if to_drop:
            drop_set = set(to_drop)
            _cache_order[:] = [x for x in _cache_order if x not in drop_set]


_SINGLEFLIGHT_WAIT_SEC = 600
_inflight_events: dict[tuple[str, tuple[Any, ...]], threading.Event] = {}
_inflight_lock = threading.Lock()


def singleflight_build(
    mode: str,
    key_tuple: tuple[Any, ...],
    builder: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """
    Coalesce concurrent cache misses for the same key into one ``builder`` call.

    ``builder`` must store the result via ``set_`` before returning the payload.
    Waiters block until the leader finishes, then read from the cache.
    """
    hit = get(mode, key_tuple)
    if hit is not None:
        return hit

    k = _cache_key(mode, *key_tuple)
    with _inflight_lock:
        evt = _inflight_events.get(k)
        if evt is not None:
            leader = False
        else:
            evt = threading.Event()
            _inflight_events[k] = evt
            leader = True

    if not leader:
        wait_started = time.perf_counter()
        while True:
            evt.wait(timeout=_SINGLEFLIGHT_WAIT_SEC)
            hit = get(mode, key_tuple)
            if hit is not None:
                waited_ms = (time.perf_counter() - wait_started) * 1000
                if waited_ms >= 500:
                    logger.warning("Slow cache flight wait: mode=%s duration_ms=%.0f", mode, waited_ms)
                return hit
            # Timed-out waiters must not become a new leader while this flight is still
            # registered (including the brief window after the leader finishes ``builder``
            # but before ``finally`` clears tracking).
            with _inflight_lock:
                still_inflight = _inflight_events.get(k) is evt
            if still_inflight:
                continue
            break
        return singleflight_build(mode, key_tuple, builder)

    # Another worker may have finished and populated the cache while we waited for the lock.
    hit = get(mode, key_tuple)
    if hit is not None:
        evt.set()
        with _inflight_lock:
            if _inflight_events.get(k) is evt:
                _inflight_events.pop(k, None)
        return hit

    build_started = time.perf_counter()
    try:
        return builder()
    finally:
        built_ms = (time.perf_counter() - build_started) * 1000
        if built_ms >= 500:
            logger.warning("Slow cache build: mode=%s duration_ms=%.0f", mode, built_ms)
        # Signal before removing from tracking so blocked waiters wake while the
        # entry still exists; removal afterward prevents stale-event joins.
        evt.set()
        with _inflight_lock:
            _inflight_events.pop(k, None)
