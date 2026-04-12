"""In-memory search result cache with log-based invalidation and TTL fallback."""
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

from app.chat_db import chat_log_fingerprint, local_chat_log_ids_for_player
from app.config import CHAT_DB_PATH, LOGS_DIR, STATS_DB_PATH
from app.search.search import local_log_ids_for_player, log_match_matching_log_ids
from app.stats_db import stats_db_fingerprint, stats_player_stats_cache_token

logger = logging.getLogger(__name__)

# Cache entry: { "payload": {...}, "log_ids": frozenset[int], "created_at": float }
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
        cached_ids: frozenset[int] = entry["log_ids"]
        logs_dir = Path(LOGS_DIR)
        if mode == "chat":
            steamid64 = key_tuple[0]
            current = local_chat_log_ids_for_player(steamid64, CHAT_DB_PATH)
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
            stats_path = Path(STATS_DB_PATH)
            if stats_path.is_file():
                current = stats_player_stats_cache_token(STATS_DB_PATH, steamid64)
            else:
                current = local_log_ids_for_player(steamid64, logs_dir)
            if current != cached_ids:
                return False
        elif mode == "profile":
            steamid64 = key_tuple[0]
            stats_path = Path(STATS_DB_PATH)
            if stats_path.is_file():
                current = stats_player_stats_cache_token(STATS_DB_PATH, steamid64)
            else:
                current = frozenset()
            if current != cached_ids:
                return False
        elif mode == "logmatch":
            steamids_tuple = key_tuple[0]
            current = log_match_matching_log_ids(list(steamids_tuple), logs_dir)
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
    if not _is_valid(entry, mode, key_tuple):
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
        if k in _cache_order:
            _cache_order[:] = [x for x in _cache_order if x != k]
        _cache_order.append(k)
        return entry["payload"]


def set_(mode: str, key_tuple: tuple[Any, ...], payload: dict[str, Any], log_ids: frozenset[int]) -> None:
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
