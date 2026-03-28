"""In-memory search result cache with log-based invalidation and TTL fallback."""
import logging
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

from app.config import LOGS_DIR
from app.search.search import local_log_ids_for_player, log_match_matching_log_ids

logger = logging.getLogger(__name__)

# Cache entry: { "payload": {...}, "log_ids": frozenset[int], "created_at": float }
CACHE_TTL_SEC = 86400 * 2  # 2 days fallback
CACHE_MAX_ENTRIES = 500

# (endpoint, key_tuple) -> entry
_cache: dict[tuple[str, tuple[Any, ...]], dict[str, Any]] = {}
_cache_order: list[tuple[str, tuple[Any, ...]]] = []  # LRU order


def _evict_lru() -> None:
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
        if mode == "chat" or mode == "stats" or mode == "coplayers":
            steamid64 = key_tuple[0]
            current = local_log_ids_for_player(steamid64, logs_dir)
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
    """Return cached payload if present and still valid; else None."""
    k = _cache_key(mode, *key_tuple)
    entry = _cache.get(k)
    if entry is None:
        return None
    if not _is_valid(entry, mode, key_tuple):
        _cache.pop(k, None)
        if k in _cache_order:
            _cache_order[:] = [x for x in _cache_order if x != k]
        return None
    if k in _cache_order:
        _cache_order[:] = [x for x in _cache_order if x != k]
    _cache_order.append(k)
    return entry["payload"]


def set_(mode: str, key_tuple: tuple[Any, ...], payload: dict[str, Any], log_ids: frozenset[int]) -> None:
    """Store result in cache."""
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
