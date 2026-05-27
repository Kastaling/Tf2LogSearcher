"""Configuration from environment."""
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


# Logs directory: ONLY (id).json log files; no state or skip list here
LOGS_DIR = Path(_str("LOGS_DIR", "/data/logs"))

# Downloader state directory: downloader_state.json and skipped_log_ids.json (kept out of LOGS_DIR)
DOWNLOADER_STATE_DIR = Path(_str("DOWNLOADER_STATE_DIR", "/app/downloader_state"))

# Curated logs.tf log IDs excluded from indexing and all search (downloader_state/poisoned_log_ids.json).
_POISONED_PATH_RAW = _str("POISONED_LOG_IDS_PATH", "").strip()
POISONED_LOG_IDS_PATH = (
    Path(_POISONED_PATH_RAW)
    if _POISONED_PATH_RAW
    else DOWNLOADER_STATE_DIR / "poisoned_log_ids.json"
)

# logs.tf API base URL
LOGS_TF_API_BASE = _str("LOGS_TF_API_BASE", "https://logs.tf").rstrip("/")

# Steam Web API key (optional). Required for resolving vanity URLs/names. Never exposed to the frontend.
STEAM_WEB_API_KEY = (os.environ.get("STEAM_WEB_API_KEY") or "").strip() or None
if STEAM_WEB_API_KEY:
    logger.info("Steam Web API key is set; vanity URL/name resolution enabled.")
else:
    logger.warning("STEAM_WEB_API_KEY not set; vanity URL/name resolution will be disabled. Set it in .env or server environment.")

# Downloader: seconds between full cycles
DOWNLOAD_INTERVAL_SEC = _int("DOWNLOAD_INTERVAL_SEC", 3600)

# Downloader: minimum seconds between writing progress.json for the web UI
PROGRESS_UPDATE_INTERVAL_SEC = _int("PROGRESS_UPDATE_INTERVAL_SEC", 300)

# Downloader rate limiting
REQUEST_DELAY_MS = _int("REQUEST_DELAY_MS", 300)
MAX_REQUESTS_BEFORE_BACKOFF = _int("MAX_REQUESTS_BEFORE_BACKOFF", 1500)
BACKOFF_SEC = _int("BACKOFF_SEC", 60)
RETRY_ATTEMPTS = _int("RETRY_ATTEMPTS", 3)

# Web app: request log CSV path (keep separate from LOGS_DIR to avoid mixing with many JSON files)
REQUEST_LOG_PATH = Path(_str("REQUEST_LOG_PATH", "/data/request_logs/request_log.csv"))

# Chat SQLite database file (populated by downloader and backfill script)
CHAT_DB_PATH = Path(_str("CHAT_DB_PATH", "/data/chat/chat.db"))


def _optional_positive_int(name: str) -> int | None:
    """Unset or empty env → None. Positive int → cap. Invalid or non-positive → None (logged)."""
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r — using unlimited chat result rows", name, raw)
        return None
    if n <= 0:
        logger.warning("%s must be positive (got %s) — using unlimited chat result rows", name, n)
        return None
    return n


# Optional hard cap on chat search hits when a Steam ID is set (per-search row limit).
# Unset = no limit (full history for the filters you chose; UI lazy-loads rendering).
# Set on public instances if you need to bound worst-case memory/time (e.g. 50000).
CHAT_SEARCH_MAX_RESULTS_STEAMID = _optional_positive_int("CHAT_SEARCH_MAX_RESULTS_STEAMID")

# Avatar URL cache (Steam Web API; separate SQLite file)
AVATAR_DB_PATH = Path(_str("AVATAR_DB_PATH", "./downloader_state/avatars.db"))

# Default (unfiltered) player profile JSON disk cache
_PROFILE_CACHE_PATH_RAW = _str("PROFILE_CACHE_DB_PATH", "").strip()
PROFILE_CACHE_DB_PATH = (
    Path(_PROFILE_CACHE_PATH_RAW)
    if _PROFILE_CACHE_PATH_RAW
    else (DOWNLOADER_STATE_DIR / "profile_cache.db")
)
# Safety TTL for disk rows; primary invalidation is per-player stats token match.
PROFILE_CACHE_MAX_AGE_SEC = _int("PROFILE_CACHE_MAX_AGE_SEC", 365 * 24 * 60 * 60)

# Public profile view counters (total + unique visitor buckets)
_PROFILE_VIEWS_PATH_RAW = _str("PROFILE_VIEWS_DB_PATH", "").strip()
PROFILE_VIEWS_DB_PATH = (
    Path(_PROFILE_VIEWS_PATH_RAW)
    if _PROFILE_VIEWS_PATH_RAW
    else (DOWNLOADER_STATE_DIR / "profile_views.db")
)
# Optional HMAC key so visitor fingerprints are not guessable from IP+UA alone. Unset = SHA-256 only.
_PROFILE_HASH_SECRET_RAW = (os.environ.get("PROFILE_VIEW_HASH_SECRET") or "").strip()
PROFILE_VIEW_HASH_SECRET: bytes | None = (
    _PROFILE_HASH_SECRET_RAW.encode("utf-8") if _PROFILE_HASH_SECRET_RAW else None
)

# Built-in log detail page link mode: external (logs.tf) or internal (/log/{id})
_LOG_DETAIL_LINK_MODE_RAW = _str("LOG_DETAIL_LINK_MODE", "external").strip().lower()
LOG_DETAIL_LINK_MODE = (
    "internal" if _LOG_DETAIL_LINK_MODE_RAW == "internal" else "external"
)
if _LOG_DETAIL_LINK_MODE_RAW not in ("external", "internal", ""):
    logger.warning(
        "Invalid LOG_DETAIL_LINK_MODE=%r — using external",
        os.environ.get("LOG_DETAIL_LINK_MODE"),
    )

_LOG_DETAIL_CACHE_PATH_RAW = _str("LOG_DETAIL_CACHE_DB_PATH", "").strip()
LOG_DETAIL_CACHE_DB_PATH = (
    Path(_LOG_DETAIL_CACHE_PATH_RAW)
    if _LOG_DETAIL_CACHE_PATH_RAW
    else (DOWNLOADER_STATE_DIR / "log_detail_cache.db")
)

# SQLite DB for per-log player stats (populated by downloader and backfill script)
STATS_DB_PATH = Path(_str("STATS_DB_PATH", "./downloader_state/stats.db"))

# Raw TF2 server log zip files (stored as .zip, never unzipped to disk)
RAW_LOGS_DIR = Path(_str("RAW_LOGS_DIR", "./raw_logs"))

# SQLite DB for position events parsed from raw logs
RAW_EVENTS_DB_PATH = Path(_str("RAW_EVENTS_DB_PATH", "./downloader_state/raw_events.db"))

# Feature flags
DOWNLOAD_JSON_ENABLED = _str("DOWNLOAD_JSON_ENABLED", "1") == "1"
DOWNLOAD_RAW_ENABLED = _str("DOWNLOAD_RAW_ENABLED", "1") == "1"

# Storage stats visibility — disabled by default (set SHOW_STORAGE_STATS=1 to enable).
# When off, /api/storage-stats returns {"enabled": false} and the frontend shows nothing.
# Accept common truthy spellings (Compose and shells sometimes use true/yes).
_SHOW_STORAGE_STATS_TRUTHY = frozenset({"1", "true", "yes", "on"})
SHOW_STORAGE_STATS = _str("SHOW_STORAGE_STATS", "0").lower() in _SHOW_STORAGE_STATS_TRUTHY

# Persistent JSON for /api/storage-stats (large LOGS_DIR scans). Default: next to downloader state.
_STORAGE_CACHE_PATH_RAW = _str("STORAGE_STATS_CACHE_FILE", "").strip()
STORAGE_STATS_CACHE_FILE = (
    Path(_STORAGE_CACHE_PATH_RAW) if _STORAGE_CACHE_PATH_RAW else (DOWNLOADER_STATE_DIR / "storage_stats_cache.json")
)
# Short in-process TTL so we do not re-read the JSON file on every HTTP request.
STORAGE_STATS_MEMORY_TTL_SEC = _int("STORAGE_STATS_MEMORY_TTL_SEC", 120)
# If the on-disk snapshot is older than this, still serve it but enqueue a background rescan.
STORAGE_STATS_RECOMPUTE_AFTER_SEC = _int("STORAGE_STATS_RECOMPUTE_AFTER_SEC", 21_600)
