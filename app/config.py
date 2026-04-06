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

# Avatar URL cache (Steam Web API; separate SQLite file)
AVATAR_DB_PATH = Path(_str("AVATAR_DB_PATH", "./downloader_state/avatars.db"))
