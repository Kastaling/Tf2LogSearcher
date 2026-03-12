"""Configuration from environment."""
import os
from pathlib import Path


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
