"""Thread-safe request log CSV appender."""
import csv
import io
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.config import REQUEST_LOG_PATH

# CSV columns (unified so one row per request; empty string when N/A)
CSV_HEADER = [
    "timestamp_utc",
    "endpoint",
    "method",
    "client_ip",
    "host",
    "user_agent",
    "referer",
    "word",
    "steamid",
    "gamemode",
    "classes",
    "steamids",
    "result_count",
    "status_code",
    "duration_ms",
]

_lock = threading.Lock()


def ensure_header(path: Path) -> None:
    """Write CSV header if file does not exist or is empty."""
    if path.exists() and path.stat().st_size > 0:
        return
    with _lock:
        if path.exists() and path.stat().st_size > 0:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(CSV_HEADER)


def append_request_log(row: dict[str, str | int | None]) -> None:
    """Append one row to the request log CSV. Thread-safe."""
    path = REQUEST_LOG_PATH
    ensure_header(path)
    # Build row in header order; missing keys become ""
    values = [row.get(k, "") for k in CSV_HEADER]
    with _lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(values)
