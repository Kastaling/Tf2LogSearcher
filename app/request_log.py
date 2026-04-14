"""Thread-safe request log CSV appender."""
import csv
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.config import REQUEST_LOG_PATH

logger = logging.getLogger(__name__)

# CSV columns (unified so one row per request; empty string when N/A).
# date_from / date_to: YYYY-MM-DD when applicable; map_query / filters as submitted.
# lb_* columns: /api/leaderboard only (lb_type, lb_class_filter, min_logs). The generic
# "classes" column is for stats-style class lists, not leaderboard (leave empty there).
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
    "date_from",
    "date_to",
    "map_query",
    "lb_type",
    "lb_class_filter",
    "min_logs",
    "result_count",
    "status_code",
    "duration_ms",
]

_lock = threading.Lock()


def ensure_header(path: Path) -> None:
    """Write CSV header if file does not exist, is empty, or has a legacy column count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        if not path.exists() or path.stat().st_size == 0:
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(CSV_HEADER)
            return
        with open(path, "r", encoding="utf-8", newline="") as f:
            try:
                first = next(csv.reader(f))
            except StopIteration:
                first = []
        if len(first) == len(CSV_HEADER):
            return
        backup = path.with_name(
            path.stem + ".legacy." + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + path.suffix
        )
        path.rename(backup)
        logger.info(
            "Request log schema updated (%s columns -> %s); previous file moved to %s",
            len(first),
            len(CSV_HEADER),
            backup,
        )
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADER)


def append_request_log(row: dict[str, str | int | None]) -> None:
    """Append one row to the request log CSV. Thread-safe."""
    path = REQUEST_LOG_PATH
    ensure_header(path)
    # Build row in header order; missing keys become ""
    values: list[str | int] = []
    for k in CSV_HEADER:
        v = row.get(k, "")
        if v is None:
            values.append("")
        elif isinstance(v, bool):
            values.append(str(v).lower())
        else:
            values.append(v)
    with _lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(values)
