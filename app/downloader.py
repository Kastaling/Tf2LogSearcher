"""Auto-downloader: fetch newest logs from logs.tf using offset cursor, skip list, and rate limiting."""
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from app.config import (
    LOGS_DIR,
    DOWNLOADER_STATE_DIR,
    DOWNLOAD_INTERVAL_SEC,
    PROGRESS_UPDATE_INTERVAL_SEC,
    REQUEST_DELAY_MS,
    MAX_REQUESTS_BEFORE_BACKOFF,
    BACKOFF_SEC,
    RETRY_ATTEMPTS,
    CHAT_DB_PATH,
    STATS_DB_PATH,
)
from app.chat_db import (
    ALIAS_FTS_CYCLE_BUSY_ATTEMPTS,
    ALIAS_FTS_PROGRESS_HEARTBEAT_SEC,
    alias_fts_rebuild_pending,
    connect_chat_db,
    init_chat_db,
    replace_chat_for_log,
    run_alias_fts_rebuild_if_needed,
)
from app.stats_db import connect_stats_db, init_stats_db, replace_stats_for_log
from app.logs_tf import fetch_log_list, fetch_log_json
from app.subscriptions import check_log_for_subscriptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

STATE_FILE = "downloader_state.json"
SKIP_FILE = "skipped_log_ids.json"
LIMIT = 1000
# Number of recent writes to use for download rate (ETA fallback)
RECENT_WRITES_SIZE = 100
# Minimum seconds since process start before using aggregated rate for ETA
MIN_ELAPSED_FOR_AGGREGATED_SEC = 60.0


def _human_bytes(n: int) -> str:
    """Format bytes as human-readable (kB, MB, GB, TB)."""
    if n < 0:
        n = 0
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if n < 1024:
            if unit == "B":
                return f"{n} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _human_duration(seconds: float) -> str:
    """Format seconds as human-readable (e.g. 2d 3h 15m 30s)."""
    if seconds <= 0 or not (seconds < 1e10):
        return "Complete"
    s = int(round(seconds))
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if s or not parts:
        parts.append(f"{s}s")
    return " ".join(parts)


def _log_dir_stats(logs_dir: Path) -> tuple[int, int, int | None, int | None]:
    """Return (total_bytes, file_count, min_id, max_id). Only (id).json files."""
    total = 0
    ids: list[int] = []
    for p in logs_dir.iterdir():
        if not p.is_file() or p.suffix != ".json":
            continue
        stem = p.stem
        if stem.isdigit():
            total += p.stat().st_size
            ids.append(int(stem))
    return total, len(ids), min(ids) if ids else None, max(ids) if ids else None


def _format_eta(recent_writes: list[tuple[float, int]], min_id: int | None) -> str:
    """Compute ETA from recent write rate. Target oldest = log 1; when done use min_id <= 1."""
    if min_id is None:
        return "N/A (no logs yet)"
    remaining = max(0, min_id - 1)
    if remaining == 0:
        return "Complete"
    if len(recent_writes) < 2:
        return "N/A (need more data)"
    first_ts, _ = recent_writes[0]
    last_ts, _ = recent_writes[-1]
    elapsed = last_ts - first_ts
    if elapsed <= 0:
        return "N/A"
    rate = len(recent_writes) / elapsed
    return _format_eta_from_rate(rate, remaining)


def _format_eta_from_rate(rate: float | None, remaining: int) -> str:
    """Compute ETA string from an explicit rate (logs/s) and remaining count. Used for aggregated or recent rate."""
    if remaining <= 0:
        return "Complete"
    if rate is None or rate <= 0:
        return "N/A"
    return _human_duration(remaining / rate)


def _aggregated_rate_logs_per_sec(session_start_time: float, session_downloads: int) -> float | None:
    """Session-based rate (logs/s) since process start. Returns None until MIN_ELAPSED_FOR_AGGREGATED_SEC and at least one download."""
    if session_start_time <= 0 or session_downloads <= 0:
        return None
    elapsed = time.time() - session_start_time
    if elapsed < MIN_ELAPSED_FOR_AGGREGATED_SEC:
        return None
    return session_downloads / elapsed


def _log_stats_and_eta(logs_dir: Path, recent_writes: list[tuple[float, int]]) -> None:
    """Log LOGS_DIR total size, file count, min/max id, remaining logs, rate, and ETA."""
    total_bytes, count, min_id, max_id = _log_dir_stats(logs_dir)
    size_str = _human_bytes(total_bytes)
    if min_id is None:
        logger.info("LOGS_DIR: %s (%s files) | ETA: %s", size_str, count, "N/A (no logs yet)")
        return
    remaining = max(0, min_id - 1)
    eta_str = _format_eta(recent_writes, min_id)
    # Rate for display (logs/s) from recent window
    rate_str = "N/A"
    if len(recent_writes) >= 2:
        first_ts, _ = recent_writes[0]
        last_ts, _ = recent_writes[-1]
        elapsed = last_ts - first_ts
        if elapsed > 0:
            rate_str = f"{len(recent_writes) / elapsed:.1f} logs/s"
    logger.info(
        "LOGS_DIR: %s (%s files) | range %s–%s | remaining: %s | %s | ETA: %s",
        size_str, count, min_id, max_id, remaining, rate_str, eta_str,
    )


PROGRESS_FILENAME = "progress.json"


def _rate_logs_per_sec(recent_writes: list[tuple[float, int]]) -> float | None:
    """Return logs per second from recent window, or None if not enough data."""
    if len(recent_writes) < 2:
        return None
    first_ts, _ = recent_writes[0]
    last_ts, _ = recent_writes[-1]
    elapsed = last_ts - first_ts
    if elapsed <= 0:
        return None
    return len(recent_writes) / elapsed


# Unix timestamp range for validation (roughly 2001–2033)
_EARLIEST_LOG_DATE_MIN = int(1e9)
_EARLIEST_LOG_DATE_MAX = int(2e9)


def _earliest_log_timestamp(logs_dir: Path, min_id: int | None) -> int | None:
    """Read info.date (Unix seconds) from the log file with min_id. Returns None on missing/invalid."""
    if min_id is None:
        return None
    path = logs_dir / f"{min_id}.json"
    if not path.is_file():
        return None
    try:
        data = path.read_text(encoding="utf-8", errors="replace")
        obj = json.loads(data)
    except (OSError, ValueError):
        return None
    info = obj.get("info") if isinstance(obj, dict) else None
    if not isinstance(info, dict):
        return None
    date_val = info.get("date")
    if not isinstance(date_val, (int, float)):
        return None
    ts = int(date_val)
    if not (_EARLIEST_LOG_DATE_MIN <= ts <= _EARLIEST_LOG_DATE_MAX):
        return None
    return ts


def _write_progress_if_due(
    logs_dir: Path,
    state_dir: Path,
    recent_writes: list[tuple[float, int]],
    last_progress_write_ref: list[float],
    downloads_since_progress_ref: list[int],
    session_start_time_ref: list[float],
    session_downloads_ref: list[int],
) -> None:
    """
    Write progress.json for the web UI at most every PROGRESS_UPDATE_INTERVAL_SEC.
    Uses atomic write (temp file + rename). All payload values are server-controlled (no user input).
    ETA uses aggregated session rate when available (after MIN_ELAPSED_FOR_AGGREGATED_SEC), else recent-window rate.
    """
    now = time.time()
    if last_progress_write_ref[0] > 0 and (now - last_progress_write_ref[0]) < PROGRESS_UPDATE_INTERVAL_SEC:
        return
    total_bytes, count, min_id, max_id = _log_dir_stats(logs_dir)
    remaining = max(0, min_id - 1) if min_id is not None else 0
    backfill_complete = min_id is not None and min_id <= 1
    recent_rate = _rate_logs_per_sec(recent_writes)
    aggregated_rate = _aggregated_rate_logs_per_sec(session_start_time_ref[0], session_downloads_ref[0])
    preferred_rate = aggregated_rate if aggregated_rate is not None else recent_rate
    if min_id is None:
        eta_str = "N/A (no logs yet)"
    elif remaining == 0:
        eta_str = "Complete"
    else:
        eta_str = _format_eta_from_rate(preferred_rate, remaining)
        if eta_str == "N/A" and recent_rate is not None:
            eta_str = _format_eta_from_rate(recent_rate, remaining)
    earliest_ts = _earliest_log_timestamp(logs_dir, min_id)
    logs_this_update = downloads_since_progress_ref[0]
    downloads_since_progress_ref[0] = 0  # reset for next interval
    payload: dict[str, int | float | str | None] = {
        "min_id": min_id,
        "max_id": max_id,
        "total_files": count,
        "total_bytes": total_bytes,
        "total_bytes_human": _human_bytes(total_bytes),
        "remaining": remaining,
        "eta_human": eta_str,
        "rate_logs_per_sec": round(recent_rate, 2) if recent_rate is not None else None,
        "rate_logs_per_sec_aggregated": round(aggregated_rate, 2) if aggregated_rate is not None else None,
        "backfill_complete": backfill_complete,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "earliest_log_timestamp": earliest_ts,
        "logs_downloaded_since_last_update": logs_this_update,
    }
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        target = state_dir / PROGRESS_FILENAME
        tmp = target.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(target)
        last_progress_write_ref[0] = now
    except OSError as e:
        logger.warning("Could not write progress.json: %s", e)


def load_skip_list(state_dir: Path) -> set[int]:
    path = state_dir / SKIP_FILE
    if not path.exists():
        return set()
    try:
        data = path.read_text(encoding="utf-8")
        return set(int(x) for x in json.loads(data))
    except (OSError, ValueError, TypeError):
        return set()


def save_skip_list(state_dir: Path, skipped: set[int]) -> None:
    path = state_dir / SKIP_FILE
    state_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(skipped)), encoding="utf-8")


def _min_log_id_in_logs_dir(logs_dir: Path) -> int | None:
    """Return the minimum (oldest) log ID in logs_dir, or None if empty. Only considers (id).json files."""
    ids = []
    for p in logs_dir.iterdir():
        if not p.is_file() or p.suffix != ".json":
            continue
        stem = p.stem
        if stem.isdigit():
            ids.append(int(stem))
    return min(ids) if ids else None


def load_next_offset(state_dir: Path, logs_dir: Path) -> int:
    """Load next_offset from state file. If missing, recover from min log ID in logs_dir."""
    path = state_dir / STATE_FILE
    if path.exists():
        try:
            data = path.read_text(encoding="utf-8")
            return int(json.loads(data).get("next_offset", 0))
        except (OSError, ValueError, TypeError, KeyError):
            pass
    # Recover from logs_dir: find offset by walking API from newest until we pass min_id
    min_id = _min_log_id_in_logs_dir(logs_dir)
    if min_id is None:
        logger.info("No state file and no logs in LOGS_DIR; starting at offset 0")
        return 0
    logger.info("No state file; recovering next_offset from min log ID in /logs: %s", min_id)
    offset = 0
    while True:
        logs = fetch_log_list(offset, LIMIT)
        if not logs:
            logger.info("Recovery: reached end of API at offset %s", offset)
            return offset
        ids = [int(e["id"]) for e in logs if e.get("id") is not None]
        if min_id in ids:
            next_offset = offset + len(logs)
            logger.info("Recovery: min_id %s found at offset %s; next_offset=%s", min_id, offset, next_offset)
            save_next_offset(state_dir, next_offset)
            return next_offset
        offset += len(logs)
        if offset % 10000 == 0 and offset > 0:
            logger.info("Recovery: scanned up to offset %s...", offset)
        if len(logs) < LIMIT:
            return offset


def save_next_offset(state_dir: Path, next_offset: int) -> None:
    path = state_dir / STATE_FILE
    state_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"next_offset": next_offset}), encoding="utf-8")


def fetch_log_json_with_retry(log_id: int):
    """Fetch log JSON with retries and backoff on 429/5xx/timeout. Returns (data, success)."""
    import requests
    from app.config import LOGS_TF_API_BASE
    url = f"{LOGS_TF_API_BASE}/json/{log_id}"
    last_exc = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else 60
                logger.info("Rate limited (429), waiting %s s", wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                time.sleep(30 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("success") is True:
                return data, True
            logger.info("Log %s: API returned success=false", log_id)
            return data, False  # e.g. success: false
        except requests.RequestException as e:
            last_exc = e
            logger.warning("Log %s attempt %s: %s", log_id, attempt + 1, e)
            time.sleep(30 * (attempt + 1))
        except (ValueError, TypeError):
            return None, False
    return None, False


def run_catch_up_newest(
    logs_dir: Path,
    state_dir: Path,
    skipped: set[int],
    request_count_ref: list[int],
    recent_writes: list[tuple[float, int]],
    last_progress_write_ref: list[float],
    downloads_since_progress_ref: list[int],
    session_start_time_ref: list[float],
    session_downloads_ref: list[int],
    chat_db_conn: sqlite3.Connection | None = None,
    stats_db_conn: sqlite3.Connection | None = None,
) -> int:
    """Phase 1: Fetch offset=0 (newest logs). Download any we don't have. Does not change next_offset."""
    logger.info("Phase 1: Checking offset=0 for NEW logs (catch up newest first)")
    logs = fetch_log_list(0, LIMIT)
    if not logs:
        logger.info("Phase 1: No logs at offset 0")
        return 0
    logger.info("Phase 1: Got %s log IDs at offset 0", len(logs))
    downloaded = 0
    for entry in logs:
        log_id = entry.get("id")
        if log_id is None:
            continue
        log_id = int(log_id)
        if log_id in skipped:
            continue
        path = logs_dir / f"{log_id}.json"
        if path.exists():
            continue
        request_count_ref[0] += 1
        if request_count_ref[0] > 0 and request_count_ref[0] % MAX_REQUESTS_BEFORE_BACKOFF == 0:
            logger.info("Backoff after %s requests for %s s", request_count_ref[0], BACKOFF_SEC)
            time.sleep(BACKOFF_SEC)
        time.sleep(REQUEST_DELAY_MS / 1000.0)
        data, success = fetch_log_json_with_retry(log_id)
        if success and data:
            logs_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            if chat_db_conn is not None:
                try:
                    with chat_db_conn:
                        n_chat = replace_chat_for_log(chat_db_conn, log_id, data)
                    logger.info("Indexed chat for log %s (%s message(s))", log_id, n_chat)
                except Exception as e:
                    logger.warning("Chat DB indexing failed for log %s: %s", log_id, e)
            if stats_db_conn is not None:
                try:
                    with stats_db_conn:
                        n_stats = replace_stats_for_log(stats_db_conn, log_id, data)
                    logger.info("Indexed stats for log %s (%s player row(s))", log_id, n_stats)
                except Exception as e:
                    logger.warning("Stats DB indexing failed for log %s: %s", log_id, e)
            size_bytes = path.stat().st_size
            recent_writes.append((time.time(), log_id))
            downloads_since_progress_ref[0] += 1
            session_downloads_ref[0] += 1
            if len(recent_writes) > RECENT_WRITES_SIZE:
                del recent_writes[: len(recent_writes) - RECENT_WRITES_SIZE]
            downloaded += 1
            logger.info("Wrote new log %s (%s)", log_id, _human_bytes(size_bytes))
            try:
                check_log_for_subscriptions(log_id, logs_dir, state_dir)
            except Exception as e:
                logger.warning("Webhook check failed for log %s: %s", log_id, e)
        else:
            skipped.add(log_id)
            save_skip_list(state_dir, skipped)
            logger.info("Skipped log %s (failed or invalid)", log_id)
    logger.info("Phase 1 done: downloaded %s new log(s) from offset 0", downloaded)
    _log_stats_and_eta(logs_dir, recent_writes)
    _write_progress_if_due(logs_dir, state_dir, recent_writes, last_progress_write_ref, downloads_since_progress_ref, session_start_time_ref, session_downloads_ref)
    return downloaded


def run_backfill_from_offset(
    logs_dir: Path,
    state_dir: Path,
    skipped: set[int],
    next_offset: int,
    request_count_ref: list[int],
    recent_writes: list[tuple[float, int]],
    last_progress_write_ref: list[float],
    downloads_since_progress_ref: list[int],
    session_start_time_ref: list[float],
    session_downloads_ref: list[int],
    chat_db_conn: sqlite3.Connection | None = None,
    stats_db_conn: sqlite3.Connection | None = None,
) -> int:
    """Phase 2: Continue from next_offset toward older logs (work toward 1st/oldest log). Returns new next_offset."""
    logger.info("Phase 2: Continuing backfill from offset=%s toward oldest logs", next_offset)
    while True:
        logs = fetch_log_list(next_offset, LIMIT)
        if not logs:
            logger.info("No more logs at offset %s (reached end of API)", next_offset)
            save_next_offset(state_dir, next_offset)
            _log_stats_and_eta(logs_dir, recent_writes)
            _write_progress_if_due(logs_dir, state_dir, recent_writes, last_progress_write_ref, downloads_since_progress_ref, session_start_time_ref, session_downloads_ref)
            return next_offset
        logger.info("Got %s log IDs from API (offset %s)", len(logs), next_offset)
        downloaded = 0
        skipped_this_page = 0
        already_had = 0
        for entry in logs:
            log_id = entry.get("id")
            if log_id is None:
                continue
            log_id = int(log_id)
            if log_id in skipped:
                skipped_this_page += 1
                continue
            path = logs_dir / f"{log_id}.json"
            if path.exists():
                already_had += 1
                continue
            request_count_ref[0] += 1
            if request_count_ref[0] > 0 and request_count_ref[0] % MAX_REQUESTS_BEFORE_BACKOFF == 0:
                logger.info("Backoff after %s requests for %s s", request_count_ref[0], BACKOFF_SEC)
                save_next_offset(state_dir, next_offset)
                time.sleep(BACKOFF_SEC)
            time.sleep(REQUEST_DELAY_MS / 1000.0)
            data, success = fetch_log_json_with_retry(log_id)
            if success and data:
                logs_dir.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                if chat_db_conn is not None:
                    try:
                        with chat_db_conn:
                            n_chat = replace_chat_for_log(chat_db_conn, log_id, data)
                        logger.info("Indexed chat for log %s (%s message(s))", log_id, n_chat)
                    except Exception as e:
                        logger.warning("Chat DB indexing failed for log %s: %s", log_id, e)
                if stats_db_conn is not None:
                    try:
                        with stats_db_conn:
                            n_stats = replace_stats_for_log(stats_db_conn, log_id, data)
                        logger.info("Indexed stats for log %s (%s player row(s))", log_id, n_stats)
                    except Exception as e:
                        logger.warning("Stats DB indexing failed for log %s: %s", log_id, e)
                size_bytes = path.stat().st_size
                recent_writes.append((time.time(), log_id))
                downloads_since_progress_ref[0] += 1
                session_downloads_ref[0] += 1
                if len(recent_writes) > RECENT_WRITES_SIZE:
                    del recent_writes[: len(recent_writes) - RECENT_WRITES_SIZE]
                downloaded += 1
                logger.info("Wrote log %s (%s)", log_id, _human_bytes(size_bytes))
                try:
                    check_log_for_subscriptions(log_id, logs_dir, state_dir)
                except Exception as e:
                    logger.warning("Webhook check failed for log %s: %s", log_id, e)
            else:
                skipped.add(log_id)
                save_skip_list(state_dir, skipped)
                skipped_this_page += 1
                logger.info("Skipped log %s (failed or invalid)", log_id)
        next_offset += len(logs)
        save_next_offset(state_dir, next_offset)
        _log_stats_and_eta(logs_dir, recent_writes)
        _write_progress_if_due(logs_dir, state_dir, recent_writes, last_progress_write_ref, downloads_since_progress_ref, session_start_time_ref, session_downloads_ref)
        logger.info("Page done: offset now %s | downloaded=%s skipped=%s already_had=%s", next_offset, downloaded, skipped_this_page, already_had)
        if len(logs) < LIMIT:
            break
    return next_offset


def run_once(logs_dir: Path, state_dir: Path, skipped: set[int], next_offset: int) -> int:
    """Legacy helper: run backfill only (no Phase 1). Returns new next_offset."""
    request_count_ref = [0]
    return run_backfill_from_offset(
        logs_dir,
        state_dir,
        skipped,
        next_offset,
        request_count_ref,
        [],
        [0.0],
        [0],
        [0.0],
        [0],
        None,
        None,
    )


def main() -> None:
    logs_dir = LOGS_DIR
    state_dir = DOWNLOADER_STATE_DIR
    chat_db_path = CHAT_DB_PATH
    stats_db_path = STATS_DB_PATH
    logger.info(
        "Downloader started. LOGS_DIR=%s (log files only) STATE_DIR=%s CHAT_DB_PATH=%s STATS_DB_PATH=%s",
        logs_dir,
        state_dir,
        chat_db_path,
        stats_db_path,
    )
    chat_db_conn: sqlite3.Connection | None = None
    try:
        chat_db_conn = connect_chat_db(chat_db_path)
        init_chat_db(chat_db_conn)
        if alias_fts_rebuild_pending(chat_db_conn):
            logger.info("")
            logger.info("%s", "=" * 80)
            logger.info(
                "CHAT DB: Player-name index rebuild required — log downloads wait until it completes."
            )
            logger.info(
                "CHAT DB: First run or post-upgrade; large DBs may need many minutes for this step."
            )
            logger.info(
                "CHAT DB: Heartbeat messages every ~%ss while SQLite rebuilds the alias index.",
                int(ALIAS_FTS_PROGRESS_HEARTBEAT_SEC),
            )
            logger.info("%s", "=" * 80)
            logger.info("")
        run_alias_fts_rebuild_if_needed(chat_db_conn, log_progress=True)
        if alias_fts_rebuild_pending(chat_db_conn):
            logger.warning(
                "CHAT DB: Alias FTS still not marked ready (e.g. lock contention). "
                "Player-name search may stay unavailable; will retry at the start of each download cycle."
            )
        else:
            logger.info("CHAT DB: Player-name index ready — proceeding with log downloads.")
    except Exception as e:
        logger.exception("Failed to open/init chat DB (%s). Continuing without DB indexing: %s", chat_db_path, e)
        chat_db_conn = None

    stats_db_conn: sqlite3.Connection | None = None
    _stats_tmp: sqlite3.Connection | None = None
    try:
        _stats_tmp = connect_stats_db(stats_db_path)
        init_stats_db(_stats_tmp)
        stats_db_conn = _stats_tmp
        _stats_tmp = None
        logger.info("Stats DB ready at %s", stats_db_path)
    except Exception as e:
        logger.warning("Failed to open/init stats DB (%s). Continuing without stats indexing: %s", stats_db_path, e)
        if _stats_tmp is not None:
            try:
                _stats_tmp.close()
            except Exception:
                pass

    try:
        recent_writes: list[tuple[float, int]] = []  # sliding window for ETA rate (fallback)
        last_progress_write_ref: list[float] = [0.0]  # last time we wrote progress.json
        downloads_since_progress_ref: list[int] = [0]  # count of logs written since last progress update
        session_start_time_ref: list[float] = [time.time()]  # process start for aggregated ETA rate
        session_downloads_ref: list[int] = [0]  # total logs written this run for aggregated ETA rate
        while True:
            if chat_db_conn is not None and alias_fts_rebuild_pending(chat_db_conn):
                logger.info(
                    "CHAT DB: Retrying alias FTS rebuild before this cycle (downloads wait until done or skipped)."
                )
                run_alias_fts_rebuild_if_needed(
                    chat_db_conn,
                    log_progress=True,
                    busy_attempts=ALIAS_FTS_CYCLE_BUSY_ATTEMPTS,
                )
            skipped = load_skip_list(state_dir)
            next_offset = load_next_offset(state_dir, logs_dir)
            logger.info("Resuming: next_offset=%s skip_list_size=%s", next_offset, len(skipped))
            request_count_ref = [0]  # shared across Phase 1 and Phase 2 for backoff
            try:
                # Phase 1: always check offset=0 for new logs first (even if we're millions of logs behind)
                run_catch_up_newest(
                    logs_dir,
                    state_dir,
                    skipped,
                    request_count_ref,
                    recent_writes,
                    last_progress_write_ref,
                    downloads_since_progress_ref,
                    session_start_time_ref,
                    session_downloads_ref,
                    chat_db_conn,
                    stats_db_conn,
                )
                # Phase 2: continue backfill from saved offset toward oldest log
                next_offset = run_backfill_from_offset(
                    logs_dir,
                    state_dir,
                    skipped,
                    next_offset,
                    request_count_ref,
                    recent_writes,
                    last_progress_write_ref,
                    downloads_since_progress_ref,
                    session_start_time_ref,
                    session_downloads_ref,
                    chat_db_conn,
                    stats_db_conn,
                )
            except Exception as e:
                logger.exception("Run failed: %s", e)
            logger.info("Cycle complete. Sleeping %s s until next run.", DOWNLOAD_INTERVAL_SEC)
            time.sleep(DOWNLOAD_INTERVAL_SEC)
    finally:
        if stats_db_conn is not None:
            try:
                stats_db_conn.close()
            except Exception:
                pass
        if chat_db_conn is not None:
            try:
                chat_db_conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
