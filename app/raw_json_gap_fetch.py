"""
Download raw `log_<id>.log.zip` from logs.tf for every log ID that already has `{id}.json`
on disk but does not yet have a raw zip, then parse into `raw_events.db`.

Intended for large libraries (millions of JSON files). Properties:

- **Idempotent / resumable**: skips IDs that already have `log_<id>.log.zip`; safe to Ctrl+C
  and re-run.
- **Rate limits**: uses the same `REQUEST_DELAY_MS`, `MAX_REQUESTS_BEFORE_BACKOFF`, and
  `BACKOFF_SEC` as the downloader (only sleeps before an HTTP fetch).
- **Batched SQLite commits**: commits every `--batch-size` successful DB writes (default 50)
  to avoid millions of tiny transactions.

Does not use `DOWNLOAD_JSON_ENABLED` / `DOWNLOAD_RAW_ENABLED`; this is an explicit admin tool.

Usage (host, typical):

    docker compose stop downloader
    docker compose run --rm downloader python -m app.raw_json_gap_fetch
    docker compose up -d downloader

Options:

    --logs-dir, --raw-logs-dir, --db-path  (defaults from app.config)
    --from-id / --to-id     optional inclusive range filter
    --limit N               stop after N fetch attempts (testing)
    --batch-size N          SQLite commit every N successful imports (default 50)
    --progress-every N      log a summary every N examined IDs (default 2000)
    --shard-index I --shard-total T   only process IDs where (id % T) == I (parallel sharding)
    --dry-run               list counts only (no network, no DB writes)

For ~2M gaps at ~300ms between requests, wall time is on the order of **weeks** single-threaded;
use sharding, tune `REQUEST_DELAY_MS` only if logs.tf policy allows, and run under `tmux`/`screen`.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from app.config import (
    BACKOFF_SEC,
    LOGS_DIR,
    MAX_REQUESTS_BEFORE_BACKOFF,
    RAW_EVENTS_DB_PATH,
    RAW_LOGS_DIR,
    REQUEST_DELAY_MS,
)
from app.raw_zip_io import extract_log_content_from_zip, fetch_raw_log_zip_with_retry, save_raw_log_zip
from app.raw_db import connect_raw_db, init_raw_db, replace_raw_events_for_log
from app.raw_log_parser import parse_raw_log

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)


def _collect_json_ids(
    logs_dir: Path,
    *,
    id_min: int | None,
    id_max: int | None,
) -> list[int]:
    """All numeric `*.json` stems under ``logs_dir``, filtered by optional inclusive bounds."""
    out: list[int] = []
    if not logs_dir.is_dir():
        return out
    with os.scandir(logs_dir) as it:
        for entry in it:
            if not entry.is_file(follow_symlinks=False):
                continue
            name = entry.name
            if not name.endswith(".json"):
                continue
            stem = name[:-5]
            if not stem.isdigit():
                continue
            lid = int(stem)
            if id_min is not None and lid < id_min:
                continue
            if id_max is not None and lid > id_max:
                continue
            out.append(lid)
    out.sort()
    return out


def _rate_limit_before_fetch(request_count: list[int]) -> None:
    """Mirror downloader: count requests, backoff periodically, delay between fetches."""
    request_count[0] += 1
    if request_count[0] > 0 and request_count[0] % MAX_REQUESTS_BEFORE_BACKOFF == 0:
        logger.info("Backoff after %s raw zip requests, sleeping %s s", request_count[0], BACKOFF_SEC)
        time.sleep(BACKOFF_SEC)
    time.sleep(REQUEST_DELAY_MS / 1000.0)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch raw log zips for JSON logs missing raw, then index raw_events.db."
    )
    ap.add_argument("--logs-dir", type=Path, default=LOGS_DIR, help="Directory of N.json files")
    ap.add_argument("--raw-logs-dir", type=Path, default=RAW_LOGS_DIR, help="Where to write log_N.log.zip")
    ap.add_argument("--db-path", type=Path, default=RAW_EVENTS_DB_PATH, help="raw_events.db path")
    ap.add_argument("--from-id", type=int, default=None, help="Minimum log ID (inclusive)")
    ap.add_argument("--to-id", type=int, default=None, help="Maximum log ID (inclusive)")
    ap.add_argument("--limit", type=int, default=None, help="Max number of HTTP fetch attempts (testing)")
    ap.add_argument("--batch-size", type=int, default=50, help="SQLite commit every N successful imports")
    ap.add_argument(
        "--progress-every",
        type=int,
        default=2000,
        help="Log a progress line every N examined log IDs (including skips)",
    )
    ap.add_argument("--shard-index", type=int, default=0, help="Shard index I for parallel runs (use with --shard-total)")
    ap.add_argument("--shard-total", type=int, default=0, help="Shard count T; process only IDs with id %% T == shard-index")
    ap.add_argument("--dry-run", action="store_true", help="Scan only; no HTTP or DB writes")
    args = ap.parse_args()

    logs_dir: Path = args.logs_dir
    raw_dir: Path = args.raw_logs_dir
    db_path: Path = args.db_path
    batch_size = max(1, int(args.batch_size))
    progress_every = max(1, int(args.progress_every))
    shard_index = int(args.shard_index)
    shard_total = int(args.shard_total)

    if shard_total > 0 and not (0 <= shard_index < shard_total):
        logger.error("--shard-index must satisfy 0 <= shard-index < shard-total")
        sys.exit(2)

    logger.info(
        "Scanning %s for *.json (bounds %s–%s)…",
        logs_dir,
        args.from_id if args.from_id is not None else "min",
        args.to_id if args.to_id is not None else "max",
    )
    t_scan = time.perf_counter()
    all_ids = _collect_json_ids(logs_dir, id_min=args.from_id, id_max=args.to_id)
    logger.info("Found %s JSON log file(s) in %.1fs", len(all_ids), time.perf_counter() - t_scan)

    if shard_total > 0:
        before = len(all_ids)
        all_ids = [i for i in all_ids if i % shard_total == shard_index]
        logger.info("Shard %s/%s: %s ID(s) after filter (was %s)", shard_index, shard_total, len(all_ids), before)

    if args.dry_run:
        missing = 0
        for lid in all_ids:
            if not (raw_dir / f"log_{lid}.log.zip").is_file():
                missing += 1
        logger.info(
            "Dry run: would attempt up to %s raw download(s) (JSON present, raw zip missing).",
            missing,
        )
        return

    conn = connect_raw_db(db_path)
    try:
        init_raw_db(conn)
    except Exception as e:
        logger.exception("Could not init raw DB: %s", e)
        sys.exit(1)

    request_count: list[int] = [0]
    examined = 0
    skipped_have_zip = 0
    fetch_attempts = 0
    fetch_ok = 0
    no_zip_on_server = 0
    save_failed = 0
    parse_failed = 0
    limit = args.limit

    t0 = time.perf_counter()
    in_batch = 0
    conn.execute("BEGIN")

    try:
        for lid in all_ids:
            examined += 1
            jp = logs_dir / f"{lid}.json"
            rp = raw_dir / f"log_{lid}.log.zip"

            if not jp.is_file():
                continue

            if rp.is_file():
                skipped_have_zip += 1
                if examined % progress_every == 0:
                    _log_progress(
                        examined,
                        len(all_ids),
                        skipped_have_zip,
                        fetch_attempts,
                        fetch_ok,
                        no_zip_on_server,
                        t0,
                    )
                continue

            if limit is not None and fetch_attempts >= limit:
                logger.info("--limit %s reached; stopping.", limit)
                break

            fetch_attempts += 1
            _rate_limit_before_fetch(request_count)

            zip_bytes = fetch_raw_log_zip_with_retry(lid)
            if zip_bytes is None:
                no_zip_on_server += 1
                if examined % progress_every == 0:
                    _log_progress(
                        examined,
                        len(all_ids),
                        skipped_have_zip,
                        fetch_attempts,
                        fetch_ok,
                        no_zip_on_server,
                        t0,
                    )
                continue

            saved = save_raw_log_zip(lid, zip_bytes, raw_dir)
            if saved is None:
                save_failed += 1
                continue

            content = extract_log_content_from_zip(zip_bytes)
            if content is None:
                logger.warning("Could not read raw log from zip for log %s", lid)
                parse_failed += 1
                continue

            try:
                parsed = parse_raw_log(lid, content)
                conn.execute("SAVEPOINT raw_gap")
                try:
                    replace_raw_events_for_log(conn, lid, parsed)
                except Exception as e:
                    conn.execute("ROLLBACK TO SAVEPOINT raw_gap")
                    raise
                conn.execute("RELEASE SAVEPOINT raw_gap")
            except Exception as e:
                logger.warning("Raw parse/store failed for log %s: %s", lid, e)
                parse_failed += 1
                continue

            fetch_ok += 1
            in_batch += 1
            if in_batch >= batch_size:
                conn.commit()
                logger.info(
                    "Committed batch (%s successful fetch+import(s) this run); total fetch_ok=%s",
                    batch_size,
                    fetch_ok,
                )
                in_batch = 0
                conn.execute("BEGIN")

            if fetch_ok <= 5 or fetch_ok % max(1, progress_every // 10) == 0:
                logger.info(
                    "Imported raw_events for log %s (examined=%s fetch_ok=%s)",
                    lid,
                    examined,
                    fetch_ok,
                )

            if examined % progress_every == 0:
                _log_progress(
                    examined,
                    len(all_ids),
                    skipped_have_zip,
                    fetch_attempts,
                    fetch_ok,
                    no_zip_on_server,
                    t0,
                )

        conn.commit()
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()

    elapsed = time.perf_counter() - t0
    logger.info(
        "Done in %.1fs | examined=%s skipped_have_zip=%s fetch_attempts=%s fetch_ok=%s "
        "no_zip_404_or_fail=%s save_failed=%s parse_failed=%s",
        elapsed,
        examined,
        skipped_have_zip,
        fetch_attempts,
        fetch_ok,
        no_zip_on_server,
        save_failed,
        parse_failed,
    )


def _log_progress(
    examined: int,
    total_ids: int,
    skipped_have_zip: int,
    fetch_attempts: int,
    fetch_ok: int,
    no_zip: int,
    t0: float,
) -> None:
    elapsed = time.perf_counter() - t0
    rate = examined / elapsed if elapsed > 0 else 0.0
    remaining = max(0, total_ids - examined)
    eta_s = remaining / rate if rate > 0 else 0.0
    logger.info(
        "Progress: examined %s/%s (%.1f%%) | have_zip=%s | fetch_try=%s ok=%s no_zip=%s | %.2f ids/s | elapsed=%.0fs ETA~%.0fh",
        examined,
        total_ids,
        100.0 * examined / total_ids if total_ids else 0.0,
        skipped_have_zip,
        fetch_attempts,
        fetch_ok,
        no_zip,
        rate,
        elapsed,
        eta_s / 3600.0,
    )


if __name__ == "__main__":
    main()
