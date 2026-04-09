"""
Targeted backfill: populate player_names in stats DB from existing local log JSON files.
Much faster than a full stats backfill — only reads names dict from each log.

Usage: python -m app.player_names_backfill [--logs-dir ...] [--db-path ...] [--batch-size 1000]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from app.config import LOGS_DIR, STATS_DB_PATH
from app.logs_tf import steamid3_to_steamid64
from app.stats_db import connect_stats_db, init_stats_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)


def _iter_log_files(logs_dir: Path) -> list[Path]:
    out: list[Path] = []
    for p in logs_dir.iterdir():
        if not p.is_file() or p.suffix != ".json":
            continue
        if not p.stem.isdigit():
            continue
        out.append(p)
    out.sort(key=lambda p: int(p.stem))
    return out


def _extract_name_rows(log_id: int, logtext: dict[str, Any]) -> list[tuple[str, str, int, int | None]]:
    """Returns list of (steamid64, alias, log_id, date_ts) tuples."""
    info = logtext.get("info") or {}
    try:
        raw = info.get("date")
        date_ts = int(raw) if raw is not None else None
    except (TypeError, ValueError):
        date_ts = None
    names = logtext.get("names")
    if not isinstance(names, dict):
        return []
    rows: list[tuple[str, str, int, int | None]] = []
    for steamid3, alias_raw in names.items():
        sid3 = str(steamid3).strip()
        sid64 = steamid3_to_steamid64(sid3)
        if not sid64:
            continue
        alias = str(alias_raw or "").strip()
        if not alias:
            continue
        rows.append((sid64, alias, log_id, date_ts))
    return rows


def run_backfill(logs_dir: Path, db_path: Path, batch_size: int) -> None:
    files = _iter_log_files(logs_dir)
    logger.info("Found %s local log file(s) to scan in %s", len(files), logs_dir)
    conn = connect_stats_db(db_path)
    try:
        init_stats_db(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise
    logger.info("Writing player_names rows into %s", db_path)

    start = time.time()
    processed = 0
    names_rows_total = 0
    parse_errors = 0

    conn.execute("BEGIN")
    try:
        for p in files:
            processed += 1
            try:
                log_id = int(p.stem)
                logtext = json.loads(p.read_text(encoding="utf-8", errors="replace"))
                rows = _extract_name_rows(log_id, logtext)
                if rows:
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO player_names (steamid64, alias, log_id, date_ts)
                        VALUES (?, ?, ?, ?)
                        """,
                        rows,
                    )
                    names_rows_total += len(rows)
            except (OSError, ValueError, TypeError) as e:
                parse_errors += 1
                logger.warning("Skipping %s due to parse/read error: %s", p.name, e)
                continue

            if processed % batch_size == 0:
                conn.commit()
                elapsed = max(0.001, time.time() - start)
                logger.info(
                    "Progress: %s/%s logs processed, %s name rows written (%.1f logs/s)",
                    processed,
                    len(files),
                    names_rows_total,
                    processed / elapsed,
                )
                conn.execute("BEGIN")
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    elapsed = max(0.001, time.time() - start)
    logger.info(
        "Player names backfill complete: processed=%s name_rows=%s parse_errors=%s elapsed=%.1fs (%.1f logs/s)",
        processed,
        names_rows_total,
        parse_errors,
        elapsed,
        processed / elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill player_names in stats SQLite DB from local log JSON files (names dict only)."
    )
    parser.add_argument("--logs-dir", default=str(LOGS_DIR), help="Directory containing local <id>.json log files")
    parser.add_argument("--db-path", default=str(STATS_DB_PATH), help="SQLite DB file path (stats DB)")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Commit every N log files (higher is faster; lower uses less rollback work on failure)",
    )
    args = parser.parse_args()

    logs_dir = Path(args.logs_dir)
    db_path = Path(args.db_path)
    batch_size = max(1, int(args.batch_size))
    if not logs_dir.exists() or not logs_dir.is_dir():
        raise SystemExit(f"Invalid --logs-dir: {logs_dir}")

    run_backfill(logs_dir, db_path, batch_size)


if __name__ == "__main__":
    main()
