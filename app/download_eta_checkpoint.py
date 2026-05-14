"""Persistent downloader ETA helpers (checkpoint survives process restarts)."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

CHECKPOINT_VERSION = 1
ETA_CHECKPOINT_FILENAME = "download_eta_checkpoint.json"
ETA_CHECKPOINT_MIN_SAVE_INTERVAL_SEC = 30.0
MAX_RECENT_PERSIST = 100
_WALL_MIN = 946684800.0  # ~2000-01-01
_WALL_MAX = 2524651200.0  # ~2050


def _sanitize_recent(raw: Any) -> list[tuple[float, int]]:
    out: list[tuple[float, int]] = []
    if not isinstance(raw, list):
        return []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        try:
            fts = float(item[0])
            ilid = int(item[1])
        except (TypeError, ValueError):
            continue
        if ilid < 1 or ilid > 999_999_999:
            continue
        out.append((fts, ilid))
    return out[-MAX_RECENT_PERSIST:]


def load_eta_checkpoint(state_dir: Path) -> tuple[float, int, list[tuple[float, int]]]:
    """
    Return (wall_start_unix, total_downloads, recent_writes).

    On missing or invalid data: ``(now, 0, [])`` so a new aggregate window starts clean.
    """
    now = time.time()
    path = state_dir / ETA_CHECKPOINT_FILENAME
    if not path.is_file():
        return (now, 0, [])
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return (now, 0, [])
        if data.get("v") != CHECKPOINT_VERSION:
            return (now, 0, [])
        wall_start = float(data["wall_start"])
        total_downloads = int(data["total_downloads"])
        recent = _sanitize_recent(data.get("recent_writes"))
        if wall_start < _WALL_MIN or wall_start > _WALL_MAX or wall_start > now + 3600:
            return (now, 0, [])
        if now + 120 < wall_start:
            return (now, 0, [])
        if total_downloads < 0:
            total_downloads = 0
        return (wall_start, total_downloads, recent)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError, KeyError):
        return (now, 0, [])


def save_eta_checkpoint(
    state_dir: Path,
    wall_start: float,
    total_downloads: int,
    recent_writes: list[tuple[float, int]],
) -> None:
    """Atomic write (temp + rename). Interrupt-safe on POSIX."""
    now = time.time()
    if wall_start < _WALL_MIN or wall_start > _WALL_MAX or wall_start > now + 3600:
        wall_start = now
    if total_downloads < 0:
        total_downloads = 0
    tail = recent_writes[-MAX_RECENT_PERSIST:]
    payload = {
        "v": CHECKPOINT_VERSION,
        "wall_start": wall_start,
        "total_downloads": int(total_downloads),
        "recent_writes": [[ts, lid] for ts, lid in tail],
        "saved_at": now,
    }
    state_dir.mkdir(parents=True, exist_ok=True)
    target = state_dir / ETA_CHECKPOINT_FILENAME
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    tmp.replace(target)


def maybe_save_eta_checkpoint(
    state_dir: Path,
    wall_start: float,
    total_downloads: int,
    recent_writes: list[tuple[float, int]],
    last_save_ref: list[float],
    *,
    force: bool = False,
) -> None:
    """Rate-limit writes unless ``force`` (e.g. when flushing ``progress.json``)."""
    now = time.time()
    if not force and (now - last_save_ref[0]) < ETA_CHECKPOINT_MIN_SAVE_INTERVAL_SEC:
        return
    save_eta_checkpoint(state_dir, wall_start, total_downloads, recent_writes)
    last_save_ref[0] = now
