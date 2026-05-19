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
# Gaps longer than this between consecutive writes start a new active segment (idle / pause).
WRITE_GAP_THRESHOLD_SEC = 120.0
# Ignore tiny segments when picking a stable rate (avoids noisy ETAs after resume).
MIN_SEGMENT_WRITES_FOR_RATE = 8
MIN_SEGMENT_DURATION_SEC_FOR_RATE = 20.0
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


def split_recent_write_segments(
    recent_writes: list[tuple[float, int]],
    *,
    gap_threshold_sec: float = WRITE_GAP_THRESHOLD_SEC,
) -> list[list[tuple[float, int]]]:
    """Split ``recent_writes`` into continuous download bursts (exclude idle gaps)."""
    if not recent_writes:
        return []
    segments: list[list[tuple[float, int]]] = []
    current: list[tuple[float, int]] = [recent_writes[0]]
    for i in range(1, len(recent_writes)):
        prev_ts = recent_writes[i - 1][0]
        ts = recent_writes[i][0]
        if ts - prev_ts > gap_threshold_sec:
            segments.append(current)
            current = []
        current.append(recent_writes[i])
    segments.append(current)
    return segments


def trailing_write_burst(
    recent_writes: list[tuple[float, int]],
    *,
    gap_threshold_sec: float = WRITE_GAP_THRESHOLD_SEC,
) -> list[tuple[float, int]]:
    """Most recent consecutive writes with no inter-write gap above threshold."""
    if not recent_writes:
        return []
    burst = [recent_writes[-1]]
    for i in range(len(recent_writes) - 1, 0, -1):
        if recent_writes[i][0] - recent_writes[i - 1][0] > gap_threshold_sec:
            break
        burst.append(recent_writes[i - 1])
    burst.reverse()
    return burst


def _segment_rate(segment: list[tuple[float, int]]) -> float | None:
    if len(segment) < 2:
        return None
    elapsed = segment[-1][0] - segment[0][0]
    if elapsed <= 0:
        return None
    return len(segment) / elapsed


def rate_from_trailing_burst(
    recent_writes: list[tuple[float, int]],
    *,
    gap_threshold_sec: float = WRITE_GAP_THRESHOLD_SEC,
) -> float | None:
    """Instantaneous rate from the latest continuous burst."""
    return _segment_rate(trailing_write_burst(recent_writes, gap_threshold_sec=gap_threshold_sec))


def rate_from_active_segments(
    recent_writes: list[tuple[float, int]],
    *,
    gap_threshold_sec: float = WRITE_GAP_THRESHOLD_SEC,
    min_segment_writes: int = MIN_SEGMENT_WRITES_FOR_RATE,
) -> float | None:
    """Weighted rate across all bursts in the window (idle time excluded)."""
    total_logs = 0
    total_sec = 0.0
    for segment in split_recent_write_segments(recent_writes, gap_threshold_sec=gap_threshold_sec):
        if len(segment) < min_segment_writes:
            continue
        elapsed = segment[-1][0] - segment[0][0]
        if elapsed <= 0:
            continue
        total_logs += len(segment)
        total_sec += elapsed
    if total_sec <= 0 or total_logs < min_segment_writes:
        return None
    return total_logs / total_sec


def rate_from_recent_segments(
    recent_writes: list[tuple[float, int]],
    *,
    gap_threshold_sec: float = WRITE_GAP_THRESHOLD_SEC,
    min_segment_writes: int = MIN_SEGMENT_WRITES_FOR_RATE,
    min_segment_duration_sec: float = MIN_SEGMENT_DURATION_SEC_FOR_RATE,
) -> float | None:
    """
    Prefer the newest burst with enough samples; else the best recent burst; else trailing burst.
    """
    segments = split_recent_write_segments(recent_writes, gap_threshold_sec=gap_threshold_sec)
    qualifying: list[tuple[float, int, float]] = []
    for segment in segments:
        if len(segment) < min_segment_writes:
            continue
        elapsed = segment[-1][0] - segment[0][0]
        if elapsed < min_segment_duration_sec:
            continue
        rate = len(segment) / elapsed
        qualifying.append((rate, len(segment), elapsed))
    if qualifying:
        return qualifying[-1][0]
    return rate_from_trailing_burst(recent_writes, gap_threshold_sec=gap_threshold_sec)


def reconcile_aggregate_window(
    wall_start: float,
    total_downloads: int,
    recent_writes: list[tuple[float, int]],
    *,
    now: float | None = None,
    gap_threshold_sec: float = WRITE_GAP_THRESHOLD_SEC,
) -> tuple[float, int]:
    """
    If ``recent_writes`` contains a long idle gap, move ``wall_start`` to the best recent burst
    so wall-clock fallback rates ignore downtime. ``total_downloads`` is never reduced.
    """
    if not recent_writes or total_downloads <= 0:
        return wall_start, total_downloads
    segments = split_recent_write_segments(recent_writes, gap_threshold_sec=gap_threshold_sec)
    if len(segments) <= 1:
        return wall_start, total_downloads
    tail = segments[-1]
    if len(tail) == 1:
        return tail[0][0], total_downloads
    qualifying = [s for s in segments if len(s) >= 2]
    if not qualifying:
        return wall_start, total_downloads
    ref = qualifying[-1]
    return ref[0][0], total_downloads


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
        wall_start, total_downloads = reconcile_aggregate_window(
            wall_start, total_downloads, recent, now=now
        )
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
