"""Wall-clock ETA for the JSON backfill frontier, independent of download bursts."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

HISTORY_FILENAME = "eta_frontier_history.json"
HISTORY_VERSION = 1
HISTORY_WINDOW_SEC = 7 * 24 * 60 * 60
MIN_OBSERVATION_SEC = 6 * 60 * 60
MIN_FRONTIER_ADVANCE = 30
MAX_NO_PROGRESS_SEC = 2 * 60 * 60
MAX_SAMPLES = 2500


@dataclass(frozen=True)
class FrontierForecast:
    seconds: float | None
    observation_hours: float | None
    status: str
    no_progress_hours: float | None = None


def _valid_samples(raw: object, now: float) -> list[tuple[float, int]]:
    if not isinstance(raw, list):
        return []
    samples: list[tuple[float, int]] = []
    for row in raw:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            continue
        try:
            ts = float(row[0])
            frontier = int(row[1])
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(ts) or ts > now + 60 or ts < now - HISTORY_WINDOW_SEC:
            continue
        if not 1 <= frontier <= 999_999_999:
            continue
        if samples and (ts <= samples[-1][0] or frontier > samples[-1][1]):
            continue
        samples.append((ts, frontier))
    return samples[-MAX_SAMPLES:]


def _previous_progress_sample(state_dir: Path, now: float) -> list[tuple[float, int]]:
    """Seed a new history from the existing progress snapshot after an upgrade."""
    try:
        data = json.loads((state_dir / "progress.json").read_text(encoding="utf-8"))
        updated = datetime.fromisoformat(str(data["updated_at"]).replace("Z", "+00:00"))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        return _valid_samples([[updated.timestamp(), data["min_id"]]], now)
    except (OSError, ValueError, TypeError, KeyError):
        return []


def load_frontier_history(state_dir: Path, now: float) -> list[tuple[float, int]]:
    path = state_dir / HISTORY_FILENAME
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("v") == HISTORY_VERSION:
            return _valid_samples(data.get("samples"), now)
    except (OSError, ValueError, UnicodeDecodeError):
        pass
    return _previous_progress_sample(state_dir, now)


def record_frontier(
    samples: list[tuple[float, int]], now: float, min_id: int | None
) -> list[tuple[float, int]]:
    """Append the observed lowest JSON ID; reset if the library frontier regresses."""
    if min_id is None or min_id < 1:
        return []
    history = _valid_samples(samples, now)
    if history and (now <= history[-1][0] or min_id > history[-1][1]):
        history = []
    history.append((now, min_id))
    return history[-MAX_SAMPLES:]


def forecast_frontier(samples: list[tuple[float, int]], now: float) -> FrontierForecast:
    history = _valid_samples(samples, now)
    if not history:
        return FrontierForecast(None, None, "no_history")
    if history[-1][1] <= 1:
        return FrontierForecast(0, None, "complete")
    if len(history) < 2:
        return FrontierForecast(None, None, "warming_up")
    span = now - history[0][0]
    if span < MIN_OBSERVATION_SEC:
        return FrontierForecast(None, span / 3600, "warming_up")
    last_advance = next(
        (
            history[i][0]
            for i in range(len(history) - 1, 0, -1)
            if history[i][1] < history[i - 1][1]
        ),
        None,
    )
    advance = history[0][1] - history[-1][1]
    if advance < MIN_FRONTIER_ADVANCE:
        return FrontierForecast(None, span / 3600, "insufficient_progress")
    seconds = (history[-1][1] - 1) * span / advance
    no_progress_hours = (now - last_advance) / 3600 if last_advance is not None else None
    status = "stalled" if last_advance is None or now - last_advance > MAX_NO_PROGRESS_SEC else "ready"
    return FrontierForecast(seconds, span / 3600, status, no_progress_hours)


def save_frontier_history(state_dir: Path, samples: list[tuple[float, int]]) -> None:
    """Persist a small, bounded history alongside the downloader state."""
    state_dir.mkdir(parents=True, exist_ok=True)
    target = state_dir / HISTORY_FILENAME
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps({"v": HISTORY_VERSION, "samples": samples}, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(target)
