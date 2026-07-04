"""Persistent set of log IDs whose raw zip is permanently unavailable on logs.tf."""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

RAW_UNAVAILABLE_FILE = "raw_zip_unavailable.json"
_MAX_LOG_ID = 999_999_999


def load_raw_unavailable(state_dir: Path) -> set[int]:
    path = state_dir / RAW_UNAVAILABLE_FILE
    if not path.is_file():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return set()
    if not isinstance(data, list):
        return set()
    out: set[int] = set()
    for item in data:
        try:
            lid = int(item)
        except (TypeError, ValueError):
            continue
        if 1 <= lid <= _MAX_LOG_ID:
            out.add(lid)
    return out


def save_raw_unavailable(state_dir: Path, unavailable: set[int]) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / RAW_UNAVAILABLE_FILE
    path.write_text(json.dumps(sorted(unavailable)), encoding="utf-8")


def try_save_raw_unavailable(state_dir: Path, unavailable: set[int]) -> bool:
    """Persist unavailable IDs; return True on success."""
    try:
        save_raw_unavailable(state_dir, unavailable)
        return True
    except OSError as e:
        logger.warning("Could not save %s: %s", RAW_UNAVAILABLE_FILE, e)
        return False


def mark_raw_unavailable(unavailable: set[int], log_id: int) -> bool:
    """Add log_id to the in-memory set. Returns True when the set changed."""
    if log_id in unavailable:
        return False
    unavailable.add(log_id)
    return True
