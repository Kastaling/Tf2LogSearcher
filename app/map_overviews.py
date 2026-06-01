"""Map overview image slugs and Hammer bounds helpers for heatmaps."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from app.search.search import _map_canonical_key

_MODE_PREFIXES: frozenset[str] = frozenset({
    "ad", "arena", "bball", "bonus", "cp", "ctf", "dm", "dr", "event", "gg", "gr",
    "he", "hightower", "jb", "koth", "mvm", "owl", "pass", "pd", "pl", "plr", "pq",
    "pro", "rd", "rp", "sd", "tc", "td", "tr", "ud", "vip", "vs",
})

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_]*$")

MAP_OVERVIEWS_DIR = Path(__file__).resolve().parents[1] / "static" / "map_overviews"
AVAILABLE_SLUGS_PATH = MAP_OVERVIEWS_DIR / "available_slugs.json"
BOUNDS_PATH = MAP_OVERVIEWS_DIR / "bounds.json"
BOUNDS_EXAMPLE_PATH = MAP_OVERVIEWS_DIR / "bounds.example.json"


def overview_slug_candidates(raw_map: str) -> list[str]:
    """Filename stems for ``static/map_overviews/<slug>.png``, most likely first."""
    canonical = _map_canonical_key(raw_map)
    if not canonical or canonical == "(unknown)":
        return []
    parts = [p for p in canonical.split("_") if p]
    if not parts:
        return []
    candidates: list[str] = []
    if len(parts) >= 2 and parts[0] in _MODE_PREFIXES:
        short = "_".join(parts[1:])
        if short:
            candidates.append(short)
    candidates.append(canonical)
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        c = c.lower()
        if c in seen or not _SLUG_RE.fullmatch(c):
            continue
        seen.add(c)
        out.append(c)
    return out


def load_available_slugs(path: Path | None = None) -> frozenset[str]:
    p = path or AVAILABLE_SLUGS_PATH
    if not p.is_file():
        return frozenset(
            f.stem for f in MAP_OVERVIEWS_DIR.glob("*.png") if f.is_file()
        )
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return frozenset()
    slugs = data.get("slugs") if isinstance(data, dict) else data
    if not isinstance(slugs, list):
        return frozenset()
    return frozenset(str(s).strip().lower() for s in slugs if str(s).strip())


def resolve_overview_slug(raw_map: str, available: frozenset[str] | None = None) -> str | None:
    """Pick the first candidate slug that has a PNG on disk."""
    avail = available if available is not None else load_available_slugs()
    for slug in overview_slug_candidates(raw_map):
        if slug in avail and (MAP_OVERVIEWS_DIR / f"{slug}.png").is_file():
            return slug
        if (MAP_OVERVIEWS_DIR / f"{slug}.png").is_file():
            return slug
    return None


def bounds_entry_complete(entry: dict[str, Any]) -> bool:
    """True when a slug has numeric Hammer axis bounds suitable for projecting kills."""
    try:
        xmin = float(entry["xmin"])
        xmax = float(entry["xmax"])
        ymin = float(entry["ymin"])
        ymax = float(entry["ymax"])
    except (KeyError, TypeError, ValueError):
        return False
    return xmax > xmin and ymax > ymin


def load_bounds(path: Path | None = None) -> dict[str, dict[str, Any]]:
    for p in (path, BOUNDS_PATH, BOUNDS_EXAMPLE_PATH):
        if p is None or not p.is_file():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if not str(k).startswith("_") and isinstance(v, dict)}
    return {}


def production_bounds_for_slug(slug: str) -> dict[str, Any] | None:
    """Per-map bounds from ``bounds.json`` only (never ``bounds.example.json``)."""
    if not slug or not BOUNDS_PATH.is_file():
        return None
    try:
        data = json.loads(BOUNDS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    entry = data.get(slug)
    if not isinstance(entry, dict) or not bounds_entry_complete(entry):
        return None
    return entry


def log_detail_heatmaps_ready(
    map_name: str,
    *,
    feature_enabled: bool,
    events_indexed: bool,
    kill_count: int,
) -> bool:
    """Whether the log detail page may render coordinate heatmaps for this log."""
    if not feature_enabled or not events_indexed or kill_count <= 0:
        return False
    slug = resolve_overview_slug(map_name)
    if not slug:
        return False
    return production_bounds_for_slug(slug) is not None


def overview_image_url(slug: str) -> str:
    return f"/static/map_overviews/{slug}.png"
