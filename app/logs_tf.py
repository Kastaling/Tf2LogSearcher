"""logs.tf API helpers and Steam ID conversion."""
import re
import time
from typing import Any

import requests

from app.config import LOGS_TF_API_BASE

# Timeout for API requests (seconds)
REQUEST_TIMEOUT = 30

STEAMID64_OFFSET = 76561197960265728

_STEAMID3_RE = re.compile(r"^\[U:1:(\d+)\]$")


def steamid64_to_steamid3(steamid64: str | int) -> str:
    """Convert SteamID64 to SteamID3 format [U:1:xxx]."""
    a = int(steamid64) - STEAMID64_OFFSET
    return f"[U:1:{a}]"


def steamid3_to_steamid64(steamid3: str) -> str | None:
    """Parse logs.tf SteamID3 string to 17-digit SteamID64, or None if invalid."""
    m = _STEAMID3_RE.match((steamid3 or "").strip())
    if not m:
        return None
    try:
        account_id = int(m.group(1))
    except ValueError:
        return None
    return str(STEAMID64_OFFSET + account_id)


def get_log_list_for_player(steamid64: str, max_logs: int = 30000) -> list[int]:
    """Fetch log IDs for a player from logs.tf API (paginated). Returns list of log IDs."""
    log_ids: list[int] = []
    limit = 10000
    offset = 0
    while offset < max_logs:
        url = f"{LOGS_TF_API_BASE}/api/v1/log?limit={limit}&offset={offset}&player={steamid64}"
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            # Log and return what we have so far
            if not log_ids:
                raise
            break
        logs = data.get("logs") or []
        if not logs:
            break
        for log in logs:
            lid = log.get("id")
            if lid is not None:
                log_ids.append(int(lid))
        offset += limit
        if len(logs) < limit:
            break
    return log_ids


def fetch_log_list(offset: int, limit: int) -> list[dict[str, Any]]:
    """Fetch global log list (no player filter). Returns list of log objects with 'id'."""
    url = f"{LOGS_TF_API_BASE}/api/v1/log?offset={offset}&limit={limit}"
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return data.get("logs") or []


def fetch_log_json(log_id: int) -> dict[str, Any] | None:
    """Fetch full log JSON by ID. Returns parsed JSON or None on failure."""
    url = f"{LOGS_TF_API_BASE}/json/{log_id}"
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except (requests.RequestException, ValueError):
        return None
