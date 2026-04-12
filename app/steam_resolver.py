"""Resolve any Steam user identifier to SteamID64. Uses Steam Web API only for vanity; API key never leaves server."""
import re
import threading
import time
from typing import Any

import requests

from app.logs_tf import STEAMID64_OFFSET

STEAMID64_LEN = 17
RESOLVE_VANITY_TIMEOUT = 10
RESOLVE_VANITY_URL = "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/"
# In-process cache for successful vanity→SteamID64 (reduces repeat HTTP; TTL seconds).
_VANITY_CACHE_TTL_SEC = 3600.0
_VANITY_CACHE_MAX_ENTRIES = 5000
_vanity_cache_lock = threading.Lock()
_vanity_cache: dict[str, tuple[str, float]] = {}  # vanity_lower -> (steamid64, expiry_monotonic)

# SteamID3: [U:1:account_id] or [U:0:account_id]
_STEAMID3_RE = re.compile(r"^\[U:1:(\d+)\]$", re.IGNORECASE)

# Profile URL: .../profiles/76561197960265728 (optional trailing slash or query)
_PROFILE_RE = re.compile(r"profiles/(\d{17})", re.IGNORECASE)

# Vanity URL: .../id/vanityname (vanity: alphanumeric, underscore, hyphen)
_VANITY_URL_RE = re.compile(r"steamcommunity\.com/id/([A-Za-z0-9_-]+)", re.IGNORECASE)

# Vanity name (standalone): allow same chars, 3–32 chars (Steam limits)
_VANITY_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{3,32}$")


class SteamVanityRateLimited(Exception):
    """Per-IP Steam ResolveVanityURL budget exhausted before an outbound HTTP call."""

    def __init__(self, retry_after: int) -> None:
        self.retry_after = retry_after


def _vanity_cache_prune(now: float) -> None:
    """Drop expired entries; if still over max, remove arbitrary keys (best-effort)."""
    stale = [k for k, (_, exp) in _vanity_cache.items() if exp <= now]
    for k in stale:
        _vanity_cache.pop(k, None)
    while len(_vanity_cache) > _VANITY_CACHE_MAX_ENTRIES and _vanity_cache:
        _vanity_cache.pop(next(iter(_vanity_cache)))


def resolve_to_steamid64(
    raw: str,
    api_key: str | None,
    *,
    vanity_rl_client_ip: str | None = None,
) -> tuple[str | None, str | None]:
    """
    Resolve arbitrary Steam user input to a 17-digit SteamID64.

    Supports: SteamID64, SteamID3 ([U:1:x]), profile URL, vanity URL, vanity name.
    Returns (steamid64, error_message). On success error_message is None; on failure steamid64 is None.
    API key is only used for vanity resolution; never logged or returned.

    ``vanity_rl_client_ip``: when set, enforces a per-IP limit immediately before each
    ResolveVanityURL HTTP call (in-memory vanity cache hits do not consume a slot).
    May raise :class:`SteamVanityRateLimited`.
    """
    raw = (raw or "").strip()
    if not raw:
        return None, "Steam ID is required."

    # 1) Plain 17-digit SteamID64
    if len(raw) == STEAMID64_LEN and raw.isdigit():
        return raw, None

    # 2) SteamID3 [U:1:account_id]
    m = _STEAMID3_RE.match(raw)
    if m:
        try:
            account_id = int(m.group(1))
            sid64 = str(STEAMID64_OFFSET + account_id)
            if len(sid64) == STEAMID64_LEN:
                return sid64, None
        except ValueError:
            pass
        return None, "Invalid SteamID3 format."

    # 3) Profile URL: .../profiles/76561197960265728
    if "steamcommunity" in raw.lower() or "profiles/" in raw.lower():
        pm = _PROFILE_RE.search(raw)
        if pm:
            return pm.group(1), None
        vm = _VANITY_URL_RE.search(raw)
        if vm:
            vanity = vm.group(1)
            return _resolve_vanity(vanity, api_key, vanity_rl_client_ip=vanity_rl_client_ip)
        return None, "Could not parse Steam profile URL. Use a URL like https://steamcommunity.com/profiles/76561197960265728 or https://steamcommunity.com/id/YourName"

    # 4) Standalone vanity name
    if _VANITY_NAME_RE.match(raw):
        return _resolve_vanity(raw, api_key, vanity_rl_client_ip=vanity_rl_client_ip)

    # 5) Might be a URL we didn't match (e.g. store.steampowered.com) or malformed
    if raw.startswith("http://") or raw.startswith("https://"):
        return None, "Unsupported Steam URL. Use a profile URL: https://steamcommunity.com/profiles/76561197960265728 or https://steamcommunity.com/id/YourName"
    if len(raw) == STEAMID64_LEN and not raw.isdigit():
        return None, "SteamID64 must be 17 digits."
    return None, "Could not recognize Steam ID. Use SteamID64 (17 digits), profile URL, or vanity name (requires Steam Web API key in server config)."


def _resolve_vanity(
    vanity: str,
    api_key: str | None,
    *,
    vanity_rl_client_ip: str | None = None,
) -> tuple[str | None, str | None]:
    """Call Steam Web API ResolveVanityURL. API key is never logged or returned."""
    if not api_key:
        return None, "Vanity URL/name resolution requires a Steam Web API key (set STEAM_WEB_API_KEY in server config)."
    vanity = vanity.strip()
    if not vanity:
        return None, "Vanity name is empty."
    cache_key = vanity.lower()
    now = time.monotonic()
    with _vanity_cache_lock:
        ent = _vanity_cache.get(cache_key)
        if ent is not None:
            sid_cached, exp = ent
            if exp > now:
                return sid_cached, None
        _vanity_cache_prune(now)
    # ``None`` skips RL (e.g. CLI); empty string still limits as "unknown" in rate_limit.
    if vanity_rl_client_ip is not None:
        from app.rate_limit import steam_vanity_retry_after_if_limited

        ra = steam_vanity_retry_after_if_limited(vanity_rl_client_ip)
        if ra is not None:
            raise SteamVanityRateLimited(ra)
    try:
        # Key is sent only in server-side request; never in response or logs
        r = requests.get(
            RESOLVE_VANITY_URL,
            params={"key": api_key, "vanityurl": vanity},
            timeout=RESOLVE_VANITY_TIMEOUT,
        )
        r.raise_for_status()
        data: dict[str, Any] = r.json()
        resp = data.get("response") if isinstance(data, dict) else None
        if not isinstance(resp, dict):
            return None, "Invalid response from Steam API."
        success = resp.get("success")
        if success == 1:
            sid = resp.get("steamid")
            if isinstance(sid, str) and len(sid) == STEAMID64_LEN and sid.isdigit():
                exp = time.monotonic() + _VANITY_CACHE_TTL_SEC
                with _vanity_cache_lock:
                    _vanity_cache[cache_key] = (sid, exp)
                    _vanity_cache_prune(time.monotonic())
                return sid, None
            return None, "Invalid Steam ID in API response."
        if success == 42:
            return None, "No Steam account found for that vanity URL or name."
        msg = resp.get("message", "Unknown error")
        return None, f"Steam API: {msg}"
    except requests.Timeout:
        return None, "Steam API request timed out. Try again later."
    except requests.RequestException:
        # Do not expose internal details; do not log API key
        return None, "Could not reach Steam API. Try again later."
