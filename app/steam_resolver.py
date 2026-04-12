"""Resolve any Steam user identifier to SteamID64. Uses Steam Web API only for vanity; API key never leaves server."""
import re
from typing import Any

import requests

from app.logs_tf import STEAMID64_OFFSET

STEAMID64_LEN = 17
RESOLVE_VANITY_TIMEOUT = 10
RESOLVE_VANITY_URL = "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/"

# SteamID3: [U:1:account_id] or [U:0:account_id]
_STEAMID3_RE = re.compile(r"^\[U:1:(\d+)\]$", re.IGNORECASE)

# Profile URL: .../profiles/76561197960265728 (optional trailing slash or query)
_PROFILE_RE = re.compile(r"profiles/(\d{17})", re.IGNORECASE)

# Vanity URL: .../id/vanityname (vanity: alphanumeric, underscore, hyphen)
_VANITY_URL_RE = re.compile(r"steamcommunity\.com/id/([A-Za-z0-9_-]+)", re.IGNORECASE)

# Vanity name (standalone): allow same chars, 3–32 chars (Steam limits)
_VANITY_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{3,32}$")


def steam_input_requires_vanity_http(raw: str) -> bool:
    """
    True if ``resolve_to_steamid64`` would call the Steam ResolveVanityURL HTTP API.

    Used to rate-limit before external calls; local-only paths (SteamID64, SteamID3,
    ``.../profiles/765...``) return False.
    """
    raw = (raw or "").strip()
    if not raw:
        return False
    if len(raw) == STEAMID64_LEN and raw.isdigit():
        return False
    if _STEAMID3_RE.match(raw):
        return False
    if "steamcommunity" in raw.lower() or "profiles/" in raw.lower():
        if _PROFILE_RE.search(raw):
            return False
        if _VANITY_URL_RE.search(raw):
            return True
        return False
    if _VANITY_NAME_RE.match(raw):
        return True
    return False


def resolve_to_steamid64(raw: str, api_key: str | None) -> tuple[str | None, str | None]:
    """
    Resolve arbitrary Steam user input to a 17-digit SteamID64.

    Supports: SteamID64, SteamID3 ([U:1:x]), profile URL, vanity URL, vanity name.
    Returns (steamid64, error_message). On success error_message is None; on failure steamid64 is None.
    API key is only used for vanity resolution; never logged or returned.
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
            return _resolve_vanity(vanity, api_key)
        return None, "Could not parse Steam profile URL. Use a URL like https://steamcommunity.com/profiles/76561197960265728 or https://steamcommunity.com/id/YourName"

    # 4) Standalone vanity name
    if _VANITY_NAME_RE.match(raw):
        return _resolve_vanity(raw, api_key)

    # 5) Might be a URL we didn't match (e.g. store.steampowered.com) or malformed
    if raw.startswith("http://") or raw.startswith("https://"):
        return None, "Unsupported Steam URL. Use a profile URL: https://steamcommunity.com/profiles/76561197960265728 or https://steamcommunity.com/id/YourName"
    if len(raw) == STEAMID64_LEN and not raw.isdigit():
        return None, "SteamID64 must be 17 digits."
    return None, "Could not recognize Steam ID. Use SteamID64 (17 digits), profile URL, or vanity name (requires Steam Web API key in server config)."


def _resolve_vanity(vanity: str, api_key: str | None) -> tuple[str | None, str | None]:
    """Call Steam Web API ResolveVanityURL. API key is never logged or returned."""
    if not api_key:
        return None, "Vanity URL/name resolution requires a Steam Web API key (set STEAM_WEB_API_KEY in server config)."
    vanity = vanity.strip()
    if not vanity:
        return None, "Vanity name is empty."
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
                return sid, None
            return None, "Invalid Steam ID in API response."
        if success == 42:
            return None, "No Steam account found for that vanity URL or name."
        msg = resp.get("message", "Unknown error")
        return None, f"Steam API: {msg}"
    except requests.Timeout:
        return None, "Steam API request timed out. Try again later."
    except requests.RequestException as e:
        # Do not expose internal details; do not log API key
        return None, "Could not reach Steam API. Try again later."
