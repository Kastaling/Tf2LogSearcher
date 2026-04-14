"""API routes for search endpoints and request logging."""
import asyncio
import json
import logging
import re
import sqlite3
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

from app.config import (
    AVATAR_DB_PATH,
    CHAT_DB_PATH,
    DOWNLOAD_RAW_ENABLED,
    DOWNLOADER_STATE_DIR,
    LOGS_DIR,
    RAW_EVENTS_DB_PATH,
    REQUEST_LOG_PATH,
    STATS_DB_PATH,
    STEAM_WEB_API_KEY,
)
from app.avatar_db import (
    connect_avatar_db,
    get_cached_avatar,
    get_cached_avatars_bulk,
    set_cached_avatar,
    set_cached_avatars_bulk,
)
from app.chat_db import chat_log_fingerprint, count_chat_messages
from app.raw_db import count_raw_library_rows
from app.stats_db import count_stats_index_rows, stats_db_fingerprint, stats_player_stats_cache_token
from app.rate_limit import rate_limit_exceeded
from app.request_log import append_request_log
from app.search.search import (
    LEADERBOARD_MIN_LOGS_DEFAULT,
    LEADERBOARD_MIN_LOGS_MAX,
    LEADERBOARD_TYPE_KEYS,
    PlayerNameIndexNotReadyError,
    _LOGMATCH_CLASS_TYPES,
    STATS_SEARCH_DEFAULT_CLASSES,
    chat_leaderboard_search_sqlite,
    chat_search,
    chat_search_sqlite,
    coplayers_search,
    log_match,
    player_name_search_sqlite,
    player_profile,
    stats_leaderboard,
    stats_search,
)
from app.search_cache import get as cache_get, set_ as cache_set
from app.steam_resolver import SteamVanityRateLimited, resolve_to_steamid64
from app.subscriptions import (
    LEADERBOARD_SUB_WORD_MIN_LEN,
    add_subscription,
    deactivate_by_token,
    is_valid_discord_webhook_url,
    send_welcome_message,
)

CHAT_SEARCH_MAX_WORD_LENGTH = 200
MAP_QUERY_MAX_LENGTH = 100
STEAMID64_LEN = 17
PLAYER_NAME_QUERY_MIN_LENGTH = 3
PLAYER_NAME_QUERY_MAX_LENGTH = 64
PLAYER_NAME_RESULT_LIMIT = 200


router = APIRouter()
logger = logging.getLogger(__name__)

_AVATAR_BATCH_MAX = 100


async def _fetch_steam_avatar_urls(steamid64s: list[str]) -> dict[str, str]:
    """
    Fetch avatar URLs from Steam GetPlayerSummaries (async).
    Returns {steamid64: avatarfull_url} for valid https URLs only.
    """
    if not STEAM_WEB_API_KEY or not steamid64s:
        return {}
    steamids_param = ",".join(steamid64s[:_AVATAR_BATCH_MAX])
    url = (
        "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
        f"?key={STEAM_WEB_API_KEY}&steamids={steamids_param}"
    )
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
        players = data.get("response", {}).get("players") or []
        out: dict[str, str] = {}
        for p in players:
            if not isinstance(p, dict):
                continue
            sid = str(p.get("steamid", "")).strip()
            if not re.fullmatch(r"\d{17}", sid):
                continue
            avatar = p.get("avatarfull", "")
            if not isinstance(avatar, str):
                continue
            avatar = avatar.strip()
            if avatar.startswith("https://"):
                out[sid] = avatar
        return out
    except Exception:
        logger.exception("Steam avatar fetch failed")
        return {}


def _fetch_steam_avatar_url_sync(steamid64: str) -> str | None:
    """
    Fetch a single player's ``avatarfull`` URL from Steam (sync).
    Same validation as ``_fetch_steam_avatar_urls``; for use from worker threads (e.g. profile).
    """
    if not STEAM_WEB_API_KEY or not re.fullmatch(r"\d{17}", steamid64):
        return None
    url = (
        "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
        f"?key={STEAM_WEB_API_KEY}&steamids={steamid64}"
    )
    try:
        with httpx.Client(timeout=5.0) as client:
            r = client.get(url)
            r.raise_for_status()
            data = r.json()
        players = data.get("response", {}).get("players") or []
        for p in players:
            if not isinstance(p, dict):
                continue
            sid = str(p.get("steamid", "")).strip()
            if sid != steamid64:
                continue
            avatar = p.get("avatarfull", "")
            if not isinstance(avatar, str):
                return None
            avatar = avatar.strip()
            if avatar.startswith("https://"):
                return avatar
        return None
    except Exception:
        logger.exception("Steam avatar sync fetch failed")
        return None


def _resolve_profile_avatar_url(steamid64: str) -> str | None:
    """
    Cached avatar from ``avatars.db`` if fresh, else Steam ``avatarfull`` (largest size) and cache it.
    Returns None if unavailable.
    """
    if not re.fullmatch(r"\d{17}", steamid64):
        return None
    conn = connect_avatar_db(AVATAR_DB_PATH)
    try:
        cached = get_cached_avatar(conn, steamid64)
        if cached and cached.startswith("https://"):
            return cached
        new_url = _fetch_steam_avatar_url_sync(steamid64)
        if new_url:
            try:
                set_cached_avatar(conn, steamid64, new_url)
            except Exception:
                pass
            return new_url
        return None
    finally:
        conn.close()


def _profile_response_payload(stats_payload: dict[str, Any], steamid64: str) -> dict[str, Any]:
    """Attach ``avatar_url`` for the profile player (not stored in search cache)."""
    out = dict(stats_payload)
    out["avatar_url"] = _resolve_profile_avatar_url(steamid64)
    return out


_PREFETCH_MAX = 20  # cap how many profiles to warm per trigger
_PREFETCH_GAMEMODE_DEFAULT = ""  # warm unfiltered profiles only


def _prefetch_profiles_background(steamid64s: list[str]) -> None:
    """
    Warm the in-process profile cache for a list of SteamID64s.
    Runs in a worker thread; does not block the HTTP response.
    Silently skips players whose profiles are already cached or whose DB data is absent.
    Caps at _PREFETCH_MAX players to bound the work per trigger.
    """
    candidates = [s for s in steamid64s if s and re.fullmatch(r"\d{17}", s)][: _PREFETCH_MAX]
    gm = _PREFETCH_GAMEMODE_DEFAULT
    for sid64 in candidates:
        ck = (sid64, gm, "", "", "")
        if cache_get("profile", ck) is not None:
            continue
        try:
            profile, _ = player_profile(sid64, gamemode=gm)
            token = stats_player_stats_cache_token(STATS_DB_PATH, sid64)
            cache_set("profile", ck, profile, token)
        except Exception:
            pass


def _client_ip(request: Request) -> str:
    """Prefer X-Forwarded-For when behind NPM/proxy."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return str(request.client.host)
    return ""


def _log_request(
    request: Request,
    endpoint: str,
    status_code: int,
    duration_ms: int,
    result_count: int | None = None,
    word: str = "",
    steamid: str = "",
    gamemode: str = "",
    classes: str = "",
    steamids: str = "",
) -> None:
    """Write one row to request log CSV."""
    try:
        append_request_log({
            "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "endpoint": endpoint,
            "method": request.method,
            "client_ip": _client_ip(request),
            "host": request.headers.get("host", ""),
            "user_agent": request.headers.get("user-agent", ""),
            "referer": request.headers.get("referer", ""),
            "word": word,
            "steamid": steamid,
            "gamemode": gamemode,
            "classes": classes,
            "steamids": steamids,
            "result_count": result_count if result_count is not None else "",
            "status_code": status_code,
            "duration_ms": duration_ms,
        })
    except Exception:
        pass  # Do not fail the request if logging fails


def _parse_iso_date_or_none(value: str) -> tuple[date | None, str | None]:
    """Parse YYYY-MM-DD date string; empty means None."""
    s = (value or "").strip()
    if not s:
        return None, None
    try:
        return date.fromisoformat(s), None
    except ValueError:
        return None, "Invalid date format. Use YYYY-MM-DD."


def _api_search_chat_impl(
    request: Request,
    word: str,
    steamid_input: str,
    date_from_raw: str,
    date_to_raw: str,
    map_query_raw: str,
) -> JSONResponse:
    """Shared implementation for POST and GET chat search. Returns JSONResponse."""
    start = time.perf_counter()
    steamid_input = (steamid_input or "").strip()
    word = (word or "").strip()
    is_leaderboard = steamid_input == ""
    if len(word) > CHAT_SEARCH_MAX_WORD_LENGTH:
        return JSONResponse(
            {"results": [], "total": 0, "error": "Search word is too long."},
            status_code=400,
        )
    if is_leaderboard and len(word) < 3:
        return JSONResponse(
            {"results": [], "total": 0, "error": "Search word must be at least 3 characters when Steam ID is empty."},
            status_code=400,
        )
    map_query = (map_query_raw or "").strip()
    if len(map_query) > MAP_QUERY_MAX_LENGTH:
        return JSONResponse(
            {"results": [], "total": 0, "error": "Map filter is too long."},
            status_code=400,
        )
    date_from, err = _parse_iso_date_or_none(date_from_raw)
    if err:
        return JSONResponse({"results": [], "total": 0, "error": err}, status_code=400)
    date_to, err = _parse_iso_date_or_none(date_to_raw)
    if err:
        return JSONResponse({"results": [], "total": 0, "error": err}, status_code=400)
    if date_from is not None and date_to is not None and date_from > date_to:
        return JSONResponse({"results": [], "total": 0, "error": "date_from must be before or equal to date_to."}, status_code=400)

    steamid64 = ""
    if not is_leaderboard:
        steamid64, resolve_error = resolve_to_steamid64(steamid_input, STEAM_WEB_API_KEY)
        if resolve_error is not None:
            return JSONResponse(
                {"results": [], "total": 0, "error": resolve_error},
                status_code=400,
            )
        assert steamid64 is not None

    if is_leaderboard:
        cache_key = (
            word.lower(),
            date_from.isoformat() if date_from else "",
            date_to.isoformat() if date_to else "",
            map_query.lower(),
        )
        cache_mode = "chatlb"
    else:
        cache_key = (
            steamid64,
            word,
            date_from.isoformat() if date_from else "",
            date_to.isoformat() if date_to else "",
            map_query.lower(),
        )
        cache_mode = "chat"
    cached = cache_get(cache_mode, cache_key)
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/chat", 200, duration_ms, result_count=cached.get("total", 0), word=word, steamid=steamid64)
        return JSONResponse(cached)

    status_code = 200
    result_count = 0
    try:
        if is_leaderboard:
            rows, result_count, logs_searched = chat_leaderboard_search_sqlite(
                word,
                CHAT_DB_PATH,
                date_from=date_from,
                date_to=date_to,
                map_query=map_query,
            )
            payload = {
                "leaderboard": True,
                "rows": rows,
                "total": result_count,
                "word": word,
                "logs_searched": logs_searched,
            }
            cache_set("chatlb", cache_key, payload, chat_log_fingerprint(CHAT_DB_PATH))
        else:
            try:
                results, result_count, searched_user_name, log_ids_used = chat_search_sqlite(
                    word,
                    steamid64,
                    CHAT_DB_PATH,
                    date_from=date_from,
                    date_to=date_to,
                    map_query=map_query,
                )
            except Exception as db_err:
                # Safety fallback: preserve legacy behavior if DB is unavailable/corrupt.
                logger.warning("SQLite chat search failed; falling back to JSON scan: %s", db_err)
                results, result_count, searched_user_name, log_ids_used = chat_search(
                    word,
                    steamid64,
                    LOGS_DIR,
                    date_from=date_from,
                    date_to=date_to,
                    map_query=map_query,
                )
            payload = {
                "leaderboard": False,
                "results": results,
                "total": result_count,
                "searched_user_name": searched_user_name,
                "resolved_steamid64": steamid64,
                "logs_searched": len(log_ids_used),
            }
            cache_set("chat", cache_key, payload, log_ids_used)
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/chat", status_code, duration_ms, result_count=result_count, word=word, steamid=steamid64)
        return JSONResponse(payload)
    except Exception as e:
        status_code = 500
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/chat", status_code, duration_ms, result_count=None, word=word, steamid=steamid64)
        return JSONResponse({"results": [], "total": 0, "error": str(e)}, status_code=500)


@router.post("/api/search/chat")
async def api_search_chat(
    request: Request,
    word: str = Form(""),
    steamid: str = Form(""),
    date_from: str = Form(""),
    date_to: str = Form(""),
    map_query: str = Form(""),
):
    """Chat search: Steam ID (any format) required; word optional."""
    return _api_search_chat_impl(
        request,
        (word or "").strip(),
        (steamid or "").strip(),
        date_from or "",
        date_to or "",
        map_query or "",
    )


@router.get("/api/search/chat")
async def api_search_chat_get(
    request: Request,
    word: str = Query(""),
    steamid: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    map_query: str = Query(""),
):
    """GET variant for shareable links; same response as POST."""
    return _api_search_chat_impl(
        request,
        (word or "").strip(),
        (steamid or "").strip(),
        date_from or "",
        date_to or "",
        map_query or "",
    )


def _api_search_stats_impl(
    request: Request,
    steamid: str,
    gamemode: str,
    classes: str,
    date_from_raw: str,
    date_to_raw: str,
    map_query_raw: str,
) -> JSONResponse:
    """Shared impl for POST/GET stats search."""
    start = time.perf_counter()
    steamid_input = (steamid or "").strip()
    if not steamid_input:
        return JSONResponse({"rows": [], "error": "Steam ID is required."}, status_code=400)
    steamid64, resolve_error = resolve_to_steamid64(steamid_input, STEAM_WEB_API_KEY)
    if resolve_error is not None:
        return JSONResponse({"rows": [], "error": resolve_error}, status_code=400)
    assert steamid64 is not None
    date_from, err = _parse_iso_date_or_none(date_from_raw)
    if err:
        return JSONResponse({"rows": [], "error": err}, status_code=400)
    date_to, err = _parse_iso_date_or_none(date_to_raw)
    if err:
        return JSONResponse({"rows": [], "error": err}, status_code=400)
    if date_from is not None and date_to is not None and date_from > date_to:
        return JSONResponse({"rows": [], "error": "date_from must be before or equal to date_to."}, status_code=400)
    map_query = (map_query_raw or "").strip()
    if len(map_query) > MAP_QUERY_MAX_LENGTH:
        return JSONResponse({"rows": [], "error": "Map filter is too long."}, status_code=400)
    class_list = [c.strip() for c in (classes or "").split(",") if c.strip()]
    if not class_list:
        class_list = list(STATS_SEARCH_DEFAULT_CLASSES)
    class_tuple = tuple(sorted(c.lower() for c in class_list))
    cache_key = (
        steamid64,
        gamemode,
        class_tuple,
        date_from.isoformat() if date_from else "",
        date_to.isoformat() if date_to else "",
        map_query.lower(),
    )
    cached = cache_get("stats", cache_key)
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/stats", 200, duration_ms, result_count=len(cached.get("rows", [])), steamid=steamid64, gamemode=gamemode, classes=classes)
        return JSONResponse(cached)
    try:
        rows, log_ids_used = stats_search(
            steamid64,
            gamemode,
            class_list,
            LOGS_DIR,
            date_from=date_from,
            date_to=date_to,
            map_query=map_query,
        )
        payload = {"rows": rows}
        cache_set("stats", cache_key, payload, stats_player_stats_cache_token(STATS_DB_PATH, steamid64))
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/stats", 200, duration_ms, result_count=len(rows), steamid=steamid64, gamemode=gamemode, classes=classes)
        return JSONResponse(payload)
    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/stats", 500, duration_ms, steamid=steamid64, gamemode=gamemode, classes=classes)
        return JSONResponse({"rows": [], "error": str(e)}, status_code=500)


def _api_search_coplayers_impl(
    request: Request,
    steamid: str,
    gamemode: str,
    map_query_raw: str,
) -> JSONResponse:
    """Shared impl for POST/GET frequent co-players search."""
    start = time.perf_counter()
    steamid_input = (steamid or "").strip()
    if not steamid_input:
        return JSONResponse({"rows": [], "error": "Steam ID is required."}, status_code=400)
    steamid64, resolve_error = resolve_to_steamid64(steamid_input, STEAM_WEB_API_KEY)
    if resolve_error is not None:
        return JSONResponse({"rows": [], "error": resolve_error}, status_code=400)
    assert steamid64 is not None
    map_query = (map_query_raw or "").strip()
    if len(map_query) > MAP_QUERY_MAX_LENGTH:
        return JSONResponse({"rows": [], "error": "Map filter is too long."}, status_code=400)
    gm = (gamemode or "").strip()
    if gm not in ("", "hl", "7s", "6s", "ud"):
        gm = ""
    cache_key = (steamid64, gm, map_query.lower())
    cached = cache_get("coplayers", cache_key)
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/search/coplayers",
            200,
            duration_ms,
            result_count=len(cached.get("rows", [])),
            steamid=steamid64,
            gamemode=gm,
        )
        body = dict(cached)
        body["resolved_steamid64"] = steamid64
        return JSONResponse(body)
    try:
        rows, log_ids_used = coplayers_search(steamid64, LOGS_DIR, gamemode=gm, map_query=map_query)
        payload = {"rows": rows, "logs_searched": len(log_ids_used), "resolved_steamid64": steamid64}
        cache_set("coplayers", cache_key, payload, stats_player_stats_cache_token(STATS_DB_PATH, steamid64))
        _sids_to_warm = [r["steamid64"] for r in rows if r.get("steamid64")]
        if _sids_to_warm:
            import threading

            threading.Thread(
                target=_prefetch_profiles_background,
                args=(_sids_to_warm,),
                daemon=True,
                name="profile-prefetch-coplayers",
            ).start()
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/search/coplayers",
            200,
            duration_ms,
            result_count=len(rows),
            steamid=steamid64,
            gamemode=gm,
        )
        return JSONResponse(payload)
    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/coplayers", 500, duration_ms, steamid=steamid64, gamemode=gm)
        return JSONResponse({"rows": [], "error": str(e)}, status_code=500)


@router.post("/api/search/coplayers")
async def api_search_coplayers(
    request: Request,
    steamid: str = Form(""),
    gamemode: str = Form(""),
    map_query: str = Form(""),
):
    return _api_search_coplayers_impl(request, steamid or "", gamemode or "", map_query or "")


@router.get("/api/search/coplayers")
async def api_search_coplayers_get(
    request: Request,
    steamid: str = Query(""),
    gamemode: str = Query(""),
    map_query: str = Query(""),
):
    return _api_search_coplayers_impl(request, steamid or "", gamemode or "", map_query or "")


def _api_search_player_name_impl(request: Request, q_raw: str) -> JSONResponse:
    """Substring search on chat aliases; returns SteamID64 + counts per account."""
    start = time.perf_counter()
    q = (q_raw or "").strip()
    if len(q) < PLAYER_NAME_QUERY_MIN_LENGTH:
        return JSONResponse(
            {
                "rows": [],
                "error": f"Query must be at least {PLAYER_NAME_QUERY_MIN_LENGTH} characters.",
            },
            status_code=400,
        )
    if len(q) > PLAYER_NAME_QUERY_MAX_LENGTH:
        return JSONResponse({"rows": [], "error": "Query is too long."}, status_code=400)
    if any(ord(c) < 32 for c in q):
        return JSONResponse({"rows": [], "error": "Invalid query."}, status_code=400)

    cache_key = (q.lower(),)
    cached = cache_get("playername", cache_key)
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/search/player-name",
            200,
            duration_ms,
            result_count=len(cached.get("rows", [])),
            word=q[:CHAT_SEARCH_MAX_WORD_LENGTH],
        )
        return JSONResponse(cached)

    if not CHAT_DB_PATH.is_file():
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/search/player-name",
            200,
            duration_ms,
            result_count=0,
            word=q[:CHAT_SEARCH_MAX_WORD_LENGTH],
        )
        return JSONResponse({
            "rows": [],
            "limit": PLAYER_NAME_RESULT_LIMIT,
            "note": "Chat database not available.",
        })

    try:
        rows = player_name_search_sqlite(q, CHAT_DB_PATH, limit=PLAYER_NAME_RESULT_LIMIT)
        payload: dict[str, Any] = {"rows": rows, "limit": PLAYER_NAME_RESULT_LIMIT}
        cache_set("playername", cache_key, payload, chat_log_fingerprint(CHAT_DB_PATH))
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/search/player-name",
            200,
            duration_ms,
            result_count=len(rows),
            word=q[:CHAT_SEARCH_MAX_WORD_LENGTH],
        )
        return JSONResponse(payload)
    except PlayerNameIndexNotReadyError as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/search/player-name",
            503,
            duration_ms,
            word=q[:CHAT_SEARCH_MAX_WORD_LENGTH],
        )
        return JSONResponse(
            {
                "rows": [],
                "limit": PLAYER_NAME_RESULT_LIMIT,
                "error": str(e),
                "index_status": "building",
            },
            status_code=503,
        )
    except sqlite3.OperationalError as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        err = str(e).lower()
        if "locked" in err or "busy" in err:
            _log_request(
                request,
                "/api/search/player-name",
                503,
                duration_ms,
                word=q[:CHAT_SEARCH_MAX_WORD_LENGTH],
            )
            return JSONResponse(
                {
                    "rows": [],
                    "limit": PLAYER_NAME_RESULT_LIMIT,
                    "error": "Chat database is busy. Try again in a few seconds.",
                },
                status_code=503,
            )
        _log_request(
            request,
            "/api/search/player-name",
            500,
            duration_ms,
            word=q[:CHAT_SEARCH_MAX_WORD_LENGTH],
        )
        return JSONResponse({"rows": [], "error": str(e)}, status_code=500)
    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/search/player-name",
            500,
            duration_ms,
            word=q[:CHAT_SEARCH_MAX_WORD_LENGTH],
        )
        return JSONResponse({"rows": [], "error": str(e)}, status_code=500)


@router.post("/api/search/player-name")
async def api_search_player_name(
    request: Request,
    q: str = Form(""),
):
    return await asyncio.to_thread(_api_search_player_name_impl, request, q or "")


@router.get("/api/search/player-name")
async def api_search_player_name_get(
    request: Request,
    q: str = Query(""),
):
    return await asyncio.to_thread(_api_search_player_name_impl, request, q or "")


@router.get("/api/avatar/{steamid64}")
async def api_avatar(steamid64: str):
    """Return cached or freshly fetched Steam avatar URL (not logged to request CSV)."""
    if not re.fullmatch(r"\d{17}", steamid64):
        return JSONResponse({"error": "steamid64 must be exactly 17 digits"}, status_code=400)
    conn = connect_avatar_db(AVATAR_DB_PATH)
    try:
        cached = get_cached_avatar(conn, steamid64)
        if cached:
            return JSONResponse({"url": cached})
        if not STEAM_WEB_API_KEY:
            return JSONResponse({"url": None, "error": "Steam API key not configured"})
        fetched = await _fetch_steam_avatar_urls([steamid64])
        new_url = fetched.get(steamid64)
        if new_url:
            set_cached_avatar(conn, steamid64, new_url)
        return JSONResponse({"url": new_url})
    finally:
        conn.close()


@router.get("/api/avatars/batch")
async def api_avatars_batch(steamids: str = Query("")):
    """Batch avatar URLs (cache + Steam); not logged to request CSV."""
    raw_parts = [p.strip() for p in (steamids or "").split(",") if p.strip()]
    seen: set[str] = set()
    valid: list[str] = []
    for p in raw_parts:
        if not re.fullmatch(r"\d{17}", p):
            continue
        if p in seen:
            continue
        seen.add(p)
        valid.append(p)
        if len(valid) >= _AVATAR_BATCH_MAX:
            break

    if not valid:
        return JSONResponse({"avatars": {}})

    conn = connect_avatar_db(AVATAR_DB_PATH)
    try:
        try:
            cached = get_cached_avatars_bulk(conn, valid)
        except Exception:
            cached = {}
        missing = [s for s in valid if s not in cached]

        new_from_steam: dict[str, str] = {}
        if missing and STEAM_WEB_API_KEY:
            new_from_steam = await _fetch_steam_avatar_urls(missing)
            if new_from_steam:
                try:
                    set_cached_avatars_bulk(conn, new_from_steam)
                except Exception:
                    pass

        avatars: dict[str, str | None] = {}
        for sid in valid:
            if sid in cached:
                avatars[sid] = cached[sid]
            elif sid in new_from_steam:
                avatars[sid] = new_from_steam[sid]
            else:
                avatars[sid] = None
        return JSONResponse({"avatars": avatars})
    finally:
        conn.close()


@router.post("/api/search/stats")
async def api_search_stats(
    request: Request,
    steamid: str = Form(""),
    gamemode: str = Form("hl"),
    classes: str = Form(""),
    date_from: str = Form(""),
    date_to: str = Form(""),
    map_query: str = Form(""),
):
    return _api_search_stats_impl(
        request,
        steamid or "",
        gamemode or "hl",
        classes or "",
        date_from or "",
        date_to or "",
        map_query or "",
    )


@router.get("/api/search/stats")
async def api_search_stats_get(
    request: Request,
    steamid: str = Query(""),
    gamemode: str = Query("hl"),
    classes: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    map_query: str = Query(""),
):
    return _api_search_stats_impl(
        request,
        steamid or "",
        gamemode or "hl",
        classes or "",
        date_from or "",
        date_to or "",
        map_query or "",
    )


def _api_search_logmatch_impl(request: Request, steamids: str, map_query_raw: str) -> JSONResponse:
    """Shared impl for POST/GET logmatch search."""
    start = time.perf_counter()
    raw_list = [s.strip() for s in (steamids or "").replace(",", " ").split() if s.strip()]
    map_query = (map_query_raw or "").strip()
    if len(map_query) > MAP_QUERY_MAX_LENGTH:
        return JSONResponse({"results": [], "total": 0, "error": "Map filter is too long."}, status_code=400)
    if not raw_list:
        return JSONResponse({"results": [], "total": 0, "error": "At least one Steam ID is required."}, status_code=400)
    sid_list: list[str] = []
    for i, raw in enumerate(raw_list):
        steamid64, resolve_error = resolve_to_steamid64(raw, STEAM_WEB_API_KEY)
        if resolve_error is not None:
            return JSONResponse(
                {"results": [], "total": 0, "error": f"Could not resolve Steam ID {i + 1}: {resolve_error}"},
                status_code=400,
            )
        assert steamid64 is not None
        sid_list.append(steamid64)
    sid_tuple = tuple(sorted(sid_list))
    cached = cache_get("logmatch", (sid_tuple, map_query.lower()))
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/logmatch", 200, duration_ms, result_count=cached.get("total", 0), steamids=",".join(sid_list))
        return JSONResponse(cached)

    status_code = 200
    result_count = 0
    try:
        results, result_count, matching_log_ids, head_to_head = log_match(
            sid_list, LOGS_DIR, search_inputs=raw_list, map_query=map_query
        )
        payload = {"results": results, "total": result_count, "head_to_head": head_to_head}
        cache_set("logmatch", (sid_tuple, map_query.lower()), payload, matching_log_ids)
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/logmatch", status_code, duration_ms, result_count=result_count, steamids=",".join(sid_list))
        return JSONResponse(payload)
    except Exception as e:
        status_code = 500
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/logmatch", status_code, duration_ms, steamids=",".join(sid_list))
        return JSONResponse({"results": [], "total": 0, "error": str(e)}, status_code=500)


@router.post("/api/search/logmatch")
async def api_search_logmatch(request: Request, steamids: str = Form(""), map_query: str = Form("")):
    return await asyncio.to_thread(_api_search_logmatch_impl, request, steamids or "", map_query or "")


@router.get("/api/search/logmatch")
async def api_search_logmatch_get(request: Request, steamids: str = Query(""), map_query: str = Query("")):
    return await asyncio.to_thread(_api_search_logmatch_impl, request, steamids or "", map_query or "")


@router.post("/api/chat-subscriptions")
async def api_add_chat_subscription(
    request: Request,
    webhook_url: str = Form(""),
    steamid: str = Form(""),
    word: str = Form(""),
):
    """
    Subscribe a Discord webhook to chat search alerts for (steamid, word), or for a global
    leaderboard word alert when steamid is empty (same rules as leaderboard search: word length).
    """
    webhook_url = (webhook_url or "").strip()
    steamid_input = (steamid or "").strip()
    word = (word or "").strip()
    if not word:
        return JSONResponse({"ok": False, "error": "A search word is required (not full chat history)."}, status_code=400)
    if len(word) > CHAT_SEARCH_MAX_WORD_LENGTH:
        return JSONResponse({"ok": False, "error": "Search word is too long."}, status_code=400)
    if not is_valid_discord_webhook_url(webhook_url):
        return JSONResponse(
            {"ok": False, "error": "Invalid Discord webhook URL. Use a URL like https://discord.com/api/webhooks/123.../abc..."},
            status_code=400,
        )
    if not steamid_input:
        if len(word) < LEADERBOARD_SUB_WORD_MIN_LEN:
            return JSONResponse(
                {
                    "ok": False,
                    "error": (
                        f"When Steam ID is empty, the word must be at least {LEADERBOARD_SUB_WORD_MIN_LEN} "
                        "characters (same as chat leaderboard search)."
                    ),
                },
                status_code=400,
            )
        steamid64 = ""
    else:
        resolved, resolve_error = resolve_to_steamid64(steamid_input, STEAM_WEB_API_KEY)
        if resolve_error is not None:
            return JSONResponse({"ok": False, "error": resolve_error}, status_code=400)
        assert resolved is not None
        steamid64 = resolved
    state_dir = DOWNLOADER_STATE_DIR.resolve()
    ok, err, deactivate_token = add_subscription(state_dir, webhook_url, steamid64, word)
    if not ok:
        return JSONResponse({"ok": False, "error": err or "Failed to save subscription."}, status_code=400)
    if deactivate_token:
        base = str(request.base_url).rstrip("/")
        deactivate_url = f"{base}/api/chat-subscriptions/deactivate?token={deactivate_token}"
        send_welcome_message(webhook_url, word, steamid64, deactivate_url)
    return JSONResponse({"ok": True})


@router.get("/api/chat-subscriptions/deactivate")
async def api_deactivate_chat_subscription(request: Request, token: str = Query("")):
    """Deactivate a subscription via its secret token (link from the welcome Discord message)."""
    state_dir = DOWNLOADER_STATE_DIR.resolve()
    if deactivate_by_token(state_dir, token):
        return HTMLResponse(
            "<!DOCTYPE html><html><head><meta charset='utf-8'><title>Webhook deactivated</title></head>"
            "<body><p>Webhook deactivated. You can close this tab.</p></body></html>",
            status_code=200,
        )
    return HTMLResponse(
        "<!DOCTYPE html><html><head><meta charset='utf-8'><title>Not found</title></head>"
        "<body><p>Invalid or already used deactivation link.</p></body></html>",
        status_code=404,
    )


def _progress_json_path() -> Path | None:
    """Return path to progress.json only if it is exactly state_dir/progress.json (path traversal safety)."""
    state_dir = DOWNLOADER_STATE_DIR.resolve()
    progress_path = (state_dir / "progress.json").resolve()
    if progress_path.parent != state_dir or progress_path.name != "progress.json":
        return None
    return progress_path


@router.get("/api/download-progress")
async def api_download_progress(request: Request):
    """
    Return downloader progress JSON for the UI. Read-only; no user input.
    Returns 404 if progress file is missing or path is invalid.
    """
    progress_path = _progress_json_path()
    if progress_path is None or not progress_path.is_file():
        return JSONResponse({"error": "Progress not available"}, status_code=404)
    try:
        raw = progress_path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, ValueError) as _:
        return JSONResponse({"error": "Progress not available"}, status_code=404)
    # Return only the known payload keys (allowlist) so we never leak internal fields
    allowed = {
        "min_id", "max_id", "total_files",
        "remaining", "eta_human", "rate_logs_per_sec", "rate_logs_per_sec_aggregated",
        "backfill_complete", "updated_at",
        "earliest_log_timestamp", "logs_downloaded_since_last_update",
        "download_json_enabled", "download_raw_enabled",
        "logs_json_this_update", "logs_raw_this_update",
        "logs_json_failed_this_update",
        "raw_failed_zip_this_update", "raw_failed_save_this_update",
        "raw_failed_extract_this_update", "raw_failed_index_this_update",
    }
    out = {k: data[k] for k in allowed if k in data}
    return JSONResponse(out)


# Heavy COUNT(*) on chat_messages — cache so /api/download-progress stays fast (reads progress.json only).
_CHAT_MESSAGE_COUNT_CACHE_TTL_SEC = 300.0
_chat_message_count_cache: dict[str, Any] = {"n": None, "ts": 0.0}


@router.get("/api/chat-message-count")
async def api_chat_message_count():
    """Total rows in chat_messages; cached ~5 min. Not logged to request CSV."""
    now = time.time()
    ts = float(_chat_message_count_cache["ts"])
    if ts > 0 and (now - ts) < _CHAT_MESSAGE_COUNT_CACHE_TTL_SEC:
        return JSONResponse({"chat_message_count": _chat_message_count_cache["n"]})
    n = await asyncio.to_thread(count_chat_messages, CHAT_DB_PATH)
    _chat_message_count_cache["n"] = n
    _chat_message_count_cache["ts"] = now
    return JSONResponse({"chat_message_count": n})


# Cheap aggregates on raw_events.db — short TTL so polling does not hammer SQLite.
_RAW_EVENTS_STATS_CACHE_TTL_SEC = 120.0
_raw_events_stats_cache: dict[str, Any] = {"payload": None, "ts": 0.0}


@router.get("/api/raw-events-stats")
async def api_raw_events_stats():
    """raw_logs COUNT + SUM(kill_count); cached ~2 min. Omitted when DOWNLOAD_RAW_ENABLED is off."""
    if not DOWNLOAD_RAW_ENABLED:
        return JSONResponse(
            {
                "download_raw_enabled": False,
                "raw_logs_count": None,
                "kill_events_total": None,
            }
        )
    now = time.time()
    ts = float(_raw_events_stats_cache["ts"])
    if ts > 0 and (now - ts) < _RAW_EVENTS_STATS_CACHE_TTL_SEC:
        return JSONResponse(_raw_events_stats_cache["payload"])
    cnt, kill_sum = await asyncio.to_thread(count_raw_library_rows, RAW_EVENTS_DB_PATH)
    payload = {
        "download_raw_enabled": True,
        "raw_logs_count": cnt,
        "kill_events_total": kill_sum,
    }
    _raw_events_stats_cache["payload"] = payload
    _raw_events_stats_cache["ts"] = now
    return JSONResponse(payload)


_STATS_INDEX_COUNTS_CACHE_TTL_SEC = 120.0
_stats_index_counts_cache: dict[str, Any] = {"payload": None, "ts": 0.0}


@router.get("/api/stats-index-counts")
async def api_stats_index_counts():
    """log_players and player_stats_agg row counts; cached ~2 min."""
    now = time.time()
    ts = float(_stats_index_counts_cache["ts"])
    if ts > 0 and (now - ts) < _STATS_INDEX_COUNTS_CACHE_TTL_SEC:
        return JSONResponse(_stats_index_counts_cache["payload"])
    lp, agg = await asyncio.to_thread(count_stats_index_rows, STATS_DB_PATH)
    payload = {
        "log_players_count": lp,
        "leaderboard_players_count": agg,
    }
    _stats_index_counts_cache["payload"] = payload
    _stats_index_counts_cache["ts"] = now
    return JSONResponse(payload)


def _static_path(name: str) -> Path:
    """Path to a file under static/ (relative to app root)."""
    return Path(__file__).resolve().parent.parent / "static" / name


@router.get("/favicon.ico")
async def favicon():
    """Serve the site favicon."""
    path = _static_path("favicon.ico")
    if not path.is_file():
        return JSONResponse({"error": "Not found"}, status_code=404)
    return FileResponse(path, media_type="image/x-icon")


def _api_profile_impl(
    request: Request,
    steamid: str,
    gamemode: str,
    date_from_raw: str,
    date_to_raw: str,
    map_query_raw: str,
) -> JSONResponse:
    """Shared impl for POST/GET player profile."""
    start = time.perf_counter()
    steamid_input = (steamid or "").strip()
    if not steamid_input:
        return JSONResponse({"error": "Steam ID is required."}, status_code=400)
    try:
        steamid64, resolve_error = resolve_to_steamid64(
            steamid_input,
            STEAM_WEB_API_KEY,
            vanity_rl_client_ip=_client_ip(request),
        )
    except SteamVanityRateLimited as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/player/profile",
            429,
            duration_ms,
            steamid=steamid_input,
        )
        return JSONResponse(
            {
                "error": "Too many Steam vanity lookups. Please try again in a moment.",
                "retry_after": e.retry_after,
            },
            status_code=429,
            headers={"Retry-After": str(e.retry_after)},
        )
    if resolve_error is not None:
        return JSONResponse({"error": resolve_error}, status_code=400)
    assert steamid64 is not None
    date_from, err = _parse_iso_date_or_none(date_from_raw)
    if err:
        return JSONResponse({"error": err}, status_code=400)
    date_to, err = _parse_iso_date_or_none(date_to_raw)
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if date_from is not None and date_to is not None and date_from > date_to:
        return JSONResponse({"error": "date_from must be before or equal to date_to."}, status_code=400)
    map_query = (map_query_raw or "").strip()
    if len(map_query) > MAP_QUERY_MAX_LENGTH:
        return JSONResponse({"error": "Map filter is too long."}, status_code=400)
    gm = (gamemode or "").strip()
    if gm not in ("", "hl", "7s", "6s", "ud"):
        gm = ""
    cache_key = (
        steamid64,
        gm,
        date_from.isoformat() if date_from else "",
        date_to.isoformat() if date_to else "",
        map_query.lower(),
    )
    cached = cache_get("profile", cache_key)
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/player/profile", 200, duration_ms, steamid=steamid64, gamemode=gm)
        return JSONResponse(
            _profile_response_payload(cached, steamid64),
            headers={"Cache-Control": "private, max-age=300"},
        )
    rl = rate_limit_exceeded(kind="profile", client_ip=_client_ip(request))
    if rl is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/player/profile",
            429,
            duration_ms,
            steamid=steamid64,
            gamemode=gm,
        )
        return rl
    try:
        profile, log_ids = player_profile(
            steamid64,
            gamemode=gm,
            date_from=date_from,
            date_to=date_to,
            map_query=map_query,
        )
        cache_set("profile", cache_key, profile, stats_player_stats_cache_token(STATS_DB_PATH, steamid64))
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/player/profile", 200, duration_ms, steamid=steamid64, gamemode=gm)
        return JSONResponse(
            _profile_response_payload(profile, steamid64),
            headers={"Cache-Control": "private, max-age=300"},
        )
    except RuntimeError:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/player/profile", 404, duration_ms, steamid=steamid64, gamemode=gm)
        return JSONResponse(
            {"error": "Stats DB not available for this player.", "steamid64": steamid64},
            status_code=404,
        )
    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/player/profile", 500, duration_ms, steamid=steamid64, gamemode=gm)
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/player/profile")
async def api_player_profile(
    request: Request,
    steamid: str = Form(""),
    gamemode: str = Form(""),
    date_from: str = Form(""),
    date_to: str = Form(""),
    map_query: str = Form(""),
):
    return await asyncio.to_thread(
        _api_profile_impl,
        request,
        steamid or "",
        gamemode or "",
        date_from or "",
        date_to or "",
        map_query or "",
    )


@router.get("/api/player/profile")
async def api_player_profile_get(
    request: Request,
    steamid: str = Query(""),
    gamemode: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    map_query: str = Query(""),
):
    return await asyncio.to_thread(
        _api_profile_impl,
        request,
        steamid or "",
        gamemode or "",
        date_from or "",
        date_to or "",
        map_query or "",
    )


def _api_leaderboard_impl(
    request: Request,
    lb_type: str,
    gamemode: str,
    class_filter: str,
    date_from_raw: str,
    date_to_raw: str,
    map_query_raw: str,
    min_logs_raw: str,
) -> JSONResponse:
    start = time.perf_counter()
    lt = (lb_type or "").strip().lower()
    if lt not in LEADERBOARD_TYPE_KEYS:
        return JSONResponse({"error": "Invalid leaderboard type."}, status_code=400)
    date_from, err = _parse_iso_date_or_none(date_from_raw)
    if err:
        return JSONResponse({"error": err}, status_code=400)
    date_to, err = _parse_iso_date_or_none(date_to_raw)
    if err:
        return JSONResponse({"error": err}, status_code=400)
    if date_from is not None and date_to is not None and date_from > date_to:
        return JSONResponse({"error": "date_from must be before or equal to date_to."}, status_code=400)
    map_query = (map_query_raw or "").strip()
    if len(map_query) > MAP_QUERY_MAX_LENGTH:
        return JSONResponse({"error": "Map filter is too long."}, status_code=400)
    gm = (gamemode or "").strip()
    if gm not in ("", "hl", "7s", "6s", "ud"):
        gm = ""
    cf = (class_filter or "").strip().lower()
    if cf and cf not in _LOGMATCH_CLASS_TYPES:
        return JSONResponse({"error": "Invalid class filter."}, status_code=400)
    try:
        ml = int((min_logs_raw or "").strip() or str(LEADERBOARD_MIN_LOGS_DEFAULT))
    except ValueError:
        ml = LEADERBOARD_MIN_LOGS_DEFAULT
    ml = max(1, min(ml, LEADERBOARD_MIN_LOGS_MAX))
    cache_key = (
        lt,
        gm,
        cf,
        date_from.isoformat() if date_from else "",
        date_to.isoformat() if date_to else "",
        map_query.lower(),
        ml,
    )
    cached = cache_get("leaderboard", cache_key)
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/leaderboard",
            200,
            duration_ms,
            result_count=len(cached.get("rows") or []),
            classes=f"lb:{lt}",
        )
        return JSONResponse(cached)
    rl = rate_limit_exceeded(kind="leaderboard", client_ip=_client_ip(request))
    if rl is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/leaderboard",
            429,
            duration_ms,
            classes=f"lb:{lt}",
        )
        return rl
    try:
        rows, total_logs = stats_leaderboard(
            lt,
            gamemode=gm,
            class_filter=cf,
            date_from=date_from,
            date_to=date_to,
            map_query=map_query,
            min_logs=ml,
        )
        payload = {"rows": rows, "total_logs": total_logs, "lb_type": lt}
        cache_set("leaderboard", cache_key, payload, stats_db_fingerprint(STATS_DB_PATH))
        _lb_sids = [r["steamid64"] for r in rows if r.get("steamid64")]
        if _lb_sids:
            import threading

            threading.Thread(
                target=_prefetch_profiles_background,
                args=(_lb_sids,),
                daemon=True,
                name="profile-prefetch-leaderboard",
            ).start()
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(
            request,
            "/api/leaderboard",
            200,
            duration_ms,
            result_count=len(rows),
            classes=f"lb:{lt}",
        )
        return JSONResponse(payload)
    except RuntimeError as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/leaderboard", 503, duration_ms, classes=f"lb:{lt}")
        return JSONResponse({"error": str(e)}, status_code=503)
    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/leaderboard", 500, duration_ms, classes=f"lb:{lt}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/leaderboard")
async def api_leaderboard_get(
    request: Request,
    lb_type: str = Query("dpm"),
    gamemode: str = Query(""),
    class_filter: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    map_query: str = Query(""),
    min_logs: str = Query(""),
):
    return await asyncio.to_thread(
        _api_leaderboard_impl,
        request,
        lb_type or "",
        gamemode or "",
        class_filter or "",
        date_from or "",
        date_to or "",
        map_query or "",
        min_logs or "",
    )


@router.get("/")
async def index(request: Request):
    """Serve the main search page (HTML)."""
    return await _serve_index(request, "/")


@router.get("/results")
async def results_page(request: Request):
    """
    Serve results page HTML with social meta tags.

    Discord/Twitter/Slack do not execute JS; embeds require server-rendered meta tags.
    """
    return await _serve_results_with_embed(request)


def _escape_meta(s: str) -> str:
    """Escape for HTML attribute meta content."""
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _truncate(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else (s[: n - 1] + "…")


def _steam_profile_image_url(steamid64: str) -> str | None:
    """
    Public Steam avatar URL for Open Graph / Discord link previews.
    Only accepts a validated 17-digit SteamID64; never interpolates untrusted strings.
    """
    s = (steamid64 or "").strip()
    if len(s) != STEAMID64_LEN or not s.isdigit():
        return None
    return f"https://avatars.steamstatic.com/{s}.jpg"


def _build_results_embed_meta(request: Request) -> str:
    """
    Build OpenGraph/Twitter meta tags for /results based on query params + cached payloads.

    Never raises; returns empty string on any error.
    """
    try:
        qp = dict(request.query_params)
        mode = (qp.get("mode") or "").strip()
        # Canonical full URL for embed
        base = str(request.base_url).rstrip("/")
        full_url = f"{base}{request.url.path}"
        if request.url.query:
            full_url += f"?{request.url.query}"

        title = "TF2 Log Searcher"
        desc = "TF2 logs.tf search results."
        og_image_url: str | None = None

        # Try to pull cached payloads to include counts without doing heavy work.
        if mode == "chat":
            steamid_in = (qp.get("steamid") or "").strip()
            word = (qp.get("word") or "").strip()
            date_from = (qp.get("date_from") or "").strip()
            date_to = (qp.get("date_to") or "").strip()
            map_query = (qp.get("map_query") or "").strip()
            if not steamid_in:
                # leaderboard
                title = f'Chat leaderboard: "{word}"'
                ck = (word.lower(), date_from, date_to, map_query.lower())
                cached = cache_get("chatlb", ck) or {}
                logs_s = cached.get("logs_searched")
                n_rows = len(cached.get("rows") or [])
                desc = f'Top {n_rows} player(s) for "{word}".'
                if isinstance(logs_s, (int, float)) and logs_s:
                    desc = f'Top {n_rows} player(s) for "{word}" across {int(logs_s)} log(s).'
            else:
                title = "Chat search results"
                # Best effort resolve for cache lookup; if it fails, still show generic embed.
                steamid64, err = resolve_to_steamid64(steamid_in, STEAM_WEB_API_KEY)
                if err is None and steamid64:
                    og_image_url = _steam_profile_image_url(steamid64)
                    ck = (steamid64, word, date_from, date_to, map_query.lower())
                    cached = cache_get("chat", ck) or {}
                    total = cached.get("total")
                    logs_s = cached.get("logs_searched")
                    name = cached.get("searched_user_name") or steamid64
                    if isinstance(total, (int, float)):
                        if word:
                            desc = f'"{word}" said {int(total)} time(s) by {name}.'
                        else:
                            desc = f"Chat history for {name}."
                        if isinstance(logs_s, (int, float)) and logs_s:
                            desc += f" Across {int(logs_s)} log(s)."
                else:
                    desc = "Chat search results."
        elif mode == "stats":
            steamid_in = (qp.get("steamid") or "").strip()
            gamemode = (qp.get("gamemode") or "").strip()
            classes = (qp.get("classes") or "").strip()
            date_from = (qp.get("date_from") or "").strip()
            date_to = (qp.get("date_to") or "").strip()
            map_query = (qp.get("map_query") or "").strip()
            title = "Stats Sorter results"
            if steamid_in:
                steamid64, err = resolve_to_steamid64(steamid_in, STEAM_WEB_API_KEY)
                if err is None and steamid64:
                    og_image_url = _steam_profile_image_url(steamid64)
                    class_parts = [c.strip() for c in classes.split(",") if c.strip()]
                    if not class_parts:
                        class_tuple = tuple(c.lower() for c in STATS_SEARCH_DEFAULT_CLASSES)
                    else:
                        class_tuple = tuple(sorted(c.lower() for c in class_parts))
                    ck = (steamid64, gamemode, class_tuple, date_from, date_to, map_query.lower())
                    cached = cache_get("stats", ck) or {}
                    n = len(cached.get("rows") or [])
                    desc = f"Found {n} row(s) of stats."
            # Add filter summary (safe and short)
            bits = []
            if gamemode:
                bits.append(f"mode {gamemode}")
            if classes:
                bits.append(f"classes {classes}")
            if map_query:
                bits.append(f'map "{map_query}"')
            if date_from or date_to:
                bits.append(f"{date_from or '…'} to {date_to or '…'}")
            if bits:
                desc = f"{desc} ({', '.join(bits)})."
        elif mode == "coplayers":
            steamid_in = (qp.get("steamid") or "").strip()
            gamemode = (qp.get("gamemode") or "").strip()
            map_query = (qp.get("map_query") or "").strip()
            title = "Frequent co-players"
            if steamid_in:
                steamid64, err = resolve_to_steamid64(steamid_in, STEAM_WEB_API_KEY)
                if err is None and steamid64:
                    og_image_url = _steam_profile_image_url(steamid64)
                    gm = gamemode if gamemode in ("", "hl", "7s", "6s", "ud") else ""
                    ck = (steamid64, gm, map_query.lower())
                    cached = cache_get("coplayers", ck) or {}
                    n = len(cached.get("rows") or [])
                    logs_s = cached.get("logs_searched")
                    desc = f"Found {n} co-player(s)."
                    if isinstance(logs_s, (int, float)) and logs_s:
                        desc += f" Across {int(logs_s)} log(s)."
        elif mode == "playername":
            q = (qp.get("q") or "").strip()
            title = "Player name search"
            if len(q) >= PLAYER_NAME_QUERY_MIN_LENGTH and len(q) <= PLAYER_NAME_QUERY_MAX_LENGTH:
                ck = (q.lower(),)
                cached = cache_get("playername", ck) or {}
                n = len(cached.get("rows") or [])
                title = f'Players named like "{_truncate(q, 40)}"'
                desc = f"Found {n} matching account(s) in chat history."
        elif mode == "logmatch":
            steamids = (qp.get("steamids") or "").strip()
            map_query = (qp.get("map_query") or "").strip()
            title = "Multi-party log search"
            if steamids:
                raw_list = [s.strip() for s in steamids.replace(",", " ").split() if s.strip()]
                sid_list: list[str] = []
                for raw in raw_list[:20]:  # hard cap for embed work
                    sid64, err = resolve_to_steamid64(raw, STEAM_WEB_API_KEY)
                    if err is None and sid64:
                        sid_list.append(sid64)
                sid_tuple = tuple(sorted(sid_list))
                if sid_tuple:
                    if len(sid_tuple) == 1:
                        og_image_url = _steam_profile_image_url(sid_tuple[0])
                    ck = (sid_tuple, map_query.lower())
                    cached = cache_get("logmatch", ck) or {}
                    total = cached.get("total")
                    if isinstance(total, (int, float)):
                        desc = f"Found {int(total)} matching log(s)."
        elif mode == "profile":
            steamid_in = (qp.get("steamid") or "").strip()
            title = "Player profile"
            desc = "TF2 competitive log stats."
            if steamid_in:
                steamid64, err = resolve_to_steamid64(steamid_in, STEAM_WEB_API_KEY)
                if err is None and steamid64:
                    og_image_url = _steam_profile_image_url(steamid64)
                    gm = (qp.get("gamemode") or "").strip()
                    if gm not in ("", "hl", "7s", "6s", "ud"):
                        gm = ""
                    date_from = (qp.get("date_from") or "").strip()
                    date_to = (qp.get("date_to") or "").strip()
                    map_query = (qp.get("map_query") or "").strip()
                    ck = (steamid64, gm, date_from, date_to, map_query.lower())
                    cached = cache_get("profile", ck) or {}
                    dn = (cached.get("display_name") or "").strip()
                    title = _truncate(dn, 80) if dn else f"Profile · {steamid64}"
                    parts: list[str] = []
                    lc = cached.get("logs_count")
                    if isinstance(lc, (int, float)):
                        parts.append(f"{int(lc)} log(s)")
                    ov = cached.get("overview") or {}
                    wr = ov.get("win_rate")
                    if wr is not None:
                        try:
                            parts.append(f"{round(float(wr) * 100, 1)}% win rate")
                        except (TypeError, ValueError):
                            pass
                    if parts:
                        desc = " · ".join(parts)
                    extra_bits: list[str] = []
                    if gm:
                        extra_bits.append(f"mode {gm}")
                    if map_query:
                        extra_bits.append(f'map "{map_query}"')
                    if date_from or date_to:
                        extra_bits.append(f"{date_from or '…'} to {date_to or '…'}")
                    if extra_bits:
                        desc = f"{desc} ({', '.join(extra_bits)})."
        elif mode == "leaderboard":
            lb_t = (qp.get("lb_type") or "dpm").strip().lower()
            if lb_t not in LEADERBOARD_TYPE_KEYS:
                lb_t = "dpm"
            class_filter_e = (qp.get("class_filter") or "").strip().lower()
            gamemode_e = (qp.get("gamemode") or "").strip()
            if gamemode_e not in ("", "hl", "7s", "6s", "ud"):
                gamemode_e = ""
            title = f"Stats Leaderboard — {lb_t.upper()}"
            try:
                ml_e = int((qp.get("min_logs") or "").strip() or str(LEADERBOARD_MIN_LOGS_DEFAULT))
            except ValueError:
                ml_e = LEADERBOARD_MIN_LOGS_DEFAULT
            ml_e = max(1, min(ml_e, LEADERBOARD_MIN_LOGS_MAX))
            ck = (
                lb_t,
                gamemode_e,
                class_filter_e,
                (qp.get("date_from") or "").strip(),
                (qp.get("date_to") or "").strip(),
                (qp.get("map_query") or "").strip().lower(),
                ml_e,
            )
            cached = cache_get("leaderboard", ck) or {}
            n = len(cached.get("rows") or [])
            total = cached.get("total_logs")
            desc = f"Top {n} player(s)"
            if total is not None:
                try:
                    desc += f" across {int(total)} log(s)"
                except (TypeError, ValueError):
                    pass
            bits: list[str] = []
            if gamemode_e:
                bits.append(f"mode {gamemode_e}")
            if class_filter_e:
                bits.append(class_filter_e)
            if bits:
                desc += f" ({', '.join(bits)})"

        title = _truncate(title, 80)
        desc = _truncate(desc, 220)

        esc_title = _escape_meta(title)
        esc_desc = _escape_meta(desc)
        esc_url = _escape_meta(full_url)
        # Open Graph / Twitter: Discord and other clients fetch og:image for link previews.
        image_block = ""
        if og_image_url:
            esc_img = _escape_meta(og_image_url)
            image_block = (
                f'\n  <meta property="og:image" content="{esc_img}">'
                f'\n  <meta property="og:image:secure_url" content="{esc_img}">'
                f'\n  <meta name="twitter:image" content="{esc_img}">'
            )
        return (
            f'\n  <meta property="og:type" content="website">'
            f'\n  <meta property="og:site_name" content="TF2 Log Searcher">'
            f'\n  <meta property="og:title" content="{esc_title}">'
            f'\n  <meta property="og:description" content="{esc_desc}">'
            f'\n  <meta property="og:url" content="{esc_url}">'
            f'\n  <meta name="twitter:card" content="summary">'
            f'\n  <meta name="twitter:title" content="{esc_title}">'
            f'\n  <meta name="twitter:description" content="{esc_desc}">'
            f"{image_block}\n"
        )
    except Exception:
        return ""


async def _serve_results_with_embed(request: Request) -> HTMLResponse:
    start = time.perf_counter()
    static_path = Path(__file__).resolve().parent.parent / "static" / "index.html"
    try:
        if not static_path.is_file():
            duration_ms = int((time.perf_counter() - start) * 1000)
            _log_request(request, "/results", 200, duration_ms)
            return HTMLResponse("<html><body><h1>Tf2LogSearcher</h1><p>ok</p></body></html>")
        raw = static_path.read_text(encoding="utf-8", errors="replace")
        meta = _build_results_embed_meta(request)
        if meta:
            # Insert immediately after <title> tag if present, else after <head>.
            needle = "<title>TF2 Log Searcher</title>"
            if needle in raw:
                raw = raw.replace(needle, needle + meta, 1)
            else:
                raw = raw.replace("<head>", "<head>" + meta, 1)
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/results", 200, duration_ms)
        return HTMLResponse(raw, media_type="text/html")
    except Exception:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/results", 500, duration_ms)
        raise


async def _serve_index(request: Request, path: str):
    """Serve static/index.html for / and /results."""
    start = time.perf_counter()
    try:
        static_path = Path(__file__).resolve().parent.parent / "static" / "index.html"
        if static_path.exists():
            duration_ms = int((time.perf_counter() - start) * 1000)
            _log_request(request, path, 200, duration_ms)
            return FileResponse(static_path, media_type="text/html")
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, path, 200, duration_ms)
        return HTMLResponse("<html><body><h1>Tf2LogSearcher</h1><p>ok</p></body></html>")
    except Exception:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, path, 500, duration_ms)
        raise
