"""API routes for search endpoints and request logging."""
import asyncio
import json
import logging
import sqlite3
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

from app.config import LOGS_DIR, REQUEST_LOG_PATH, DOWNLOADER_STATE_DIR, STEAM_WEB_API_KEY, CHAT_DB_PATH
from app.chat_db import chat_log_fingerprint
from app.request_log import append_request_log
from app.search.search import (
    PlayerNameIndexNotReadyError,
    chat_leaderboard_search_sqlite,
    chat_search,
    chat_search_sqlite,
    coplayers_search,
    log_match,
    player_name_search_sqlite,
    stats_search,
)
from app.search_cache import get as cache_get, set_ as cache_set
from app.steam_resolver import resolve_to_steamid64
from app.subscriptions import add_subscription, deactivate_by_token, is_valid_discord_webhook_url, send_welcome_message

CHAT_SEARCH_MAX_WORD_LENGTH = 200
MAP_QUERY_MAX_LENGTH = 100
STEAMID64_LEN = 17
PLAYER_NAME_QUERY_MIN_LENGTH = 2
PLAYER_NAME_QUERY_MAX_LENGTH = 64
PLAYER_NAME_RESULT_LIMIT = 200


router = APIRouter()
logger = logging.getLogger(__name__)


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
    class_tuple = tuple(sorted(c.lower() for c in class_list if c))
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
        cache_set("stats", cache_key, payload, log_ids_used)
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
        return JSONResponse(cached)
    try:
        rows, log_ids_used = coplayers_search(steamid64, LOGS_DIR, gamemode=gm, map_query=map_query)
        payload = {"rows": rows, "logs_searched": len(log_ids_used)}
        cache_set("coplayers", cache_key, payload, log_ids_used)
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
        results, result_count, matching_log_ids = log_match(
            sid_list, LOGS_DIR, search_inputs=raw_list, map_query=map_query
        )
        payload = {"results": results, "total": result_count}
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
    return _api_search_logmatch_impl(request, steamids or "", map_query or "")


@router.get("/api/search/logmatch")
async def api_search_logmatch_get(request: Request, steamids: str = Query(""), map_query: str = Query("")):
    return _api_search_logmatch_impl(request, steamids or "", map_query or "")


@router.post("/api/chat-subscriptions")
async def api_add_chat_subscription(
    request: Request,
    webhook_url: str = Form(""),
    steamid: str = Form(""),
    word: str = Form(""),
):
    """
    Subscribe a Discord webhook to chat search alerts for (steamid, word).
    Only valid when word is non-empty. Webhook URL is validated strictly.
    """
    webhook_url = (webhook_url or "").strip()
    steamid_input = (steamid or "").strip()
    word = (word or "").strip()
    if not word:
        return JSONResponse({"ok": False, "error": "A search word is required (not full chat history)."}, status_code=400)
    if not steamid_input:
        return JSONResponse({"ok": False, "error": "Steam ID is required."}, status_code=400)
    if not is_valid_discord_webhook_url(webhook_url):
        return JSONResponse(
            {"ok": False, "error": "Invalid Discord webhook URL. Use a URL like https://discord.com/api/webhooks/123.../abc..."},
            status_code=400,
        )
    steamid64, resolve_error = resolve_to_steamid64(steamid_input, STEAM_WEB_API_KEY)
    if resolve_error is not None:
        return JSONResponse({"ok": False, "error": resolve_error}, status_code=400)
    assert steamid64 is not None
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
    }
    out = {k: data[k] for k in allowed if k in data}
    return JSONResponse(out)


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
                    class_tuple = tuple(sorted(c.lower() for c in classes.split(",") if c.strip()))
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
                    ck = (sid_tuple, map_query.lower())
                    cached = cache_get("logmatch", ck) or {}
                    total = cached.get("total")
                    if isinstance(total, (int, float)):
                        desc = f"Found {int(total)} matching log(s)."

        title = _truncate(title, 80)
        desc = _truncate(desc, 220)

        esc_title = _escape_meta(title)
        esc_desc = _escape_meta(desc)
        esc_url = _escape_meta(full_url)
        # Minimal OG/Twitter tags (no images for now)
        return (
            f'\n  <meta property="og:type" content="website">'
            f'\n  <meta property="og:site_name" content="TF2 Log Searcher">'
            f'\n  <meta property="og:title" content="{esc_title}">'
            f'\n  <meta property="og:description" content="{esc_desc}">'
            f'\n  <meta property="og:url" content="{esc_url}">'
            f'\n  <meta name="twitter:card" content="summary">'
            f'\n  <meta name="twitter:title" content="{esc_title}">'
            f'\n  <meta name="twitter:description" content="{esc_desc}">\n'
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
