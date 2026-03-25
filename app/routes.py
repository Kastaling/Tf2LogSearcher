"""API routes for search endpoints and request logging."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

from app.config import LOGS_DIR, REQUEST_LOG_PATH, DOWNLOADER_STATE_DIR, STEAM_WEB_API_KEY
from app.request_log import append_request_log
from app.search.search import chat_search, stats_search, log_match
from app.search_cache import get as cache_get, set_ as cache_set
from app.steam_resolver import resolve_to_steamid64
from app.subscriptions import add_subscription, deactivate_by_token, is_valid_discord_webhook_url, send_welcome_message

CHAT_SEARCH_MAX_WORD_LENGTH = 200
STEAMID64_LEN = 17


router = APIRouter()


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


def _api_search_chat_impl(request: Request, word: str, steamid_input: str) -> JSONResponse:
    """Shared implementation for POST and GET chat search. Returns JSONResponse."""
    start = time.perf_counter()
    if not steamid_input:
        return JSONResponse(
            {"results": [], "total": 0, "error": "Steam ID is required."},
            status_code=400,
        )
    if len(word) > CHAT_SEARCH_MAX_WORD_LENGTH:
        return JSONResponse(
            {"results": [], "total": 0, "error": "Search word is too long."},
            status_code=400,
        )

    steamid64, resolve_error = resolve_to_steamid64(steamid_input, STEAM_WEB_API_KEY)
    if resolve_error is not None:
        return JSONResponse(
            {"results": [], "total": 0, "error": resolve_error},
            status_code=400,
        )
    assert steamid64 is not None

    cache_key = (steamid64, word)
    cached = cache_get("chat", cache_key)
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/chat", 200, duration_ms, result_count=cached.get("total", 0), word=word, steamid=steamid64)
        return JSONResponse(cached)

    status_code = 200
    result_count = 0
    try:
        results, result_count, searched_user_name, log_ids_used = chat_search(word, steamid64, LOGS_DIR)
        payload = {
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
async def api_search_chat(request: Request, word: str = Form(""), steamid: str = Form("")):
    """Chat search: Steam ID (any format) required; word optional."""
    return _api_search_chat_impl(request, (word or "").strip(), (steamid or "").strip())


@router.get("/api/search/chat")
async def api_search_chat_get(request: Request, word: str = Query(""), steamid: str = Query("")):
    """GET variant for shareable links; same response as POST."""
    return _api_search_chat_impl(request, (word or "").strip(), (steamid or "").strip())


def _api_search_stats_impl(request: Request, steamid: str, gamemode: str, classes: str) -> JSONResponse:
    """Shared impl for POST/GET stats search."""
    start = time.perf_counter()
    steamid_input = (steamid or "").strip()
    if not steamid_input:
        return JSONResponse({"rows": [], "error": "Steam ID is required."}, status_code=400)
    steamid64, resolve_error = resolve_to_steamid64(steamid_input, STEAM_WEB_API_KEY)
    if resolve_error is not None:
        return JSONResponse({"rows": [], "error": resolve_error}, status_code=400)
    assert steamid64 is not None
    class_list = [c.strip() for c in (classes or "").split(",") if c.strip()]
    class_tuple = tuple(sorted(c.lower() for c in class_list if c))
    cache_key = (steamid64, gamemode, class_tuple)
    cached = cache_get("stats", cache_key)
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/stats", 200, duration_ms, result_count=len(cached.get("rows", [])), steamid=steamid64, gamemode=gamemode, classes=classes)
        return JSONResponse(cached)
    try:
        rows, log_ids_used = stats_search(steamid64, gamemode, class_list, LOGS_DIR)
        payload = {"rows": rows}
        cache_set("stats", cache_key, payload, log_ids_used)
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/stats", 200, duration_ms, result_count=len(rows), steamid=steamid64, gamemode=gamemode, classes=classes)
        return JSONResponse(payload)
    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/stats", 500, duration_ms, steamid=steamid64, gamemode=gamemode, classes=classes)
        return JSONResponse({"rows": [], "error": str(e)}, status_code=500)


@router.post("/api/search/stats")
async def api_search_stats(
    request: Request,
    steamid: str = Form(""),
    gamemode: str = Form("hl"),
    classes: str = Form(""),
):
    return _api_search_stats_impl(request, steamid or "", gamemode or "hl", classes or "")


@router.get("/api/search/stats")
async def api_search_stats_get(
    request: Request,
    steamid: str = Query(""),
    gamemode: str = Query("hl"),
    classes: str = Query(""),
):
    return _api_search_stats_impl(request, steamid or "", gamemode or "hl", classes or "")


def _api_search_logmatch_impl(request: Request, steamids: str) -> JSONResponse:
    """Shared impl for POST/GET logmatch search."""
    start = time.perf_counter()
    raw_list = [s.strip() for s in (steamids or "").replace(",", " ").split() if s.strip()]
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
    cached = cache_get("logmatch", (sid_tuple,))
    if cached is not None:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/logmatch", 200, duration_ms, result_count=cached.get("total", 0), steamids=",".join(sid_list))
        return JSONResponse(cached)

    status_code = 200
    result_count = 0
    try:
        results, result_count, matching_log_ids = log_match(sid_list, LOGS_DIR)
        payload = {"results": results, "total": result_count}
        cache_set("logmatch", (sid_tuple,), payload, matching_log_ids)
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/logmatch", status_code, duration_ms, result_count=result_count, steamids=",".join(sid_list))
        return JSONResponse(payload)
    except Exception as e:
        status_code = 500
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/logmatch", status_code, duration_ms, steamids=",".join(sid_list))
        return JSONResponse({"results": [], "total": 0, "error": str(e)}, status_code=500)


@router.post("/api/search/logmatch")
async def api_search_logmatch(request: Request, steamids: str = Form("")):
    return _api_search_logmatch_impl(request, steamids or "")


@router.get("/api/search/logmatch")
async def api_search_logmatch_get(request: Request, steamids: str = Query("")):
    return _api_search_logmatch_impl(request, steamids or "")


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
    """Serve the results page (same HTML; client reads URL params and fetches API)."""
    return await _serve_index(request, "/results")


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
