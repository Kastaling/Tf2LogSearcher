"""API routes for search endpoints and request logging."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

from app.config import LOGS_DIR, REQUEST_LOG_PATH, DOWNLOADER_STATE_DIR, STEAM_WEB_API_KEY
from app.request_log import append_request_log
from app.search.search import chat_search, stats_search, log_match
from app.steam_resolver import resolve_to_steamid64

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


@router.post("/api/search/chat")
async def api_search_chat(request: Request, word: str = Form(""), steamid: str = Form("")):
    """Chat search: Steam ID (any format) required; word optional. Resolves ID server-side; API key never sent to client."""
    start = time.perf_counter()
    word = (word or "").strip()
    steamid_input = (steamid or "").strip()

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

    status_code = 200
    result_count = 0
    try:
        results, result_count, searched_user_name = chat_search(word, steamid64, LOGS_DIR)
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/chat", status_code, duration_ms, result_count=result_count, word=word, steamid=steamid64)
        return JSONResponse({
            "results": results,
            "total": result_count,
            "searched_user_name": searched_user_name,
            "resolved_steamid64": steamid64,
        })
    except Exception as e:
        status_code = 500
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/chat", status_code, duration_ms, result_count=None, word=word, steamid=steamid64)
        return JSONResponse({"results": [], "total": 0, "error": str(e)}, status_code=500)


@router.post("/api/search/stats")
async def api_search_stats(
    request: Request,
    steamid: str = Form(""),
    gamemode: str = Form("hl"),
    classes: str = Form(""),  # comma-separated
):
    """Stats search: Steam ID (any format), gamemode, classes. Resolves ID server-side."""
    start = time.perf_counter()
    steamid_input = (steamid or "").strip()
    if not steamid_input:
        return JSONResponse({"rows": [], "error": "Steam ID is required."}, status_code=400)
    steamid64, resolve_error = resolve_to_steamid64(steamid_input, STEAM_WEB_API_KEY)
    if resolve_error is not None:
        return JSONResponse({"rows": [], "error": resolve_error}, status_code=400)
    assert steamid64 is not None

    status_code = 200
    result_count = 0
    class_list = [c.strip() for c in classes.split(",") if c.strip()]
    try:
        rows = stats_search(steamid64, gamemode, class_list, LOGS_DIR)
        result_count = len(rows)
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/stats", status_code, duration_ms, result_count=result_count, steamid=steamid64, gamemode=gamemode, classes=classes)
        return JSONResponse({"rows": rows})
    except Exception as e:
        status_code = 500
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/stats", status_code, duration_ms, steamid=steamid64, gamemode=gamemode, classes=classes)
        return JSONResponse({"rows": [], "error": str(e)}, status_code=500)


@router.post("/api/search/logmatch")
async def api_search_logmatch(request: Request, steamids: str = Form("")):
    """Log match: space- or comma-separated Steam IDs (any format). Each resolved server-side."""
    start = time.perf_counter()
    raw_list = [s.strip() for s in steamids.replace(",", " ").split() if s.strip()]
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
    status_code = 200
    result_count = 0
    try:
        results, result_count = log_match(sid_list, LOGS_DIR)
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/logmatch", status_code, duration_ms, result_count=result_count, steamids=",".join(sid_list))
        return JSONResponse({"results": results, "total": result_count})
    except Exception as e:
        status_code = 500
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/api/search/logmatch", status_code, duration_ms, steamids=",".join(sid_list))
        return JSONResponse({"results": [], "total": 0, "error": str(e)}, status_code=500)


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
    start = time.perf_counter()
    try:
        # Serve from static/index.html if present
        static_path = Path(__file__).resolve().parent.parent / "static" / "index.html"
        if static_path.exists():
            duration_ms = int((time.perf_counter() - start) * 1000)
            _log_request(request, "/", 200, duration_ms)
            return FileResponse(static_path, media_type="text/html")
        # Placeholder until frontend is built
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/", 200, duration_ms)
        return HTMLResponse("<html><body><h1>Tf2LogSearcher</h1><p>ok</p></body></html>")
    except Exception:
        duration_ms = int((time.perf_counter() - start) * 1000)
        _log_request(request, "/", 500, duration_ms)
        raise
