"""Tf2LogSearcher web application entry point."""
import logging
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv

# Repo-root .env (directory of app/). In Docker, .env is usually not in the image — variables
# must come from Compose/Kubernetes env_file / environment. Explicit path avoids relying on CWD.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from starlette.datastructures import Headers
from starlette.responses import FileResponse, Response
from starlette.staticfiles import NotModifiedResponse, StaticFiles
from starlette.types import Scope

from app.routes import router

logger = logging.getLogger(__name__)


def _set_static_file_headers(res: FileResponse) -> None:
    """Unhashed URLs: revalidate (If-None-Match) on each load; 304 for unchanged files.

    Each path under /static/ has its own ETag/Last-Modified (Starlette FileResponse).
    Long max-age+immutable is only safe with content-addressed (hashed) filenames.
    """
    res.headers.setdefault("Cache-Control", "public, max-age=0, must-revalidate")
    res.headers.setdefault("X-Content-Type-Options", "nosniff")


class CachePolicyStaticFiles(StaticFiles):
    """StaticFiles with explicit cache and MIME-sniffing policy."""

    def file_response(
        self,
        full_path: str | os.PathLike[str],
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        request_headers = Headers(scope=scope)
        response = FileResponse(full_path, status_code=status_code, stat_result=stat_result)
        _set_static_file_headers(response)
        if self.is_not_modified(response.headers, request_headers):
            return NotModifiedResponse(response.headers)
        return response


def _is_transient_sqlite_contention(exc: BaseException) -> bool:
    """True when SQLite reports contention that may clear if we wait (shared stats.db with downloader)."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    msg = str(exc).lower()
    # SQLITE_BUSY → often "database is busy"; legacy / some builds → "database is locked"
    return "locked" in msg or "busy" in msg


def _init_stats_db_background() -> None:
    """Schema/index DDL can take a long time on large DBs; must not block HTTP startup."""
    from app.config import STATS_DB_PATH
    from app.stats_db import connect_stats_db, init_stats_db

    # Downloader may hold stats.db for long writes; connect/init can fail transiently — retry with backoff.
    max_attempts = 36
    sleep_s = 10
    last_err: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            sconn = connect_stats_db(STATS_DB_PATH)
            try:
                init_stats_db(sconn)
            finally:
                sconn.close()
            logger.info("Stats DB initialized at %s", STATS_DB_PATH)
            return
        except sqlite3.OperationalError as e:
            last_err = e
            if not _is_transient_sqlite_contention(e):
                logger.exception("Stats DB init failed (%s)", STATS_DB_PATH)
                return
            logger.warning(
                "Stats DB busy or locked (attempt %s/%s): %s; retry in %ss (downloader may be using the file).",
                attempt,
                max_attempts,
                e,
                sleep_s,
            )
            time.sleep(sleep_s)
        except Exception:
            logger.exception("Stats DB init failed (%s); stats features may be unavailable until restart.", STATS_DB_PATH)
            return
    logger.error(
        "Stats DB init gave up after %s attempts (%s): %s",
        max_attempts,
        STATS_DB_PATH,
        last_err,
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    from app.avatar_db import connect_avatar_db, init_avatar_db
    from app.config import AVATAR_DB_PATH, DOWNLOAD_RAW_ENABLED, RAW_EVENTS_DB_PATH
    from app.raw_db import connect_raw_db, init_raw_db

    conn = connect_avatar_db(AVATAR_DB_PATH)
    try:
        init_avatar_db(conn)
    finally:
        conn.close()

    if DOWNLOAD_RAW_ENABLED or Path(RAW_EVENTS_DB_PATH).is_file():
        rconn = connect_raw_db(RAW_EVENTS_DB_PATH)
        try:
            init_raw_db(rconn)
        finally:
            rconn.close()

    # Run in a daemon thread so uvicorn can listen immediately. Otherwise init_stats_db (CREATE INDEX
    # on large tables) blocks the whole app for minutes and contends with the downloader for locks.
    threading.Thread(target=_init_stats_db_background, name="stats-db-init", daemon=True).start()
    yield


app = FastAPI(title="Tf2LogSearcher", lifespan=lifespan)
app.include_router(router)

_static_dir = Path(__file__).resolve().parent.parent / "static"
if _static_dir.is_dir():
    app.mount("/static", CachePolicyStaticFiles(directory=str(_static_dir)), name="static")


@app.get("/health", response_class=PlainTextResponse)
def health():
    """Health check."""
    return "ok"
