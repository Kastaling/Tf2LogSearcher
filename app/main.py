"""Tf2LogSearcher web application entry point."""
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles

from app.routes import router


@asynccontextmanager
async def lifespan(_app: FastAPI):
    from app.avatar_db import connect_avatar_db, init_avatar_db
    from app.config import AVATAR_DB_PATH, STATS_DB_PATH
    from app.stats_db import connect_stats_db, init_stats_db

    conn = connect_avatar_db(AVATAR_DB_PATH)
    try:
        init_avatar_db(conn)
    finally:
        conn.close()

    sconn = connect_stats_db(STATS_DB_PATH)
    try:
        init_stats_db(sconn)
    finally:
        sconn.close()
    yield


app = FastAPI(title="Tf2LogSearcher", lifespan=lifespan)
app.include_router(router)

_static_dir = Path(__file__).resolve().parent.parent / "static"
if _static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


@app.get("/health", response_class=PlainTextResponse)
def health():
    """Health check."""
    return "ok"
