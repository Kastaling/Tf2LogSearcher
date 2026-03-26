"""Tf2LogSearcher web application entry point."""
from dotenv import load_dotenv
load_dotenv()

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles

from app.routes import router

app = FastAPI(title="Tf2LogSearcher")
app.include_router(router)

_static_dir = Path(__file__).resolve().parent.parent / "static"
if _static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


@app.get("/health", response_class=PlainTextResponse)
def health():
    """Health check."""
    return "ok"
