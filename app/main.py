"""Tf2LogSearcher web application entry point."""
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from app.routes import router

app = FastAPI(title="Tf2LogSearcher")
app.include_router(router)


@app.get("/health", response_class=PlainTextResponse)
def health():
    """Health check."""
    return "ok"
