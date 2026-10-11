"""Request timing is available without disclosing raw request data."""
from __future__ import annotations

import httpx
import pytest
import threading

from app.main import app
from app import routes


@pytest.mark.anyio
async def test_health_exposes_server_timing_without_query_data(caplog) -> None:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health?private_search=secret")
    assert response.status_code == 200
    assert response.headers["Server-Timing"].startswith("app;dur=")
    assert "secret" not in caplog.text


def test_search_build_capacity_releases_slot(monkeypatch) -> None:
    slots = threading.BoundedSemaphore(1)
    monkeypatch.setattr(routes, "_SEARCH_BUILD_SLOTS", slots)
    assert routes._run_search_build(lambda: 7) == 7
    slots.acquire()
    try:
        with pytest.raises(RuntimeError, match="temporarily at capacity"):
            routes._run_search_build(lambda: 8)
    finally:
        slots.release()
