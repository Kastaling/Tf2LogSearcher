"""Tests for the async Steam avatar fetch helper."""
import os

import httpx
import pytest
import respx

os.environ.setdefault("STEAM_WEB_API_KEY", "FAKE_KEY_FOR_TESTS")

import app.routes as routes_module

FAKE_AVATAR_URL = "https://avatars.steamstatic.com/abc123_full.jpg"
FAKE_STEAMID = "76561198000000001"


@pytest.mark.anyio
@respx.mock
async def test_fetch_avatar_urls_success():
    respx.get("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "players": [{"steamid": FAKE_STEAMID, "avatarfull": FAKE_AVATAR_URL}]
                }
            },
        )
    )

    result = await routes_module._fetch_steam_avatar_urls([FAKE_STEAMID])
    assert result == {FAKE_STEAMID: FAKE_AVATAR_URL}


@pytest.mark.anyio
@respx.mock
async def test_fetch_avatar_urls_strips_whitespace_around_url():
    """Leading/trailing whitespace must not reject valid https avatar URLs."""
    spaced = "  " + FAKE_AVATAR_URL + "  \n"
    respx.get("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "players": [{"steamid": FAKE_STEAMID, "avatarfull": spaced}]
                }
            },
        )
    )

    result = await routes_module._fetch_steam_avatar_urls([FAKE_STEAMID])
    assert result == {FAKE_STEAMID: FAKE_AVATAR_URL}


@pytest.mark.anyio
@respx.mock
async def test_fetch_avatar_urls_http_error_returns_empty():
    respx.get("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/").mock(
        return_value=httpx.Response(500)
    )

    result = await routes_module._fetch_steam_avatar_urls([FAKE_STEAMID])
    assert result == {}


@pytest.mark.anyio
async def test_fetch_avatar_urls_no_key(monkeypatch):
    monkeypatch.setattr(routes_module, "STEAM_WEB_API_KEY", None)
    result = await routes_module._fetch_steam_avatar_urls([FAKE_STEAMID])
    assert result == {}
