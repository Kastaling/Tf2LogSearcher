"""Discord webhook subscriptions for chat search alerts. Shared by web app and downloader."""
import json
import logging
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from app.logs_tf import steamid3_to_steamid64, steamid64_to_steamid3

DEACTIVATE_TOKEN_BYTES = 32
# Must match chat search API when Steam ID is empty (global leaderboard).
LEADERBOARD_SUB_WORD_MIN_LEN = 3
# Same cap as chat search word length in routes.
CHAT_SUB_WORD_MAX_LEN = 200

logger = logging.getLogger(__name__)

# Strict Discord webhook URL: https://discord.com/api/webhooks/{id}/{token} or discordapp.com
# Webhook ID: snowflake 17-19 digits; token: alphanumeric, underscore, hyphen
DISCORD_WEBHOOK_URL_RE = re.compile(
    r"^https://(?:discord\.com|discordapp\.com)/api/webhooks/(\d{17,19})/([A-Za-z0-9_-]+)/?$"
)
WEBHOOK_REQUEST_TIMEOUT = 8
# HTTP status: treat as dead webhook (stop sending)
DEAD_WEBHOOK_STATUSES = {404, 410}
LOGS_TF_URL_BASE = "https://logs.tf"
STEAM_AVATAR_URL = "https://avatars.steamstatic.com/{steamid64}.jpg"
WELCOME_EMBED_ICON_URL = "https://logs.tf/img/favicon.ico"


def _discord_field_plain(s: str, max_len: int) -> str:
    """Strip characters that break Discord markdown in embed field values."""
    out = (s or "").replace("*", "").replace("_", "").replace("`", "").replace("\\", "")
    return out[:max_len]


def is_valid_discord_webhook_url(url: str) -> bool:
    """Return True only if the string is a valid Discord webhook URL (format only)."""
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    return bool(DISCORD_WEBHOOK_URL_RE.fullmatch(url))


def _subscriptions_path(state_dir: Path) -> Path:
    return state_dir / "chat_webhook_subscriptions.json"


def _load_raw(state_dir: Path) -> list[dict[str, Any]]:
    path = _subscriptions_path(state_dir)
    if not path.is_file():
        return []
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, list):
            return []
        return data
    except (OSError, ValueError, TypeError):
        return []


def _with_lock(state_dir: Path, modify: Callable[[list[dict[str, Any]]], None]) -> None:
    """Acquire exclusive lock (when fcntl available), load, call modify(data), save."""
    state_dir.mkdir(parents=True, exist_ok=True)
    path = _subscriptions_path(state_dir)
    path.touch(exist_ok=True)
    with open(path, "r+", encoding="utf-8") as f:
        try:
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except (ImportError, OSError):
            pass
        try:
            raw = f.read()
            data = json.loads(raw) if raw.strip() else []
            if not isinstance(data, list):
                data = []
            modify(data)
            f.seek(0)
            f.truncate()
            f.write(json.dumps(data, indent=2))
            f.flush()
        finally:
            try:
                import fcntl
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except (ImportError, OSError):
                pass


def _save(state_dir: Path, data: list[dict[str, Any]]) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    path = _subscriptions_path(state_dir)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


def add_subscription(
    state_dir: Path,
    webhook_url: str,
    steamid64: str,
    word: str,
) -> tuple[bool, str, str | None]:
    """
    Add or reactivate a chat subscription. word must be non-empty.
    If steamid64 is empty, this is a global leaderboard word alert (any player in new logs).
    Returns (success, error_message, deactivate_token). On success error_message is empty and token is set.
    """
    webhook_url = (webhook_url or "").strip()
    word = (word or "").strip()
    steamid64 = (steamid64 or "").strip()
    if not word:
        return False, "Subscription requires a search word (not full chat history).", None
    if len(word) > CHAT_SUB_WORD_MAX_LEN:
        return False, "Search word is too long.", None
    is_leaderboard_only = steamid64 == ""
    if is_leaderboard_only:
        if len(word) < LEADERBOARD_SUB_WORD_MIN_LEN:
            return (
                False,
                f"Global word alerts require at least {LEADERBOARD_SUB_WORD_MIN_LEN} characters (same as leaderboard search).",
                None,
            )
    elif len(steamid64) != 17 or not steamid64.isdigit():
        return False, "Invalid Steam ID.", None
    if not is_valid_discord_webhook_url(webhook_url):
        return False, "Invalid Discord webhook URL. Use a URL like https://discord.com/api/webhooks/123.../abc...", None

    token_holder: list[str] = []

    def do_add(data: list[dict[str, Any]]) -> None:
        for sub in data:
            if not isinstance(sub, dict):
                continue
            if (
                sub.get("webhook_url") == webhook_url
                and (sub.get("steamid64") or "") == steamid64
                and sub.get("word") == word
            ):
                sub["active"] = True
                if is_leaderboard_only:
                    sub["leaderboard_only"] = True
                elif "leaderboard_only" in sub:
                    del sub["leaderboard_only"]
                if not sub.get("deactivate_token"):
                    sub["deactivate_token"] = secrets.token_urlsafe(DEACTIVATE_TOKEN_BYTES)
                token_holder.append(sub["deactivate_token"])
                return
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        token = secrets.token_urlsafe(DEACTIVATE_TOKEN_BYTES)
        token_holder.append(token)
        row: dict[str, Any] = {
            "webhook_url": webhook_url,
            "steamid64": steamid64,
            "word": word,
            "active": True,
            "created_at": now,
            "deactivate_token": token,
        }
        if is_leaderboard_only:
            row["leaderboard_only"] = True
        data.append(row)

    try:
        _with_lock(state_dir, do_add)
    except OSError as e:
        logger.warning("Failed to save subscriptions: %s", e)
        return False, "Could not save subscription.", None
    return True, "", token_holder[0] if token_holder else None


def deactivate_by_token(state_dir: Path, token: str) -> bool:
    """Find subscription by deactivate_token, set active=False. Returns True if found and deactivated."""
    if not token or not isinstance(token, str) or len(token) > 200:
        return False
    token = token.strip()
    found = [False]

    def do_deactivate(data: list[dict[str, Any]]) -> None:
        for sub in data:
            if not isinstance(sub, dict):
                continue
            if sub.get("deactivate_token") == token:
                sub["active"] = False
                found[0] = True
                logger.info("Webhook deactivated via link for steamid64 %s", sub.get("steamid64", "?"))
                return
    try:
        _with_lock(state_dir, do_deactivate)
    except (OSError, ValueError, TypeError):
        return False
    return found[0]


def send_welcome_message(
    webhook_url: str,
    word: str,
    steamid64: str,
    deactivate_url: str,
    player_name: str | None = None,
) -> bool:
    """Send a welcome/success message to the webhook with a deactivate link. Returns True if Discord accepted."""
    word_safe = word[:100].replace("\\", "\\\\").replace("`", "\\`").replace("*", "\\*")  # avoid breaking Discord markdown
    steamid64 = (steamid64 or "").strip()
    if steamid64:
        name = (player_name or f"Steam ID {steamid64}")[:256]
        avatar_url = STEAM_AVATAR_URL.format(steamid64=steamid64)
        fields: list[dict[str, Any]] = [
            {"name": "Player", "value": name, "inline": True},
            {"name": "Word", "value": word_safe, "inline": True},
            {"name": "\u200b", "value": f"[**DEACTIVATE THIS WEBHOOK**]({deactivate_url})", "inline": False},
        ]
        payload = {
            "embeds": [
                {
                    "author": {"name": "TF2 Log Searcher", "icon_url": avatar_url},
                    "title": "Subscription active",
                    "description": "You will get a message here when new logs match this search.",
                    "color": 0x5E9CA0,
                    "thumbnail": {"url": avatar_url},
                    "fields": fields,
                }
            ]
        }
    else:
        icon_url = WELCOME_EMBED_ICON_URL
        fields = [
            {
                "name": "Alert",
                "value": "New logs containing this word (any player), same as chat leaderboard search.",
                "inline": False,
            },
            {"name": "Word", "value": word_safe, "inline": True},
            {"name": "\u200b", "value": f"[**DEACTIVATE THIS WEBHOOK**]({deactivate_url})", "inline": False},
        ]
        payload = {
            "embeds": [
                {
                    "author": {"name": "TF2 Log Searcher", "icon_url": icon_url},
                    "title": "Leaderboard word alert active",
                    "description": "You will get a message here when a newly downloaded log contains this word in chat.",
                    "color": 0x5E9CA0,
                    "thumbnail": {"url": icon_url},
                    "fields": fields,
                }
            ]
        }
    try:
        r = requests.post(webhook_url, json=payload, timeout=WEBHOOK_REQUEST_TIMEOUT)
        return r.status_code in (200, 204)
    except requests.RequestException:
        return False


def _mark_inactive(state_dir: Path, webhook_url: str, steamid64: str, word: str) -> None:
    def do_mark(data: list[dict[str, Any]]) -> None:
        for sub in data:
            if not isinstance(sub, dict):
                continue
            if (
                sub.get("webhook_url") == webhook_url
                and (sub.get("steamid64") or "") == steamid64
                and sub.get("word") == word
            ):
                sub["active"] = False
                logger.info(
                    "Marked webhook inactive (dead link) for %s / %s",
                    steamid64 or "leaderboard",
                    word,
                )
                return
    try:
        _with_lock(state_dir, do_mark)
    except (OSError, ValueError, TypeError):
        pass


def check_log_for_subscriptions(log_id: int, logs_dir: Path, state_dir: Path) -> None:
    """
    After a new log is written: load subscriptions, load log chat, for each active
    subscription either (a) player-specific: that player said the word, or (b) leaderboard:
    any player said the word. On 404/410 from Discord, mark that subscription inactive.
    """
    path = logs_dir / f"{log_id}.json"
    if not path.is_file():
        return
    try:
        logtext = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, ValueError):
        return
    chat = logtext.get("chat")
    if not chat:
        return
    names = logtext.get("names") or {}
    subs = _load_raw(state_dir)
    log_url = f"{LOGS_TF_URL_BASE}/{log_id}"
    info = logtext.get("info") or {}
    map_name = info.get("map") or ""

    for sub in subs:
        if not isinstance(sub, dict) or not sub.get("active"):
            continue
        webhook_url = sub.get("webhook_url")
        word = sub.get("word")
        if not webhook_url or not word:
            continue
        steamid64 = (sub.get("steamid64") or "").strip()
        word_lower = word.lower()

        if not steamid64:
            # Global word alert: any chat line in this log contains the word.
            lb_lines: list[str] = []
            first_avatar: str | None = None
            for msg in chat:
                m = (msg.get("msg") or "")
                if word_lower not in m.lower():
                    continue
                sid3 = str(msg.get("steamid") or "").strip()
                pname = (names.get(sid3) if isinstance(names, dict) else None) or sid3 or "Unknown"
                line = f"**{_discord_field_plain(pname, 200)}**: {_discord_field_plain(m, 500)}"
                lb_lines.append(line)
                if first_avatar is None and sid3:
                    s64 = steamid3_to_steamid64(sid3)
                    if s64:
                        first_avatar = STEAM_AVATAR_URL.format(steamid64=s64)
            if not lb_lines:
                continue
            description = "\n".join(lb_lines[:5])
            if len(lb_lines) > 5:
                description += f"\n... and {len(lb_lines) - 5} more"
            thumb = first_avatar or WELCOME_EMBED_ICON_URL
            payload = {
                "embeds": [
                    {
                        "author": {
                            "name": "TF2 Log Searcher",
                            "icon_url": WELCOME_EMBED_ICON_URL,
                        },
                        "title": f'Leaderboard word match: "{word[:200]}"',
                        "description": description[:2000],
                        "color": 0x5E9CA0,
                        "thumbnail": {"url": thumb},
                        "fields": [
                            {"name": "Log", "value": f"[View log ({map_name})]({log_url})", "inline": False},
                        ],
                    }
                ]
            }
        else:
            if len(steamid64) != 17 or not steamid64.isdigit():
                continue
            steamid3 = steamid64_to_steamid3(steamid64)
            matches: list[str] = []
            for msg in chat:
                if msg.get("steamid") != steamid3:
                    continue
                m = (msg.get("msg") or "")
                if word_lower not in m.lower():
                    continue
                matches.append(m)
            if not matches:
                continue
            player_name = names.get(steamid3) or f"Steam ID {steamid64}"
            avatar_url = STEAM_AVATAR_URL.format(steamid64=steamid64)
            description = "\n".join(matches[:5])  # cap at 5 lines
            if len(matches) > 5:
                description += f"\n... and {len(matches) - 5} more"
            payload = {
                "embeds": [
                    {
                        "author": {
                            "name": player_name[:256],
                            "icon_url": avatar_url,
                        },
                        "title": f'Chat match: "{word[:200]}"',
                        "description": description[:2000],  # Discord embed description limit
                        "color": 0x5E9CA0,
                        "thumbnail": {"url": avatar_url},
                        "fields": [
                            {"name": "Log", "value": f"[View log ({map_name})]({log_url})", "inline": False},
                        ],
                    }
                ]
            }
        try:
            r = requests.post(
                webhook_url,
                json=payload,
                timeout=WEBHOOK_REQUEST_TIMEOUT,
            )
            if r.status_code in DEAD_WEBHOOK_STATUSES:
                _mark_inactive(state_dir, webhook_url, steamid64, word)
            elif r.status_code >= 400:
                logger.warning("Webhook %s returned %s for log %s", webhook_url[:50], r.status_code, log_id)
        except requests.RequestException as e:
            logger.warning("Webhook request failed for log %s: %s", log_id, e)
