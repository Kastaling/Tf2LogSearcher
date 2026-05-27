"""Shared log page URLs (internal detail page vs external logs.tf)."""
from __future__ import annotations

import re

from app.config import LOG_DETAIL_LINK_MODE, LOGS_TF_API_BASE

_LOG_ID_RE = re.compile(r"^\d+$")


def is_internal_log_links() -> bool:
    return LOG_DETAIL_LINK_MODE == "internal"


def parse_log_id(log_id: str | int) -> int | None:
    s = str(log_id).strip()
    if not _LOG_ID_RE.fullmatch(s):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def external_log_url(log_id: str | int) -> str:
    return f"{LOGS_TF_API_BASE}/{int(log_id)}"


def log_url_for_id(log_id: str | int) -> str:
    """Return internal ``/log/{id}`` or external logs.tf URL per ``LOG_DETAIL_LINK_MODE``."""
    if is_internal_log_links():
        return f"/log/{int(log_id)}"
    return external_log_url(log_id)
