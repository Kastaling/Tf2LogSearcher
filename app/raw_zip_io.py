"""Fetch and decode logs.tf raw `log_<id>.log.zip` files (shared by downloader, backfill, gap-fetch)."""
from __future__ import annotations

import io
import logging
import time
import zipfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from app.config import LOGS_TF_API_BASE, RETRY_ATTEMPTS

logger = logging.getLogger(__name__)


class RawZipFetchOutcome(str, Enum):
    OK = "ok"
    NOT_AVAILABLE = "not_available"
    FAILED = "failed"


@dataclass(frozen=True)
class RawZipFetchResult:
    data: bytes | None
    outcome: RawZipFetchOutcome
    status_code: int | None = None


def fetch_raw_log_zip_with_retry(log_id: int) -> RawZipFetchResult:
    """
    Download log_{log_id}.log.zip from logs.tf.

    404/403 are permanent absences (no retries). 429/5xx/timeouts retry with backoff.
    """
    import requests

    url = f"{LOGS_TF_API_BASE}/logs/log_{log_id}.log.zip"
    last_exc = None
    last_status: int | None = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = requests.get(url, timeout=30)
            last_status = r.status_code
            if r.status_code in (404, 403):
                logger.info(
                    "Raw zip log %s: not available (%s)",
                    log_id,
                    "404 Not Found" if r.status_code == 404 else "403 Forbidden",
                )
                return RawZipFetchResult(None, RawZipFetchOutcome.NOT_AVAILABLE, r.status_code)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else 60
                logger.info("Rate limited (429) on raw zip, waiting %s s", wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                time.sleep(30 * (attempt + 1))
                continue
            r.raise_for_status()
            return RawZipFetchResult(r.content, RawZipFetchOutcome.OK, r.status_code)
        except requests.RequestException as e:
            last_exc = e
            logger.warning("Raw zip log %s attempt %s: %s", log_id, attempt + 1, e)
            time.sleep(30 * (attempt + 1))
    if last_exc:
        logger.warning("Raw zip log %s: giving up after %s attempts", log_id, RETRY_ATTEMPTS)
    return RawZipFetchResult(None, RawZipFetchOutcome.FAILED, last_status)


def save_raw_log_zip(log_id: int, zip_bytes: bytes, raw_logs_dir: Path) -> Path | None:
    """
    Save zip_bytes to raw_logs_dir/log_{id}.log.zip.
    Returns path on success, None on OSError.
    """
    try:
        raw_logs_dir.mkdir(parents=True, exist_ok=True)
        path = raw_logs_dir / f"log_{log_id}.log.zip"
        path.write_bytes(zip_bytes)
        return path
    except OSError as e:
        logger.warning("Could not save raw zip for log %s: %s", log_id, e)
        return None


def _zip_entry_name_safe(name: str) -> bool:
    """Reject zip-slip paths (e.g. ../../outside)."""
    if not name or name.startswith("/"):
        return False
    parts = Path(name.replace("\\", "/")).parts
    return ".." not in parts


def extract_log_content_from_zip(zip_bytes: bytes) -> str | None:
    """
    Extract the .log file content from zip bytes in memory.
    Returns decoded string content, or None on failure.
    Never writes to disk.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            if not names:
                return None
            choice: str | None = None
            for n in names:
                if not _zip_entry_name_safe(n):
                    continue
                if n.lower().endswith(".log"):
                    choice = n
                    break
            if choice is None:
                for n in names:
                    if _zip_entry_name_safe(n):
                        choice = n
                        break
            if choice is None:
                return None
            raw = zf.read(choice)
            return raw.decode("utf-8", errors="replace")
    except Exception:
        return None
