"""Human-readable download rate formatting (logs/s)."""
from __future__ import annotations

import math


def format_rate_logs_per_sec(rate: float | None) -> str | None:
    """
    Format logs/s for UI and logs with enough precision for very slow backfills.

    Fast rates use compact fixed decimals; sub-1 rates use three significant figures
    so values like 0.0008 do not display as 0.
    """
    if rate is None:
        return None
    if not math.isfinite(rate):
        return None
    if rate <= 0:
        return "0"
    if rate >= 100:
        return f"{rate:.0f}"
    if rate >= 10:
        return f"{rate:.1f}".rstrip("0").rstrip(".")
    if rate >= 1:
        return f"{rate:.2f}".rstrip("0").rstrip(".")
    text = f"{rate:.3g}"
    if "e" in text or "E" in text:
        exp = math.floor(math.log10(rate))
        decimals = min(8, max(3, -exp + 2))
        text = f"{rate:.{decimals}f}".rstrip("0").rstrip(".")
    return text


def format_rate_logs_per_sec_display(rate: float | None, *, fallback: str = "N/A") -> str:
    """Like ``format_rate_logs_per_sec`` but always returns a display string."""
    formatted = format_rate_logs_per_sec(rate)
    return formatted if formatted is not None else fallback


def sanitize_rate_for_api(rate: float | None) -> float | None:
    """Drop non-finite or negative rates before writing progress.json."""
    if rate is None:
        return None
    if not math.isfinite(rate) or rate < 0:
        return None
    return rate
