"""Tests for download rate display formatting."""
from app.rate_format import (
    format_rate_logs_per_sec,
    format_rate_logs_per_sec_display,
    sanitize_rate_for_api,
)


def test_format_rate_none_and_zero():
    assert format_rate_logs_per_sec(None) is None
    assert format_rate_logs_per_sec(0) == "0"
    assert format_rate_logs_per_sec(-1) == "0"


def test_format_rate_fast():
    assert format_rate_logs_per_sec(123.456) == "123"
    assert format_rate_logs_per_sec(12.34) == "12.3"
    assert format_rate_logs_per_sec(1.234) == "1.23"
    assert format_rate_logs_per_sec(1.0) == "1"


def test_format_rate_slow_backfill():
    assert format_rate_logs_per_sec(0.383) == "0.383"
    assert format_rate_logs_per_sec(0.000816) == "0.000816"
    assert format_rate_logs_per_sec(0.00183) == "0.00183"


def test_format_rate_avoids_scientific_notation():
    out = format_rate_logs_per_sec(0.000000123)
    assert out is not None
    assert "e" not in out.lower()
    assert out.startswith("0.")
    assert float(out) > 0


def test_format_rate_non_finite():
    assert format_rate_logs_per_sec(float("nan")) is None
    assert format_rate_logs_per_sec(float("inf")) is None
    assert format_rate_logs_per_sec(float("-inf")) is None


def test_format_rate_display_fallback():
    assert format_rate_logs_per_sec_display(None) == "N/A"
    assert format_rate_logs_per_sec_display(float("nan")) == "N/A"
    assert format_rate_logs_per_sec_display(0.383) == "0.383"


def test_sanitize_rate_for_api():
    assert sanitize_rate_for_api(None) is None
    assert sanitize_rate_for_api(float("nan")) is None
    assert sanitize_rate_for_api(float("inf")) is None
    assert sanitize_rate_for_api(-1.0) is None
    assert sanitize_rate_for_api(0.0) == 0.0
    assert sanitize_rate_for_api(0.383) == 0.383
