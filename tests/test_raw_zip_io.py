"""Tests for logs.tf raw zip fetch behavior."""
from unittest.mock import MagicMock, patch

import pytest

from app.raw_zip_io import RawZipFetchOutcome, fetch_raw_log_zip_with_retry


def _mock_response(status_code: int, *, content: bytes = b"zip") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.headers = {}
    resp.raise_for_status.side_effect = None
    if status_code >= 400:
        import requests

        resp.raise_for_status.side_effect = requests.HTTPError(f"{status_code}")
    return resp


@patch("requests.get")
@patch("app.raw_zip_io.time.sleep")
def test_fetch_403_is_immediate_not_available(mock_sleep, mock_get):
    mock_get.return_value = _mock_response(403)
    result = fetch_raw_log_zip_with_retry(499624)
    assert result.outcome == RawZipFetchOutcome.NOT_AVAILABLE
    assert result.status_code == 403
    assert result.data is None
    assert mock_get.call_count == 1
    mock_sleep.assert_not_called()


@patch("requests.get")
@patch("app.raw_zip_io.time.sleep")
def test_fetch_404_is_immediate_not_available(mock_sleep, mock_get):
    mock_get.return_value = _mock_response(404)
    result = fetch_raw_log_zip_with_retry(123)
    assert result.outcome == RawZipFetchOutcome.NOT_AVAILABLE
    assert result.status_code == 404
    assert mock_get.call_count == 1
    mock_sleep.assert_not_called()


@patch("requests.get")
@patch("app.raw_zip_io.time.sleep")
def test_fetch_200_returns_bytes(mock_sleep, mock_get):
    mock_get.return_value = _mock_response(200, content=b"payload")
    result = fetch_raw_log_zip_with_retry(123)
    assert result.outcome == RawZipFetchOutcome.OK
    assert result.data == b"payload"
    mock_sleep.assert_not_called()


@patch("requests.get")
@patch("app.raw_zip_io.time.sleep")
def test_fetch_500_retries_then_fails(mock_sleep, mock_get):
    mock_get.return_value = _mock_response(500)
    with patch("app.raw_zip_io.RETRY_ATTEMPTS", 3):
        result = fetch_raw_log_zip_with_retry(123)
    assert result.outcome == RawZipFetchOutcome.FAILED
    assert mock_get.call_count == 3
    assert mock_sleep.call_count == 3
