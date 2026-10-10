"""ETA must reflect actual JSON frontier movement over elapsed wall time."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

from app import downloader, routes
from app.eta_forecast import (
    forecast_frontier,
    load_frontier_history,
    record_frontier,
    save_frontier_history,
)


def test_weeklong_frontier_rate_includes_raw_work_and_idle_time() -> None:
    now = 1_800_000_000.0
    history = [(now - 7 * 86400, 350_000), (now, 325_000)]
    forecast = forecast_frontier(history, now)
    assert forecast.status == "ready"
    assert forecast.seconds is not None
    assert forecast.seconds > 90 * 86400  # Far longer than a recent fast burst implies.
    assert forecast.observation_hours == 7 * 24


def test_eta_is_withheld_during_warmup_but_retained_with_stall_warning() -> None:
    now = 1_800_000_000.0
    assert forecast_frontier([(now - 3600, 500), (now, 400)], now).status == "warming_up"
    stalled = [(now - 10 * 3600, 500), (now - 3 * 3600, 400), (now, 400)]
    forecast = forecast_frontier(stalled, now)
    assert forecast.status == "stalled"
    assert forecast.seconds is not None
    assert forecast.no_progress_hours == 3


def test_daylong_slow_progress_gives_coarse_multi_year_estimate() -> None:
    now = 1_800_000_000.0
    history = [(now - 25.3 * 3600, 326020), (now - 8 * 3600, 325899), (now, 325899)]
    forecast = forecast_frontier(history, now)
    assert forecast.status == "stalled"
    assert forecast.seconds is not None
    assert downloader._human_eta(forecast.seconds) == "About 7.8 years"


def test_no_movement_cannot_produce_a_forecast() -> None:
    now = 1_800_000_000.0
    forecast = forecast_frontier([(now - 25 * 3600, 500), (now, 500)], now)
    assert forecast.status == "insufficient_progress"
    assert forecast.seconds is None


def test_frontier_history_resets_on_regression_and_survives_restart(tmp_path) -> None:
    now = 1_800_000_000.0
    save_frontier_history(tmp_path, [(now - 3600, 500)])
    loaded = load_frontier_history(tmp_path, now)
    assert loaded == [(now - 3600, 500)]
    assert record_frontier(loaded, now, 600) == [(now, 600)]


def test_new_history_seeds_from_existing_progress(tmp_path) -> None:
    now = datetime.now(timezone.utc)
    (tmp_path / "progress.json").write_text(
        json.dumps({"updated_at": (now - timedelta(hours=7)).isoformat(), "min_id": 100}),
        encoding="utf-8",
    )
    loaded = load_frontier_history(tmp_path, now.timestamp())
    assert len(loaded) == 1 and loaded[0][1] == 100
    forecast = forecast_frontier(record_frontier(loaded, now.timestamp(), 50), now.timestamp())
    assert forecast.status == "ready" and forecast.seconds is not None


def test_progress_uses_frontier_history_instead_of_download_burst(tmp_path, monkeypatch) -> None:
    logs = tmp_path / "logs"
    state = tmp_path / "state"
    logs.mkdir()
    state.mkdir()
    (logs / "50.json").write_text('{"info":{}}', encoding="utf-8")
    now = datetime.now(timezone.utc)
    (state / "progress.json").write_text(
        json.dumps({"updated_at": (now - timedelta(hours=7)).isoformat(), "min_id": 100}),
        encoding="utf-8",
    )
    def unexpected_scan(_path):
        raise AssertionError("periodic progress must use cached library counters")

    monkeypatch.setattr(downloader, "_log_dir_stats", unexpected_scan)
    downloader._write_progress_if_due(
        logs, state,
        [(now.timestamp() - 2, 52), (now.timestamp() - 1, 51)],
        [0.0], [downloader._empty_progress_interval()], [now.timestamp() - 7 * 3600],
        [2], [0.0], [(13, 1, 50, 50)],
    )
    progress = json.loads((state / "progress.json").read_text(encoding="utf-8"))
    assert progress["eta_human"] == "About 7 hours"
    assert progress["eta_observation_hours"] == 7.0
    assert progress["rate_logs_per_sec"] > 1  # Active burst is deliberately faster.


def test_json_write_updates_cached_frontier_and_count() -> None:
    stats = [(100, 2, 50, 90)]
    downloader._track_json_write(stats, 40, 15)
    assert stats == [(115, 3, 40, 90)]


def test_progress_hides_old_burst_rate_during_timeout_stall(tmp_path) -> None:
    now = datetime.now(timezone.utc).timestamp()
    downloader._write_progress_if_due(
        tmp_path / "logs", tmp_path / "state",
        [(now - 4 * 3600 - 2, 51), (now - 4 * 3600, 50)],
        [0.0], [downloader._empty_progress_interval()], [now - 7 * 3600],
        [2], [0.0], [(13, 1, 50, 50)],
    )
    progress = json.loads((tmp_path / "state" / "progress.json").read_text(encoding="utf-8"))
    assert progress["rate_logs_per_sec"] is None
    assert progress["rate_logs_per_sec_aggregated"] is None


def test_initial_library_scan_counts_only_numeric_json_files(tmp_path) -> None:
    (tmp_path / "50.json").write_text("abc", encoding="utf-8")
    (tmp_path / "90.json").write_text("de", encoding="utf-8")
    (tmp_path / "progress.json").write_text("x", encoding="utf-8")
    assert downloader._log_dir_stats(tmp_path) == (5, 2, 50, 90)


def test_api_hides_stale_eta_but_preserves_progress_fields(tmp_path, monkeypatch) -> None:
    path = tmp_path / "progress.json"
    path.write_text(
        json.dumps({
            "updated_at": (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat(),
            "min_id": 123,
            "eta_human": "5d",
            "eta_observation_hours": 72,
            "eta_no_progress_hours": 4,
            "rate_logs_per_sec": 0.638,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(routes, "_progress_json_path", lambda: path)
    response = asyncio.run(routes.api_download_progress(None))
    data = json.loads(response.body)
    assert data["min_id"] == 123
    assert data["eta_human"] == "Unavailable (progress is stale)"
    assert data["eta_observation_hours"] is None
    assert data["eta_no_progress_hours"] is None
    assert data["rate_logs_per_sec"] is None


def test_api_hides_eta_when_progress_timestamp_is_invalid(tmp_path, monkeypatch) -> None:
    path = tmp_path / "progress.json"
    path.write_text('{"updated_at":"bad","eta_human":"5d"}', encoding="utf-8")
    monkeypatch.setattr(routes, "_progress_json_path", lambda: path)
    response = asyncio.run(routes.api_download_progress(None))
    assert json.loads(response.body)["eta_human"] == "Unavailable (progress is stale)"
