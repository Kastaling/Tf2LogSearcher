from __future__ import annotations

from app.map_overviews import (
    bounds_entry_complete,
    load_available_slugs,
    log_detail_heatmaps_ready,
    overview_slug_candidates,
    resolve_overview_slug,
)


def test_overview_slug_candidates_strips_mode_prefix() -> None:
    assert "gullywash" in overview_slug_candidates("cp_gullywash")
    assert overview_slug_candidates("cp_gullywash")[0] == "gullywash"


def test_resolve_overview_slug_prefers_available() -> None:
    avail = frozenset({"process", "gullywash"})
    assert resolve_overview_slug("cp_process", avail) == "process"
    assert resolve_overview_slug("cp_unknown_map_xyz", avail) is None


def test_bounds_entry_complete() -> None:
    assert bounds_entry_complete({"xmin": 0, "xmax": 10, "ymin": 0, "ymax": 10})
    assert not bounds_entry_complete({"xmin": 10, "xmax": 0, "ymin": 0, "ymax": 10})
    assert not bounds_entry_complete({})


def test_log_detail_heatmaps_ready_requires_feature_flag() -> None:
    assert not log_detail_heatmaps_ready(
        "cp_process",
        feature_enabled=False,
        events_indexed=True,
        kill_count=10,
    )
