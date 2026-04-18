"""Unit tests for logs.tf damage taken extraction into ``log_players``."""

from app.stats_db import _damage_taken_from_player_stats


def test_dt_null_falls_through_to_dt_real():
    stats = {"dt": None, "dt_real": 8451}
    assert _damage_taken_from_player_stats(stats) == 8451


def test_explicit_zero_damage_taken_is_honored():
    stats = {"damage_taken": 0}
    assert _damage_taken_from_player_stats(stats) == 0


def test_prefers_named_fields_before_compact_keys():
    stats = {"dt": None, "dt_real": None, "damage_taken": 3200}
    assert _damage_taken_from_player_stats(stats) == 3200


def test_class_stats_fallback_sums_per_class():
    stats = {
        "class_stats": [
            {"type": "soldier", "dmg": 1000, "dt": 100},
            {"type": "scout", "dmg": 500, "dt_real": 50},
        ]
    }
    assert _damage_taken_from_player_stats(stats) == 150


def test_top_level_wins_over_class_stats():
    stats = {
        "damage_taken": 999,
        "class_stats": [{"type": "soldier", "dt": 1}],
    }
    assert _damage_taken_from_player_stats(stats) == 999
