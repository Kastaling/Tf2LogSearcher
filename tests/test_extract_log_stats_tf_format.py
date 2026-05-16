"""Regression: logs.tf /json puts weapons under class_stats and healspread/classkills at log root."""

from __future__ import annotations

from app.stats_db import extract_log_stats


def test_extract_logs_tf_class_stats_weapons_and_root_healspread_classkills() -> None:
    """Typical /json shape: no top-level player.weapon; breakdowns live elsewhere."""
    logtext = {
        "info": {"map": "cp_process", "date": 1_700_000_000, "total_length": 600},
        "players": {
            "[U:1:10]": {
                "team": "Red",
                "kills": 3,
                "assists": 0,
                "deaths": 1,
                "dmg": 300,
                "class_stats": [
                    {
                        "type": "soldier",
                        "total_time": 600,
                        "kills": 3,
                        "dmg": 300,
                        "weapon": {
                            "rocketlauncher": {"kills": 2, "dmg": 200, "shots": 8, "hits": 3},
                            "shotgun_soldier": {"kills": 1, "dmg": 100, "shots": 4, "hits": 2},
                        },
                    },
                ],
            },
            "[U:1:20]": {
                "team": "Blue",
                "kills": 0,
                "assists": 0,
                "deaths": 0,
                "dmg": 0,
                "class_stats": [],
            },
        },
        "healspread": {
            "[U:1:20]": {"[U:1:10]": 1500},
        },
        "classkills": {
            "[U:1:10]": {"medic": 2, "scout": 1},
        },
    }
    data = extract_log_stats(42, logtext)
    weapons = {(r["steamid64"], r["weapon"]): r for r in data["weapon_rows"]}
    sid10 = "76561197960265738"  # [U:1:10]
    assert (sid10, "rocketlauncher") in weapons
    assert weapons[sid10, "rocketlauncher"]["kills"] == 2
    assert weapons[sid10, "shotgun_soldier"]["kills"] == 1

    assert len(data["healspread_rows"]) == 1
    h = data["healspread_rows"][0]
    assert h["healing"] == 1500
    # healer [U:1:20], patient [U:1:10]
    assert h["healer_steamid64"] == "76561197960265748"
    assert h["patient_steamid64"] == sid10

    ck = {(r["steamid64"], r["victim_class"]): r["kills"] for r in data["classkill_rows"]}
    assert ck[(sid10, "medic")] == 2
    assert ck[(sid10, "scout")] == 1


def test_extract_merges_top_level_weapon_with_class_stats() -> None:
    logtext = {
        "info": {"map": "cp_badlands", "date": 1, "total_length": 120},
        "players": {
            "[U:1:7]": {
                "team": "Red",
                "kills": 2,
                "assists": 0,
                "deaths": 0,
                "dmg": 50,
                "weapon": {
                    "scattergun": {"kills": 1, "dmg": 25, "shots": 0, "hits": 0},
                },
                "class_stats": [
                    {
                        "type": "scout",
                        "total_time": 120,
                        "kills": 2,
                        "dmg": 50,
                        "weapon": {
                            "scattergun": {"kills": 1, "dmg": 25, "shots": 2, "hits": 1},
                        },
                    },
                ],
            },
        },
    }
    data = extract_log_stats(1, logtext)
    w = next(r for r in data["weapon_rows"] if r["weapon"] == "scattergun")
    assert w["kills"] == 2
    assert w["damage"] == 50
    assert w["shots"] == 2
    assert w["hits"] == 1
    assert w["avg_damage"] is not None and abs(w["avg_damage"] - 50.0) < 0.01
