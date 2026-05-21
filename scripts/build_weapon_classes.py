#!/usr/bin/env python3
"""
Regenerate app/weapon_classes.py from static/items_game.txt (item_logname -> used_by_classes).

Run from repo root:  python scripts/build_weapon_classes.py
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.build_weapon_names import (  # noqa: E402
    ITEMS_GAME,
    LOG_TOKEN_ALIASES,
    _extract_top_level_section,
    _infer_logname,
    _iter_kv_string_pairs,
    _resolve_prefab,
    _split_top_blocks,
)

OUT = ROOT / "app" / "weapon_classes.py"

# logs.tf uses heavyweapons; items_game used_by_classes uses heavy.
_CLASS_REMAP = {"heavy": "heavyweapons"}

# Log names with no item_logname row — assign playable class(es).
LOG_CLASS_ALIASES: dict[str, tuple[str, ...]] = {
    "tf_projectile_rocket": ("soldier",),
    "tf_projectile_pipe": ("demoman",),
    "tf_projectile_pipe_remote": ("demoman",),
    "tf_projectile_arrow": ("sniper",),
    "compound_bow": ("sniper",),
    "world": (
        "scout",
        "soldier",
        "pyro",
        "demoman",
        "heavyweapons",
        "engineer",
        "medic",
        "sniper",
        "spy",
    ),
    "player": (
        "scout",
        "soldier",
        "pyro",
        "demoman",
        "heavyweapons",
        "engineer",
        "medic",
        "sniper",
        "spy",
    ),
    "environmental_kill": (
        "scout",
        "soldier",
        "pyro",
        "demoman",
        "heavyweapons",
        "engineer",
        "medic",
        "sniper",
        "spy",
    ),
    "trigger_hurt": (
        "scout",
        "soldier",
        "pyro",
        "demoman",
        "heavyweapons",
        "engineer",
        "medic",
        "sniper",
        "spy",
    ),
    "suicide": (
        "scout",
        "soldier",
        "pyro",
        "demoman",
        "heavyweapons",
        "engineer",
        "medic",
        "sniper",
        "spy",
    ),
    "bleed_kill": (
        "scout",
        "soldier",
        "pyro",
        "demoman",
        "heavyweapons",
        "engineer",
        "medic",
        "sniper",
        "spy",
    ),
    "deflect_rocket": ("pyro",),
    "deflect_promode": ("pyro",),
    "deflect_flare": ("pyro",),
    "deflect_arrow": ("pyro",),
    "deflect_sticky": ("pyro",),
    "deflect_ball": ("pyro",),
    "obj_sentrygun": ("engineer",),
    "obj_sentrygun2": ("engineer",),
    "obj_sentrygun3": ("engineer",),
    "obj_minisentry": ("engineer",),
    "obj_attachment_sapper": ("spy",),
    "pda_engineer": ("engineer",),
    "builder": ("engineer",),
    "building_carried_dispenser": ("engineer",),
    "building_carried_sentrygun": ("engineer",),
    "building_carried_teleporter": ("engineer",),
    "shotgun_primary": ("engineer",),
    "shotgun_soldier": ("soldier",),
    "shotgun_pyro": ("pyro",),
    "shotgun_hwg": ("heavyweapons",),
    "pistol_scout": ("scout",),
    "pistol": ("scout", "soldier", "pyro", "engineer"),
}

_SUFFIX_CLASS: tuple[tuple[str, str], ...] = (
    ("_scout", "scout"),
    ("_soldier", "soldier"),
    ("_pyro", "pyro"),
    ("_demoman", "demoman"),
    ("_hwg", "heavyweapons"),
    ("_heavy", "heavyweapons"),
    ("_engineer", "engineer"),
    ("_medic", "medic"),
    ("_sniper", "sniper"),
    ("_spy", "spy"),
)

PLAYABLE_CLASSES: tuple[str, ...] = (
    "scout",
    "soldier",
    "pyro",
    "demoman",
    "heavyweapons",
    "engineer",
    "medic",
    "sniper",
    "spy",
)


def _parse_used_by_classes(blob: str) -> list[str]:
    m = re.search(r'"used_by_classes"\s*\n?\s*\{([^}]*)\}', blob, re.DOTALL)
    if not m:
        return []
    out: list[str] = []
    for cls in re.findall(r'"([a-z_]+)"\s*"1"', m.group(1)):
        norm = _CLASS_REMAP.get(cls, cls)
        if norm in PLAYABLE_CLASSES and norm not in out:
            out.append(norm)
    return out


def _suffix_classes(logname: str) -> list[str]:
    for suf, cls in _SUFFIX_CLASS:
        if logname.endswith(suf):
            return [cls]
    return []


def build_weapon_classes() -> dict[str, tuple[str, ...]]:
    ig = ITEMS_GAME.read_text(encoding="utf-8", errors="replace")
    prefab_inner = _extract_top_level_section(ig, "prefabs")
    prefab_blocks = _split_top_blocks(prefab_inner)
    items_inner = _extract_top_level_section(ig, "items")
    item_blocks = _split_top_blocks(items_inner)
    blocks = {**prefab_blocks, **item_blocks}
    memo: dict[str, dict[str, str]] = {}

    log_to_classes: dict[str, set[str]] = defaultdict(set)

    for _key, content in {**prefab_blocks, **item_blocks}.items():
        try:
            kvs = dict(_iter_kv_string_pairs(content))
        except Exception:
            continue
        if "prefab" in kvs:
            base: dict[str, str] = {}
            for part in kvs["prefab"].split():
                base.update(_resolve_prefab(part, blocks, memo))
            base.update(kvs)
            resolved = base
        else:
            resolved = kvs
        if resolved.get("item_class", "").startswith("tf_wearable"):
            continue
        ln = _infer_logname(resolved)
        if not ln:
            continue
        for cls in _parse_used_by_classes(content):
            log_to_classes[ln].add(cls)

    for log_key, classes in LOG_CLASS_ALIASES.items():
        log_to_classes[log_key].update(classes)

    for log_key in LOG_TOKEN_ALIASES:
        if log_key not in log_to_classes:
            for cls in _suffix_classes(log_key):
                log_to_classes[log_key].add(cls)

    # Ensure every known log name from weapon_names has at least suffix / alias class.
    from app.weapon_names import WEAPON_NAMES  # noqa: E402

    for log_key in WEAPON_NAMES:
        if log_key in log_to_classes and log_to_classes[log_key]:
            continue
        for cls in _suffix_classes(log_key):
            log_to_classes[log_key].add(cls)

    by_class: dict[str, list[str]] = {c: [] for c in PLAYABLE_CLASSES}
    for log_key, clset in sorted(log_to_classes.items()):
        if not clset:
            continue
        for cls in sorted(clset):
            if cls in by_class and log_key not in by_class[cls]:
                by_class[cls].append(log_key)

    for cls in PLAYABLE_CLASSES:
        by_class[cls].sort(key=lambda w: (w.replace("_", " "), w))

    return {c: tuple(by_class[c]) for c in PLAYABLE_CLASSES}


def emit_py(by_class: dict[str, tuple[str, ...]]) -> str:
    today = date.today().isoformat()
    lines = [
        '"""',
        "Weapon internal log names grouped by playable class (leaderboard weapon picker).",
        "",
        "Auto-generated by scripts/build_weapon_classes.py from static/items_game.txt.",
        f"Regenerate date: {today}.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "PLAYABLE_CLASSES: tuple[str, ...] = (",
    ]
    for c in PLAYABLE_CLASSES:
        lines.append(f'    "{c}",')
    lines.append(")")
    lines.append("")
    lines.append("WEAPONS_BY_CLASS: dict[str, tuple[str, ...]] = {")
    for c in PLAYABLE_CLASSES:
        weapons = by_class.get(c, ())
        lines.append(f'    "{c}": (')
        for w in weapons:
            lines.append(f'        "{w}",')
        lines.append("    ),")
    lines.append("}")
    lines.append("")
    lines.append("WEAPON_KILL_WHITELIST: frozenset[str] = frozenset(")
    lines.append("    w for weapons in WEAPONS_BY_CLASS.values() for w in weapons")
    lines.append(")")
    lines.append("")
    lines.append("")
    lines.append("def weapons_for_class(player_class: str) -> tuple[str, ...]:")
    lines.append('    """Return weapon log names for a playable class (empty if unknown)."""')
    lines.append("    pc = (player_class or \"\").strip().lower()")
    lines.append("    return WEAPONS_BY_CLASS.get(pc, ())")
    lines.append("")
    lines.append("")
    lines.append("def is_weapon_kill_whitelisted(weapon: str) -> bool:")
    lines.append('    """True if weapon is a known logs.tf weapon id (parameterized leaderboard filter)."""')
    lines.append("    w = (weapon or \"\").strip().lower()")
    lines.append("    return bool(w) and w in WEAPON_KILL_WHITELIST")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    by_class = build_weapon_classes()
    OUT.write_text(emit_py(by_class), encoding="utf-8")
    total = sum(len(v) for v in by_class.values())
    unique = len({w for weapons in by_class.values() for w in weapons})
    print(f"Wrote {OUT} ({total} class-weapon entries, {unique} unique weapons)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
