#!/usr/bin/env python3
"""
Regenerate app/weapon_names.py from static/items_game.txt and static/tf_english.txt.

Run from repo root:  python scripts/build_weapon_names.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ITEMS_GAME = ROOT / "static" / "items_game.txt"
TF_ENGLISH = ROOT / "static" / "tf_english.txt"
OUT = ROOT / "app" / "weapon_names.py"

# Log weapon names that never appear as item_logname — map to TF_english token (no #).
LOG_TOKEN_ALIASES: dict[str, str] = {
    "tf_projectile_rocket": "TF_Weapon_RocketLauncher",
    "tf_projectile_pipe": "TF_Weapon_GrenadeLauncher",
    "tf_projectile_pipe_remote": "TF_Weapon_PipebombLauncher",
    "tf_projectile_arrow": "TF_Weapon_CompoundBow",
    "compound_bow": "TF_Weapon_CompoundBow",
    "bat_fish": "TF_TheHolyMackerel",
    "obj_sentrygun": "TF_Object_Sentry",
    "obj_attachment_sapper": "TF_Weapon_Spy_Sapper",
    "pda_engineer": "TF_Weapon_PDA_Engineer_Builder",
}

# Prefer friendlier labels than raw token or English file defaults.
DISPLAY_OVERRIDES: dict[str, str] = {
    "world": "World",
    "player": "World (environmental)",
    "tf_projectile_arrow": "Huntsman",
    "compound_bow": "Huntsman",
    "deflect_rocket": "Reflected rocket",
    "deflect_promode": "Reflected rocket",
    "deflect_flare": "Reflected flare",
    "deflect_arrow": "Reflected arrow",
    "deflect_sticky": "Reflected stickybomb",
    "deflect_ball": "Reflected baseball",
    "obj_sentrygun": "Sentry Gun (Level 1)",
    "obj_sentrygun2": "Sentry Gun (Level 2)",
    "obj_sentrygun3": "Sentry Gun (Level 3)",
    "obj_minisentry": "Mini-Sentry Gun",
    "bleed_kill": "Bleed",
    "trigger_hurt": "World",
    "environmental_kill": "Environmental",
    "suicide": "Suicide",
    "feign_death": "Feign death",
    "hammer_kill": "Hammer unit",
    "taunt_sniper": "Fencing taunt",
    "tfguild": "Merasmus / Halloween",
    "building_carried_dispenser": "Dispenser (building)",
    "building_carried_sentrygun": "Sentry Gun (building)",
    "building_carried_teleporter": "Teleporter (building)",
    # Stock multiclass pistol / shotgun: logs.tf uses per-class keys; TF strings look redundant.
    "pistol_scout": "Pistol",
    "shotgun_primary": "Shotgun",
    "shotgun_soldier": "Shotgun",
    "shotgun_pyro": "Shotgun",
    "shotgun_hwg": "Shotgun",
}


def _find_matching_brace(s: str, open_idx: int) -> int:
    depth = 0
    i = open_idx
    while i < len(s):
        c = s[i]
        if c == '"':
            i += 1
            while i < len(s):
                if s[i] == "\\":
                    i += 2
                    continue
                if s[i] == '"':
                    break
                i += 1
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise ValueError("unbalanced braces")


def _extract_top_level_section(text: str, section_key: str) -> str:
    """Match only root-level blocks: one tab, then \"section_key\", newline, tab, {."""
    pat = f'\n\t"{section_key}"\n\t{{'
    idx = text.find(pat)
    if idx < 0:
        raise SystemExit(f"top-level section {section_key!r} not found")
    brace = idx + len(pat) - 1
    end = _find_matching_brace(text, brace)
    return text[brace + 1 : end]


def _iter_kv_string_pairs(blob: str) -> Iterator[tuple[str, str]]:
    """Yield (key, value) for top-level "key" "value" pairs; skip nested { }."""
    i = 0
    n = len(blob)
    while i < n:
        while i < n and blob[i] in " \t\n\r":
            i += 1
        if i >= n:
            break
        if blob[i] == "/":
            while i < n and blob[i] != "\n":
                i += 1
            continue
        if blob[i] != '"':
            i += 1
            continue
        i += 1
        ks = i
        while i < n and blob[i] != '"':
            i += 1
        key = blob[ks:i]
        i += 1
        while i < n and blob[i] in " \t\n\r":
            i += 1
        if i < n and blob[i] == "{":
            end = _find_matching_brace(blob, i)
            i = end + 1
            continue
        if i >= n or blob[i] != '"':
            continue
        i += 1
        vs = i
        while i < n and blob[i] != '"':
            i += 1
        val = blob[vs:i]
        i += 1
        yield key, val


def _split_top_blocks(blob: str) -> dict[str, str]:
    out: dict[str, str] = {}
    i = 0
    n = len(blob)
    while i < n:
        while i < n and blob[i] in " \t\n\r":
            i += 1
        if i >= n:
            break
        if blob[i] == "/":
            while i < n and blob[i] != "\n":
                i += 1
            continue
        if blob[i] != '"':
            i += 1
            continue
        i += 1
        ks = i
        while i < n and blob[i] != '"':
            i += 1
        key = blob[ks:i]
        i += 1
        while i < n and blob[i] in " \t\n\r":
            i += 1
        if i >= n or blob[i] != "{":
            continue
        end = _find_matching_brace(blob, i)
        inner = blob[i + 1 : end]
        out[key] = inner
        i = end + 1
    return out


def _parse_tf_tokens(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    idx = text.find('"Tokens"')
    if idx < 0:
        raise SystemExit("Tokens not found in tf_english")
    i = idx + len('"Tokens"')
    while i < len(text) and text[i] in " \t\n\r":
        i += 1
    if i >= len(text) or text[i] != "{":
        raise SystemExit("Tokens brace not found")
    end = _find_matching_brace(text, i)
    inner = text[i + 1 : end]
    lang: dict[str, str] = {}
    for k, v in _iter_kv_string_pairs(inner):
        lang[k] = v.replace("\\n", "\n")
    return lang


def _resolve_prefab(name: str, blocks: dict[str, str], memo: dict[str, dict[str, str]]) -> dict[str, str]:
    if name in memo:
        return memo[name]
    if name not in blocks:
        memo[name] = {}
        return memo[name]
    flat = dict(_iter_kv_string_pairs(blocks[name]))
    if "prefab" not in flat:
        memo[name] = flat
        return flat
    merged: dict[str, str] = {}
    for part in flat["prefab"].split():
        sub = _resolve_prefab(part, blocks, memo)
        merged.update(sub)
    merged.update(flat)
    memo[name] = merged
    return merged


def _display_from_kvs(kvs: dict[str, str], lang: dict[str, str]) -> str | None:
    for fld in ("item_name", "item_type_name"):
        if fld not in kvs:
            continue
        raw = kvs[fld]
        if raw.startswith("#"):
            stem = raw[1:]
            if stem in lang:
                s = lang[stem]
                return re.sub(r"[\n\r\t]", " ", s).strip()
        elif raw in lang:
            s = lang[raw]
            return re.sub(r"[\n\r\t]", " ", s).strip()
    if "name" in kvs and kvs["name"] in lang:
        s = lang[kvs["name"]]
        return re.sub(r"[\n\r\t]", " ", s).strip()
    return None


def _infer_logname(kvs: dict[str, str]) -> str | None:
    if "item_logname" in kvs:
        return kvs["item_logname"]
    ic = kvs.get("item_class", "")
    if ic.startswith("tf_weapon_"):
        return ic[len("tf_weapon_") :]
    if "particle_suffix" in kvs:
        return kvs["particle_suffix"]
    return None


def build_mapping() -> dict[str, str]:
    lang = _parse_tf_tokens(TF_ENGLISH)
    ig = ITEMS_GAME.read_text(encoding="utf-8", errors="replace")
    prefab_inner = _extract_top_level_section(ig, "prefabs")
    prefab_blocks = _split_top_blocks(prefab_inner)
    items_inner = _extract_top_level_section(ig, "items")
    item_blocks = _split_top_blocks(items_inner)
    # Item defs win over prefab templates on name collision (should not happen).
    blocks = {**prefab_blocks, **item_blocks}
    memo: dict[str, dict[str, str]] = {}
    out: dict[str, str] = {}

    for key, content in item_blocks.items():
        try:
            kvs = dict(_iter_kv_string_pairs(content))
        except Exception:
            continue
        resolved: dict[str, str]
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
        tags = resolved.get("tags")
        if not tags:  # not all weapons have tags at top level after merge
            pass

        ln = _infer_logname(resolved)
        if not ln:
            continue
        disp = _display_from_kvs(resolved, lang)
        if disp:
            # Strip accelerator markers like &1
            disp = re.sub(r"&\d\s*", "", disp)
            disp = disp.strip()
        newv = disp or ""
        if ln not in out:
            out[ln] = newv
        elif not out[ln] and newv:
            out[ln] = newv

    # Aliases / environmental
    for log_key, token in LOG_TOKEN_ALIASES.items():
        if log_key in out and out[log_key]:
            continue
        if token in lang:
            out[log_key] = re.sub(r"[\n\r\t]", " ", lang[token]).strip()
        else:
            out[log_key] = ""

    out.update(DISPLAY_OVERRIDES)

    return out


def emit_py(m: dict[str, str]) -> str:
    from datetime import date

    today = date.today().isoformat()
    keys = sorted(m.keys())
    lines = [
        '"""',
        "Mapping from logs.tf internal weapon names to human-readable display names.",
        "",
        "Auto-generated by scripts/build_weapon_names.py from static/items_game.txt and static/tf_english.txt",
        f"(items_game item_logname / inferred log names). Regenerate date: {today}.",
        "",
        "Values that are empty strings could not be resolved from localization; they fall back to the raw",
        "internal name in get_weapon_name().",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "WEAPON_NAMES: dict[str, str] = {",
    ]
    for k in keys:
        v = m[k]
        ks = repr(k)
        vs = repr(v)
        lines.append(f"    {ks}: {vs},")
    lines += [
        "}",
        "",
        "",
        "def get_weapon_name(internal_name: str) -> str:",
        '    """',
        "    Return the display name for an internal weapon name.",
        "    Falls back to the raw internal name if not mapped or mapping is empty.",
        '    """',
        '    name = WEAPON_NAMES.get(internal_name or "", "")',
        "    return name if name else (internal_name or \"\")",
        "",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    if not ITEMS_GAME.is_file():
        print("missing", ITEMS_GAME, file=sys.stderr)
        sys.exit(1)
    if not TF_ENGLISH.is_file():
        print("missing", TF_ENGLISH, file=sys.stderr)
        sys.exit(1)
    m = build_mapping()
    OUT.write_text(emit_py(m), encoding="utf-8")
    print(f"wrote {OUT} ({len(m)} entries)")


if __name__ == "__main__":
    main()
