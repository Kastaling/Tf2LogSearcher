"""Search logic: chat search, stats search, log match. Pure Python, no HTTP."""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.logs_tf import get_log_list_for_player, steamid64_to_steamid3

LOGS_TF_URL_BASE = "https://logs.tf"

# logs.tf player class_stats "type" values we expose to the API (whitelist).
_LOGMATCH_CLASS_TYPES: frozenset[str] = frozenset({
    "scout",
    "soldier",
    "pyro",
    "demoman",
    "heavyweapons",
    "engineer",
    "medic",
    "sniper",
    "spy",
})


def _class_playtime_for_logmatch(stats: dict[str, Any]) -> list[dict[str, Any]]:
    """Per-class playtime in seconds from logs.tf class_stats (longest first)."""
    raw = stats.get("class_stats")
    if not isinstance(raw, list):
        return []
    pairs: list[tuple[str, int]] = []
    for cs in raw:
        if not isinstance(cs, dict):
            continue
        ctype = cs.get("type")
        if not isinstance(ctype, str) or ctype not in _LOGMATCH_CLASS_TYPES:
            continue
        try:
            sec = int(cs.get("total_time") or 0)
        except (TypeError, ValueError):
            continue
        if sec <= 0:
            continue
        pairs.append((ctype, sec))
    pairs.sort(key=lambda x: -x[1])
    return [{"class": a, "seconds": b} for a, b in pairs]


# Limits to prevent runaway queries and huge responses
CHAT_SEARCH_MAX_RESULTS_WITH_STEAMID = 5000   # when showing one player's chat (with or without word filter)
CHAT_CONTEXT_PREVIEW_MAX_CHARS = 220


def local_log_ids_for_player(steamid64: str, logs_dir: str | Path) -> frozenset[int]:
    """Set of log IDs we have locally for this player (intersection of API list and existing files)."""
    logs_dir = Path(logs_dir)
    log_ids = get_log_list_for_player(steamid64)
    return frozenset(lid for lid in log_ids if (logs_dir / f"{lid}.json").exists())


def _player_count_filter(player_count: int, gamemode: str) -> bool:
    """True if player count matches gamemode (hl, 7s, 6s, ud)."""
    if gamemode == "hl":
        return player_count >= 18
    if gamemode == "7s":
        return 14 <= player_count <= 17
    if gamemode == "6s":
        return 12 <= player_count <= 13
    if gamemode == "ud":
        return 4 <= player_count <= 6
    return False


def chat_search(word: str, steamid: str, logs_dir: str | Path) -> tuple[list[dict[str, Any]], int, str | None, frozenset[int]]:
    """
    Search chat for a player. Returns (results, total_count, searched_user_name, log_ids_used).

    log_ids_used: set of log IDs we had locally and considered (for cache invalidation).
    """
    logs_dir = Path(logs_dir)
    word = (word or "").strip()
    steamid = (steamid or "").strip()
    has_word = bool(word)
    word_lower = word.lower() if has_word else ""
    results: list[dict[str, Any]] = []
    log_ids_used: set[int] = set()

    steamid3 = steamid64_to_steamid3(steamid)
    log_ids = get_log_list_for_player(steamid)
    for log_id in log_ids:
        if len(results) >= CHAT_SEARCH_MAX_RESULTS_WITH_STEAMID:
            break
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        log_ids_used.add(log_id)
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue
        chat = logtext.get("chat")
        if not chat:
            continue
        players = logtext.get("players") or {}
        player_info = players.get(steamid3) if isinstance(players, dict) else None
        team_raw = player_info.get("team") if isinstance(player_info, dict) else None
        team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)
        for idx, msg in enumerate(chat):
            if msg.get("steamid") != steamid3:
                continue
            m = msg.get("msg") or ""
            if has_word and word_lower not in m.lower():
                continue
            alias = msg.get("name") or ""
            prev_entry = chat[idx - 1] if idx > 0 else None
            next_entry = chat[idx + 1] if idx + 1 < len(chat) else None

            def _ctx(entry: Any) -> dict[str, Any] | None:
                if not isinstance(entry, dict):
                    return None
                t = str(entry.get("msg") or "").strip()
                if not t:
                    return None
                if len(t) > CHAT_CONTEXT_PREVIEW_MAX_CHARS:
                    t = t[: CHAT_CONTEXT_PREVIEW_MAX_CHARS - 1] + "…"
                n = str(entry.get("name") or "").strip()
                sid3 = str(entry.get("steamid") or "").strip()
                ctx_team = None
                p = players.get(sid3) if isinstance(players, dict) and sid3 else None
                if isinstance(p, dict):
                    tr = p.get("team")
                    ctx_team = "Red" if tr == "Red" else ("Blue" if tr == "Blue" else None)
                return {"name": n, "msg": t, "team": ctx_team}

            results.append({
                "log_id": log_id,
                "alias": alias,
                "msg": m,
                "context_prev": _ctx(prev_entry),
                "context_next": _ctx(next_entry),
                "url": f"{LOGS_TF_URL_BASE}/{log_id}",
                "team": team,
            })
    searched_name = results[0]["alias"] if results else None
    if searched_name is not None:
        searched_name = searched_name.strip() or None
    return results, len(results), searched_name, frozenset(log_ids_used)


def stats_search(
    steamid: str,
    gamemode: str,
    class_list: list[str],
    logs_dir: str | Path,
) -> tuple[list[dict[str, Any]], frozenset[int]]:
    """Stats by gamemode and classes. Returns (rows, log_ids_used) for table rendering and cache invalidation."""
    logs_dir = Path(logs_dir)
    steamid3 = steamid64_to_steamid3(steamid)
    log_ids = get_log_list_for_player(steamid)
    class_set = set(c.strip().lower() for c in class_list if c)
    rows: list[dict[str, Any]] = []
    log_ids_used: set[int] = set()
    for log_id in log_ids:
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        log_ids_used.add(log_id)
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue
        names = logtext.get("names") or {}
        namesid = list(names.keys())
        if not _player_count_filter(len(namesid), gamemode):
            continue
        players = logtext.get("players") or {}
        stats = players.get(steamid3)
        if not stats:
            continue
        classstats = stats.get("class_stats")
        if not classstats:
            continue
        logclasslist = [c.get("type") for c in classstats if c.get("type")]
        for cls in class_set:
            if cls not in logclasslist:
                continue
            idx = logclasslist.index(cls)
            cs = classstats[idx]
            kills = int(cs.get("kills") or 0)
            assists = int(cs.get("assists") or 0)
            deaths = int(cs.get("deaths") or 0)
            if deaths == 0:
                kadr = kills + assists
                kdr = float(kills)
            else:
                kadr = round((kills + assists) / deaths, 2)
                kdr = round(kills / deaths, 2)
            dmg = int(cs.get("dmg") or 0)
            total_time = cs.get("total_time") or 1
            dpm = round((dmg / total_time) * 60, 2)
            hs = stats.get("headshots_hit") or 0
            bs = stats.get("backstabs") or 0
            info = logtext.get("info") or {}
            map_name = info.get("map") or ""
            date_ts = info.get("date") or 0
            date_str = datetime.fromtimestamp(date_ts, tz=timezone.utc).strftime(
                "%I:%M:%S %p %Z %m/%d/%Y"
            )
            alias = names.get(steamid3) or ""
            team_raw = stats.get("team") if isinstance(stats, dict) else None
            team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)
            rows.append({
                "alias": alias,
                "team": team,
                "character": cls,
                "kills": kills,
                "assists": assists,
                "deaths": deaths,
                "kdr": kdr,
                "kadr": kadr,
                "dpm": dpm,
                "dmg": dmg,
                "headshots_hit": hs,
                "backstabs": bs,
                "map": map_name,
                "date": date_str,
                "url": f"{LOGS_TF_URL_BASE}/{log_id}",
            })
    return rows, frozenset(log_ids_used)


def _player_stats_row_logmatch(
    steamid3: str,
    logtext: dict[str, Any],
    *,
    search_input: str,
    steamid64: str,
) -> dict[str, Any] | None:
    """One row of match stats for a player (logs.tf aggregate player block)."""
    players = logtext.get("players") or {}
    if not isinstance(players, dict):
        return None
    stats = players.get(steamid3)
    if not isinstance(stats, dict):
        return None
    names = logtext.get("names") or {}
    alias_raw = names.get(steamid3) if isinstance(names, dict) else ""
    alias = (str(alias_raw).strip() if alias_raw is not None else "") or ""
    team_raw = stats.get("team")
    team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)
    kills = int(stats.get("kills") or 0)
    deaths = int(stats.get("deaths") or 0)
    assists = int(stats.get("assists") or 0)
    if deaths == 0:
        kadr = float(kills + assists)
        kdr = float(kills)
    else:
        kadr = round((kills + assists) / deaths, 2)
        kdr = round(kills / deaths, 2)
    dmg = int(stats.get("dmg") or 0)
    dapm_raw = stats.get("dapm")
    if dapm_raw is not None:
        try:
            dpm = round(float(dapm_raw), 2)
        except (TypeError, ValueError):
            dpm = 0.0
    else:
        info = logtext.get("info") or {}
        length_sec = int(info.get("total_length") or logtext.get("length") or 0)
        if length_sec <= 0:
            length_sec = 1
        dpm = round((dmg / length_sec) * 60, 2)
    hs = int(stats.get("headshots_hit") or stats.get("headshots") or 0)
    bs = int(stats.get("backstabs") or 0)
    ubers = int(stats.get("ubers") or 0)
    drops = int(stats.get("drops") or 0)
    return {
        "alias": alias,
        "team": team,
        "search_input": search_input,
        "resolved_steamid64": steamid64,
        "class_playtime": _class_playtime_for_logmatch(stats),
        "kills": kills,
        "assists": assists,
        "deaths": deaths,
        "kdr": kdr,
        "kadr": kadr,
        "dpm": dpm,
        "dmg": dmg,
        "headshots_hit": hs,
        "backstabs": bs,
        "ubers": ubers,
        "drops": drops,
    }


def log_match(
    steamids: list[str],
    logs_dir: str | Path,
    *,
    search_inputs: list[str] | None = None,
) -> tuple[list[dict[str, Any]], int, frozenset[int]]:
    """Logs where all given players participated. Returns (results, total, matching_log_ids) for cache invalidation."""
    logs_dir = Path(logs_dir)
    if not steamids:
        return [], 0, frozenset()
    labels: list[str] = (
        list(search_inputs)
        if search_inputs is not None and len(search_inputs) == len(steamids)
        else [str(s) for s in steamids]
    )
    steamid3s = [steamid64_to_steamid3(s) for s in steamids]
    steamid3_set = set(steamid3s)
    log_ids = get_log_list_for_player(steamids[0])
    results: list[dict[str, Any]] = []
    matching_log_ids: set[int] = set()
    for log_id in log_ids:
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue
        names = logtext.get("names") or {}
        namesid = set(names.keys())
        if not steamid3_set.issubset(namesid):
            continue
        matching_log_ids.add(log_id)
        info = logtext.get("info") or {}
        title = info.get("title") or ""
        map_name = info.get("map") or ""
        date_ts = int(info.get("date") or 0)
        date_str = datetime.fromtimestamp(date_ts, tz=timezone.utc).strftime(
            "%m/%d/%Y %I:%M:%S %p %Z"
        )
        player_stats: list[dict[str, Any]] = []
        for i, sid3 in enumerate(steamid3s):
            row = _player_stats_row_logmatch(
                sid3,
                logtext,
                search_input=labels[i],
                steamid64=str(steamids[i]),
            )
            if row is not None:
                player_stats.append(row)
        results.append({
            "log_id": log_id,
            "title": title,
            "map": map_name,
            "date": date_str,
            "date_ts": date_ts,
            "url": f"{LOGS_TF_URL_BASE}/{log_id}",
            "player_stats": player_stats,
        })
    return results, len(results), frozenset(matching_log_ids)


def log_match_matching_log_ids(steamids: list[str], logs_dir: str | Path) -> frozenset[int]:
    """Return the set of log IDs that contain all given players (for cache invalidation without building full result)."""
    logs_dir = Path(logs_dir)
    if not steamids:
        return frozenset()
    steamid3s = [steamid64_to_steamid3(s) for s in steamids]
    steamid3_set = set(steamid3s)
    log_ids = get_log_list_for_player(steamids[0])
    out: set[int] = set()
    for log_id in log_ids:
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue
        names = logtext.get("names") or {}
        if steamid3_set.issubset(set(names.keys())):
            out.add(log_id)
    return frozenset(out)
