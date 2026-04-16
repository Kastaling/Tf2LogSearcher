"""
Heuristics for detecting logs.tf uploads that merge multiple games (“combined” logs).

Used to exclude inflated per-game stats from leaderboards and profile “top logs”. Rules are
conservative phrase matches and structural checks (multiple ``logs.tf/<id>`` URLs in one chat line),
not tied to a single third-party tool.

Callers can use the Python helpers for JSON/logtext inspection, or ``stats_log_exclusion_sql`` /
``chat_log_exclusion_exists_sql`` (with ``ATTACH DATABASE ... AS chat``) for SQL filters.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

# --- Title / metadata (``logs.title`` in stats DB) ---
_TITLE_COMBINED_NEEDLES: tuple[str, ...] = (
    "combined log",
    "logs were combined",
    "log was combined",
)
# e.g. ``PLAT GRAND FINALS (DK VS HOOD) (2-1)`` — match / series score tail, not a single-game title.
_TITLE_SERIES_SCORE_TAIL_RE = re.compile(r"\)\s*\(\s*\d+\s*-\s*\d+\s*\)\s*\Z")
# Must match ``stats_log_exclusion_sql`` (``length(trim(title))`` threshold).
_TITLE_SERIES_SCORE_MIN_LEN = 12

# --- Chat message text (``chat_messages.msg``) ---
_CHAT_MSG_COMBINED_NEEDLES: tuple[str, ...] = (
    "logs were combined",
    "log was combined",
    "following logs were combined",
    "the following logs were combined",
    "combined using",
    "were combined:",
)

_LOGS_TF_ID_RE = re.compile(r"logs\.tf/\d+", re.IGNORECASE)
# Length of ``logs.tf/`` — character after it must be a digit for a real log ID (matches Python regex).
_LOGS_TF_PATH_PREFIX_LEN = len("logs.tf/")
_CHAT_ATTACH_NAME = "chat"


def map_field_suggests_combined_log(map_name: str | None) -> bool:
    """
    True if the map field is missing/blank (common when combiners skip it) or lists multiple maps.

    Detects comma / ``+`` separators, and **whitespace-separated tokens** (e.g. ``upwd steel cascade``
    from tools that paste several short map names with spaces and no other delimiter).

    Also flags a **single token with no underscore** (e.g. ``GG``): real logs.tf maps are almost
    always ``snake_case`` with a prefix (``cp_``, ``koth_``, ``pl_``, …); lone words are usually
    placeholders or junk.
    """
    if map_name is None:
        return True
    s = map_name.strip()
    if not s:
        return True
    if "," in s or "+" in s:
        return True
    # Any run of whitespace => 2+ tokens (logs.tf map names are almost always single ``snake_case``).
    # ``str.split()`` treats ASCII and Unicode whitespace (incl. NBSP) as separators.
    if len(s.split()) >= 2:
        return True
    token = s.split()[0]
    if "_" not in token:
        return True
    return False


def title_suggests_combined_log(title: str | None) -> bool:
    """True if ``logs.title`` contains known combined-log phrases (case-insensitive)."""
    if not title:
        return False
    low = title.lower()
    if any(n in low for n in _TITLE_COMBINED_NEEDLES):
        return True
    t = title.strip()
    if len(t) >= _TITLE_SERIES_SCORE_MIN_LEN and _TITLE_SERIES_SCORE_TAIL_RE.search(t):
        return True
    return False


def chat_alias_suggests_combined_bot(alias: str | None) -> bool:
    """True for fake client names like ``Jack's Log Combiner`` (substring ``log combiner``)."""
    if not alias:
        return False
    return "log combiner" in alias.lower()


def chat_message_suggests_combined_log(message: str) -> bool:
    """
    True if a single chat line looks like a merge announcement (phrases and/or two ``logs.tf`` IDs).
    """
    if not message:
        return False
    low = message.lower()
    if any(n in low for n in _CHAT_MSG_COMBINED_NEEDLES):
        return True
    if len(_LOGS_TF_ID_RE.findall(message)) >= 2:
        return True
    return False


def log_metadata_suggests_combined_log(*, map_name: str | None, title: str | None) -> bool:
    """OR of map-field and title heuristics (stats ``logs`` row only, no chat)."""
    return map_field_suggests_combined_log(map_name) or title_suggests_combined_log(title)


def logtext_suggests_combined_log(logtext: dict[str, Any]) -> bool:
    """
    Inspect a logs.tf-style JSON dict (same shape as stored ``(id).json``) for combined-log signals.

    Checks ``info.map``, ``info.title``, and ``chat`` entries (msg + name).
    """
    info = logtext.get("info") if isinstance(logtext.get("info"), dict) else {}
    map_raw = info.get("map")
    map_name = None if map_raw is None else str(map_raw)
    title_raw = info.get("title")
    title = None if title_raw is None else str(title_raw)
    if log_metadata_suggests_combined_log(map_name=map_name, title=title):
        return True
    chat = logtext.get("chat")
    if not isinstance(chat, list):
        return False
    for entry in chat:
        if not isinstance(entry, dict):
            continue
        msg = entry.get("msg")
        if isinstance(msg, str) and chat_message_suggests_combined_log(msg):
            return True
        alias = entry.get("name")
        if isinstance(alias, str) and chat_alias_suggests_combined_bot(alias):
            return True
    return False


def _title_series_score_tail_sql(table_alias: str) -> str:
    """
    SQL expression (truthy when non-zero) matching ``_TITLE_SERIES_SCORE_TAIL_RE`` on ``trim(title)``.

    Walks **backward** from the character before the final ``)`` (suffix ``)\\s*(\\s*\\d+\\s*-\\s*\\d+\\s*)``),
    mirroring ``re.compile(r"\\)\\s*\\(\\s*\\d+\\s*-\\s*\\d+\\s*\\)\\s*\\Z")`` on stripped titles.
    """
    t = table_alias
    s = f"trim({t}.title)"
    m = _TITLE_SERIES_SCORE_MIN_LEN
    # ``\\s`` for typical logs.tf titles (ASCII + NBSP).
    ws = (
        f"(substr({s}, walk.pos, 1) IN (' ', char(9), char(10), char(13), char(160)))"
    )
    ch = f"substr({s}, walk.pos, 1)"
    return f"""(
  length({s}) >= {m}
  AND substr({s}, length({s}), 1) = ')'
  AND EXISTS (
    WITH RECURSIVE
    walk(pos, phase, n2, n1) AS (
      SELECT length({s}) - 1, 'd2', 0, 0
      UNION ALL
      SELECT
        CASE phase
          WHEN 'd2' THEN
            CASE
              WHEN {ch} GLOB '[0-9]' THEN walk.pos - 1
              WHEN walk.n2 > 0 AND {ws} THEN walk.pos - 1
              WHEN walk.n2 > 0 AND {ch} = '-' THEN walk.pos - 1
              ELSE walk.pos
            END
          WHEN 'wh' THEN
            CASE
              WHEN {ws} THEN walk.pos - 1
              WHEN {ch} = '-' THEN walk.pos - 1
              ELSE walk.pos
            END
          WHEN 'd1' THEN
            CASE
              WHEN {ch} GLOB '[0-9]' THEN walk.pos - 1
              WHEN walk.n1 > 0 AND {ws} THEN walk.pos - 1
              WHEN walk.n1 > 0 AND {ch} = '(' THEN walk.pos - 1
              ELSE walk.pos
            END
          WHEN 'wo' THEN
            CASE
              WHEN {ws} THEN walk.pos - 1
              WHEN {ch} = '(' THEN walk.pos - 1
              ELSE walk.pos
            END
          WHEN 'wr' THEN
            CASE
              WHEN {ws} THEN walk.pos - 1
              WHEN {ch} = ')' THEN walk.pos - 1
              ELSE walk.pos
            END
          ELSE walk.pos
        END,
        CASE phase
          WHEN 'd2' THEN
            CASE
              WHEN {ch} GLOB '[0-9]' THEN 'd2'
              WHEN walk.n2 > 0 AND {ws} THEN 'wh'
              WHEN walk.n2 > 0 AND {ch} = '-' THEN 'd1'
              ELSE 'fail'
            END
          WHEN 'wh' THEN
            CASE
              WHEN {ws} THEN 'wh'
              WHEN {ch} = '-' THEN 'd1'
              ELSE 'fail'
            END
          WHEN 'd1' THEN
            CASE
              WHEN {ch} GLOB '[0-9]' THEN 'd1'
              WHEN walk.n1 > 0 AND {ws} THEN 'wo'
              WHEN walk.n1 > 0 AND {ch} = '(' THEN 'wr'
              ELSE 'fail'
            END
          WHEN 'wo' THEN
            CASE
              WHEN {ws} THEN 'wo'
              WHEN {ch} = '(' THEN 'wr'
              ELSE 'fail'
            END
          WHEN 'wr' THEN
            CASE
              WHEN {ws} THEN 'wr'
              WHEN {ch} = ')' THEN 'ok'
              ELSE 'fail'
            END
          ELSE 'fail'
        END,
        CASE WHEN phase = 'd2' AND {ch} GLOB '[0-9]' THEN walk.n2 + 1 ELSE walk.n2 END,
        CASE WHEN phase = 'd1' AND {ch} GLOB '[0-9]' THEN walk.n1 + 1 ELSE walk.n1 END
      FROM walk
      WHERE walk.phase NOT IN ('ok', 'fail') AND walk.pos >= 1
    )
    SELECT 1 FROM walk WHERE walk.phase = 'ok' LIMIT 1
  )
)"""


def stats_log_exclusion_sql(table_alias: str = "l") -> str:
    """
    SQL fragment: `` AND NOT ( <combined> )`` for the stats ``logs`` table.

    Excludes empty/missing map, multi-map field (including space/tab/newline-separated tokens),
    single-token maps without ``_``, title phrases, and series-score titles (aligned with Python
    ``_TITLE_SERIES_SCORE_TAIL_RE``).
    Parameter-free (safe to splice).
    """
    t = table_alias
    title_ors = " OR ".join(
        f"instr(lower({t}.title), '{_sql_quote(n)}') > 0" for n in _TITLE_COMBINED_NEEDLES
    )
    # Mirror ``split()``-style multi-token detection: space, tab, LF, CR, NBSP (common paste).
    map_ws = (
        f"(instr(trim({t}.map), ' ') > 0 OR instr(trim({t}.map), char(9)) > 0"
        f" OR instr(trim({t}.map), char(10)) > 0 OR instr(trim({t}.map), char(13)) > 0"
        f" OR instr(trim({t}.map), char(160)) > 0)"
    )
    # One segment, non-empty, no underscore — not ``cp_*`` / ``koth_*`` style (e.g. ``GG``).
    map_ws_neg = (
        f"(NOT (instr(trim({t}.map), ' ') > 0 OR instr(trim({t}.map), char(9)) > 0"
        f" OR instr(trim({t}.map), char(10)) > 0 OR instr(trim({t}.map), char(13)) > 0"
        f" OR instr(trim({t}.map), char(160)) > 0))"
    )
    map_no_underscore = (
        f"(trim({t}.map) != '' AND instr(trim({t}.map), '_') = 0 AND {map_ws_neg})"
    )
    # Series score suffix — aligned with ``_TITLE_SERIES_SCORE_TAIL_RE`` / ``title_suggests_combined_log``.
    title_series_sql = _title_series_score_tail_sql(t)
    return (
        f" AND NOT ("
        f"({t}.map IS NULL OR trim({t}.map) = '')"
        f" OR instr({t}.map, ',') > 0 OR instr({t}.map, '+') > 0"
        f" OR {map_ws}"
        f" OR {map_no_underscore}"
        f" OR ({title_ors})"
        f" OR {title_series_sql}"
        f")"
    )


def _sql_quote(s: str) -> str:
    return s.replace("'", "''")


def _chat_msg_two_logs_tf_id_urls_sql(cm: str) -> str:
    """
    True when ``msg`` has two ``logs.tf/<digits>``-style links (aligned with ``_LOGS_TF_ID_RE``).

    Uses ``instr`` / ``substr`` only (no ``replace`` length hack) so ``logs.tf/`` without digits
    does not count toward a second link.
    """
    low = f"lower({cm}.msg)"
    p = _LOGS_TF_PATH_PREFIX_LEN
    p1 = f"instr({low}, 'logs.tf/')"
    # Second ``logs.tf/`` after the first (2-arg ``instr`` only — some SQLite builds reject 3-arg).
    q2 = f"instr(substr({low}, {p1} + {p}), 'logs.tf/')"
    p2 = f"(CASE WHEN {p1} > 0 AND {q2} > 0 THEN {p1} + {p} + {q2} - 1 ELSE 0 END)"
    d1 = f"substr({low}, {p1} + {p}, 1)"
    d2 = f"substr({low}, {p2} + {p}, 1)"
    return f"({p1} > 0 AND {d1} GLOB '[0-9]' AND {p2} != 0 AND {d2} GLOB '[0-9]')"


def chat_log_exclusion_exists_sql(schema: str, logs_table_alias: str = "l") -> str:
    """
    SQL fragment: `` AND NOT EXISTS (...)`` detecting combined-log chat lines.

    Requires ``ATTACH DATABASE ... AS {schema}`` with ``chat_messages`` visible as ``{schema}.chat_messages``.
    """
    cm = f"{schema}.chat_messages"
    lt = logs_table_alias
    msg_ors = " OR ".join(
        f"instr(lower({cm}.msg), '{_sql_quote(n)}') > 0" for n in _CHAT_MSG_COMBINED_NEEDLES
    )
    two_urls = _chat_msg_two_logs_tf_id_urls_sql(cm)
    alias_bot = f"instr(lower({cm}.alias), 'log combiner') > 0"
    return (
        f" AND NOT EXISTS ("
        f"SELECT 1 FROM {cm} WHERE {cm}.log_id = {lt}.log_id"
        f" AND ({msg_ors} OR {two_urls} OR {alias_bot})"
        f")"
    )


def try_attach_chat_db(conn: sqlite3.Connection, path: str | Path) -> bool:
    """
    Attach the chat DB as ``chat`` for EXISTS subqueries. Idempotent if already attached.

    Returns False if the file is missing or attach fails (caller skips chat-based exclusion).
    """
    for row in conn.execute("PRAGMA database_list"):
        if len(row) >= 2 and row[1] == _CHAT_ATTACH_NAME:
            try:
                conn.execute(f"SELECT 1 FROM {_CHAT_ATTACH_NAME}.chat_messages LIMIT 1")
            except sqlite3.Error:
                pass
            else:
                return True
    p = Path(path)
    if not p.is_file():
        return False
    try:
        # SQLite only parameterizes the file path; ``AS <name>`` must be a literal schema identifier.
        conn.execute(
            f"ATTACH DATABASE ? AS {_CHAT_ATTACH_NAME}",
            (str(p.resolve()),),
        )
        conn.execute(f"SELECT 1 FROM {_CHAT_ATTACH_NAME}.chat_messages LIMIT 1")
    except sqlite3.Error:
        try:
            conn.execute(f"DETACH DATABASE {_CHAT_ATTACH_NAME}")
        except sqlite3.Error:
            pass
        return False
    return True


def detach_chat_db(conn: sqlite3.Connection) -> None:
    """Detach the ``chat`` schema if present."""
    try:
        conn.execute(f"DETACH DATABASE {_CHAT_ATTACH_NAME}")
    except sqlite3.Error:
        pass
