"""Unit tests for ``app.combined_logs`` (combined / merged logs.tf uploads)."""

import sqlite3

import pytest

from app.combined_logs import (
    chat_alias_suggests_combined_bot,
    _chat_msg_two_logs_tf_id_urls_sql,
    _title_series_score_tail_sql,
    chat_message_suggests_combined_log,
    log_metadata_suggests_combined_log,
    logtext_suggests_combined_log,
    map_field_suggests_combined_log,
    title_suggests_combined_log,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, True),
        ("", True),
        ("   ", True),
        ("cp_process_final", False),
        ("cp_granary_pro_rc17a3", False),
        ("pl_vigil, cp_process", True),
        ("pl_vigil + cp_process", True),
        ("upwd steel cascade", True),
        ("cp_steel\tkoth_clearcut", True),
        ("GG", True),
        ("test", True),
    ],
)
def test_map_field_suggests_combined_log(raw: str | None, expected: bool) -> None:
    assert map_field_suggests_combined_log(raw) is expected


@pytest.mark.parametrize(
    "title,expected",
    [
        (None, False),
        ("", False),
        ("pug on process", False),
        ("Combined Log", True),
        ("logs were combined here", True),
        ("PLAT GRAND FINALS (DK VS HOOD) (2-1)", True),
        ("Series (A) (2-1)", True),
        ("we won (2-1)", False),
    ],
)
def test_title_suggests_combined_log(title: str | None, expected: bool) -> None:
    assert title_suggests_combined_log(title) is expected


@pytest.mark.parametrize(
    "msg,expected",
    [
        ("gg", False),
        (
            "The following logs were combined: https://logs.tf/3921375 & https://logs.tf/3921388",
            True,
        ),
        ("The logs were combined using: https://example.com/tool", True),
        ("https://logs.tf/1 https://logs.tf/2", True),
        ("logs.tf/ logs.tf/", False),
    ],
)
def test_chat_message_suggests_combined_log(msg: str, expected: bool) -> None:
    assert chat_message_suggests_combined_log(msg) is expected


def test_title_series_score_tail_sql_matches_suffix_regex() -> None:
    """Aligned with ``_TITLE_SERIES_SCORE_TAIL_RE`` (not the loose ``) (`` + ``-`` substring test)."""
    expr = _title_series_score_tail_sql("l")
    conn = sqlite3.connect(":memory:")
    cases = [
        ("Something long (teams) (2-1) extra", 0),
        ("PLAT GRAND FINALS (DK VS HOOD) (2-1)", 1),
        ("Tournament (x) (really-good) epilogue text", 0),
    ]
    for title, want in cases:
        row = conn.execute("SELECT " + expr + " FROM (SELECT ? AS title) l", (title,)).fetchone()
        assert int(row[0]) == want
    conn.close()


def test_chat_two_logs_tf_urls_sql_requires_digit_ids() -> None:
    expr = _chat_msg_two_logs_tf_id_urls_sql("t")
    conn = sqlite3.connect(":memory:")
    false_positive = "logs.tf/ logs.tf/"
    row_fp = conn.execute(
        "SELECT " + expr + " FROM (SELECT ? AS msg) t",
        (false_positive,),
    ).fetchone()
    assert int(row_fp[0]) == 0
    good = "The following logs were combined: https://logs.tf/3921375 & https://logs.tf/3921388"
    row_ok = conn.execute(
        "SELECT " + expr + " FROM (SELECT ? AS msg) t",
        (good,),
    ).fetchone()
    assert int(row_ok[0]) == 1
    conn.close()


def test_chat_alias_bot() -> None:
    assert chat_alias_suggests_combined_bot("Jack's Log Combiner") is True
    assert chat_alias_suggests_combined_bot("b4nny") is False


def test_log_metadata_combined() -> None:
    assert log_metadata_suggests_combined_log(map_name="cp_process", title="Combined Log") is True
    assert log_metadata_suggests_combined_log(map_name="cp_process", title="scrims") is False
    assert log_metadata_suggests_combined_log(map_name="", title="scrims") is True


def test_logtext_jack_style() -> None:
    lt = {
        "info": {"map": "cp_process", "title": "x", "date": 1},
        "chat": [
            {
                "msg": "The following logs were combined: https://logs.tf/1 & https://logs.tf/2",
                "name": "Jack's Log Combiner",
                "steamid": "[U:1:1]",
            },
        ],
        "players": {},
    }
    assert logtext_suggests_combined_log(lt) is True


def test_logtext_clean() -> None:
    lt = {
        "info": {"map": "cp_process_final", "title": "Test", "date": 1},
        "chat": [{"msg": "gg", "name": "a", "steamid": "[U:1:1]"}],
        "players": {},
    }
    assert logtext_suggests_combined_log(lt) is False


def test_logtext_space_separated_maps() -> None:
    lt = {
        "info": {"map": "upwd steel cascade", "title": "pug", "date": 1},
        "chat": [],
        "players": {},
    }
    assert logtext_suggests_combined_log(lt) is True


def test_stats_log_exclusion_sql_series_title_aligns_with_python() -> None:
    """Mid-length ``(…) (n-m)`` titles must be excluded in SQL, not only in ``title_suggests_combined_log``."""
    import sqlite3

    from app.combined_logs import stats_log_exclusion_sql

    frag = stats_log_exclusion_sql("l")
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE logs (map TEXT, title TEXT)")
    conn.execute("INSERT INTO logs VALUES ('koth_bagel', 'Series (A) (2-1)')")
    kept = conn.execute("SELECT COUNT(*) FROM logs l WHERE 1=1 " + frag).fetchone()[0]
    assert kept == 0
    conn.execute("DELETE FROM logs")
    conn.execute("INSERT INTO logs VALUES ('koth_bagel', 'evening pug')")
    kept2 = conn.execute("SELECT COUNT(*) FROM logs l WHERE 1=1 " + frag).fetchone()[0]
    assert kept2 == 1
