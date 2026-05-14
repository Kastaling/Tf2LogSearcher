"""Tests for raw_events.db aggregate helpers used by progress UI."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.raw_db import count_raw_library_rows, init_raw_db


def test_count_raw_library_rows_uses_row_count_not_max_id(tmp_path: Path) -> None:
    """MAX(log_id) must not be used as a proxy for rows — gaps and deletes skew it."""
    db_path = tmp_path / "raw_events.db"
    conn = sqlite3.connect(str(db_path))
    init_raw_db(conn)
    conn.executemany(
        """
        INSERT INTO raw_logs (log_id, imported_at, kill_count)
        VALUES (?, ?, ?)
        """,
        [
            (1, 0, 2),
            (2, 0, 1),
            (99, 0, 3),
        ],
    )
    conn.commit()
    conn.close()

    n, kill_sum = count_raw_library_rows(db_path)
    assert n == 3
    assert kill_sum == 6
