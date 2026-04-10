"""
Rebuild ``player_stats_agg`` from ``log_players`` + ``logs`` (global leaderboard precompute).

Run after deploying the aggregate table or if aggregates are missing/stale:

    python -m app.rebuild_agg
    python -m app.rebuild_agg --db /path/to/stats.db

The downloader keeps rows updated per log via ``refresh_player_stats_agg_for_steamids``;
this script performs a full refresh (expensive on large databases).
"""
from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from app.config import STATS_DB_PATH
    from app.stats_db import connect_stats_db, init_stats_db, rebuild_player_stats_agg

    p = argparse.ArgumentParser(description="Rebuild player_stats_agg from log_players.")
    p.add_argument(
        "--db",
        default=str(STATS_DB_PATH),
        help="Path to stats.db (default: STATS_DB_PATH)",
    )
    args = p.parse_args(argv)

    conn = connect_stats_db(args.db)
    try:
        init_stats_db(conn)
        n = rebuild_player_stats_agg(conn)
    except Exception:
        logger.exception("rebuild_player_stats_agg failed")
        return 1
    finally:
        conn.close()

    print(f"player_stats_agg rebuilt: {n} player row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
