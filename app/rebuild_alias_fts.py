"""CLI: run a full alias trigram FTS rebuild. Prefer stopping the downloader first to avoid lock contention."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from app.chat_db import rebuild_alias_fts_if_needed
from app.config import CHAT_DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild chat_messages_alias_fts (player name search index).")
    parser.add_argument(
        "--db-path",
        default=str(CHAT_DB_PATH),
        help="Path to chat SQLite DB (default: CHAT_DB_PATH)",
    )
    args = parser.parse_args()
    path = Path(args.db_path)
    if not path.is_file():
        raise SystemExit(f"Database file not found: {path}")
    logger.info("Starting alias FTS rebuild on %s ...", path)
    rebuild_alias_fts_if_needed(path)
    logger.info("Done.")


if __name__ == "__main__":
    main()
