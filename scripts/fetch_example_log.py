#!/usr/bin/env python3
"""Download logs.tf JSON + raw zip into examples/ for local testing."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.downloader import fetch_log_json_with_retry  # noqa: E402
from app.raw_zip_io import RawZipFetchOutcome, fetch_raw_log_zip_with_retry, save_raw_log_zip  # noqa: E402

EXAMPLES_DIR = REPO_ROOT / "examples"


def _json_path(log_id: int) -> Path:
    return EXAMPLES_DIR / f"{log_id}.json"


def _zip_path(log_id: int) -> Path:
    return EXAMPLES_DIR / f"log_{log_id}.log.zip"


def fetch_one(
    log_id: int,
    *,
    force: bool,
    json_only: bool,
    raw_only: bool,
    require_raw: bool,
    delay_sec: float,
) -> tuple[bool, bool]:
    """Download one log. Returns (success, made_api_request)."""
    EXAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    ok = True
    made_request = False

    if not raw_only:
        jp = _json_path(log_id)
        if jp.is_file() and not force:
            print(f"[{log_id}] JSON exists ({jp.name}); use --force to replace")
        else:
            data, success = fetch_log_json_with_retry(log_id)
            made_request = True
            if not success or not isinstance(data, dict):
                print(f"[{log_id}] JSON fetch failed", file=sys.stderr)
                ok = False
            elif data.get("success") is not True:
                print(f"[{log_id}] API returned success=false", file=sys.stderr)
                ok = False
            else:
                jp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                print(f"[{log_id}] wrote {jp.relative_to(REPO_ROOT)}")

    if not json_only:
        zp = _zip_path(log_id)
        if zp.is_file() and not force:
            print(f"[{log_id}] raw zip exists ({zp.name}); use --force to replace")
        else:
            if made_request and delay_sec > 0:
                time.sleep(delay_sec)
            fetch_result = fetch_raw_log_zip_with_retry(log_id)
            made_request = True
            if fetch_result.outcome != RawZipFetchOutcome.OK or fetch_result.data is None:
                reason = "not available on logs.tf" if fetch_result.outcome == RawZipFetchOutcome.NOT_AVAILABLE else "fetch error"
                print(
                    f"[{log_id}] raw zip {reason}",
                    file=sys.stderr,
                )
                if require_raw:
                    ok = False
            else:
                saved = save_raw_log_zip(log_id, fetch_result.data, EXAMPLES_DIR)
                if saved is None:
                    print(f"[{log_id}] could not save raw zip", file=sys.stderr)
                    ok = False
                else:
                    print(f"[{log_id}] wrote {saved.relative_to(REPO_ROOT)}")

    return ok, made_request


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download logs.tf JSON and raw log zip into examples/."
    )
    parser.add_argument(
        "log_ids",
        nargs="+",
        type=int,
        metavar="LOG_ID",
        help="One or more logs.tf log IDs",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace existing example files",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Download parsed JSON only",
    )
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="Download raw log zip only",
    )
    parser.add_argument(
        "--require-raw",
        action="store_true",
        help="Exit with error if raw zip is missing (default: warn only)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.3,
        metavar="SEC",
        help="Pause between logs.tf requests (default: 0.3)",
    )
    args = parser.parse_args()
    if args.json_only and args.raw_only:
        print("Use at most one of --json-only and --raw-only", file=sys.stderr)
        return 2

    all_ok = True
    prev_made_request = False
    for log_id in args.log_ids:
        if prev_made_request and args.delay > 0:
            time.sleep(args.delay)
        ok, made_request = fetch_one(
            log_id,
            force=args.force,
            json_only=args.json_only,
            raw_only=args.raw_only,
            require_raw=args.require_raw,
            delay_sec=args.delay,
        )
        if not ok:
            all_ok = False
        prev_made_request = made_request
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
