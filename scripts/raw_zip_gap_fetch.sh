#!/usr/bin/env bash
# Download raw log_<id>.log.zip from logs.tf for every {id}.json on disk that is missing a zip,
# then import parsed events into raw_events.db (app.raw_json_gap_fetch).
#
# Use this after your JSON library is complete but raw zips were never fetched (e.g. millions of
# gaps). The job is idempotent: re-run safely after Ctrl+C; already-downloaded zips are skipped.
#
# By default starts a new tmux session (detach: Ctrl-b then d). Ctrl+C forwards to docker compose.
#
# Usage (from repo root):
#   ./scripts/raw_zip_gap_fetch.sh
#   ./scripts/raw_zip_gap_fetch.sh --dry-run
#   ./scripts/raw_zip_gap_fetch.sh --shard-index 0 --shard-total 4
#   ./scripts/raw_zip_gap_fetch.sh --no-tmux --limit 100
#
# Environment:
#   DOCKER_COMPOSE_CMD          Override compose (e.g. "docker compose -f prod.yml")
#   TF2LS_TMUX_SESSION          tmux session name (default: tf2ls-raw-gap-YYYYMMDD-HHMMSS)
#   TF2LS_RAW_GAP_NO_TMUX=1     Same as --no-tmux
#
# Requires the default **downloader** service (with ./raw_logs mounted), not downloader-json.
# Prerequisites: docker-compose.yml from docker-compose.example.yml; stop conflicting downloaders.

set -euo pipefail

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SELF_SCRIPT="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"

use_tmux=1
parsed=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --no-tmux|--foreground)
      use_tmux=0
      shift
      ;;
    *)
      parsed+=("$1")
      shift
      ;;
  esac
done
set -- "${parsed[@]}"

if [[ -n "${TF2LS_RAW_GAP_NO_TMUX:-}" ]]; then
  use_tmux=0
fi

cd "${REPO_ROOT}"

if [[ ! -f docker-compose.yml ]] && [[ ! -f docker-compose.yaml ]]; then
  echo "error: no docker-compose.yml in ${REPO_ROOT}. Copy docker-compose.example.yml per README." >&2
  exit 1
fi

resolve_compose() {
  if [[ -n "${DOCKER_COMPOSE_CMD:-}" ]]; then
    echo "${DOCKER_COMPOSE_CMD}"
    return
  fi
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
    return
  fi
  if docker-compose version >/dev/null 2>&1; then
    echo "docker-compose"
    return
  fi
  echo "error: need Docker Compose (try: docker compose OR docker-compose)" >&2
  exit 1
}

if [[ "${use_tmux}" -eq 1 ]] && [[ -z "${TMUX:-}" ]]; then
  if command -v tmux >/dev/null 2>&1 && [[ -t 0 ]] && [[ -t 1 ]]; then
    sess="${TF2LS_TMUX_SESSION:-tf2ls-raw-gap-$(date +%Y%m%d-%H%M%S)}"
    echo "Starting raw zip gap fetch in tmux session: ${sess}" >&2
    echo "  Detach (leave it running):  Ctrl-b  then  d" >&2
    echo "  Reattach later:              tmux attach -t ${sess}" >&2
    echo "  Stop fetch:                  Ctrl+C  (safe to re-run; skips completed zips)" >&2
    exec tmux new-session -s "${sess}" -c "${REPO_ROOT}" bash "${SELF_SCRIPT}" "$@"
  else
    if [[ "${use_tmux}" -eq 1 ]]; then
      if ! command -v tmux >/dev/null 2>&1; then
        echo "warning: tmux not found; running in current shell. Install tmux or pass --no-tmux." >&2
      elif [[ ! -t 0 ]] || [[ ! -t 1 ]]; then
        echo "warning: not a TTY; running without tmux. Pass --no-tmux to silence this." >&2
      fi
    fi
  fi
fi

DC="$(resolve_compose)"

echo "[1/3] Stopping downloader (avoids overlapping logs.tf traffic and SQLite writers on raw DB)..."
${DC} stop downloader 2>/dev/null || true

GAP_ARGS=("$@")

echo "[2/3] Running python -m app.raw_json_gap_fetch"
if [[ ${#GAP_ARGS[@]} -gt 0 ]]; then
  printf '      args:' >&2
  printf ' %q' "${GAP_ARGS[@]}" >&2
  printf '\n' >&2
fi
echo "      (Ctrl+C interrupts docker; finished zips and DB batches already committed stay.)" >&2

set +e
${DC} run --rm downloader python -m app.raw_json_gap_fetch "${GAP_ARGS[@]}" &
dc_pid=$!
set -e

forward_signal() {
  local sig=$1
  if kill -0 "$dc_pid" 2>/dev/null; then
    kill "-${sig}" "$dc_pid" 2>/dev/null || true
  fi
}

cleanup_trap() {
  forward_signal INT
  wait "$dc_pid" 2>/dev/null || true
  trap - INT TERM
  echo "" >&2
  echo "[info] Interrupted. Re-run this script anytime; IDs with zips on disk are skipped." >&2
  echo "[info] Downloader is still stopped; when ready: ${DC} up -d downloader" >&2
  exit 130
}

term_trap() {
  forward_signal TERM
  wait "$dc_pid" 2>/dev/null || true
  trap - INT TERM
  echo "" >&2
  echo "[info] Terminated during gap fetch." >&2
  echo "[info] Downloader is still stopped; when ready: ${DC} up -d downloader" >&2
  exit 143
}

trap cleanup_trap INT
trap term_trap TERM

set +e
wait "$dc_pid"
gap_ec=$?
set -e
trap - INT TERM

if [[ "$gap_ec" -ne 0 ]]; then
  echo "[error] raw_json_gap_fetch exited with code ${gap_ec}" >&2
  exit "$gap_ec"
fi

echo "[3/3] Starting downloader..."
${DC} up -d downloader

echo "Done. Raw zips and raw_events.db are up to date for processed IDs."
