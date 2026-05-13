#!/usr/bin/env bash
# Rebuild player_stats_agg for stats leaderboards while the downloader is stopped.
#
# By default, runs inside tmux so the rebuild can continue after you detach.
# If run from inside tmux, opens a new window in the current session; otherwise,
# opens a temporary tmux session. The tmux window/session closes when the rebuild
# command finishes.
#
# Usage (from repo root):
#   ./scripts/rebuild_stats_agg_tmux.sh
#   ./scripts/rebuild_stats_agg_tmux.sh --no-tmux
#   ./scripts/rebuild_stats_agg_tmux.sh -- --db /app/downloader_state/stats.db
#
# Environment:
#   DOCKER_COMPOSE_CMD          Override compose command (e.g. "docker compose -f prod.yml")
#   TF2LS_TMUX_SESSION          tmux session name when outside tmux
#   TF2LS_TMUX_WINDOW           tmux window name when inside tmux
#   TF2LS_REBUILD_AGG_NO_TMUX=1 Same as --no-tmux
#
# Stops the normal `downloader` service to avoid concurrent SQLite writes.
# Restarts it only after a successful aggregate rebuild.

set -euo pipefail

usage() {
  sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SELF_SCRIPT="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"

use_tmux=1
rebuild_args=()
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
    --)
      shift
      rebuild_args+=("$@")
      break
      ;;
    *)
      rebuild_args+=("$1")
      shift
      ;;
  esac
done

if [[ -n "${TF2LS_REBUILD_AGG_NO_TMUX:-}" ]]; then
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

if [[ "${use_tmux}" -eq 1 ]]; then
  if ! command -v tmux >/dev/null 2>&1; then
    echo "warning: tmux not found; running in current shell. Install tmux or pass --no-tmux." >&2
  elif [[ -n "${TMUX:-}" ]]; then
    win="${TF2LS_TMUX_WINDOW:-tf2ls-rebuild-agg}"
    echo "Opening rebuild in tmux window: ${win}" >&2
    echo "  Detach from tmux:  Ctrl-b  then  d" >&2
    echo "  Stop rebuild:      Ctrl+C in the new window" >&2
    tmux new-window -n "${win}" -c "${REPO_ROOT}" bash "${SELF_SCRIPT}" --no-tmux "${rebuild_args[@]}"
    exit 0
  elif [[ -t 0 ]] && [[ -t 1 ]]; then
    sess="${TF2LS_TMUX_SESSION:-tf2ls-rebuild-agg-$(date +%Y%m%d-%H%M%S)}"
    echo "Starting rebuild in tmux session: ${sess}" >&2
    echo "  Detach (leave it running):  Ctrl-b  then  d" >&2
    echo "  Reattach later:              tmux attach -t ${sess}" >&2
    echo "  Stop rebuild:                Ctrl+C" >&2
    exec tmux new-session -s "${sess}" -c "${REPO_ROOT}" bash "${SELF_SCRIPT}" --no-tmux "${rebuild_args[@]}"
  else
    echo "warning: not a TTY; running without tmux. Pass --no-tmux to silence this." >&2
  fi
fi

DC="$(resolve_compose)"

echo "[1/3] Stopping downloader (avoids concurrent writes to stats.db)..."
${DC} stop downloader 2>/dev/null || true

echo "[2/3] Rebuilding stats leaderboard aggregates..."
echo "      Running: python -m app.rebuild_agg ${rebuild_args[*]}" >&2

set +e
${DC} run --rm downloader python -m app.rebuild_agg "${rebuild_args[@]}" &
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
  echo "[info] Interrupted. Downloader is still stopped; when ready: ${DC} up -d downloader" >&2
  exit 130
}

term_trap() {
  forward_signal TERM
  wait "$dc_pid" 2>/dev/null || true
  trap - INT TERM
  echo "" >&2
  echo "[info] Terminated during rebuild. Downloader is still stopped; when ready: ${DC} up -d downloader" >&2
  exit 143
}

trap cleanup_trap INT
trap term_trap TERM

set +e
wait "$dc_pid"
rebuild_ec=$?
set -e
trap - INT TERM

if [[ "$rebuild_ec" -ne 0 ]]; then
  echo "[error] app.rebuild_agg exited with code ${rebuild_ec}" >&2
  echo "[info] Downloader is still stopped; when ready: ${DC} up -d downloader" >&2
  exit "$rebuild_ec"
fi

echo "[3/3] Starting downloader..."
${DC} up -d downloader

echo "Done. player_stats_agg was rebuilt and downloader is running."
