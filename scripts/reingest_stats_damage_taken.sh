#!/usr/bin/env bash
# Re-parse all local logs.tf JSON files into the stats SQLite DB using the current importer.
# Use this after upgrading damage-taken extraction (e.g. dt / dt_real handling) so log_players
# and player_stats_agg reflect corrected values — without re-downloading JSON from logs.tf.
#
# By default, re-runs inside a new tmux session so you can detach (Ctrl-b d) and interrupt
# safely from the pane (Ctrl+C forwards to docker compose, which stops the container).
#
# Usage (from repo root):
#   ./scripts/reingest_stats_damage_taken.sh
#   ./scripts/reingest_stats_damage_taken.sh --batch-size 1000
#   ./scripts/reingest_stats_damage_taken.sh --no-tmux    # CI / cron / no tmux installed
#   BATCH_SIZE=750 ./scripts/reingest_stats_damage_taken.sh
#
# Environment:
#   DOCKER_COMPOSE_CMD  Override compose command (e.g. "docker compose -f prod.yml")
#   TF2LS_TMUX_SESSION  tmux session name (default: tf2ls-reingest-YYYYMMDD-HHMMSS)
#   TF2LS_REINGEST_NO_TMUX=1  Same as --no-tmux
#
# Prerequisites: docker-compose.example.yml copied to docker-compose.yml (or equivalent).
# Same pattern as README "Stats DB backfill"; stops the downloader to keep a single SQLite writer.

set -euo pipefail

usage() {
  sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# Absolute path for tmux re-exec (works when cwd or symlinks differ).
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

if [[ -n "${TF2LS_REINGEST_NO_TMUX:-}" ]]; then
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

# Re-exec inside tmux: detach with Ctrl-b then d; interrupt backfill with Ctrl+C.
if [[ "${use_tmux}" -eq 1 ]] && [[ -z "${TMUX:-}" ]]; then
  if command -v tmux >/dev/null 2>&1 && [[ -t 0 ]] && [[ -t 1 ]]; then
    sess="${TF2LS_TMUX_SESSION:-tf2ls-reingest-$(date +%Y%m%d-%H%M%S)}"
    echo "Starting re-ingest in tmux session: ${sess}" >&2
    echo "  Detach (leave it running):  Ctrl-b  then  d" >&2
    echo "  Reattach later:              tmux attach -t ${sess}" >&2
    echo "  Stop backfill:               Ctrl+C  (commits up to last batch are kept)" >&2
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

echo "[1/3] Stopping downloader (avoids concurrent writes to stats.db)..."
${DC} stop downloader 2>/dev/null || true

BACKFILL_ARGS=()
if [[ $# -gt 0 ]]; then
  BACKFILL_ARGS+=("$@")
elif [[ -n "${BATCH_SIZE:-}" ]]; then
  BACKFILL_ARGS+=(--batch-size "${BATCH_SIZE}")
else
  BACKFILL_ARGS+=(--batch-size 500)
fi

echo "[2/3] Running stats backfill (${BACKFILL_ARGS[*]})..."
echo "      (Ctrl+C interrupts docker; SQLite keeps commits every --batch-size logs.)" >&2

# Run compose in background so we can forward INT/TERM to it (reliable when nested in tmux/ssh).
set +e
${DC} run --rm downloader python -m app.stats_backfill "${BACKFILL_ARGS[@]}" &
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
  # wait for docker compose to exit
  wait "$dc_pid" 2>/dev/null || true
  trap - INT TERM
  echo "" >&2
  echo "[info] Interrupted. Partial progress is preserved up to the last committed batch." >&2
  echo "[info] Downloader is still stopped; when ready: ${DC} up -d downloader" >&2
  exit 130
}

term_trap() {
  forward_signal TERM
  wait "$dc_pid" 2>/dev/null || true
  trap - INT TERM
  echo "" >&2
  echo "[info] Terminated during backfill." >&2
  echo "[info] Downloader is still stopped; when ready: ${DC} up -d downloader" >&2
  exit 143
}

trap cleanup_trap INT
trap term_trap TERM

set +e
wait "$dc_pid"
bf_ec=$?
set -e
trap - INT TERM

if [[ "$bf_ec" -ne 0 ]]; then
  echo "[error] stats_backfill exited with code ${bf_ec}" >&2
  exit "$bf_ec"
fi

echo "[3/3] Starting downloader..."
${DC} up -d downloader

echo "Done. player_stats_agg was rebuilt at end of backfill."
