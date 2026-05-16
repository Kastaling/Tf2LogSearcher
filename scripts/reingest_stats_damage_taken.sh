#!/usr/bin/env bash
# Re-parse all local logs.tf JSON files into the stats SQLite DB using the current importer.
# Use this after importer changes (for example damage taken, class_stats weapons, root
# healspread, or root classkills) so stats.db reflects corrected values without
# re-downloading JSON from logs.tf.
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
#   TF2LS_MAINT_LOG_DIR Directory for verbose maintenance logs (default: ./maintenance_logs)
#   TF2LS_MAINT_LOG_FILE Exact log file path to append to (set automatically for tmux child)
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

init_log_file() {
  local log_dir old_umask ts
  ts="$(date +%Y%m%d-%H%M%S)"
  log_dir="${TF2LS_MAINT_LOG_DIR:-${REPO_ROOT}/maintenance_logs}"
  mkdir -p -- "${log_dir}"
  if [[ -z "${TF2LS_MAINT_LOG_FILE:-}" ]]; then
    TF2LS_MAINT_LOG_FILE="${log_dir}/reingest_stats-${ts}.log"
    export TF2LS_MAINT_LOG_FILE
  fi
  old_umask="$(umask)"
  umask 077
  touch -- "${TF2LS_MAINT_LOG_FILE}"
  umask "${old_umask}"
}

start_logging() {
  exec > >(tee -a "${TF2LS_MAINT_LOG_FILE}") 2>&1
  echo "================================================================"
  echo "Tf2LogSearcher stats re-ingest"
  echo "Started:       $(date -Is)"
  echo "Repository:    ${REPO_ROOT}"
  echo "Script:        ${SELF_SCRIPT}"
  echo "Log file:      ${TF2LS_MAINT_LOG_FILE}"
  echo "Arguments:     ${*:-<none>}"
  if command -v git >/dev/null 2>&1; then
    echo "Git HEAD:      $(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  fi
  echo "Importer note: app.stats_backfill uses the current app.stats_db.extract_log_stats logic."
  echo "               That includes class_stats weapons plus root healspread/classkills."
  echo "================================================================"
}

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
init_log_file
start_logging "$@"

if [[ ! -f docker-compose.yml ]] && [[ ! -f docker-compose.yaml ]]; then
  echo "error: no docker-compose.yml in ${REPO_ROOT}. Copy docker-compose.example.yml per README." >&2
  exit 1
fi

resolve_compose() {
  if [[ -n "${DOCKER_COMPOSE_CMD:-}" ]]; then
    # Deliberately split into argv tokens, never eval. Keep overrides simple:
    #   DOCKER_COMPOSE_CMD="docker compose -f docker-compose.prod.yml"
    read -r -a COMPOSE_CMD <<< "${DOCKER_COMPOSE_CMD}"
    return
  fi
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD=(docker compose)
    return
  fi
  if docker-compose version >/dev/null 2>&1; then
    COMPOSE_CMD=(docker-compose)
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
    echo "  Verbose log:                 ${TF2LS_MAINT_LOG_FILE}" >&2
    exec tmux new-session -s "${sess}" -c "${REPO_ROOT}" \
      env TF2LS_MAINT_LOG_FILE="${TF2LS_MAINT_LOG_FILE}" bash "${SELF_SCRIPT}" --no-tmux "$@"
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

COMPOSE_CMD=()
resolve_compose
compose_display="${COMPOSE_CMD[*]}"
started_at_epoch="$(date +%s)"

echo "[1/3] Stopping downloader (avoids concurrent writes to stats.db)..."
set +e
"${COMPOSE_CMD[@]}" stop downloader
stop_ec=$?
set -e
if [[ "${stop_ec}" -eq 0 ]]; then
  echo "[ok] Downloader stopped (or was already stopped)."
else
  echo "[warning] Downloader stop returned ${stop_ec}; continuing because stats_backfill is the only intended writer now." >&2
fi

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
echo "      Compose command: ${compose_display}" >&2
echo "      Full command: ${compose_display} run --rm downloader python -m app.stats_backfill ${BACKFILL_ARGS[*]}" >&2

# Run compose in background so we can forward INT/TERM to it (reliable when nested in tmux/ssh).
set +e
"${COMPOSE_CMD[@]}" run --rm downloader python -m app.stats_backfill "${BACKFILL_ARGS[@]}" &
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
  echo "[info] Downloader is still stopped; when ready: ${compose_display} up -d downloader" >&2
  echo "[info] Verbose log: ${TF2LS_MAINT_LOG_FILE}" >&2
  exit 130
}

term_trap() {
  forward_signal TERM
  wait "$dc_pid" 2>/dev/null || true
  trap - INT TERM
  echo "" >&2
  echo "[info] Terminated during backfill." >&2
  echo "[info] Downloader is still stopped; when ready: ${compose_display} up -d downloader" >&2
  echo "[info] Verbose log: ${TF2LS_MAINT_LOG_FILE}" >&2
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
  echo "[summary] Re-ingest failed. Downloader was not restarted automatically." >&2
  echo "[summary] Review this log before retrying: ${TF2LS_MAINT_LOG_FILE}" >&2
  exit "$bf_ec"
fi

echo "[3/3] Starting downloader..."
set +e
"${COMPOSE_CMD[@]}" up -d downloader
up_ec=$?
set -e
ended_at_epoch="$(date +%s)"
elapsed=$((ended_at_epoch - started_at_epoch))

if [[ "${up_ec}" -ne 0 ]]; then
  echo "[error] Re-ingest succeeded, but restarting downloader failed with code ${up_ec}." >&2
  echo "[summary] Worked: stats_backfill completed and player_stats_agg rebuild was attempted by app.stats_backfill." >&2
  echo "[summary] Did not work: downloader restart failed; start it manually with: ${compose_display} up -d downloader" >&2
  echo "[summary] Elapsed: ${elapsed}s" >&2
  echo "[summary] Verbose log: ${TF2LS_MAINT_LOG_FILE}" >&2
  exit "${up_ec}"
fi

echo "[summary] Re-ingest complete."
echo "[summary] Worked: stats_backfill parsed local JSON through the current importer and completed successfully."
echo "[summary] Worked: player_stats_agg rebuild was attempted by app.stats_backfill."
echo "[summary] Worked: downloader restarted successfully."
echo "[summary] Elapsed: ${elapsed}s"
echo "[summary] Verbose log: ${TF2LS_MAINT_LOG_FILE}"
