#!/usr/bin/env bash
# Re-parse all local log_*.log.zip files into raw_events.db using the current raw_log_parser.
# Use this after parser changes (for example charge_ready, lost_advantage, or future event
# types) so raw_events.db reflects new fields without re-downloading zips from logs.tf.
#
# By default, re-runs inside a new tmux session so you can detach (Ctrl-b d) and interrupt
# safely from the pane (Ctrl+C forwards to docker compose, which stops the container).
#
# Usage (from repo root):
#   ./scripts/reingest_raw_events_tmux.sh
#   ./scripts/reingest_raw_events_tmux.sh --batch-size 500
#   ./scripts/reingest_raw_events_tmux.sh --no-tmux    # CI / cron / no tmux installed
#   BATCH_SIZE=250 ./scripts/reingest_raw_events_tmux.sh
#
# Resume without re-reading earlier zips (use first number from last good Progress line):
#   ./scripts/reingest_raw_events_tmux.sh --skip-files 2409500
# Only log ids in a numeric band (after sorting zip filenames):
#   ./scripts/reingest_raw_events_tmux.sh --min-log-id 3000000
#   ./scripts/reingest_raw_events_tmux.sh --min-log-id 3000000 --max-log-id 4000000
#
# Environment:
#   DOCKER_COMPOSE_CMD              Override compose command (e.g. "docker compose -f prod.yml")
#   TF2LS_TMUX_SESSION              tmux session name (default: tf2ls-reingest-raw-YYYYMMDD-HHMMSS)
#   TF2LS_REINGEST_RAW_NO_TMUX=1    Same as --no-tmux
#   TF2LS_MAINT_LOG_DIR             Directory for verbose maintenance logs (default: ./maintenance_logs)
#   TF2LS_MAINT_LOG_FILE            Exact log file path to append to (set automatically for tmux child)
#
# Prerequisites: docker-compose.example.yml copied to docker-compose.yml (or equivalent).
# Stops the downloader to keep a single SQLite writer on raw_events.db.

set -euo pipefail

usage() {
  sed -n '2,42p' "$0" | sed 's/^# \{0,1\}//'
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SELF_SCRIPT="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"

init_log_file() {
  local log_dir old_umask ts
  ts="$(date +%Y%m%d-%H%M%S)"
  log_dir="${TF2LS_MAINT_LOG_DIR:-${REPO_ROOT}/maintenance_logs}"
  mkdir -p -- "${log_dir}"
  if [[ -z "${TF2LS_MAINT_LOG_FILE:-}" ]]; then
    TF2LS_MAINT_LOG_FILE="${log_dir}/reingest_raw_events-${ts}.log"
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
  echo "Tf2LogSearcher raw events re-ingest"
  echo "Started:       $(date -Is)"
  echo "Repository:    ${REPO_ROOT}"
  echo "Script:        ${SELF_SCRIPT}"
  echo "Log file:      ${TF2LS_MAINT_LOG_FILE}"
  echo "Arguments:     ${*:-<none>}"
  if command -v git >/dev/null 2>&1; then
    echo "Git HEAD:      $(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  fi
  echo "Parser note:   app.raw_backfill re-parses log_*.log.zip with the current app.raw_log_parser."
  echo "               Replaces per-log rows in raw_events.db (charge_ready, lost_advantage, etc.)."
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

if [[ -n "${TF2LS_REINGEST_RAW_NO_TMUX:-}" ]]; then
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

if [[ "${use_tmux}" -eq 1 ]] && [[ -z "${TMUX:-}" ]]; then
  if command -v tmux >/dev/null 2>&1 && [[ -t 0 ]] && [[ -t 1 ]]; then
    sess="${TF2LS_TMUX_SESSION:-tf2ls-reingest-raw-$(date +%Y%m%d-%H%M%S)}"
    echo "Starting raw re-ingest in tmux session: ${sess}" >&2
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

echo "[1/3] Stopping downloader (avoids concurrent writes to raw_events.db)..."
set +e
"${COMPOSE_CMD[@]}" stop downloader
stop_ec=$?
set -e
if [[ "${stop_ec}" -eq 0 ]]; then
  echo "[ok] Downloader stopped (or was already stopped)."
else
  echo "[warning] Downloader stop returned ${stop_ec}; continuing because raw_backfill is the only intended writer now." >&2
fi

BACKFILL_ARGS=()
if [[ $# -gt 0 ]]; then
  BACKFILL_ARGS+=("$@")
elif [[ -n "${BATCH_SIZE:-}" ]]; then
  BACKFILL_ARGS+=(--batch-size "${BATCH_SIZE}")
else
  BACKFILL_ARGS+=(--batch-size 200)
fi

echo "[2/3] Running raw events backfill (${BACKFILL_ARGS[*]})..."
echo "      (Ctrl+C interrupts docker; SQLite keeps commits every --batch-size logs.)" >&2
echo "      Compose command: ${compose_display}" >&2
echo "      Full command: ${compose_display} run --rm downloader python -m app.raw_backfill ${BACKFILL_ARGS[*]}" >&2

set +e
"${COMPOSE_CMD[@]}" run --rm downloader python -m app.raw_backfill "${BACKFILL_ARGS[@]}" &
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
  echo "[error] raw_backfill exited with code ${bf_ec}" >&2
  echo "[summary] Raw re-ingest failed. Downloader was not restarted automatically." >&2
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
  echo "[error] Raw re-ingest succeeded, but restarting downloader failed with code ${up_ec}." >&2
  echo "[summary] Worked: raw_backfill re-parsed local zips into raw_events.db successfully." >&2
  echo "[summary] Did not work: downloader restart failed; start it manually with: ${compose_display} up -d downloader" >&2
  echo "[summary] Elapsed: ${elapsed}s" >&2
  echo "[summary] Verbose log: ${TF2LS_MAINT_LOG_FILE}" >&2
  exit "${up_ec}"
fi

echo "[summary] Raw re-ingest complete."
echo "[summary] Worked: raw_backfill re-parsed local zips with the current parser."
echo "[summary] Worked: downloader restarted successfully."
echo "[summary] Elapsed: ${elapsed}s"
echo "[summary] Verbose log: ${TF2LS_MAINT_LOG_FILE}"
