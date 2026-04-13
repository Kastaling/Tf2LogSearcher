# Tf2LogSearcher

A small web app and downloader for searching [logs.tf](https://logs.tf) TF2 match logs. You can run the full stack (web UI + downloader) or **only the downloader** to build a local log library.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Quick start (web + downloader)

1. **Clone the repo**
   ```bash
   git clone https://github.com/Kastaling/Tf2LogSearcher.git
   cd Tf2LogSearcher
   ```

2. **Create your Compose file** (tracked template → local `docker-compose.yml`, which is gitignored so you can customize ports/volumes without polluting the repo):
   ```bash
   cp docker-compose.example.yml docker-compose.yml
   ```

3. **Optional:** Copy `.env.example` to `.env` and adjust settings (log paths, rate limits, etc.):
   ```bash
   cp .env.example .env
   ```

4. **Start both services**
   ```bash
   docker-compose up -d
   ```
   - Web UI: http://localhost:8027  
   - The default **downloader** service (no profile) downloads **both** logs.tf JSON and raw `.log.zip` files. JSONs go to `./logs`; raw zips go to `./raw_logs`; position/event rows are stored in `./downloader_state/raw_events.db`. State (offset, skip list, progress) is in `./downloader_state`.

**Alternative downloader modes** (only one downloader variant should run at a time):

- **JSON only** (no raw zips / raw DB updates):
  ```bash
  docker-compose --profile json-only up -d downloader-json
  ```
- **Raw only** (no new JSON files; still walks the logs.tf API and downloads `log_<id>.log.zip` when missing):
  ```bash
  docker-compose --profile raw-only up -d downloader-raw
  ```

5. **View logs**
   ```bash
   docker-compose logs -f downloader
   ```

---

## Run only the downloader (no web)

If you only want to download logs and do **not** want to run the web app:

```bash
docker-compose up -d downloader
```

Only the downloader container will start. It will use the same `./logs` and `./downloader_state` directories. You can run the web part later with `docker-compose up -d web` if you change your mind.

To run the downloader in the foreground so you see logs in the terminal:

```bash
docker-compose up downloader
```

---

## Volumes and ports

| What              | Default location       | Purpose                          |
|-------------------|------------------------|----------------------------------|
| Log JSON files    | `./logs`               | Filled by downloader; read by web |
| Downloader state  | `./downloader_state`   | Offset, skip list, progress JSON  |
| Chat SQLite DB    | `./downloader_state/chat.db` | Chat index written by downloader/backfill |
| Stats SQLite DB   | `./downloader_state/stats.db` | Per-log player stats (downloader/backfill) |
| Raw log zips      | `./raw_logs`           | `log_<id>.log.zip` from logs.tf (not extracted on disk) |
| Raw events DB     | `./downloader_state/raw_events.db` | Kills (incl. XYZ + assists), spawns, uber deploy/end, caps, rounds — from raw logs (storage-only for now) |
| Request log (web)| `./request_logs`       | CSV of API requests (web only)   |

- **Web port:** 8027 (host) → 8000 (container). Change the left number in your local `docker-compose.yml` if needed.

### Docker Compose layout

The repo ships **`docker-compose.example.yml`** (web + downloader variants). Copy it to **`docker-compose.yml`** once; the latter is listed in `.gitignore` for local overrides.

**Downloader profiles** (Compose v2+): a service can set `profiles: [name]`. Services with a **non-empty** profile do **not** start on plain `docker-compose up` unless you pass `--profile name`. Here:

- **`downloader`** has `profiles: []` (empty), so it **is** included in the default project — that is the “JSON + raw” downloader.
- **`downloader-json`** has `profiles: [json-only]` — start with `--profile json-only` when you want that service instead of the default downloader (stop the default downloader first so only one runs).
- **`downloader-raw`** has `profiles: [raw-only]` — same idea for raw-only mode.

## Configuration

See `.env.example` for all options. Important ones:

- `LOGS_DIR`, `DOWNLOADER_STATE_DIR` — paths inside the container (compose maps `./logs` and `./downloader_state`).
- `DOWNLOAD_INTERVAL_SEC` — seconds between download cycles (default 3600).
- `REQUEST_DELAY_MS`, `MAX_REQUESTS_BEFORE_BACKOFF`, `BACKOFF_SEC` — rate limiting for the logs.tf API.
- `RATE_LIMIT_PROFILE_PER_MINUTE`, `RATE_LIMIT_LEADERBOARD_PER_MINUTE`, `RATE_LIMIT_WINDOW_SECONDS` — per-IP sliding-window rate limits for the profile and leaderboard endpoints (default: 10 requests per 60 seconds each). Only applied on cache misses; cached responses are always served without consuming a slot.
- `RATE_LIMIT_STEAM_VANITY_PER_MINUTE` — per-IP limit for outbound Steam vanity (ResolveVanityURL) HTTP calls (default: 10 per 60 seconds). Enforced only when a network call would be made; hits on the in-process vanity cache do not consume a slot.
- `STEAM_WEB_API_KEY` — Steam Web API key for vanity URL/name resolution.
- `REQUEST_LOG_PATH` — path to the request log CSV file.
- `CHAT_DB_PATH` — path to SQLite DB where chat rows are indexed.
- `STATS_DB_PATH` — path to SQLite DB where per-log player stats are stored.
- `RAW_LOGS_DIR` — directory for `log_<id>.log.zip` files (stored compressed; parsing reads zips in memory).
- `RAW_EVENTS_DB_PATH` — SQLite DB for position-related events from raw logs (kills with XYZ, uber deploys/charge ends, per-capper caps, spawns, round markers).
- `DOWNLOAD_JSON_ENABLED` / `DOWNLOAD_RAW_ENABLED` — set to `0` to disable that download path independently (default `1` for both).

Raw zips are typically **much larger** than JSON for the same match (often on the order of **5–20×**); plan disk space accordingly.

## Chat DB backfill (one-time migration)

If you already have downloaded JSON logs, run this once to import existing chat into SQLite:

```bash
# stop downloader first so DB writes are single-writer
docker-compose stop downloader

# run backfill inside downloader container environment
docker-compose run --rm downloader python -m app.chat_backfill --batch-size 500

# start downloader again (new logs will be indexed automatically)
docker-compose up -d downloader
```

The downloader now indexes chat for every newly fetched log into `CHAT_DB_PATH`.

## Stats DB backfill (one-time migration)

```bash
docker-compose stop downloader
docker-compose run --rm downloader python -m app.stats_backfill --batch-size 500
docker-compose up -d downloader
```

The downloader writes stats for every newly fetched log into `STATS_DB_PATH`. Re-running the backfill is safe: each log is replaced atomically.

## Raw events DB backfill (re-parse zips)

If you already have `log_*.log.zip` files under `RAW_LOGS_DIR` (e.g. after a parser upgrade), rebuild `raw_events.db` without re-downloading:

```bash
docker-compose stop downloader
docker-compose run --rm downloader python -m app.raw_backfill --batch-size 200
docker-compose up -d downloader
```

Options: `--raw-logs-dir`, `--db-path` (defaults from `app.config`), `--batch-size` (default 200). Safe to re-run: each log’s rows are replaced atomically.

## Fix log rounds (one-time migration)

If stats were imported before round duration / first-blood parsing was corrected, rebuild only the `log_rounds` table from your existing JSON files (no full stats reimport):

```bash
docker-compose stop downloader
docker-compose run --rm downloader python -m app.fix_log_rounds_from_json
docker-compose up -d downloader
```

Options: `--dry-run`, `--from-id N`, `--to-id M`, `--logs-dir`, `--db-path` (same layout as `app.stats_backfill`).

## Player names backfill (fast, run after stats backfill)

Roster display names come from each log’s `names` dict and are stored in the `player_names` table (used for search aliases even when a player never chatted). This pass only reads `names` + `info.date` from each JSON file.

Can run while the downloader is running — uses `INSERT OR REPLACE`, safe for concurrent writes to `player_names`.

```bash
docker-compose run --rm downloader python -m app.player_names_backfill --batch-size 1000
```

---

*Hosted at [search.kastal.ing](https://search.kastal.ing). Contact Kastaling on Discord for questions.*
