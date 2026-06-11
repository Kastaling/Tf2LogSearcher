# Tf2LogSearcher

A small web app and downloader for building a local [logs.tf](https://logs.tf) library and searching TF2 match logs.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Quick Start

```bash
git clone https://github.com/Kastaling/Tf2LogSearcher.git
cd Tf2LogSearcher
cp docker-compose.example.yml docker-compose.yml
# Optional: cp .env.example .env
docker compose up -d
```

The web UI is available at <http://localhost:8027>. The default `downloader` service downloads logs.tf JSON files to `./logs`, raw log zips to `./raw_logs`, and state/SQLite databases to `./downloader_state`.

Use `docker-compose` instead of `docker compose` if you are still on Compose V1.

## Downloader Modes

Run only one downloader variant at a time:

```bash
# Default: JSON + raw zips
docker compose up -d downloader

# JSON only
docker compose --profile json-only up -d downloader-json

# Raw zips only
docker compose --profile raw-only up -d downloader-raw
```

View downloader logs:

```bash
docker compose logs -f downloader
```

## Data Locations

| Data | Default location |
| --- | --- |
| Log JSON files | `./logs` |
| Raw log zips | `./raw_logs` |
| Downloader state | `./downloader_state` |
| Chat SQLite DB | `./downloader_state/chat.db` |
| Stats SQLite DB | `./downloader_state/stats.db` |
| Default profile & co-players disk cache | `./downloader_state/profile_cache.db` |
| Poisoned log blocklist (JSON) | `./downloader_state/poisoned_log_ids.json` |
| Raw events DB | `./downloader_state/raw_events.db` |
| Request logs | `./request_logs` |


The default web port is `8027` on the host mapped to `8000` in the container. Change the left side of the port mapping in your local `docker-compose.yml` if needed.

## Configuration

Copy `.env.example` to `.env` when you need to override defaults. Common options:

- `DOWNLOAD_INTERVAL_SEC` - seconds between download cycles.
- `REQUEST_DELAY_MS`, `MAX_REQUESTS_BEFORE_BACKOFF`, `BACKOFF_SEC` - logs.tf API pacing.
- `STEAM_WEB_API_KEY` - optional Steam Web API key for vanity URL and name resolution.
- `DOWNLOAD_JSON_ENABLED`, `DOWNLOAD_RAW_ENABLED` - enable or disable JSON/raw download paths.
- `RATE_LIMIT_PROFILE_PER_MINUTE`, `RATE_LIMIT_LEADERBOARD_PER_MINUTE`, `RATE_LIMIT_STEAM_VANITY_PER_MINUTE` - public endpoint and outbound Steam vanity rate limits.
- `SHOW_STORAGE_STATS` - set to `1` to show disk usage in the Log Library panel. It defaults to `0`; keep it disabled on public instances if storage details are sensitive. Restart/recreate the `web` container after changing it.

### Poisoned logs

Some logs.tf uploads are manually edited (fake chat, inflated stats), or come from repeat troll uploaders. Edit [`downloader_state/poisoned_log_ids.json`](downloader_state/poisoned_log_ids.json) (same gitignored directory as `skipped_log_ids.json` and the SQLite DBs):

- **`log_ids`** — block specific numeric log IDs.
- **`uploader_steamid64`** — block every log uploaded by those 17-digit SteamID64 values. Uploader checks happen at download/index time; on startup the service resolves those uploaders to log IDs via logs.tf and purges them from the DBs. Search queries only exclude by log ID (no uploader column needed in SQLite).

Excluded logs are omitted from chat search, word leaderboards, stats, profiles, co-players, and indexing. Optional `notes` entries are documentation only. Override the file path with `POISONED_LOG_IDS_PATH`. After editing the file, restart the `web` and `downloader` services (or wait for the next downloader cycle) so existing DB rows are purged.

Raw zips are usually much larger than JSON files, so plan disk space before enabling raw downloads at scale.

## Maintenance Commands

Stop the downloader before commands that rewrite `chat.db`, `stats.db`, or `raw_events.db`:

```bash
docker compose stop downloader
# run the maintenance command
docker compose up -d downloader
```

### Backfill Chat DB

Import chat from existing JSON logs:

```bash
docker compose run --rm downloader python -m app.chat_backfill --batch-size 500
```

### Backfill Stats DB

Import per-log player stats from existing JSON logs. Re-running is safe; rows are replaced atomically.

```bash
docker compose run --rm downloader python -m app.stats_backfill --batch-size 500
```

Resume without re-reading earlier JSON files (set `N` to the **first number** from the last successful
`Progress: … logs in this run …` line — that is how many sorted files were already committed, not
necessarily the logs.tf numeric id of the last file):

```bash
docker compose run --rm downloader python -m app.stats_backfill --skip-files 2409500
```

Only import logs whose **filename id** falls in a numeric range (after sorting):

```bash
docker compose run --rm downloader python -m app.stats_backfill --min-log-id 3000000 --max-log-id 3999999
```

### Rebuild Leaderboard Aggregates

Rebuild `player_stats_agg`, `player_classkills_agg`, and `player_weapons_agg` after adding or changing aggregate leaderboard columns (including heals, kills-by-class, kills-by-weapon, `total_deaths`, `total_killstreak`):

```bash
docker compose run --rm downloader python -m app.rebuild_agg
```

For long interactive runs, the wrapper handles stopping/starting the downloader and runs in tmux by default:

```bash
./scripts/rebuild_stats_agg_tmux.sh
```

The wrapper writes a verbose ignored log file under `maintenance_logs/` (override with
`TF2LS_MAINT_LOG_DIR`) so results remain available after the tmux window/session closes.

### Re-ingest Stats

Use this when importer logic changes and existing JSON logs need to be parsed into `stats.db` again
(for example damage taken, class-stats weapon breakdowns, root healspread, or root classkills):

```bash
./scripts/reingest_stats_damage_taken.sh
```

The wrapper stops the downloader, runs `app.stats_backfill` with the current importer, lets
`stats_backfill` rebuild `player_stats_agg`, then restarts the downloader on success. It also
writes a verbose ignored log file under `maintenance_logs/` (override with `TF2LS_MAINT_LOG_DIR`).

Pass `--no-tmux` / `--foreground` for CI, cron, or non-interactive terminals.

`app.stats_backfill` options `--skip-files`, `--min-log-id`, and `--max-log-id` are forwarded when
you append them to the wrapper (see the **Backfill Stats DB** section above).

### Backfill Raw Events DB

Re-parse existing `log_*.log.zip` files into `raw_events.db`:

```bash
docker compose run --rm downloader python -m app.raw_backfill --batch-size 200
```

### Fetch Missing Raw Zips

Download raw zips for JSON logs that already exist locally:

```bash
docker compose run --rm downloader python -m app.raw_json_gap_fetch
```

For large runs, use the tmux wrapper:

```bash
./scripts/raw_zip_gap_fetch.sh
```

Useful options include `--dry-run`, `--from-id`, `--to-id`, `--limit`, `--batch-size`, `--shard-index`, and `--shard-total`. See `python -m app.raw_json_gap_fetch --help`.

### Fix Log Rounds

Rebuild only the `log_rounds` table from existing JSON logs:

```bash
docker compose run --rm downloader python -m app.fix_log_rounds_from_json
```

Options include `--dry-run`, `--from-id`, `--to-id`, `--logs-dir`, and `--db-path`.

### Backfill Player Names

Populate `player_names` from each log's `names` payload. This can run while the downloader is active.

```bash
docker compose run --rm downloader python -m app.player_names_backfill --batch-size 1000
```

## Map overview bounds (heatmaps)

Top-down PNGs live in `static/map_overviews/`. To align Hammer world coordinates with each image, build the calibration bundle (needs `stats.db` and indexed `raw_events.db`):

```bash
python tools/map_bounds_calibrator/build_bundle.py
cd tools/map_bounds_calibrator/bundle && python -m http.server 8765
```

Open `http://localhost:8765/`, tune bounds per map, download `bounds.json`, and save it as `static/map_overviews/bounds.json`. See `tools/map_bounds_calibrator/README.md` and `static/map_overviews/README.md`.

## Tests

Run the Python suite through the isolated Docker Compose test profile:

```bash
docker compose --profile test build test
docker compose --profile test run --rm test
```

Run a targeted test file:

```bash
docker compose --profile test run --rm test python -m pytest tests/test_search_utils.py -q
```

JavaScript tests:

```bash
npm install
npm test
```

The Docker test service does not load `.env` and uses temporary data directories.

---

Hosted at [search.kastal.ing](https://search.kastal.ing). Contact Kastaling on Discord for questions.
