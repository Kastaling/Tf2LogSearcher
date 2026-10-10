# Agent guide

Tf2LogSearcher is a FastAPI service and vanilla JavaScript UI for building a local
logs.tf library and searching TF2 match chat, player statistics, profiles, and log
details. `app.downloader` fetches parsed JSON and optional raw server-log ZIPs;
the web app reads the resulting files and SQLite indexes. Read `README.md` for
operator commands and `.env.example` for configuration before changing either.

## Where to work

- `app/main.py` creates the FastAPI app, serves `static/`, and initializes DBs at
  startup; `app/routes.py` handles HTTP validation, rate limits, request logging,
  cache use, and page/API routes. `app/config.py` reads environment variables at
  import time.
- `app/downloader.py` is the ingestion loop. `app/logs_tf.py` handles logs.tf
  requests and Steam ID conversion; `app/raw_zip_io.py`, `app/raw_unavailable.py`,
  and `app/raw_log_parser.py` handle raw ZIP download outcomes and event parsing.
- `app/chat_db.py`, `app/stats_db.py`, and `app/raw_db.py` own the main SQLite
  schemas and index writes. `app/search/search.py` owns most reads. Preserve the
  shared combined-log and poisoned-log rules in `app/combined_logs.py` and
  `app/poisoned_logs.py` across all affected queries and indexes.
- `app/log_detail.py` builds sectioned `/api/log/{id}` payloads from local data;
  `app/log_detail_cache_db.py` stores the sections. `static/js/log-detail.js`
  consumes them. Other `static/js/` files consume search/profile APIs; there is
  no frontend compilation step.
- `app/*_backfill.py`, `app/rebuild_agg.py`, and `scripts/` contain maintenance
  jobs. `tests/` has pytest and Jest tests; `tests/conftest.py` sets temporary
  paths before test modules import `app.*`.
- `Dockerfile` and `docker-compose.example.yml` define production and isolated
  test services. `legacy/` is historical code, not a runtime entry point.

## Data flow and invariants

- Parsed logs are `LOGS_DIR/<numeric-id>.json`; downloader cursors, skip lists,
  progress, and other state live in `DOWNLOADER_STATE_DIR`. Raw archives are
  `RAW_LOGS_DIR/log_<id>.log.zip`; the parser reads ZIP content in memory.
- JSON ingest attempts separate per-log writes to `chat.db` and `stats.db`.
  Raw ingest separately updates `raw_events.db`; player aggregate refreshes are
  batched. These stores are not one cross-database transaction. A present JSON
  file does not prove every index succeeded. Preserve per-log transactions,
  retry/rollback handling, and aggregate cache invalidation when editing ingest.
- Steam IDs appear in SteamID3 and SteamID64 forms. Reuse `app/logs_tf.py`
  conversions. Keep gamemode normalization and combined-log filtering consistent
  between Python, SQL, and API/UI paths.
- Log-detail cache lookup requires both a source fingerprint and a section
  version. `compute_source_fingerprint` covers JSON file metadata and selected
  stats/chat/raw metadata and counts, not every possible input. In
  `app/log_detail.py`, bump affected `SECTION_VERSIONS` entries when builder
  output or logic changes; extend the fingerprint when a new mutable input
  must invalidate a section. Add new builders to both `_SECTION_BUILDERS` and
  `SECTION_VERSIONS`. Frontend-only changes need no bump. Do not clear the DB
  for routine invalidation.
- `LOG_DETAIL_LINK_MODE` defaults to external logs.tf links. `/log/{id}` serves
  the page shell; `/api/log/{id}` needs a local JSON file. The cached `summary`
  contains link mode and URLs, but configuration is absent from the fingerprint:
  account for that when changing link behavior. Raw events and map heatmaps are
  optional enrichments, not guaranteed content.
- `app/config.py` values are imported into consumers. In tests, set environment
  before import or monkeypatch the relevant consumer's imported constant.

## Setup and verification

The README's Compose workflow uses a local, ignored `docker-compose.yml` copied
from `docker-compose.example.yml`. The example's `web` service requires a local
`.env` file through `env_file`, even if the Steam API key is blank; copy
`.env.example` for a new setup. Compose exposes the web service on host port
`8027` by default. Run only one downloader variant at a time: normal, `json-only`,
or `raw-only`. `docker compose build web downloader` builds the production image;
do not start those services just to validate a code change.

The standard Python test path is the non-root Compose `test` profile, which does
not pass real API keys or mount the local log/state volumes:

```bash
docker compose --profile test build test
docker compose --profile test run --rm test
docker compose --profile test run --rm test python -m pytest tests/test_log_detail.py -q
```

Use a host virtual environment with `requirements.txt` only when it is already
set up or explicitly requested; bare host pytest often lacks dependencies.
For JavaScript, run `npm ci` once and `npm test` (`npm run test:coverage` if
needed). `package.json` defines Jest tests; there is no production JS build.
No tracked CI workflow or lint/type-check command exists. Run tests relevant to
the change, `git diff --check`, and inspect the final diff.

For log-format bugs, ignored `examples/<id>.json` and
`examples/log_<id>.log.zip` can provide matching parsed/raw samples when present.
They are absent from the test image, so dependent tests skip there. Do not make
tests require the examples or live logs.tf/Steam/Discord calls. Use the existing
`scripts/fetch_example_log.py` only when fetching a specific fixture is in scope.
`docs/` and `tools/` are also ignored local reference/calibration material;
tracked map assets and their instructions are in `static/map_overviews/`.

## Privacy, deployment, and maintenance

- `.env` may contain `STEAM_WEB_API_KEY`. State files can contain Discord webhook
  URLs and deactivation tokens; request CSVs contain IPs, user agents, referrers,
  and queries; JSON/raw logs contain player and chat data. Treat ignored
  `logs/`, `raw_logs/`, `downloader_state/`, `request_logs/`, `examples/`, and
  `maintenance_logs/` as private data. Do not print or commit their contents.
- The Docker test stage runs as a non-root user; the production stage sets no
  `USER`. Its bind mounts must be writable by the container process, and the
  production image should not be assumed to enforce non-root permissions.
- `routes._client_ip` uses the first `X-Forwarded-For` value for logging, visitor
  counts, and limits. A public reverse proxy must sanitize that header.
  `SHOW_STORAGE_STATS` is off by default because it exposes library sizes.
- Keep downloader pacing and explicit raw-fetch outcomes (`OK`,
  `NOT_AVAILABLE`, `FAILED`). A 403/404 raw ZIP is recorded as unavailable;
  transient failures must remain retryable. Preserve safe ZIP entry selection.
- Do not run backfills, rebuilds, gap fetches, poisoned-log purges, or a
  downloader against existing data as a test. Before an authorized maintenance
  rewrite of `chat.db`, `stats.db`, or `raw_events.db`, follow the README's
  service-stop procedure. The raw re-ingest wrapper stops web and downloader;
  wrappers may leave services stopped after an error or interruption.
- `--skip-files` in backfills counts already processed sorted files; it is not a
  log ID. Use ID range flags for a numeric range. Large SQLite index creation
  may take time: web stats initialization runs in a background retry loop.
- Poisoned-log changes can purge existing DB rows. Review `app/poisoned_logs.py`
  and its tests before changing exclusion or startup behavior. Map heatmaps need
  calibrated bounds and verified axis orientation before enabling them.

## History in context

The March 2026 modernization placed the earlier scripts in `legacy/`. Later
commits moved local Compose settings to an ignored file, added the isolated
Docker test target, introduced raw-event indexing, poisoned-log exclusion,
aggregate/profile caches, and sectioned log-detail caching. The July raw-fetch
change made permanent absence distinct from transient failure. Use current
code and tests to resolve any discrepancy with this history or older guides.
