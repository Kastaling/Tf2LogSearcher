# AGENTS.md

This repository is a FastAPI application that maintains a local, searchable mirror of
[logs.tf](https://logs.tf) data. A background downloader saves parsed match JSON and raw
TF2 server-log ZIPs, indexes several SQLite databases, and the web process serves a
vanilla-JavaScript UI plus JSON APIs for chat, player statistics, profiles, leaderboards,
co-players, log matching, and locally enriched log-detail pages.

## Repository map

- `app/main.py` creates the FastAPI app, mounts `static/`, and performs lightweight or
  background startup initialization. Production entry point: `app.main:app`.
- `app/routes.py` is the HTTP boundary: input validation, per-IP rate limiting, request
  logging, response caching, Steam resolution/avatar calls, and page/API routes.
- `app/search/search.py` owns most read/query behavior across the local JSON library and
  SQLite indexes. `app/combined_logs.py`, `app/gamemodes.py`, and `app/log_utils.py` hold
  shared filtering/normalization rules.
- `app/downloader.py` is the long-running ingestion entry point. `app/logs_tf.py` handles
  logs.tf API conversion/fetching; `app/raw_zip_io.py` handles raw ZIP outcomes; and
  `app/raw_log_parser.py` parses server-log events.
- `app/chat_db.py`, `app/stats_db.py`, and `app/raw_db.py` define the primary SQLite
  schemas and replace/index operations. Schema creation and migrations live in these
  modules; do not maintain a separate schema file.
- `app/log_detail.py` joins local JSON, chat/stats indexes, and optional raw events into
  sectioned log-detail payloads. `app/log_detail_cache_db.py` caches those sections.
- `app/*_backfill.py`, `app/rebuild_agg.py`, and related modules are explicit maintenance
  commands. The shell wrappers in `scripts/` coordinate long-running destructive rewrites.
- `static/index.html`, `static/css/main.css`, and `static/js/*.js` are the no-build
  frontend. JavaScript tests are under `tests/js/` and run in Jest.
- `tests/` contains Python unit and integration-style tests. They use temporary paths and
  mocked HTTP; a few richer log-detail tests use ignored `examples/<id>.json` plus
  `examples/log_<id>.log.zip` pairs and skip when the samples are absent.
- `Dockerfile` and `docker-compose.example.yml` are the tracked runtime/test definitions.
  A local `docker-compose.yml` is intentionally ignored.
- `legacy/` is the pre-modernization implementation and analysis tooling. Treat it as
  historical reference only; current behavior is defined by `app/`, `static/`, and tests.

Ignored local directories such as `logs/`, `raw_logs/`, `downloader_state/`,
`request_logs/`, `examples/`, `docs/`, and `tools/` may be large or machine-specific and
may not exist in a fresh clone. Do not make current application code depend on ignored
helpers or samples.

## Data flow and boundaries

1. The downloader pages through logs.tf. Parsed JSON is stored as `LOGS_DIR/<id>.json`.
2. The same payload replaces per-log chat rows in `chat.db` and normalized player/match
   rows in `stats.db`. Player aggregate tables are refreshed separately and feed profiles
   and leaderboards.
3. When enabled, raw ZIPs remain compressed in `RAW_LOGS_DIR`; parsed kills, captures,
   ubers, and other events are replaced per log in `raw_events.db`.
4. Web routes query indexes through `app/search/search.py`. Some cross-database queries
   attach `chat.db` to a stats connection; preserve the helper-based exclusions for
   combined uploads and poisoned logs.
5. Built-in log details require a local JSON file and optionally enrich it from all three
   indexes. Cache fingerprints invalidate when source files/rows change; section versions
   invalidate code-only changes to builder output.

Important invariants:

- `LOGS_DIR` contains only numeric `<id>.json` log files. Cursor/skip/checkpoint files
  belong in `DOWNLOADER_STATE_DIR`.
- Raw archives are `log_<id>.log.zip` and are parsed in memory; do not casually expand a
  full raw library to disk.
- Steam IDs cross APIs in both SteamID3 and 17-digit SteamID64 forms. Use conversions in
  `app/logs_tf.py` and constants/helpers already in the codebase.
- Re-ingestion replaces a log atomically. Preserve transaction boundaries, WAL/busy
  timeout behavior, foreign keys, batch commits, and rollback/error handling.
- If a builder or cached API payload in `app/log_detail.py` changes shape or logic, bump
  only the affected entry in `SECTION_VERSIONS`. Source-only changes need no bump. Do not
  delete `log_detail_cache.db` as routine invalidation.
- `app/config.py` reads environment variables into module constants at import time.
  Tests that change configuration must set/monkeypatch it before importing the consumer,
  or patch the imported module constant explicitly. `tests/conftest.py` establishes safe
  temporary paths before test collection imports application modules.
- Frontend files are served directly. Keep API field changes synchronized with their
  consumers in `static/js/`; there is no bundling or transpilation step.

## Setup and normal commands

Docker Compose is the supported default:

```bash
cp docker-compose.example.yml docker-compose.yml  # only when no local file exists
# Optional: copy .env.example to .env and set local overrides
docker compose up -d
curl --fail http://localhost:8027/health
```

Run exactly one downloader variant at a time: default `downloader` (JSON + raw), profile
`downloader-json`, or profile `downloader-raw`. See `README.md` for the exact profile
commands and all supported environment variables.

Python tests should run through the isolated, non-root Compose test target. It intentionally
does not load `env_file` and uses temporary test data:

```bash
docker compose --profile test build test
docker compose --profile test run --rm test
docker compose --profile test run --rm test python -m pytest tests/test_log_detail.py -q
```

Do not default to host `pytest`; host interpreters commonly lack the pinned project
dependencies. A host virtual environment plus `pip install -r requirements.txt` is an
acceptable explicit local workflow.

JavaScript has no production build, only tests:

```bash
npm ci
npm test
# npm run test:coverage  # when coverage is useful
```

There is currently no tracked CI workflow, formatter, linter, or static type checker.
Do not claim those checks ran or introduce incidental reformatting. For every change, run
the narrowest relevant Python/JS tests; for cross-cutting ingestion, schema, or API work,
run the full suites. Also run `git diff --check` and inspect the final diff.

For log-format/parser work, prefer the ignored paired samples in `examples/`. Do not fetch
new real logs or bulk-download data unless the task requires it. If a specific sample must
be fetched, `scripts/fetch_example_log.py` is the existing rate-limited helper.

## Security and production-data safeguards

- Never print, commit, or copy `.env`; it may contain `STEAM_WEB_API_KEY` and other local
  secrets. Use `.env.example` to document configuration. Do not bake secrets into Compose,
  Docker layers, frontend code, API responses, fixtures, or logs.
- Treat all ignored data directories as production-like unless told otherwise. Match JSON
  and raw logs contain player/chat data. Request CSVs contain IP addresses, user agents,
  referrers, and queries. State databases may contain hashed visitor identifiers, Discord
  webhook URLs/deactivation tokens, caches, and operational history.
- Never run backfills, aggregate rebuilds, poison-list purges, gap fetches, or downloader
  processes against the user's existing paths merely to verify code. Tests must use temp
  directories/mocks or the Compose test service.
- `routes._client_ip` trusts the first `X-Forwarded-For` value. Public deployment must put
  the app behind a trusted proxy that replaces/sanitizes this header, or rate limiting,
  visitor counting, and request attribution are spoofable.
- Keep `SHOW_STORAGE_STATS=0` on public instances unless exposing library sizes is
  intentional. Steam vanity/avatar and logs.tf access are outbound calls; preserve their
  limits, delays, timeouts, mocks, and graceful no-key behavior.
- Validate paths and numeric log IDs before file access. Preserve the existing ZIP path
  safety and fetch/decode error handling when changing raw-log handling.

## Operational cautions

- Do not deploy, restart services, or alter live data as part of ordinary development.
- Stop the downloader before rewriting `chat.db` or `stats.db`. Raw-event re-ingestion
  stops both downloader and web to avoid concurrent readers/writers. The documented
  wrappers leave services stopped after failure or interruption; report that state rather
  than silently restarting or retrying.
- Long maintenance commands commit in batches. `--skip-files` is a count of already
  committed sorted files, not a logs.tf ID; use `--min-log-id`/`--max-log-id` for ID ranges.
- SQLite schema/index initialization can be expensive on large libraries. The web starts
  stats initialization in a retrying background thread; do not move it back into blocking
  startup without measuring production-sized data.
- Poisoned IDs/uploaders must remain excluded consistently from downloading/indexing,
  chat/stats search, profiles, and caches. Notes in the poison JSON are non-functional.
- Map-overview bounds and generated calibration bundles are local/operator assets. Confirm
  axis orientation and bounds against actual raw positions before shipping heatmaps.

## Conventions and historical context

- Follow the existing typed Python style, `pathlib.Path`, parameterized SQL, small helper
  functions, explicit transactions, and best-effort behavior only where the surrounding
  feature is already best-effort (caches, avatars, optional enrichment).
- Add focused regression tests beside the closest existing test module. Mock network calls
  with the established `respx`/mock patterns; never make automated tests depend on live
  logs.tf, Steam, or Discord.
- Preserve unrelated worktree changes. Stage and commit only files in the requested scope.

History explains several deliberate boundaries but current code/tests remain authoritative:

- The March 2026 modernization moved the old scripts under `legacy/`; do not revive them as
  runtime entry points simply because they contain similar logic.
- April commits separated raw ZIP/index storage, replaced the tracked local Compose file
  with a tracked example, and added the non-root Compose test target.
- May commits made poisoned-log exclusion cross-cutting, added durable profile caches and
  aggregate tables, and introduced sectioned internal log-detail pages.
- June/July commits expanded raw-event enrichment/backfill recovery and replaced ambiguous
  raw-fetch return values with `RawZipFetchOutcome`; preserve the explicit distinction
  among downloaded, unavailable, and failed outcomes.
