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

2. **Optional:** Copy `.env.example` to `.env` and adjust settings (log paths, rate limits, etc.):
   ```bash
   cp .env.example .env
   ```

3. **Start both services**
   ```bash
   docker-compose up -d
   ```
   - Web UI: http://localhost:8027  
   - Downloader runs in the background and fills `./logs` with log JSONs. State is stored in `./downloader_state`.

4. **View logs**
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
| Request log (web)| `./request_logs`       | CSV of API requests (web only)   |

- **Web port:** 8027 (host) → 8000 (container). Change the left number in `docker-compose.yml` if needed.

## Configuration

See `.env.example` for all options. Important ones:

- `LOGS_DIR`, `DOWNLOADER_STATE_DIR` — paths inside the container (compose maps `./logs` and `./downloader_state`).
- `DOWNLOAD_INTERVAL_SEC` — seconds between download cycles (default 3600).
- `REQUEST_DELAY_MS`, `MAX_REQUESTS_BEFORE_BACKOFF`, `BACKOFF_SEC` — rate limiting for the logs.tf API.
- `STEAM_WEB_API_KEY` — Steam Web API key for vanity URL/name resolution.
- `REQUEST_LOG_PATH` — path to the request log CSV file.
- `CHAT_DB_PATH` — path to SQLite DB where chat rows are indexed.

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

---

*Hosted at [search.kastal.ing](https://search.kastal.ing). Contact Kastaling on Discord for questions.*
