# Gem Bid Extractor

Cybersecurity bid extractor with a 5-stage pipeline, strict Anthropic classification, append-only Excel outputs, and a Supabase-backed operations dashboard.

## Architecture

1. Pipeline 1: fetch GEM bids from the first 5 pages of `all-bids` (no date-window filtering).
2. Pipeline 2: independent LLM relevance pass over all Pipeline 1 bids.
3. Pipeline 3: independent inclusion-keyword extraction over Pipeline 1 bids.
4. Pipeline 4: combine Pipeline 2 and Pipeline 3 outputs, then dedupe by reference.
5. Pipeline 5: final LLM categorization into `EXTRACTED` and `DOUBTFUL`, then deterministic hard rejects (exclusion-hit or low-confidence) and strict doubtful retention.
6. Append-only Excel writes for extracted and doubtful rows.
7. Queue-first Supabase sync so DB failure does not stop extraction.
8. Dashboard reads the shared worklist and supports Tick and Cross actions for extracted and doubtful queues.

## Project Structure

- `main.py` - extractor entrypoint.
- `run_extractor.bat` - Windows launcher (runs extractor + auto-push to GitHub).
- `src/gem_bid_extractor/` - extractor package.
- `src/gem_bid_extractor/pipeline.py` - orchestration for both pipelines and final classification.
- `src/gem_bid_extractor/anthropic_llm.py` - strict Anthropic client with retry and DNS fallback.
- `src/gem_bid_extractor/supabase_store.py` - queue-backed Supabase sync.
- `src/gem_bid_extractor/dns_cache.py` - cached DNS/IP fallback helper.
- `output/` - generated Excel files.
- `data/` - processed state and DB sync queue files.
- `logs/` - runtime logs.
- `data/last_run_status.json` - machine-readable status of the most recent extractor run.
- `dashboard/` - Next.js operations dashboard.
- `tools/` - utility scripts (auto git push, task scheduler registration, backfill).

## Local Setup

1. Install Python dependencies:

   ```powershell
   pip install -r requirements.txt
   ```

2. Install dashboard dependencies:

   ```powershell
   cd dashboard
   npm install
   ```

3. Configure `.env` in the repository root with Anthropic and Supabase values.

4. Configure `dashboard/.env.local` with the Supabase pooler connection values.

## Run Extractor

```powershell
python main.py
```

or on Windows:

```powershell
run_extractor.bat
```

## Run Dashboard Locally

```powershell
cd dashboard
npm run dev
```

## Backfill Dashboard Data

If the dashboard shows no rows, backfill existing local Excel output into Supabase:

```powershell
python tools/backfill_dashboard_from_excel.py
```

Then refresh the dashboard URL.

## Daily Automation (Windows Task Scheduler)

The extractor runs automatically every day at **12:00 PM** via Windows Task Scheduler.

### Register the scheduled task:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\register_daily_task.ps1 -TaskName GemBidExtractorDaily -RunAt "12:00"
```

### Verify the task:

```powershell
Get-ScheduledTask -TaskName GemBidExtractorDaily | Format-List
```

### What the daily run does:

1. GEM extraction and 5-stage classification flow (P1 fetch -> P2 LLM -> P3 keyword -> P4 merge -> P5 final).
2. Excel append update (`output/Extracted_bids.xlsx`, `output/doubtful_bids.xlsx`).
3. Supabase sync for dashboard tabs.
4. Git auto-commit + GitHub push (when changes exist).

### Monitor task runs:

Open **Task Scheduler** (`taskschd.msc`) → find `GemBidExtractorDaily` → check the **History** tab and **Last Run Result** column.

## Render Keep-Awake (Dual Protection)

To reduce Render free-tier sleep behavior, two keepalive paths are configured:

1. GitHub Actions workflow every 5 minutes (`.github/workflows/keep-render-awake.yml`).
2. Optional local Windows task every 10 minutes:

   ```powershell
   powershell -ExecutionPolicy Bypass -File .\tools\register_render_keepalive_task.ps1 -IntervalMinutes 10
   ```

Manual ping test:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\keep_render_awake.ps1
```

### Daily validation checklist

After the scheduled run, verify:

1. `data/last_run_status.json` has `"status": "ok"` and recent timestamp.
2. `logs/scraper.log` has final `Run summary` and `Supabase sync: enabled`.
3. Dashboard tabs load without API errors.
4. `output/Extracted_bids.xlsx` and `output/doubtful_bids.xlsx` are updated.

## Render Deployment (Dashboard)

Deploy the dashboard as a separate Render web service.

1. Create a new Web Service in Render from the GitHub repo.
2. Set the root directory to `dashboard`.
3. Use these build and start commands:
   - Build: `npm install && npm run build`
   - Start: `npm run start`
4. Add environment variables in Render:
   - `SUPABASE_DB_HOST=aws-1-ap-northeast-1.pooler.supabase.com`
   - `SUPABASE_DB_PORT=5432`
   - `SUPABASE_DB_NAME=postgres`
   - `SUPABASE_DB_USER=postgres.rigivunjxinvyzlzctoj`
   - `SUPABASE_DB_PASSWORD=...`

### Health endpoint

The dashboard exposes `GET /api/health` for uptime and DB reachability checks.
The keep-alive GitHub workflow pings this endpoint every 10 minutes.

## Recovery Runbook

If dashboard rows look stale or missing:

1. Check `data/last_run_status.json` and `logs/scraper.log` for extractor errors.
2. Run extractor manually:
   ```powershell
   python main.py
   ```
3. If DB was unavailable during run, rerun extractor once DB is healthy (queue-first sync retries).
4. If local output exists but DB is empty, run one-time recovery:
   ```powershell
   python tools/backfill_dashboard_from_excel.py
   ```
5. Refresh dashboard and verify `/api/health` returns `{"status":"ok", ...}`.

## Notes

- The extractor uses queue-first persistence, so DB outages only delay sync instead of breaking the run.
- Anthropic calls include retries plus DNS/IP fallback using a cached host resolution path.
- Keep the repository secrets out of GitHub; use `.env` locally and Render environment variables in production.
- The `StartWhenAvailable` flag ensures missed runs (e.g. PC was asleep) execute when the machine wakes up.