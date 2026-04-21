# Gem Bid Extractor

Cybersecurity bid extractor with two independent GEM pipelines, strict Anthropic classification, append-only Excel outputs, and a Supabase-backed operations dashboard.

## Architecture

1. Pipeline 1: full GEM feed from the last 3 days, then broad LLM prefilter.
2. Pipeline 2: keyword-based GEM search.
3. Merge and dedupe by reference number.
4. Final strict Anthropic classification into `EXTRACTED`, `DOUBTFUL`, or `REJECTED`.
5. Append-only Excel writes for extracted and doubtful rows.
6. Queue-first Supabase sync so DB failure does not stop extraction.
7. Dashboard reads the shared worklist and supports Seen, Tick, and Cross actions.

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

1. GEM extraction (last 3 days of bids) and dual pipeline classification.
2. Excel append update (`output/Extracted_bids.xlsx`, `output/doubtful_bids.xlsx`).
3. Supabase sync for dashboard tabs.
4. Git auto-commit + GitHub push (when changes exist).

### Monitor task runs:

Open **Task Scheduler** (`taskschd.msc`) → find `GemBidExtractorDaily` → check the **History** tab and **Last Run Result** column.

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

## Notes

- The extractor uses queue-first persistence, so DB outages only delay sync instead of breaking the run.
- Anthropic calls include retries plus DNS/IP fallback using a cached host resolution path.
- Keep the repository secrets out of GitHub; use `.env` locally and Render environment variables in production.
- The `StartWhenAvailable` flag ensures missed runs (e.g. PC was asleep) execute when the machine wakes up.