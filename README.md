# Gem Bid Extractor

Cybersecurity bid extractor with two independent GEM pipelines, strict Anthropic classification, append-only Excel outputs, and a Supabase-backed operations dashboard.

## Current Architecture

1. Pipeline 1: full GEM feed from the last 3 days, then broad LLM prefilter.
2. Pipeline 2: keyword-based GEM search.
3. Merge and dedupe by reference number.
4. Final strict Anthropic classification into `EXTRACTED`, `DOUBTFUL`, or `REJECTED`.
5. Append-only Excel writes for extracted and doubtful rows.
6. Queue-first Supabase sync so DB failure does not stop extraction.
7. Dashboard reads the shared worklist and supports Seen, Tick, and Cross actions.

## Project Structure

- `main.py` - extractor entrypoint.
- `run_extractor.bat` - Windows launcher for one-click runs.
- `src/gem_bid_extractor/` - extractor package.
- `src/gem_bid_extractor/pipeline.py` - orchestration for both pipelines and final classification.
- `src/gem_bid_extractor/anthropic_llm.py` - strict Anthropic client with retry and DNS fallback.
- `src/gem_bid_extractor/supabase_store.py` - queue-backed Supabase sync.
- `src/gem_bid_extractor/dns_cache.py` - cached DNS/IP fallback helper.
- `output/` - generated Excel files.
- `data/` - processed state and DB sync queue files.
- `logs/` - runtime logs.
- `dashboard/` - Next.js operations dashboard.

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

## Run Dashboard

```powershell
cd dashboard
npm run dev
```

## Backfill Dashboard Data

If the dashboard is deployed correctly but shows no rows, backfill existing local Excel output into Supabase:

```powershell
python tools/backfill_dashboard_from_excel.py
```

Then refresh the dashboard URL.

## Daily Automation at 11:00 AM

To run extractor + DB sync + optional GitHub auto-push every day at 11:00 AM on Windows:

1. Ensure environment is configured in `.env`.
2. Keep `AUTO_GIT_PUSH=1` (default in `run_extractor.bat`).
3. Register the scheduled task:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\register_daily_task.ps1 -TaskName GemBidExtractorDaily -RunAt "11:00"
```

4. Verify task:

```powershell
Get-ScheduledTask -TaskName GemBidExtractorDaily | Format-List
```

This task runs `run_extractor.bat`, which performs:

1. GEM extraction and dual pipeline classification.
2. Excel append update.
3. Supabase sync for dashboard tabs.
4. Git auto-commit + GitHub push (when changes exist).

## Render Deployment

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
5. If you want the extractor on Render too, deploy it as a separate worker service or scheduled job. The dashboard should stay separate from the extractor because the extractor is long-running and API-heavy.

## Notes

- The extractor now uses queue-first persistence, so DB outages only delay sync instead of breaking the run.
- Anthropic calls include retries plus DNS/IP fallback using a cached host resolution path.
- Keep the repository secrets out of GitHub; use `.env` locally and Render environment variables in production.