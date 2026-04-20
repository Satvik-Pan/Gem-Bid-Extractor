@echo off
setlocal
cd /d "%~dp0"
python main.py
if %errorlevel% neq 0 (
  echo Extraction failed with code %errorlevel%
  exit /b %errorlevel%
)
echo Extraction completed.
echo Updated files: output\Extracted_bids.xlsx and output\doubtful_bids.xlsx
echo Dashboard source updated in Supabase worklist for extracted/doubtful/history tabs.
endlocal
