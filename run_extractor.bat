@echo off
setlocal

:: Change to the directory where this batch file lives
cd /d "%~dp0"

set "SCHEDULED_MODE=0"
if /I "%~1"=="--scheduled" set "SCHEDULED_MODE=1"

echo ============================================
echo  GEM Bid Extractor - Daily Run
echo  %date% %time%
echo ============================================

:: Run the Python extractor
python main.py
if %errorlevel% neq 0 (
  echo [ERROR] Extraction failed with code %errorlevel%
  if "%SCHEDULED_MODE%"=="0" pause
  exit /b %errorlevel%
)

:: Auto git commit and push to GitHub if there are changes
echo.
echo Running auto git sync to GitHub...
powershell -ExecutionPolicy Bypass -File "%~dp0tools\auto_git_push.ps1"
if %errorlevel% neq 0 (
  echo [ERROR] Auto git sync failed with code %errorlevel%
  if "%SCHEDULED_MODE%"=="0" pause
  exit /b %errorlevel%
)

echo.
echo ============================================
echo  Extraction completed successfully.
echo  Updated: output\Extracted_bids.xlsx
echo           output\doubtful_bids.xlsx
echo  Dashboard: Supabase synced for all tabs.
echo  Git: Changes pushed to GitHub (if any).
echo ============================================

if "%SCHEDULED_MODE%"=="0" pause

endlocal
