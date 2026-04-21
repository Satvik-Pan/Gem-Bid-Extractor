@echo off
setlocal
cd /d "%~dp0"

if "%AUTO_GIT_PUSH%"=="" set AUTO_GIT_PUSH=1

python main.py
if %errorlevel% neq 0 (
  echo Extraction failed with code %errorlevel%
  exit /b %errorlevel%
)

if /I "%AUTO_GIT_PUSH%"=="1" (
  echo Running auto git sync...
  powershell -ExecutionPolicy Bypass -File "%~dp0tools\auto_git_push.ps1"
)

echo Extraction completed.
echo Updated files: output\Extracted_bids.xlsx and output\doubtful_bids.xlsx
echo Dashboard source updated in Supabase worklist for extracted/doubtful/history tabs.
endlocal
