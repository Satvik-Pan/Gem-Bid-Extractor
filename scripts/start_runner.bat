@echo off
setlocal
cd /d "%~dp0.."
uvicorn scripts.webhook_runner:app --host 0.0.0.0 --port 8080
endlocal
