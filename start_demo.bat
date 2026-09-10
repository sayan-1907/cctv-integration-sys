@echo off
title Sentinel CCTV Command Center
echo ========================================================
echo   SENTINEL CCTV INTEGRATION SYSTEM - COMMAND CENTER
echo ========================================================
echo.
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found in .venv!
    echo Please run setup first.
    pause
    exit /b 1
)

echo [*] Launching Sentinel Command Center in default browser...
start "" "http://localhost:8000"

echo [*] Starting Sentinel Web API ^& AI Extraction Server on http://localhost:8000 ...
echo [!] Press CTRL+C at any time to stop the server safely.
.\.venv\Scripts\python.exe -m uvicorn api:app --app-dir src --host 0.0.0.0 --port 8000

pause
