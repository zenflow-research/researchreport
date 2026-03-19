@echo off
title Telegram Auto-Downloader
echo ============================================================
echo   Telegram Auto-Downloader for t.me/researchreportss
echo ============================================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH.
    echo Download from https://python.org
    pause
    exit /b 1
)

REM Install dependencies if needed
if not exist ".venv" (
    echo [setup] Creating virtual environment...
    python -m venv .venv
    echo [setup] Installing dependencies...
    .venv\Scripts\pip install -r requirements.txt
    echo.
)

REM Check config
findstr /C:"YOUR_API_ID" config.ini >nul 2>&1
if not errorlevel 1 (
    echo ERROR: You need to set your API credentials in config.ini
    echo.
    echo   1. Go to https://my.telegram.org
    echo   2. Log in with your phone number
    echo   3. Click "API development tools"
    echo   4. Create an app ^(any name/description^)
    echo   5. Copy api_id and api_hash into config.ini
    echo.
    notepad config.ini
    pause
    exit /b 1
)

echo [starting] Running downloader...
echo.
.venv\Scripts\python downloader.py

echo.
pause
