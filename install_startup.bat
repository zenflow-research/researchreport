@echo off
title Install Telegram Auto-Downloader Startup Task
echo ============================================================
echo   Installing scheduled task for auto-start on login
echo ============================================================
echo.

REM Create a VBS wrapper to run silently (no cmd window popup)
echo Creating silent launcher...
(
echo Set WshShell = CreateObject("WScript.Shell"^)
echo WshShell.Run "cmd /c ""G:\Telegram\.venv\Scripts\python.exe G:\Telegram\downloader.py""", 0, False
) > "G:\Telegram\silent_run.vbs"

REM Create scheduled task that runs at user logon
echo Creating scheduled task...
schtasks /create /tn "TelegramAutoDownloader" /tr "wscript.exe \"G:\Telegram\silent_run.vbs\"" /sc onlogon /rl highest /f

if errorlevel 1 (
    echo.
    echo ERROR: Failed to create task. Try running this script as Administrator.
    echo Right-click install_startup.bat -^> "Run as administrator"
) else (
    echo.
    echo SUCCESS! Telegram Auto-Downloader will start automatically on login.
    echo.
    echo To remove: run uninstall_startup.bat
    echo To check:  schtasks /query /tn "TelegramAutoDownloader"
)

echo.
pause
