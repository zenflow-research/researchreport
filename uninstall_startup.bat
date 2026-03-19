@echo off
title Remove Telegram Auto-Downloader Startup Task
echo Removing scheduled task...
schtasks /delete /tn "TelegramAutoDownloader" /f
echo Done.
pause
