# Project: Telegram Auto-Downloader

## Overview
Auto-downloads all files from Telegram group `t.me/researchreportss` (equity research reports) to `F:\2_Telegram_Download`. Runs as a background service on Windows with auto-start on login.

## Stack
- Python 3.10, Telethon (Telegram API client), Windows 11
- Download target: `F:\2_Telegram_Download`
- Config: `G:\Telegram\config.ini` (contains API keys — never commit)
- Session file: `G:\Telegram\session.session` (auth token — never commit)

## Key Files
| File | Purpose |
|---|---|
| `downloader.py` | Main download logic — iterates all messages, downloads media, tracks progress |
| `monitor.py` | Status dashboard — shows recent files, stats, running state |
| `config.ini` | User settings: API creds, download path, worker count, file filters |
| `progress.json` | Tracks downloaded message IDs per channel (auto-generated) |
| `file_hashes.json` | SHA-256 hash index + Telegram file IDs for deduplication |
| `silent_run.vbs` | VBS wrapper to run downloader without console window |
| `run.bat` | Manual launcher with setup |
| `monitor.bat` | Quick monitor launcher |
| `install_startup.bat` | Creates Windows scheduled task for auto-start |
| `uninstall_startup.bat` | Removes the scheduled task |

## Architecture
```
downloader.py
├── Telethon client (user account API)
├── iter_messages(reverse=True) — oldest to newest
├── Deduplication (3 layers):
│   ├── Message ID (progress.json)
│   ├── Telegram file ID (pre-download skip)
│   └── SHA-256 content hash (post-download check)
├── Concurrent downloads (asyncio semaphore, 4 workers)
└── Watchdog (keeps Telegram Desktop alive)
```

## Conventions
- Use asyncio for all I/O operations
- Progress saved every 10 downloads for crash resilience
- Filenames sanitized for Windows (no `<>:"/\|?*`)
- Duplicate filenames resolved by appending `_msgID`
- Config via `configparser` (INI format)

## Important Notes
- Telegram API rate limits: Telethon handles flood waits automatically
- First run requires interactive phone login (one-time, session persists)
- The group has ~22K+ messages and 32K+ files (~43 GB)
- Scheduled task: `TelegramAutoDownloader` (runs at logon, elevated)

## Do Not
- Never commit `config.ini`, `session.session`, or `*.session-journal`
- Never hardcode API credentials in Python files
- Never use synchronous I/O in the download pipeline
