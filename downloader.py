"""
Telegram Auto-Downloader for t.me/researchreportss
===================================================
Downloads all files from a Telegram group/channel to a local folder.
Tracks progress so it can resume where it left off.
Optionally keeps Telegram Desktop running as a watchdog.

Setup:
  1. Get api_id and api_hash from https://my.telegram.org
  2. Put them in config.ini
  3. pip install -r requirements.txt
  4. python downloader.py

First run will ask you to log in with your phone number (one-time).
"""

import asyncio
import configparser
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import (
    DocumentAttributeFilename,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
)


# ── Config ──────────────────────────────────────────────────────────────────

def load_config():
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / "config.ini"
    if not config_path.exists():
        print(f"ERROR: {config_path} not found. Copy config.ini.example and fill in your API credentials.")
        sys.exit(1)
    config.read(config_path)
    return config


# ── Progress tracking ───────────────────────────────────────────────────────

PROGRESS_FILE = Path(__file__).parent / "progress.json"
HASH_INDEX_FILE = Path(__file__).parent / "file_hashes.json"


def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def load_hash_index():
    """Load the index of file content hashes (hash -> filename)."""
    if HASH_INDEX_FILE.exists():
        with open(HASH_INDEX_FILE, "r") as f:
            return json.load(f)
    return {}


def save_hash_index(hash_index):
    with open(HASH_INDEX_FILE, "w") as f:
        json.dump(hash_index, f, indent=2)


def file_hash(filepath):
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def build_hash_index(download_path):
    """Scan existing files in download folder and build hash index."""
    index = load_hash_index()
    existing_files = set(index.values())
    for f in download_path.iterdir():
        if f.is_file() and f.name not in existing_files:
            h = file_hash(f)
            index[h] = f.name
    save_hash_index(index)
    return index


# ── Watchdog ────────────────────────────────────────────────────────────────

def find_telegram_exe():
    """Auto-detect Telegram Desktop executable path."""
    candidates = [
        Path(os.environ.get("APPDATA", "")) / "Telegram Desktop" / "Telegram.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Telegram Desktop" / "Telegram.exe",
        Path("C:/Users") / os.environ.get("USERNAME", "") / "AppData" / "Roaming" / "Telegram Desktop" / "Telegram.exe",
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return None


def is_telegram_running():
    """Check if Telegram Desktop is running."""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq Telegram.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10
        )
        return "Telegram.exe" in result.stdout
    except Exception:
        return False


def start_telegram(exe_path):
    """Launch Telegram Desktop."""
    if exe_path and Path(exe_path).exists():
        print(f"[watchdog] Starting Telegram Desktop: {exe_path}")
        subprocess.Popen([exe_path], creationflags=subprocess.DETACHED_PROCESS)
        time.sleep(5)
        return True
    print("[watchdog] Could not find Telegram Desktop executable.")
    return False


async def watchdog_loop(config):
    """Periodically check that Telegram Desktop is running."""
    if config.get("watchdog", "keep_telegram_open").lower() != "true":
        return

    exe_path = config.get("watchdog", "telegram_exe_path").strip()
    if not exe_path:
        exe_path = find_telegram_exe()

    interval = config.getint("watchdog", "check_interval", fallback=60)

    while True:
        if not is_telegram_running():
            print(f"[watchdog] Telegram Desktop not running. Restarting...")
            start_telegram(exe_path)
        await asyncio.sleep(interval)


# ── File helpers ────────────────────────────────────────────────────────────

def get_filename(message):
    """Extract filename from a message's media."""
    if not message.media:
        return None

    if isinstance(message.media, MessageMediaDocument) and message.media.document:
        doc = message.media.document
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name
        # Fallback: use mime type to guess extension
        mime = doc.mime_type or ""
        ext_map = {
            "application/pdf": ".pdf",
            "application/zip": ".zip",
            "application/x-rar-compressed": ".rar",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
            "video/mp4": ".mp4",
            "audio/mpeg": ".mp3",
            "image/jpeg": ".jpg",
            "image/png": ".png",
        }
        ext = ext_map.get(mime, "")
        return f"doc_{message.id}{ext}"

    if isinstance(message.media, MessageMediaPhoto):
        return f"photo_{message.id}.jpg"

    return None


def should_download(filename, file_types_filter):
    """Check if file matches the type filter."""
    if not file_types_filter:
        return True
    ext = Path(filename).suffix.lower().lstrip(".")
    return ext in file_types_filter


def sanitize_filename(name):
    """Remove invalid characters from filename."""
    return re.sub(r'[<>:"/\\|?*]', '_', name)


# ── Main downloader ────────────────────────────────────────────────────────

async def download_all(config):
    api_id = config.getint("telegram", "api_id")
    api_hash = config.get("telegram", "api_hash")
    channel = config.get("download", "channel")
    download_path = Path(config.get("download", "download_path"))
    workers = config.getint("download", "workers", fallback=4)
    max_size_mb = config.getint("download", "max_file_size_mb", fallback=0)
    max_size = max_size_mb * 1024 * 1024 if max_size_mb > 0 else 0

    file_types_raw = config.get("download", "file_types", fallback="").strip()
    file_types_filter = set()
    if file_types_raw:
        file_types_filter = {ft.strip().lower() for ft in file_types_raw.split(",")}

    download_path.mkdir(parents=True, exist_ok=True)

    session_path = Path(__file__).parent / "session"
    client = TelegramClient(str(session_path), api_id, api_hash)

    await client.start()
    print(f"[downloader] Logged in as: {(await client.get_me()).first_name}")

    # Resolve the channel
    entity = await client.get_entity(channel)
    print(f"[downloader] Target: {entity.title if hasattr(entity, 'title') else channel}")

    # Load progress
    progress = load_progress()
    channel_key = str(entity.id)
    downloaded_ids = set(progress.get(channel_key, {}).get("downloaded", []))
    last_max_id = progress.get(channel_key, {}).get("last_max_id", 0)

    print(f"[downloader] Already downloaded: {len(downloaded_ids)} files. Scanning for new ones...")

    # Build hash index of existing files for content deduplication
    print(f"[downloader] Building file hash index for deduplication...")
    hash_index = build_hash_index(download_path)
    print(f"[downloader] Indexed {len(hash_index)} unique files by content hash.")

    # Semaphore for concurrent downloads
    sem = asyncio.Semaphore(workers)
    stats = {"downloaded": 0, "skipped": 0, "skipped_dup": 0, "errors": 0, "total_bytes": 0}

    async def download_one(message):
        filename = get_filename(message)
        if not filename:
            return

        if message.id in downloaded_ids:
            stats["skipped"] += 1
            return

        if not should_download(filename, file_types_filter):
            stats["skipped"] += 1
            return

        # Check file size
        if max_size > 0 and hasattr(message.media, "document") and message.media.document:
            if message.media.document.size > max_size:
                print(f"  [skip] {filename} ({message.media.document.size / 1024 / 1024:.1f} MB exceeds limit)")
                stats["skipped"] += 1
                return

        safe_name = sanitize_filename(filename)
        dest = download_path / safe_name

        # Handle duplicate filenames by appending message ID
        if dest.exists():
            stem = dest.stem
            suffix = dest.suffix
            safe_name = f"{stem}_{message.id}{suffix}"
            dest = download_path / safe_name

        # Check Telegram file unique ID for early dedup (avoids downloading)
        tg_file_id = None
        if isinstance(message.media, MessageMediaDocument) and message.media.document:
            tg_file_id = str(message.media.document.id)
        elif isinstance(message.media, MessageMediaPhoto) and message.media.photo:
            tg_file_id = str(message.media.photo.id)

        if tg_file_id and f"tg:{tg_file_id}" in hash_index:
            print(f"  [skip-dup] {safe_name} (same Telegram file as {hash_index[f'tg:{tg_file_id}']})")
            stats["skipped_dup"] += 1
            downloaded_ids.add(message.id)
            return

        async with sem:
            try:
                print(f"  [downloading] {safe_name} (msg #{message.id})")
                await client.download_media(message, file=str(dest))

                if not dest.exists():
                    stats["errors"] += 1
                    return

                # Content hash dedup: delete if identical file already exists
                h = file_hash(dest)
                if h in hash_index:
                    print(f"  [skip-dup] {safe_name} identical to {hash_index[h]}, removing")
                    dest.unlink()
                    stats["skipped_dup"] += 1
                else:
                    file_size = dest.stat().st_size
                    stats["downloaded"] += 1
                    stats["total_bytes"] += file_size
                    hash_index[h] = safe_name

                # Track Telegram file ID for future runs
                if tg_file_id:
                    hash_index[f"tg:{tg_file_id}"] = safe_name

                downloaded_ids.add(message.id)

                # Save progress periodically
                if (stats["downloaded"] + stats["skipped_dup"]) % 10 == 0:
                    _save_current_progress(progress, channel_key, downloaded_ids)
                    save_hash_index(hash_index)

            except Exception as e:
                print(f"  [error] {safe_name}: {e}")
                stats["errors"] += 1

    # Iterate ALL messages (oldest first)
    print(f"[downloader] Scanning all messages in {channel}...")
    tasks = []
    count = 0

    async for message in client.iter_messages(entity, reverse=True):
        count += 1
        if count % 500 == 0:
            print(f"  [scan] Processed {count} messages...")

        if not message.media:
            continue
        if isinstance(message.media, MessageMediaWebPage):
            continue

        tasks.append(asyncio.create_task(download_one(message)))

        # Process in batches to avoid memory issues
        if len(tasks) >= workers * 4:
            await asyncio.gather(*tasks)
            tasks = []

    # Final batch
    if tasks:
        await asyncio.gather(*tasks)

    # Save final progress and hash index
    _save_current_progress(progress, channel_key, downloaded_ids)
    save_hash_index(hash_index)

    print(f"\n{'='*60}")
    print(f"[done] Scan complete!")
    print(f"  Messages scanned:  {count}")
    print(f"  Files downloaded:  {stats['downloaded']}")
    print(f"  Duplicates skipped:{stats['skipped_dup']}")
    print(f"  Other skipped:     {stats['skipped']}")
    print(f"  Errors:            {stats['errors']}")
    print(f"  Total size:        {stats['total_bytes'] / 1024 / 1024:.1f} MB")
    print(f"  Saved to:          {download_path}")
    print(f"{'='*60}")

    await client.disconnect()


def _save_current_progress(progress, channel_key, downloaded_ids):
    progress[channel_key] = {
        "downloaded": list(downloaded_ids),
        "last_max_id": max(downloaded_ids) if downloaded_ids else 0,
        "last_run": datetime.now().isoformat(),
    }
    save_progress(progress)


# ── Entry point ─────────────────────────────────────────────────────────────

async def main():
    config = load_config()

    # Start watchdog in background
    watchdog_task = asyncio.create_task(watchdog_loop(config))

    try:
        await download_all(config)
    except KeyboardInterrupt:
        print("\n[interrupted] Progress saved. Run again to resume.")
    finally:
        watchdog_task.cancel()


if __name__ == "__main__":
    print("=" * 60)
    print("  Telegram Auto-Downloader")
    print("  Target: t.me/researchreportss")
    print("=" * 60)
    asyncio.run(main())
