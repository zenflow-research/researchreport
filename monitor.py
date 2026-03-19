"""
Telegram Download Monitor
=========================
Shows download status: last files downloaded, progress stats,
and whether the downloader is currently running.
"""

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

DOWNLOAD_PATH = Path("F:/2_Telegram_Download")
PROGRESS_FILE = Path(__file__).parent / "progress.json"
HASH_INDEX_FILE = Path(__file__).parent / "file_hashes.json"


def is_downloader_running():
    """Check if the downloader python process is running."""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10
        )
        return "python.exe" in result.stdout
    except Exception:
        return False


def get_recent_files(n=10):
    """Get the N most recently modified files in the download folder."""
    if not DOWNLOAD_PATH.exists():
        return []
    files = [f for f in DOWNLOAD_PATH.iterdir() if f.is_file()]
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files[:n]


def format_size(size_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def format_time(timestamp):
    dt = datetime.fromtimestamp(timestamp)
    now = datetime.now()
    diff = now - dt
    if diff.total_seconds() < 60:
        return f"{int(diff.total_seconds())}s ago"
    elif diff.total_seconds() < 3600:
        return f"{int(diff.total_seconds() / 60)}m ago"
    elif diff.total_seconds() < 86400:
        return f"{int(diff.total_seconds() / 3600)}h ago"
    else:
        return dt.strftime("%Y-%m-%d %H:%M")


def main():
    print("=" * 65)
    print("  Telegram Download Monitor")
    print("=" * 65)

    # Downloader status
    running = is_downloader_running()
    status = "RUNNING" if running else "NOT RUNNING"
    status_color = "\033[92m" if running else "\033[91m"
    print(f"\n  Downloader: {status_color}{status}\033[0m")

    # Progress stats
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
        for channel_id, data in progress.items():
            downloaded = len(data.get("downloaded", []))
            last_run = data.get("last_run", "unknown")
            print(f"  Channel ID: {channel_id}")
            print(f"  Messages downloaded: {downloaded}")
            print(f"  Last run: {last_run}")
    else:
        print("  No progress file found (hasn't run yet)")

    # Hash index stats
    if HASH_INDEX_FILE.exists():
        with open(HASH_INDEX_FILE) as f:
            hashes = json.load(f)
        content_hashes = {k: v for k, v in hashes.items() if not k.startswith("tg:")}
        tg_ids = {k: v for k, v in hashes.items() if k.startswith("tg:")}
        print(f"  Unique files (by hash): {len(content_hashes)}")
        print(f"  Telegram file IDs tracked: {len(tg_ids)}")

    # Download folder stats
    if DOWNLOAD_PATH.exists():
        all_files = [f for f in DOWNLOAD_PATH.iterdir() if f.is_file()]
        total_size = sum(f.stat().st_size for f in all_files)
        print(f"\n  Download folder: {DOWNLOAD_PATH}")
        print(f"  Total files: {len(all_files)}")
        print(f"  Total size:  {format_size(total_size)}")
    else:
        print(f"\n  Download folder not found: {DOWNLOAD_PATH}")

    # Recent files
    recent = get_recent_files(15)
    if recent:
        print(f"\n  {'-' * 61}")
        print(f"  {'Last 15 downloaded files':^61}")
        print(f"  {'-' * 61}")
        print(f"  {'File':<40} {'Size':>8}  {'When':>10}")
        print(f"  {'-'*40} {'-'*8}  {'-'*10}")
        for f in recent:
            name = f.name
            if len(name) > 38:
                name = name[:35] + "..."
            size = format_size(f.stat().st_size)
            when = format_time(f.stat().st_mtime)
            print(f"  {name:<40} {size:>8}  {when:>10}")
    else:
        print("\n  No files downloaded yet.")

    print(f"\n{'=' * 65}")


if __name__ == "__main__":
    main()
