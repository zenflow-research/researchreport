"""
Telegram Auto-Downloader Dashboard
===================================
Web UI at http://localhost:8510
- Monitor download progress and recent files
- Edit CLAUDE.md project context
- View/edit config
"""

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template_string, request, redirect, url_for, jsonify

app = Flask(__name__)

BASE_DIR = Path(__file__).parent
DOWNLOAD_PATH = Path("F:/2_Telegram_Download")
PROGRESS_FILE = BASE_DIR / "progress.json"
HASH_INDEX_FILE = BASE_DIR / "file_hashes.json"
CLAUDE_MD_FILE = BASE_DIR / "CLAUDE.md"
CONFIG_FILE = BASE_DIR / "config.ini"

HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Telegram Downloader Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            background: #0f1117;
            color: #e0e0e0;
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            padding: 20px 32px;
            border-bottom: 1px solid #2a2a3e;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .header h1 {
            font-size: 1.4rem;
            font-weight: 600;
            color: #fff;
        }
        .header .status {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 0.9rem;
        }
        .status-dot {
            width: 10px; height: 10px;
            border-radius: 50%;
            display: inline-block;
        }
        .status-dot.running { background: #4ade80; box-shadow: 0 0 8px #4ade8066; }
        .status-dot.stopped { background: #f87171; box-shadow: 0 0 8px #f8717166; }

        .tabs {
            display: flex;
            background: #1a1a2e;
            border-bottom: 1px solid #2a2a3e;
            padding: 0 32px;
        }
        .tab {
            padding: 12px 24px;
            color: #888;
            text-decoration: none;
            border-bottom: 2px solid transparent;
            font-size: 0.9rem;
            transition: all 0.2s;
        }
        .tab:hover { color: #ccc; }
        .tab.active {
            color: #60a5fa;
            border-bottom-color: #60a5fa;
        }

        .container { max-width: 1200px; margin: 0 auto; padding: 24px 32px; }

        /* Stats cards */
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        .stat-card {
            background: #1a1a2e;
            border: 1px solid #2a2a3e;
            border-radius: 8px;
            padding: 20px;
        }
        .stat-card .label {
            font-size: 0.8rem;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }
        .stat-card .value {
            font-size: 1.8rem;
            font-weight: 700;
            color: #fff;
        }
        .stat-card .value.blue { color: #60a5fa; }
        .stat-card .value.green { color: #4ade80; }
        .stat-card .value.purple { color: #a78bfa; }

        /* File list */
        .file-table {
            width: 100%;
            background: #1a1a2e;
            border: 1px solid #2a2a3e;
            border-radius: 8px;
            overflow: hidden;
        }
        .file-table th {
            background: #16213e;
            padding: 12px 16px;
            text-align: left;
            font-size: 0.8rem;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .file-table td {
            padding: 10px 16px;
            border-top: 1px solid #2a2a3e;
            font-size: 0.85rem;
        }
        .file-table tr:hover td { background: #16213e44; }
        .file-name {
            max-width: 500px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .file-ext {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        .ext-pdf { background: #dc262622; color: #f87171; }
        .ext-xlsx, .ext-xls { background: #16a34a22; color: #4ade80; }
        .ext-docx, .ext-doc { background: #2563eb22; color: #60a5fa; }
        .ext-jpg, .ext-png, .ext-jpeg { background: #a855f722; color: #a78bfa; }
        .ext-zip, .ext-rar { background: #eab30822; color: #fbbf24; }

        /* Editor */
        .editor-wrap {
            background: #1a1a2e;
            border: 1px solid #2a2a3e;
            border-radius: 8px;
            overflow: hidden;
        }
        .editor-header {
            padding: 12px 16px;
            background: #16213e;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .editor-header h3 { font-size: 0.9rem; color: #ccc; }
        textarea {
            width: 100%;
            min-height: 500px;
            background: #0f1117;
            color: #e0e0e0;
            border: none;
            padding: 16px;
            font-family: 'Cascadia Code', 'Fira Code', 'Consolas', monospace;
            font-size: 0.85rem;
            line-height: 1.6;
            resize: vertical;
            outline: none;
        }
        textarea:focus { background: #12131a; }

        .btn {
            padding: 8px 20px;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.85rem;
            font-weight: 600;
            transition: all 0.2s;
        }
        .btn-primary {
            background: #2563eb;
            color: #fff;
        }
        .btn-primary:hover { background: #3b82f6; }
        .btn-success {
            background: #16a34a;
            color: #fff;
        }
        .btn-success:hover { background: #22c55e; }

        .save-msg {
            display: inline-block;
            margin-left: 12px;
            color: #4ade80;
            font-size: 0.85rem;
            opacity: 0;
            transition: opacity 0.3s;
        }
        .save-msg.show { opacity: 1; }

        .refresh-note {
            text-align: center;
            padding: 16px;
            color: #555;
            font-size: 0.8rem;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Telegram Auto-Downloader</h1>
        <div class="status">
            <span class="status-dot {{ 'running' if is_running else 'stopped' }}"></span>
            <span>{{ 'Running' if is_running else 'Stopped' }}</span>
            <span style="color:#555; margin-left:8px;">t.me/researchreportss</span>
        </div>
    </div>

    <div class="tabs">
        <a href="/" class="tab {{ 'active' if page == 'monitor' }}">Monitor</a>
        <a href="/claude-md" class="tab {{ 'active' if page == 'claude-md' }}">CLAUDE.md</a>
        <a href="/config" class="tab {{ 'active' if page == 'config' }}">Config</a>
    </div>

    <div class="container">
        {% if page == 'monitor' %}
            <div class="stats">
                <div class="stat-card">
                    <div class="label">Messages Processed</div>
                    <div class="value blue">{{ "{:,}".format(msgs_downloaded) }}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Files Downloaded</div>
                    <div class="value green">{{ "{:,}".format(total_files) }}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Total Size</div>
                    <div class="value purple">{{ total_size }}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Unique Hashes</div>
                    <div class="value">{{ "{:,}".format(unique_hashes) }}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Last Activity</div>
                    <div class="value" style="font-size:1rem;">{{ last_run }}</div>
                </div>
            </div>

            <h3 style="margin-bottom:12px; color:#888; font-size:0.9rem;">RECENT FILES</h3>
            <div class="file-table">
                <table style="width:100%; border-collapse:collapse;">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>File</th>
                            <th>Type</th>
                            <th>Size</th>
                            <th>Downloaded</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for f in recent_files %}
                        <tr>
                            <td style="color:#555;">{{ loop.index }}</td>
                            <td class="file-name">{{ f.name }}</td>
                            <td><span class="file-ext ext-{{ f.ext }}">{{ f.ext }}</span></td>
                            <td style="color:#888;">{{ f.size }}</td>
                            <td style="color:#888;">{{ f.when }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            <div class="refresh-note">Auto-refreshes every 30 seconds | <a href="/" style="color:#60a5fa;">Refresh now</a></div>
            <script>setTimeout(() => location.reload(), 30000);</script>

        {% elif page == 'claude-md' %}
            <form method="POST" action="/claude-md" id="claudeForm">
                <div class="editor-wrap">
                    <div class="editor-header">
                        <h3>CLAUDE.md - Project Context for Claude Code</h3>
                        <div>
                            <button type="submit" class="btn btn-primary">Save</button>
                            <span class="save-msg {{ 'show' if saved }}" id="saveMsg">Saved!</span>
                        </div>
                    </div>
                    <textarea name="content" spellcheck="false">{{ claude_md_content }}</textarea>
                </div>
            </form>
            <script>
                // Ctrl+S to save
                document.addEventListener('keydown', function(e) {
                    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
                        e.preventDefault();
                        document.getElementById('claudeForm').submit();
                    }
                });
                // Auto-hide save message
                {% if saved %}
                setTimeout(() => document.getElementById('saveMsg').classList.remove('show'), 3000);
                {% endif %}
            </script>

        {% elif page == 'config' %}
            <form method="POST" action="/config">
                <div class="editor-wrap">
                    <div class="editor-header">
                        <h3>config.ini</h3>
                        <div>
                            <button type="submit" class="btn btn-primary">Save</button>
                            <span class="save-msg {{ 'show' if saved }}" id="saveMsg">Saved! Restart downloader to apply.</span>
                        </div>
                    </div>
                    <textarea name="content" spellcheck="false" style="min-height:350px;">{{ config_content }}</textarea>
                </div>
            </form>
            <script>
                document.addEventListener('keydown', function(e) {
                    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
                        e.preventDefault();
                        document.querySelector('form').submit();
                    }
                });
                {% if saved %}
                setTimeout(() => document.getElementById('saveMsg').classList.remove('show'), 3000);
                {% endif %}
            </script>
        {% endif %}
    </div>
</body>
</html>
"""


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
        return f"{int(diff.total_seconds() / 3600)}h {int((diff.total_seconds() % 3600) / 60)}m ago"
    else:
        return dt.strftime("%Y-%m-%d %H:%M")


def is_downloader_running():
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10
        )
        return "python.exe" in result.stdout
    except Exception:
        return False


def get_stats():
    msgs_downloaded = 0
    last_run = "Never"
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
        for cid, data in progress.items():
            msgs_downloaded = len(data.get("downloaded", []))
            last_run = data.get("last_run", "Unknown")
            if last_run != "Unknown":
                try:
                    dt = datetime.fromisoformat(last_run)
                    last_run = format_time(dt.timestamp())
                except Exception:
                    pass

    unique_hashes = 0
    if HASH_INDEX_FILE.exists():
        with open(HASH_INDEX_FILE) as f:
            hashes = json.load(f)
        unique_hashes = len({k: v for k, v in hashes.items() if not k.startswith("tg:")})

    total_files = 0
    total_size_bytes = 0
    if DOWNLOAD_PATH.exists():
        all_files = [f for f in DOWNLOAD_PATH.iterdir() if f.is_file()]
        total_files = len(all_files)
        total_size_bytes = sum(f.stat().st_size for f in all_files)

    return {
        "msgs_downloaded": msgs_downloaded,
        "total_files": total_files,
        "total_size": format_size(total_size_bytes),
        "unique_hashes": unique_hashes,
        "last_run": last_run,
    }


def get_recent_files(n=25):
    if not DOWNLOAD_PATH.exists():
        return []
    files = [f for f in DOWNLOAD_PATH.iterdir() if f.is_file()]
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    result = []
    for f in files[:n]:
        ext = f.suffix.lower().lstrip(".")
        result.append({
            "name": f.name,
            "ext": ext or "file",
            "size": format_size(f.stat().st_size),
            "when": format_time(f.stat().st_mtime),
        })
    return result


@app.route("/")
def monitor():
    stats = get_stats()
    return render_template_string(
        HTML_TEMPLATE,
        page="monitor",
        is_running=is_downloader_running(),
        recent_files=get_recent_files(),
        saved=False,
        **stats,
    )


@app.route("/claude-md", methods=["GET", "POST"])
def claude_md():
    saved = False
    if request.method == "POST":
        content = request.form.get("content", "")
        CLAUDE_MD_FILE.write_text(content, encoding="utf-8")
        saved = True

    content = ""
    if CLAUDE_MD_FILE.exists():
        content = CLAUDE_MD_FILE.read_text(encoding="utf-8")

    return render_template_string(
        HTML_TEMPLATE,
        page="claude-md",
        is_running=is_downloader_running(),
        claude_md_content=content,
        saved=saved,
        # Dummy values for template
        msgs_downloaded=0, total_files=0, total_size="", unique_hashes=0,
        last_run="", recent_files=[],
    )


@app.route("/config", methods=["GET", "POST"])
def config():
    saved = False
    if request.method == "POST":
        content = request.form.get("content", "")
        CONFIG_FILE.write_text(content, encoding="utf-8")
        saved = True

    content = ""
    if CONFIG_FILE.exists():
        content = CONFIG_FILE.read_text(encoding="utf-8")

    return render_template_string(
        HTML_TEMPLATE,
        page="config",
        is_running=is_downloader_running(),
        config_content=content,
        saved=saved,
        msgs_downloaded=0, total_files=0, total_size="", unique_hashes=0,
        last_run="", recent_files=[],
    )


if __name__ == "__main__":
    print("=" * 50)
    print("  Dashboard: http://localhost:8510")
    print("=" * 50)
    app.run(host="0.0.0.0", port=8510, debug=False)
