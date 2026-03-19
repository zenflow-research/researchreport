"""Flask API + React UI for Research Report Mapper.

Run:  python app.py
Open: http://localhost:8502
"""

import json
import os
import sqlite3
import subprocess
import threading
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="frontend", static_url_path="/static")

DB_PATH = os.path.join(os.path.dirname(__file__), "output", "report_mapping.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _dedup_filter():
    """Returns SQL fragment to filter duplicates. Default: hide duplicates."""
    show_dupes = request.args.get("show_dupes", "0") == "1"
    if show_dupes:
        return ""
    # Check if is_duplicate column exists
    return "AND r.is_duplicate = 0 "


def _has_dedup_column(conn):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()]
    return "is_duplicate" in cols


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    """Overall mapping statistics."""
    conn = get_db()
    try:
        has_dedup = _has_dedup_column(conn)
        show_dupes = request.args.get("show_dupes", "0") == "1"
        dedup_sql = "" if (show_dupes or not has_dedup) else "WHERE r.is_duplicate = 0"
        dedup_and = "" if (show_dupes or not has_dedup) else "AND r.is_duplicate = 0"

        total = conn.execute(f"SELECT COUNT(*) c FROM reports r {dedup_sql}").fetchone()["c"]
        mapped = conn.execute(
            f"SELECT COUNT(DISTINCT r.id) c FROM reports r "
            f"JOIN report_companies rc ON r.id = rc.report_id {dedup_and}"
        ).fetchone()["c"]

        # Duplicate stats
        total_all = conn.execute("SELECT COUNT(*) c FROM reports").fetchone()["c"]
        dupes = total_all - conn.execute(
            "SELECT COUNT(*) c FROM reports WHERE is_duplicate = 0"
        ).fetchone()["c"] if has_dedup else 0

        by_level = {}
        for lvl in [1, 2, 3]:
            cnt = conn.execute(
                f"SELECT COUNT(*) c FROM reports r WHERE mapping_level=? {dedup_and}", (lvl,)
            ).fetchone()["c"]
            by_level[lvl] = cnt

        by_type = [
            dict(r) for r in conn.execute(
                f"SELECT report_type, COUNT(*) as count FROM reports r "
                f"{dedup_sql} GROUP BY report_type ORDER BY count DESC"
            ).fetchall()
        ]

        by_broker = [
            dict(r) for r in conn.execute(
                f"SELECT broker, COUNT(*) as count FROM reports r "
                f"WHERE broker IS NOT NULL {dedup_and} "
                f"GROUP BY broker ORDER BY count DESC LIMIT 30"
            ).fetchall()
        ]

        by_sector = [
            dict(r) for r in conn.execute(
                f"SELECT rc.sector, COUNT(*) as count "
                f"FROM report_companies rc "
                f"JOIN reports r ON r.id = rc.report_id "
                f"WHERE rc.sector IS NOT NULL {dedup_and} "
                f"GROUP BY rc.sector ORDER BY count DESC"
            ).fetchall()
        ]

        by_method = [
            dict(r) for r in conn.execute(
                f"SELECT match_method, COUNT(*) as count "
                f"FROM report_companies rc "
                f"JOIN reports r ON r.id = rc.report_id "
                f"WHERE 1=1 {dedup_and} "
                f"GROUP BY match_method ORDER BY count DESC"
            ).fetchall()
        ]

        return jsonify({
            "total": total,
            "total_all": total_all,
            "mapped": mapped,
            "unmapped": total - mapped,
            "mapped_pct": round(100 * mapped / total, 1) if total else 0,
            "duplicates": dupes,
            "by_level": by_level,
            "by_type": by_type,
            "by_broker": by_broker,
            "by_sector": by_sector,
            "by_method": by_method,
        })
    finally:
        conn.close()


@app.route("/api/companies")
def api_companies():
    """Top companies by report count with search."""
    conn = get_db()
    try:
        q = request.args.get("q", "").strip()
        limit = min(int(request.args.get("limit", 50)), 200)
        offset = int(request.args.get("offset", 0))

        has_dedup = _has_dedup_column(conn)
        show_dupes = request.args.get("show_dupes", "0") == "1"
        dedup_and = "" if (show_dupes or not has_dedup) else "AND r.is_duplicate = 0"
        dedup_join = f"JOIN reports r ON r.id = rc.report_id" if dedup_and else ""

        if q:
            rows = conn.execute(f"""
                SELECT rc.data_company_id, rc.company_name, rc.nse_code,
                       rc.bse_code, rc.sector, rc.industry, rc.basic_industry,
                       COUNT(*) as report_count
                FROM report_companies rc
                {dedup_join}
                WHERE (rc.company_name LIKE ? OR rc.nse_code LIKE ?
                   OR rc.short_id LIKE ? OR rc.sector LIKE ?) {dedup_and}
                GROUP BY rc.data_company_id
                ORDER BY report_count DESC
                LIMIT ? OFFSET ?
            """, (f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%",
                  limit, offset)).fetchall()
        else:
            rows = conn.execute(f"""
                SELECT rc.data_company_id, rc.company_name, rc.nse_code,
                       rc.bse_code, rc.sector, rc.industry, rc.basic_industry,
                       COUNT(*) as report_count
                FROM report_companies rc
                {dedup_join}
                WHERE 1=1 {dedup_and}
                GROUP BY rc.data_company_id
                ORDER BY report_count DESC
                LIMIT ? OFFSET ?
            """, (limit, offset)).fetchall()

        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/company/<int:company_id>/reports")
def api_company_reports(company_id):
    """Reports for a specific company."""
    conn = get_db()
    try:
        limit = min(int(request.args.get("limit", 50)), 200)
        offset = int(request.args.get("offset", 0))

        rows = conn.execute("""
            SELECT r.id, r.filename, r.filepath, r.broker, r.report_type,
                   r.mapping_level, r.confidence, r.mapped_at, r.report_date,
                   rc.match_score, rc.match_method, rc.is_primary
            FROM reports r
            JOIN report_companies rc ON r.id = rc.report_id
            WHERE rc.data_company_id = ?
            ORDER BY r.mapped_at DESC
            LIMIT ? OFFSET ?
        """, (company_id, limit, offset)).fetchall()

        total = conn.execute(
            "SELECT COUNT(*) c FROM report_companies WHERE data_company_id=?",
            (company_id,)
        ).fetchone()["c"]

        # Company info
        info = conn.execute("""
            SELECT data_company_id, company_name, nse_code, bse_code,
                   sector, industry, basic_industry
            FROM report_companies WHERE data_company_id=? LIMIT 1
        """, (company_id,)).fetchone()

        return jsonify({
            "company": dict(info) if info else None,
            "total": total,
            "reports": [dict(r) for r in rows],
        })
    finally:
        conn.close()


@app.route("/api/reports")
def api_reports():
    """Browse all reports with filters."""
    conn = get_db()
    try:
        limit = min(int(request.args.get("limit", 50)), 200)
        offset = int(request.args.get("offset", 0))
        broker = request.args.get("broker", "").strip()
        report_type = request.args.get("type", "").strip()
        mapped = request.args.get("mapped", "").strip()  # "yes", "no", ""
        q = request.args.get("q", "").strip()

        has_dedup = _has_dedup_column(conn)
        show_dupes = request.args.get("show_dupes", "0") == "1"

        where = []
        params = []

        if not show_dupes and has_dedup:
            where.append("r.is_duplicate = 0")
        if broker:
            where.append("r.broker LIKE ?")
            params.append(f"%{broker}%")
        if report_type:
            where.append("r.report_type = ?")
            params.append(report_type)
        if mapped == "yes":
            where.append("rc.id IS NOT NULL")
        elif mapped == "no":
            where.append("rc.id IS NULL")
        if q:
            where.append("(r.filename LIKE ? OR r.raw_company_text LIKE ?)")
            params.extend([f"%{q}%", f"%{q}%"])

        where_sql = " AND ".join(where) if where else "1=1"

        rows = conn.execute(f"""
            SELECT r.id, r.filename, r.filepath, r.broker, r.report_type,
                   r.mapping_level, r.confidence, r.raw_company_text,
                   r.raw_sector_text, r.mapped_at, r.report_date,
                   rc.company_name, rc.nse_code, rc.sector,
                   rc.match_score, rc.match_method, rc.data_company_id
            FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id AND rc.is_primary=1
            WHERE {where_sql}
            ORDER BY r.id DESC
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()

        total = conn.execute(f"""
            SELECT COUNT(*) c FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id AND rc.is_primary=1
            WHERE {where_sql}
        """, params).fetchone()["c"]

        return jsonify({
            "total": total,
            "reports": [dict(r) for r in rows],
        })
    finally:
        conn.close()


@app.route("/api/sectors")
def api_sectors():
    """Sector breakdown with report counts."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT rc.sector, rc.industry,
                   COUNT(DISTINCT rc.data_company_id) as company_count,
                   COUNT(*) as report_count
            FROM report_companies rc
            WHERE rc.sector IS NOT NULL
            GROUP BY rc.sector
            ORDER BY report_count DESC
        """).fetchall()

        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/sector/<path:sector_name>")
def api_sector_detail(sector_name):
    """Companies and reports in a sector."""
    conn = get_db()
    try:
        companies = conn.execute("""
            SELECT rc.data_company_id, rc.company_name, rc.nse_code,
                   rc.industry, rc.basic_industry,
                   COUNT(*) as report_count
            FROM report_companies rc
            WHERE rc.sector = ?
            GROUP BY rc.data_company_id
            ORDER BY report_count DESC
        """, (sector_name,)).fetchall()

        return jsonify([dict(r) for r in companies])
    finally:
        conn.close()


@app.route("/api/brokers")
def api_brokers():
    """Broker list with counts."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT r.broker, COUNT(*) as total,
                   SUM(CASE WHEN rc.id IS NOT NULL THEN 1 ELSE 0 END) as mapped,
                   COUNT(DISTINCT rc.data_company_id) as companies_covered
            FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id
            WHERE r.broker IS NOT NULL
            GROUP BY r.broker
            ORDER BY total DESC
        """).fetchall()

        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/unmapped")
def api_unmapped():
    """Unmapped reports for review."""
    conn = get_db()
    try:
        limit = min(int(request.args.get("limit", 50)), 200)
        offset = int(request.args.get("offset", 0))

        rows = conn.execute("""
            SELECT r.id, r.filename, r.broker, r.report_type,
                   r.raw_company_text, r.raw_sector_text
            FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id
            WHERE rc.id IS NULL
            ORDER BY r.broker, r.filename
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()

        total = conn.execute("""
            SELECT COUNT(*) c FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id
            WHERE rc.id IS NULL
        """).fetchone()["c"]

        return jsonify({"total": total, "reports": [dict(r) for r in rows]})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Open file in default viewer
# ---------------------------------------------------------------------------

@app.route("/api/open-file", methods=["POST"])
def api_open_file():
    """Open a report PDF in the default system viewer."""
    data = request.get_json()
    filepath = data.get("filepath") if data else None
    if not filepath:
        return jsonify({"error": "No filepath provided"}), 400
    if not os.path.exists(filepath):
        return jsonify({"error": "File not found"}), 404
    # Security: only allow opening PDFs from the download directory
    if not filepath.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDF files can be opened"}), 403
    os.startfile(filepath)
    return jsonify({"status": "opened"})


# ---------------------------------------------------------------------------
# Upgrade (Ollama Level 2) control
# ---------------------------------------------------------------------------

_upgrade_lock = threading.Lock()
_upgrade_process = None  # subprocess.Popen


@app.route("/api/upgrade/start", methods=["POST"])
def api_upgrade_start():
    """Start the Ollama Level 2 upgrade in a background process."""
    global _upgrade_process

    with _upgrade_lock:
        # Check if already running
        if _upgrade_process and _upgrade_process.poll() is None:
            return jsonify({"error": "Upgrade already running", "pid": _upgrade_process.pid}), 409

        # Launch pipeline.py --upgrade as a subprocess
        script = os.path.join(os.path.dirname(__file__), "pipeline.py")
        _upgrade_process = subprocess.Popen(
            ["python", "-u", script, "--upgrade", "--level", "2"],
            cwd=os.path.dirname(__file__),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return jsonify({"status": "started", "pid": _upgrade_process.pid})


@app.route("/api/upgrade/stop", methods=["POST"])
def api_upgrade_stop():
    """Stop the running upgrade process."""
    global _upgrade_process

    with _upgrade_lock:
        if _upgrade_process and _upgrade_process.poll() is None:
            _upgrade_process.terminate()
            _upgrade_process.wait(timeout=10)
            return jsonify({"status": "stopped"})
        return jsonify({"status": "not_running"})


@app.route("/api/upgrade/status")
def api_upgrade_status():
    """Get current upgrade progress from the progress file."""
    from pipeline import read_progress

    progress = read_progress()
    if not progress:
        return jsonify({"status": "idle"})

    # Check if the process is actually still running
    with _upgrade_lock:
        if _upgrade_process and _upgrade_process.poll() is not None:
            # Process finished but status might say running
            if progress.get("status") == "running":
                progress["status"] = "completed"

    return jsonify(progress)


# ---------------------------------------------------------------------------
# Serve React SPA
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    frontend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
    return send_from_directory(frontend_dir, "index.html")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        print("ERROR: Database not found at %s" % DB_PATH)
        print("Run 'python pipeline.py --level 1' first.")
        exit(1)

    print("Starting Research Report Mapper UI...")
    print("Database: %s" % DB_PATH)
    print("Open: http://localhost:8515")
    app.run(host="0.0.0.0", port=8515, debug=False)
