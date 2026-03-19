"""Detect and mark duplicate research reports.

Duplicates arise from Telegram re-downloads with appended message IDs
(e.g., report.pdf, report_29285.pdf) or copy markers (report (2).pdf).

Usage:
    python dedup.py scan       # Find duplicates (dry run)
    python dedup.py mark       # Mark duplicates in DB (is_duplicate=1)
    python dedup.py stats      # Show dedup statistics
"""

import os
import re
import sqlite3
import sys
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "output", "report_mapping.db")


def _ensure_column(conn):
    """Add is_duplicate column if it doesn't exist."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()]
    if "is_duplicate" not in cols:
        conn.execute("ALTER TABLE reports ADD COLUMN is_duplicate INTEGER DEFAULT 0")
        conn.execute("ALTER TABLE reports ADD COLUMN duplicate_of INTEGER REFERENCES reports(id)")
        conn.commit()
        print("Added is_duplicate and duplicate_of columns")


def _normalize(fn):
    """Normalize filename to find duplicate groups."""
    base = os.path.splitext(fn)[0]
    # Remove trailing _msgid (4-6 digit number at end)
    base = re.sub(r"_\d{4,6}$", "", base)
    # Remove trailing (N) copy markers
    base = re.sub(r"\s*\(\d+\)$", "", base)
    # Remove trailing whitespace
    base = base.strip()
    return base.lower()


def find_duplicates(conn):
    """Find duplicate groups. Returns dict of normalized_name -> list of reports."""
    rows = conn.execute(
        "SELECT id, filename, filepath, file_size_bytes FROM reports ORDER BY filename"
    ).fetchall()

    groups = defaultdict(list)
    for r in rows:
        key = _normalize(r["filename"])
        groups[key].append(dict(r))

    # Validate: only keep groups where files have similar sizes
    dupes = {}
    for key, files in groups.items():
        if len(files) < 2:
            continue

        sizes = [f["file_size_bytes"] for f in files if f.get("file_size_bytes")]
        if sizes:
            median = sorted(sizes)[len(sizes) // 2]
            if median and median > 0:
                similar = all(abs(s - median) / median < 0.2 for s in sizes)
                if not similar:
                    continue  # Different sizes = different reports

        dupes[key] = files

    return dupes


def _pick_primary(files):
    """Pick the best file from a duplicate group (shortest clean name, largest size)."""
    def score(f):
        fn = f["filename"]
        # Prefer files without (N) suffix
        has_copy = bool(re.search(r"\(\d+\)", fn))
        # Prefer files without trailing _digits
        has_msgid = bool(re.search(r"_\d{4,6}\.\w+$", fn))
        # Prefer shorter filenames
        length = len(fn)
        # Prefer larger files (more complete)
        size = -(f.get("file_size_bytes") or 0)
        return (has_copy, has_msgid, length, size)

    return sorted(files, key=score)[0]


def scan():
    """Scan for duplicates (dry run)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    dupes = find_duplicates(conn)

    total_extra = sum(len(v) - 1 for v in dupes.values())
    total_files = conn.execute("SELECT COUNT(*) c FROM reports").fetchone()["c"]

    print("=== Duplicate Scan Results ===")
    print("Total reports:       %d" % total_files)
    print("Duplicate groups:    %d" % len(dupes))
    print("Extra copies:        %d" % total_extra)
    print("Unique after dedup:  %d" % (total_files - total_extra))
    print()

    # Show top 20 groups
    sorted_groups = sorted(dupes.items(), key=lambda x: -len(x[1]))
    for i, (key, files) in enumerate(sorted_groups[:20]):
        primary = _pick_primary(files)
        print("Group (%d copies) - keep: %s" % (len(files), primary["filename"][:60]))
        for f in files:
            marker = " [PRIMARY]" if f["id"] == primary["id"] else " [DUPE]"
            size = f.get("file_size_bytes") or 0
            print("  %s %s (%s)" % (marker, f["filename"][:70], _fmt_size(size)))
        print()

    conn.close()


def mark():
    """Mark duplicates in the database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _ensure_column(conn)

    # Reset all
    conn.execute("UPDATE reports SET is_duplicate=0, duplicate_of=NULL")

    dupes = find_duplicates(conn)
    marked = 0

    for key, files in dupes.items():
        primary = _pick_primary(files)
        for f in files:
            if f["id"] != primary["id"]:
                conn.execute(
                    "UPDATE reports SET is_duplicate=1, duplicate_of=? WHERE id=?",
                    (primary["id"], f["id"])
                )
                marked += 1

    conn.commit()
    total = conn.execute("SELECT COUNT(*) c FROM reports").fetchone()["c"]
    unique = conn.execute("SELECT COUNT(*) c FROM reports WHERE is_duplicate=0").fetchone()["c"]

    print("Marked %d reports as duplicates" % marked)
    print("Total: %d | Unique: %d | Duplicates: %d" % (total, unique, marked))
    conn.close()


def stats():
    """Show dedup statistics."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Check if column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()]
    if "is_duplicate" not in cols:
        print("No dedup data. Run: python dedup.py mark")
        conn.close()
        return

    total = conn.execute("SELECT COUNT(*) c FROM reports").fetchone()["c"]
    unique = conn.execute("SELECT COUNT(*) c FROM reports WHERE is_duplicate=0").fetchone()["c"]
    dupes = total - unique

    mapped_unique = conn.execute("""
        SELECT COUNT(DISTINCT r.id) c FROM reports r
        JOIN report_companies rc ON r.id = rc.report_id
        WHERE r.is_duplicate=0
    """).fetchone()["c"]

    unmapped_unique = unique - mapped_unique

    print("=== Dedup Statistics ===")
    print("Total reports:     %d" % total)
    print("Unique reports:    %d" % unique)
    print("Duplicates:        %d (%.1f%%)" % (dupes, 100 * dupes / total if total else 0))
    print()
    print("Unique mapped:     %d (%.1f%%)" % (mapped_unique, 100 * mapped_unique / unique if unique else 0))
    print("Unique unmapped:   %d (%.1f%%)" % (unmapped_unique, 100 * unmapped_unique / unique if unique else 0))

    # Top companies after dedup
    print("\n--- Top 15 Companies (deduplicated) ---")
    rows = conn.execute("""
        SELECT rc.company_name, rc.nse_code, rc.sector, COUNT(*) as cnt
        FROM reports r
        JOIN report_companies rc ON r.id = rc.report_id
        WHERE r.is_duplicate=0
        GROUP BY rc.data_company_id
        ORDER BY cnt DESC LIMIT 15
    """).fetchall()
    for r in rows:
        print("  %-30s %-12s %-25s %d" % (r["company_name"], r["nse_code"] or "?",
                                            r["sector"] or "?", r["cnt"]))

    conn.close()


def _fmt_size(n):
    if n > 1048576:
        return "%.1f MB" % (n / 1048576)
    if n > 1024:
        return "%.0f KB" % (n / 1024)
    return "%d B" % n


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "scan":
        scan()
    elif cmd == "mark":
        mark()
    elif cmd == "stats":
        stats()
    else:
        print("Unknown command: %s" % cmd)
        print(__doc__)
