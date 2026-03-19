"""Query and report on mapping results.

Usage:
    python query.py stats                    # Overall statistics
    python query.py company RELIANCE         # Reports about a company
    python query.py sector "IT Services"     # Reports about a sector
    python query.py unmapped                 # List unmapped reports
    python query.py broker "Motilal Oswal"   # Reports by broker
"""

import os
import sys
from db import get_db


def stats():
    """Print overall mapping statistics."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
        mapped = conn.execute(
            "SELECT COUNT(DISTINCT r.id) FROM reports r "
            "JOIN report_companies rc ON r.id = rc.report_id"
        ).fetchone()[0]
        unmapped = total - mapped

        print("=== Mapping Statistics ===")
        print("Total reports:  %d" % total)
        print("Mapped:         %d (%.1f%%)" % (mapped, 100 * mapped / total if total else 0))
        print("Unmapped:       %d (%.1f%%)" % (unmapped, 100 * unmapped / total if total else 0))

        # By level
        for lvl in [1, 2, 3]:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM reports WHERE mapping_level = ?", (lvl,)
            ).fetchone()[0]
            labels = {1: "Filename", 2: "Ollama", 3: "Claude"}
            print("  Level %d (%s): %d" % (lvl, labels.get(lvl, "?"), cnt))

        # By report type
        print("\n--- By Report Type ---")
        rows = conn.execute(
            "SELECT report_type, COUNT(*) as cnt FROM reports "
            "GROUP BY report_type ORDER BY cnt DESC"
        ).fetchall()
        for r in rows:
            print("  %-20s %d" % (r["report_type"], r["cnt"]))

        # Top brokers
        print("\n--- Top 15 Brokers ---")
        rows = conn.execute(
            "SELECT broker, COUNT(*) as cnt FROM reports "
            "WHERE broker IS NOT NULL "
            "GROUP BY broker ORDER BY cnt DESC LIMIT 15"
        ).fetchall()
        for r in rows:
            print("  %-30s %d" % (r["broker"], r["cnt"]))

        # Top mapped companies (with market cap from brain master)
        print("\n--- Top 15 Companies (by report count) ---")
        rows = conn.execute(
            "SELECT data_company_id, company_name, nse_code, sector, COUNT(*) as cnt "
            "FROM report_companies "
            "GROUP BY data_company_id ORDER BY cnt DESC LIMIT 15"
        ).fetchall()

        # Load market cap from brain master
        mcap_map = {}
        try:
            from company_master import load_companies
            companies = load_companies()
            for c in companies:
                cid = str(c.get("DataCompanyID", ""))
                mcap = c.get("Market Capitalization")
                if cid and mcap:
                    try:
                        mcap_map[cid] = float(mcap)
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass

        for r in rows:
            cid = str(r["data_company_id"])
            mcap = mcap_map.get(cid, 0)
            mcap_str = ""
            if mcap >= 1e5:
                mcap_str = "%.1fL Cr" % (mcap / 1e5)
            elif mcap >= 1e3:
                mcap_str = "%.0fK Cr" % (mcap / 1e3)
            elif mcap >= 1:
                mcap_str = "%.0f Cr" % mcap
            print("  %-25s %-10s %-30s %3d reports  %s"
                  % (r["company_name"], r["nse_code"] or "",
                     r["sector"] or "", r["cnt"], mcap_str))


def company(query):
    """Find reports about a specific company."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT r.filename, r.broker, r.report_type, r.confidence,
                   rc.company_name, rc.nse_code, rc.match_score, rc.match_method
            FROM reports r
            JOIN report_companies rc ON r.id = rc.report_id
            WHERE rc.company_name LIKE ? OR rc.nse_code LIKE ?
               OR rc.short_id LIKE ? OR CAST(rc.data_company_id AS TEXT) = ?
            ORDER BY r.mapped_at DESC
        """, ("%%%s%%" % query, "%%%s%%" % query,
              "%%%s%%" % query, query)).fetchall()

        print("Found %d reports for '%s':" % (len(rows), query))
        for r in rows:
            print("  [%s] %-60s broker=%-20s score=%.0f method=%s"
                  % (r["nse_code"] or "?", r["filename"][:60],
                     r["broker"] or "?", r["match_score"], r["match_method"]))


def sector(query):
    """Find reports mapped to a sector."""
    with get_db() as conn:
        # Company-specific reports in this sector
        rows = conn.execute("""
            SELECT r.filename, r.broker, rc.company_name, rc.nse_code,
                   rc.sector, rc.industry
            FROM reports r
            JOIN report_companies rc ON r.id = rc.report_id
            WHERE rc.sector LIKE ? OR rc.industry LIKE ?
               OR rc.basic_industry LIKE ?
            ORDER BY rc.sector, rc.company_name
        """, ("%%%s%%" % query, "%%%s%%" % query,
              "%%%s%%" % query)).fetchall()

        print("Found %d company reports in sector '%s':" % (len(rows), query))
        for r in rows[:30]:
            print("  %-15s %-25s %-50s %s"
                  % (r["nse_code"] or "?", r["company_name"] or "?",
                     r["filename"][:50], r["broker"] or "?"))
        if len(rows) > 30:
            print("  ... and %d more" % (len(rows) - 30))

        # Sector-level reports
        sec_rows = conn.execute("""
            SELECT r.filename, r.broker, sr.sector, sr.industry
            FROM reports r
            JOIN sector_reports sr ON r.id = sr.report_id
            WHERE sr.sector LIKE ? OR sr.industry LIKE ?
        """, ("%%%s%%" % query, "%%%s%%" % query)).fetchall()

        if sec_rows:
            print("\nSector-level reports:")
            for r in sec_rows:
                print("  %-50s %s" % (r["filename"][:50], r["broker"] or "?"))


def unmapped(limit=50):
    """List unmapped reports."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT r.filename, r.broker, r.raw_company_text, r.report_type
            FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id
            WHERE rc.id IS NULL
            ORDER BY r.filename
            LIMIT ?
        """, (limit,)).fetchall()

        total = conn.execute("""
            SELECT COUNT(*) FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id
            WHERE rc.id IS NULL
        """).fetchone()[0]

        print("Unmapped reports: %d total (showing %d)" % (total, len(rows)))
        for r in rows:
            print("  %-60s broker=%-20s raw=%s type=%s"
                  % (r["filename"][:60], r["broker"] or "?",
                     r["raw_company_text"] or "?", r["report_type"]))


def broker(query):
    """Find reports by broker."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT r.filename, r.report_type, rc.company_name, rc.nse_code,
                   rc.sector
            FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id
            WHERE r.broker LIKE ?
            ORDER BY r.mapped_at DESC
            LIMIT 50
        """, ("%%%s%%" % query,)).fetchall()

        print("Reports by broker '%s': %d (showing up to 50)" % (query, len(rows)))
        for r in rows:
            print("  %-60s %-15s %-10s %s"
                  % (r["filename"][:60], r["company_name"] or "(unmapped)",
                     r["nse_code"] or "", r["sector"] or ""))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1].lower()
    arg = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else ""

    if cmd == "stats":
        stats()
    elif cmd == "company":
        company(arg)
    elif cmd == "sector":
        sector(arg)
    elif cmd == "unmapped":
        unmapped(int(arg) if arg else 50)
    elif cmd == "broker":
        broker(arg)
    else:
        print("Unknown command: %s" % cmd)
        print(__doc__)
