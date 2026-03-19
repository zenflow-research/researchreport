"""Research Report Mapping Pipeline.

3-level cascade (inspired by Annual_report_extract):
  Level 1: Filename parsing (free, instant)
  Level 2: PDF first-page + Ollama (local, ~2s per file)
  Level 3: Claude CLI batch (expensive, ~10s per batch)

Maps each report to:
  - DataCompanyID (from G:/brain) for company-specific reports
  - Sector/Industry for sector-level reports
"""

import json
import os
import re
import sys
import time
import yaml

from db import get_db, init_db, upsert_report, upsert_report_company, insert_sector_report
from filename_parser import parse_filename
from company_master import (
    load_companies, match_by_ticker, match_by_name, match_by_sector
)


def _load_config():
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


CFG = _load_config()

PROGRESS_FILE = os.path.join(os.path.dirname(__file__), "output", "upgrade_progress.json")


def _write_progress(data):
    """Write upgrade progress to JSON file for UI consumption."""
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def read_progress():
    """Read current upgrade progress. Returns dict or None."""
    try:
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Level 1: Filename-only mapping
# ---------------------------------------------------------------------------

def level1_map(filename):
    """Map a report using only its filename.

    Returns (parsed_info, matches) where matches is list of
    (company_dict, score, method) tuples.
    """
    parsed = parse_filename(filename)
    matches = []

    if not parsed["company"]:
        return parsed, matches

    # Skip fuzzy matching for non-company-specific reports (avoid false positives)
    report_type = parsed.get("report_type")
    skip_fuzzy = report_type in ("sector_report", "market_overview",
                                  "daily_update", "earnings_preview", "book")

    # Try ticker match first (highest confidence)
    company_text = parsed["company"]
    ticker_match = match_by_ticker(company_text)
    if ticker_match:
        matches = [(ticker_match, 100.0, "exact_ticker")]
        return parsed, matches

    # Try both fuzzy name match AND ticker-from-name, then pick the best.
    # This prevents false positives like "APL Apollo Tubes" -> ticker "APL" -> "Asston Pharmaceu"
    # while still handling ticker-named companies like ITC, SRF, ABB correctly.

    # 1. Fuzzy name match
    fuzzy_matches = []
    if not skip_fuzzy:
        min_score = CFG["pipeline"]["level1"]["confidence_threshold"] * 100
        fuzzy_matches = match_by_name(company_text, min_score=min_score, max_results=5)

    # 2. Ticker-from-name match (try uppercase words as tickers)
    ticker_matches = []
    words = company_text.split()
    for word in words:
        clean_word = re.sub(r'[^a-zA-Z0-9&]', '', word)
        if len(clean_word) >= 3 and clean_word.upper() == clean_word:
            ticker_match = match_by_ticker(clean_word)
            if ticker_match:
                ticker_matches = [(ticker_match, 95.0, "ticker_from_name")]
                break

    # 3. Pick the best: compare top fuzzy vs ticker match
    best_fuzzy = fuzzy_matches[0] if fuzzy_matches else None
    best_ticker = ticker_matches[0] if ticker_matches else None

    if best_fuzzy and best_ticker:
        # Both found — prefer the one with higher score
        # (fuzzy 100 > ticker 95, but ticker 95 > fuzzy 86)
        if best_fuzzy[1] >= best_ticker[1]:
            return parsed, fuzzy_matches
        else:
            return parsed, ticker_matches
    elif best_fuzzy:
        return parsed, fuzzy_matches
    elif best_ticker:
        return parsed, ticker_matches

    return parsed, matches


# ---------------------------------------------------------------------------
# Level 2: PDF scan + Ollama
# ---------------------------------------------------------------------------

def level2_map(filepath, level1_parsed=None):
    """Scan PDF with Ollama for company identification.

    Returns (scan_result, matches).
    """
    from pdf_scanner import scan_pdf

    l2_cfg = CFG["pipeline"]["level2"]
    scan = scan_pdf(
        filepath,
        model=l2_cfg.get("ollama_model", "gemma2:latest"),
        base_url=l2_cfg.get("ollama_url", "http://localhost:11434"),
        max_pages=l2_cfg.get("max_pages", 2),
    )

    if scan.get("error"):
        return scan, []

    matches = []

    # Try ticker from scan
    if scan.get("ticker"):
        ticker_match = match_by_ticker(scan["ticker"])
        if ticker_match:
            matches = [(ticker_match, 100.0, "ollama_ticker")]
            return scan, matches

    # Try company name from scan
    if scan.get("company_name"):
        min_score = l2_cfg["confidence_threshold"] * 100
        matches = match_by_name(scan["company_name"],
                                 min_score=min_score, max_results=5)
        # Tag method as ollama-sourced
        matches = [(c, s, "ollama_" + m) for c, s, m in matches]

    return scan, matches


# ---------------------------------------------------------------------------
# Level 3: Claude CLI batch
# ---------------------------------------------------------------------------

def level3_map_batch(filepaths):
    """Use Claude CLI to process a batch of unresolved PDFs.

    Returns list of (scan_result, matches) tuples.
    """
    from pdf_scanner import scan_pdf_claude

    l3_cfg = CFG["pipeline"]["level3"]
    results_raw = scan_pdf_claude(filepaths, timeout=l3_cfg.get("timeout", 300))

    output = []
    for i, scan in enumerate(results_raw):
        if isinstance(scan, dict) and scan.get("error"):
            output.append((scan, []))
            continue

        matches = []
        if scan.get("ticker"):
            ticker_match = match_by_ticker(scan["ticker"])
            if ticker_match:
                matches = [(ticker_match, 100.0, "claude_ticker")]

        if not matches and scan.get("company_name"):
            matches = match_by_name(scan["company_name"],
                                     min_score=60, max_results=5)
            matches = [(c, s, "claude_" + m) for c, s, m in matches]

        output.append((scan, matches))

    return output


# ---------------------------------------------------------------------------
# Main pipeline orchestrator
# ---------------------------------------------------------------------------

def process_file(filename, filepath, conn, max_level=3):
    """Process a single file through the cascade pipeline.

    Returns (report_id, mapping_level, best_match_or_none).
    """
    best_match = None
    mapping_level = 0
    parsed = None
    scan = None

    # ---- LEVEL 1: Filename ----
    if CFG["pipeline"]["level1"]["enabled"]:
        parsed, matches = level1_map(filename)

        if matches and matches[0][1] >= 85:
            best_match = matches[0]
            mapping_level = 1

    # ---- LEVEL 2: Ollama PDF scan ----
    if not best_match and max_level >= 2 and CFG["pipeline"]["level2"]["enabled"]:
        scan, matches = level2_map(filepath, parsed)

        if matches and matches[0][1] >= 70:
            best_match = matches[0]
            mapping_level = 2

        # Merge scan info into parsed
        if scan and not scan.get("error"):
            if parsed and not parsed.get("broker") and scan.get("broker"):
                parsed["broker"] = scan["broker"]
            if scan.get("report_type") and (not parsed or not parsed.get("report_type")):
                if parsed:
                    parsed["report_type"] = scan["report_type"]

    # ---- Save results ----
    broker = parsed["broker"] if parsed else (scan.get("broker") if scan else None)
    report_type = (parsed.get("report_type") if parsed else None) or \
                  (scan.get("report_type") if scan else None) or "unknown"
    raw_company = parsed.get("company_raw") if parsed else \
                  (scan.get("company_name") if scan else None)
    raw_sector = scan.get("sector") if scan else None
    confidence = best_match[1] / 100.0 if best_match else 0.0

    file_size = None
    try:
        file_size = os.path.getsize(filepath)
    except OSError:
        pass

    report_id = upsert_report(
        conn,
        filename=filename,
        filepath=filepath,
        file_size_bytes=file_size,
        broker=broker,
        report_type=report_type,
        mapping_level=mapping_level if best_match else 0,
        confidence=confidence,
        raw_company_text=raw_company,
        raw_sector_text=raw_sector,
    )

    # Save company match
    if best_match:
        company, score, method = best_match
        upsert_report_company(
            conn,
            report_id=report_id,
            data_company_id=company.get("DataCompanyID"),
            company_name=company.get("Company Name"),
            short_id=company.get("Short_id"),
            nse_code=company.get("NSE Code"),
            bse_code=company.get("BSE Code"),
            sector=company.get("Sector"),
            industry=company.get("Industry"),
            basic_industry=company.get("BasicIndustry"),
            match_score=score,
            match_method=method,
            is_primary=True,
        )

    # Save sector mapping for sector reports
    if report_type == "sector_report" and raw_sector:
        sector_matches = match_by_sector(raw_sector, min_score=60)
        for sector_key, companies, score in sector_matches[:3]:
            insert_sector_report(conn, report_id, sector=sector_key,
                                  match_score=score)

    return report_id, mapping_level, best_match


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

def run_pipeline(download_dir=None, max_level=2, limit=None, skip_existing=True):
    """Run the mapping pipeline on all PDFs in the download directory.

    Args:
        download_dir:  override download directory
        max_level:     max pipeline level (1=filename only, 2=+ollama, 3=+claude)
        limit:         max files to process (None=all)
        skip_existing: skip files already in the database
    """
    download_dir = download_dir or CFG["paths"]["download_dir"]

    if not os.path.isdir(download_dir):
        print("ERROR: Download directory not found: %s" % download_dir)
        return

    # Initialize
    init_db()
    load_companies()

    # List PDF files
    print("Scanning %s for PDFs..." % download_dir)
    all_files = [f for f in os.listdir(download_dir)
                 if f.lower().endswith(".pdf")]
    print("Found %d PDF files" % len(all_files))

    # Get already-processed files
    existing = set()
    if skip_existing:
        with get_db() as conn:
            rows = conn.execute("SELECT filename FROM reports").fetchall()
            existing = {r["filename"] for r in rows}
        print("Skipping %d already-processed files" % len(existing))

    to_process = [f for f in all_files if f not in existing]
    if limit:
        to_process = to_process[:limit]
    print("Processing %d files (max_level=%d)" % (len(to_process), max_level))

    # Counters
    stats = {
        "total": len(to_process),
        "level1_mapped": 0,
        "level2_mapped": 0,
        "level3_mapped": 0,
        "unmapped": 0,
        "errors": 0,
    }

    # Process each file
    level3_queue = []  # files that need Claude
    t0 = time.time()

    with get_db() as conn:
        for i, filename in enumerate(to_process):
            filepath = os.path.join(download_dir, filename)

            try:
                report_id, level, match = process_file(
                    filename, filepath, conn, max_level=min(max_level, 2)
                )

                if match:
                    if level == 1:
                        stats["level1_mapped"] += 1
                    elif level == 2:
                        stats["level2_mapped"] += 1
                else:
                    if max_level >= 3:
                        level3_queue.append((filename, filepath, report_id))
                    else:
                        stats["unmapped"] += 1

            except Exception as e:
                stats["errors"] += 1
                print("  ERROR processing %s: %s" % (filename[:60], e))

            # Progress
            if (i + 1) % 100 == 0 or (i + 1) == len(to_process):
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                print("  [%d/%d] %.1f files/sec | L1=%d L2=%d unmapped=%d err=%d"
                      % (i + 1, stats["total"], rate,
                         stats["level1_mapped"], stats["level2_mapped"],
                         stats["unmapped"], stats["errors"]))

        # ---- LEVEL 3: Claude batch for remaining ----
        if level3_queue and max_level >= 3:
            batch_size = CFG["pipeline"]["level3"].get("batch_size", 10)
            print("\nLevel 3: Sending %d files to Claude CLI (batch_size=%d)..."
                  % (len(level3_queue), batch_size))

            for batch_start in range(0, len(level3_queue), batch_size):
                batch = level3_queue[batch_start:batch_start + batch_size]
                filepaths = [fp for _, fp, _ in batch]

                try:
                    results = level3_map_batch(filepaths)
                except Exception as e:
                    print("  Claude batch error: %s" % e)
                    stats["errors"] += len(batch)
                    continue

                for j, (scan, matches) in enumerate(results):
                    fname, fpath, rid = batch[j]

                    if matches and matches[0][1] >= 60:
                        company, score, method = matches[0]
                        upsert_report_company(
                            conn, rid, company.get("DataCompanyID"),
                            company.get("Company Name"),
                            short_id=company.get("Short_id"),
                            nse_code=company.get("NSE Code"),
                            bse_code=company.get("BSE Code"),
                            sector=company.get("Sector"),
                            industry=company.get("Industry"),
                            basic_industry=company.get("BasicIndustry"),
                            match_score=score, match_method=method,
                        )
                        # Update report record
                        conn.execute("""
                            UPDATE reports SET mapping_level=3,
                                   confidence=?, raw_company_text=?
                            WHERE id=?
                        """, (score / 100.0,
                              scan.get("company_name", ""), rid))
                        stats["level3_mapped"] += 1
                    else:
                        stats["unmapped"] += 1

    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE in %.1fs" % elapsed)
    print("  Total:        %d" % stats["total"])
    print("  Level 1 (filename): %d" % stats["level1_mapped"])
    print("  Level 2 (Ollama):   %d" % stats["level2_mapped"])
    print("  Level 3 (Claude):   %d" % stats["level3_mapped"])
    print("  Unmapped:     %d" % stats["unmapped"])
    print("  Errors:       %d" % stats["errors"])
    print("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# Upgrade: re-process unmapped files at higher level
# ---------------------------------------------------------------------------

def upgrade_unmapped(max_level=2, limit=None):
    """Re-process unmapped (and non-duplicate) files with Ollama/Claude.

    Unlike run_pipeline, this only processes files already in the DB
    that have no company match yet.
    """
    init_db()
    load_companies()

    with get_db() as conn:
        # Check for is_duplicate column
        cols = [r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()]
        has_dedup = "is_duplicate" in cols
        dedup_filter = "AND r.is_duplicate = 0" if has_dedup else ""

        # Get unmapped, non-duplicate files
        rows = conn.execute("""
            SELECT r.id, r.filename, r.filepath
            FROM reports r
            LEFT JOIN report_companies rc ON r.id = rc.report_id
            WHERE rc.id IS NULL %s
            ORDER BY r.id
        """ % dedup_filter).fetchall()

    to_process = [(r["id"], r["filename"], r["filepath"]) for r in rows]
    if limit:
        to_process = to_process[:limit]

    print("Upgrading %d unmapped files to Level %d" % (len(to_process), max_level))

    stats = {"total": len(to_process), "mapped": 0, "unmapped": 0, "errors": 0}
    recent_files = []  # last N processed files for UI display
    t0 = time.time()

    # Write initial progress
    _write_progress({
        "status": "running",
        "processed": 0,
        "total": stats["total"],
        "mapped": 0,
        "unmapped": 0,
        "errors": 0,
        "rate": 0,
        "eta_seconds": 0,
        "recent_files": [],
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    })

    # Use a raw connection (not context manager) so we can commit periodically.
    # This ensures crash safety: at most COMMIT_INTERVAL files of work lost.
    COMMIT_INTERVAL = 10
    from db import DB_PATH, insert_sector_report
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = _sqlite3.Row

    try:
        for i, (report_id, filename, filepath) in enumerate(to_process):
            file_result = {"filename": filename[:80], "matched": False, "company": None}

            try:
                # Run Level 2 scan
                scan, matches = level2_map(filepath)

                if matches and matches[0][1] >= 70:
                    company, score, method = matches[0]

                    # Update report
                    conn.execute("""
                        UPDATE reports SET mapping_level=2, confidence=?,
                               raw_company_text=?, raw_sector_text=?,
                               broker=COALESCE(broker, ?),
                               report_type=CASE WHEN report_type='unknown'
                                   THEN ? ELSE report_type END,
                               report_date=COALESCE(report_date, ?)
                        WHERE id=?
                    """, (score / 100.0,
                          scan.get("company_name"),
                          scan.get("sector"),
                          scan.get("broker"),
                          scan.get("report_type", "unknown"),
                          scan.get("report_date"),
                          report_id))

                    # Insert company match
                    upsert_report_company(
                        conn, report_id,
                        data_company_id=company.get("DataCompanyID"),
                        company_name=company.get("Company Name"),
                        short_id=company.get("Short_id"),
                        nse_code=company.get("NSE Code"),
                        bse_code=company.get("BSE Code"),
                        sector=company.get("Sector"),
                        industry=company.get("Industry"),
                        basic_industry=company.get("BasicIndustry"),
                        match_score=score, match_method=method,
                    )
                    stats["mapped"] += 1
                    file_result["matched"] = True
                    file_result["company"] = company.get("Company Name")
                    file_result["report_date"] = scan.get("report_date")
                    file_result["broker"] = scan.get("broker")
                else:
                    # Save scan results even if no match (for sector info)
                    if scan and not scan.get("error"):
                        conn.execute("""
                            UPDATE reports SET raw_company_text=?,
                                   raw_sector_text=?,
                                   broker=COALESCE(broker, ?),
                                   report_type=CASE WHEN report_type='unknown'
                                       THEN ? ELSE report_type END,
                                   report_date=COALESCE(report_date, ?)
                            WHERE id=?
                        """, (scan.get("company_name"),
                              scan.get("sector"),
                              scan.get("broker"),
                              scan.get("report_type", "unknown"),
                              scan.get("report_date"),
                              report_id))

                        # Map to sector even if company not matched
                        if scan.get("sector"):
                            sector_matches = match_by_sector(scan["sector"], min_score=60)
                            for sector_key, companies, sc in sector_matches[:1]:
                                insert_sector_report(conn, report_id,
                                                     sector=sector_key,
                                                     match_score=sc)

                        file_result["report_date"] = scan.get("report_date")
                        file_result["broker"] = scan.get("broker")
                    stats["unmapped"] += 1

            except Exception as e:
                stats["errors"] += 1
                file_result["error"] = str(e)[:100]
                if "Ollama" in str(e) or "urlopen" in str(e):
                    print("  OLLAMA ERROR: %s" % e)
                    print("  Is Ollama running? Check http://localhost:11434")
                    conn.commit()  # Save whatever we have
                    _write_progress({
                        "status": "error",
                        "error": "Ollama connection failed: %s" % str(e)[:200],
                        "processed": i,
                        "total": stats["total"],
                        "mapped": stats["mapped"],
                        "unmapped": stats["unmapped"],
                        "errors": stats["errors"],
                    })
                    break
                print("  ERROR [%s]: %s" % (filename[:50], e))

            # Commit every COMMIT_INTERVAL files for crash safety
            if (i + 1) % COMMIT_INTERVAL == 0:
                conn.commit()

            # Track recent files (keep last 10)
            recent_files.append(file_result)
            if len(recent_files) > 10:
                recent_files.pop(0)

            # Update progress every 5 files
            if (i + 1) % 5 == 0 or (i + 1) == len(to_process):
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                remaining = stats["total"] - (i + 1)
                eta = remaining / rate if rate > 0 else 0

                _write_progress({
                    "status": "running",
                    "processed": i + 1,
                    "total": stats["total"],
                    "mapped": stats["mapped"],
                    "unmapped": stats["unmapped"],
                    "errors": stats["errors"],
                    "rate": round(rate, 2),
                    "eta_seconds": int(eta),
                    "recent_files": recent_files[-10:],
                    "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                })

            # Print progress every 10 files
            if (i + 1) % 10 == 0 or (i + 1) == len(to_process):
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                print("  [%d/%d] %.2f files/sec | mapped=%d unmapped=%d err=%d"
                      % (i + 1, stats["total"], rate,
                         stats["mapped"], stats["unmapped"], stats["errors"]))

        # Final commit
        conn.commit()

    finally:
        conn.close()

    elapsed = time.time() - t0

    # Write final progress
    _write_progress({
        "status": "completed",
        "processed": stats["total"],
        "total": stats["total"],
        "mapped": stats["mapped"],
        "unmapped": stats["unmapped"],
        "errors": stats["errors"],
        "rate": round(stats["total"] / elapsed, 2) if elapsed > 0 else 0,
        "eta_seconds": 0,
        "elapsed_seconds": int(elapsed),
        "recent_files": recent_files[-10:],
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    })

    print("\n" + "=" * 60)
    print("UPGRADE COMPLETE in %.1fs" % elapsed)
    print("  Total processed: %d" % stats["total"])
    print("  Newly mapped:    %d" % stats["mapped"])
    print("  Still unmapped:  %d" % stats["unmapped"])
    print("  Errors:          %d" % stats["errors"])
    print("=" * 60)
    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Map research reports to companies in G:/brain"
    )
    parser.add_argument("--level", type=int, default=2, choices=[1, 2, 3],
                        help="Max pipeline level (1=filename, 2=+ollama, 3=+claude)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max files to process (default: all)")
    parser.add_argument("--reprocess", action="store_true",
                        help="Reprocess already-mapped files")
    parser.add_argument("--upgrade", action="store_true",
                        help="Re-process unmapped files with Ollama (Level 2)")
    parser.add_argument("--dir", type=str, default=None,
                        help="Override download directory")

    args = parser.parse_args()

    if args.upgrade:
        upgrade_unmapped(max_level=args.level, limit=args.limit)
    else:
        run_pipeline(
            download_dir=args.dir,
            max_level=args.level,
            limit=args.limit,
            skip_existing=not args.reprocess,
        )
