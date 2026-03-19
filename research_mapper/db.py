"""Database layer for research report mapping results."""

import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(__file__), "output", "report_mapping.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filename        TEXT    NOT NULL UNIQUE,
    filepath        TEXT    NOT NULL,
    file_size_bytes INTEGER,
    broker          TEXT,
    report_type     TEXT    DEFAULT 'unknown',
    mapped_at       TEXT,          -- ISO timestamp of mapping
    mapping_level   INTEGER,       -- 1=filename, 2=ollama, 3=claude
    confidence      REAL,          -- 0.0 - 1.0
    raw_company_text TEXT,         -- extracted company name before matching
    raw_sector_text  TEXT,         -- extracted sector text (if any)
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS report_companies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id       INTEGER NOT NULL REFERENCES reports(id),
    data_company_id INTEGER,       -- DataCompanyID from brain
    company_name    TEXT,
    short_id        TEXT,
    nse_code        TEXT,
    bse_code        TEXT,
    sector          TEXT,
    industry        TEXT,
    basic_industry  TEXT,
    match_score     REAL,          -- fuzzy match score 0-100
    match_method    TEXT,          -- exact_ticker, exact_name, fuzzy_name, llm
    is_primary      INTEGER DEFAULT 1,  -- 1 if main subject, 0 if mentioned
    UNIQUE(report_id, data_company_id)
);

CREATE TABLE IF NOT EXISTS sector_reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id       INTEGER NOT NULL REFERENCES reports(id),
    sector          TEXT,
    industry        TEXT,
    basic_industry  TEXT,
    match_score     REAL
);

CREATE INDEX IF NOT EXISTS idx_reports_broker       ON reports(broker);
CREATE INDEX IF NOT EXISTS idx_reports_type          ON reports(report_type);
CREATE INDEX IF NOT EXISTS idx_reports_level         ON reports(mapping_level);
CREATE INDEX IF NOT EXISTS idx_rc_company            ON report_companies(data_company_id);
CREATE INDEX IF NOT EXISTS idx_rc_sector             ON report_companies(sector);
CREATE INDEX IF NOT EXISTS idx_sr_sector             ON sector_reports(sector);
"""


@contextmanager
def get_db(db_path=None):
    """Yield a sqlite3 connection with WAL mode and foreign keys."""
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path=None):
    """Create tables if they don't exist."""
    with get_db(db_path) as conn:
        conn.executescript(SCHEMA)
        # Add report_date column if missing (added after initial schema)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()]
        if "report_date" not in cols:
            conn.execute("ALTER TABLE reports ADD COLUMN report_date TEXT")
    print("Database initialized at %s" % (db_path or DB_PATH))


def upsert_report(conn, filename, filepath, file_size_bytes=None,
                  broker=None, report_type="unknown", mapping_level=None,
                  confidence=None, raw_company_text=None,
                  raw_sector_text=None, notes=None):
    """Insert or update a report record. Returns report id."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    cur = conn.execute("SELECT id FROM reports WHERE filename = ?", (filename,))
    row = cur.fetchone()

    if row:
        conn.execute("""
            UPDATE reports SET broker=?, report_type=?, mapped_at=?,
                   mapping_level=?, confidence=?, raw_company_text=?,
                   raw_sector_text=?, notes=?
            WHERE id=?
        """, (broker, report_type, now, mapping_level, confidence,
              raw_company_text, raw_sector_text, notes, row["id"]))
        return row["id"]
    else:
        cur = conn.execute("""
            INSERT INTO reports (filename, filepath, file_size_bytes, broker,
                   report_type, mapped_at, mapping_level, confidence,
                   raw_company_text, raw_sector_text, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (filename, filepath, file_size_bytes, broker, report_type,
              now, mapping_level, confidence, raw_company_text,
              raw_sector_text, notes))
        return cur.lastrowid


def upsert_report_company(conn, report_id, data_company_id, company_name,
                           short_id=None, nse_code=None, bse_code=None,
                           sector=None, industry=None, basic_industry=None,
                           match_score=0.0, match_method="unknown",
                           is_primary=True):
    """Link a report to a matched company."""
    conn.execute("""
        INSERT INTO report_companies
            (report_id, data_company_id, company_name, short_id, nse_code,
             bse_code, sector, industry, basic_industry,
             match_score, match_method, is_primary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(report_id, data_company_id) DO UPDATE SET
            match_score=excluded.match_score,
            match_method=excluded.match_method,
            is_primary=excluded.is_primary
    """, (report_id, data_company_id, company_name, short_id, nse_code,
          bse_code, sector, industry, basic_industry,
          match_score, match_method, int(is_primary)))


def insert_sector_report(conn, report_id, sector=None, industry=None,
                          basic_industry=None, match_score=0.0):
    """Link a report to a sector (for sector-level reports)."""
    conn.execute("""
        INSERT INTO sector_reports (report_id, sector, industry,
                                    basic_industry, match_score)
        VALUES (?, ?, ?, ?, ?)
    """, (report_id, sector, industry, basic_industry, match_score))


if __name__ == "__main__":
    init_db()
