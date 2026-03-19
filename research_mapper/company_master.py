"""Load and query the company master data from G:/brain.

Provides fuzzy matching, exact ticker lookup, and sector search
against the 5,249 companies in the brain database.
"""

import csv
import os
import re
import sqlite3

# ---------------------------------------------------------------------------
# Lazy-loaded module-level cache
# ---------------------------------------------------------------------------
_COMPANIES = []       # list of dicts
_NAME_INDEX = {}      # lowercase name -> list of company dicts
_TICKER_INDEX = {}    # lowercase ticker/code -> company dict
_SECTOR_INDEX = {}    # lowercase sector -> list of company dicts

MASTER_CSV = os.path.join("G:/brain/screener_util",
                          "company_sector_mapping_merged_master.csv")
SCREENER_DB = "G:/brain/data/screener_cloud.db"

# Columns we care about from the CSV
_KEEP_COLS = [
    "DataCompanyID", "Company Name", "Short_id", "BSE Code", "NSE Code",
    "ISIN", "Macro", "Sector", "Industry", "BasicIndustry",
    "CompanyFullName", "Market Capitalization",
]


def _load_from_db():
    """Load company master from screener_cloud.db (preferred)."""
    if not os.path.exists(SCREENER_DB):
        return []

    conn = sqlite3.connect(SCREENER_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT id, company_name, short_id, bse_code, nse_code, isin,
                   macro, sector, industry, basic_industry,
                   company_full_name, mcap
            FROM companies
        """).fetchall()

        result = []
        for r in rows:
            result.append({
                "DataCompanyID": r["id"],
                "Company Name": r["company_name"] or "",
                "Short_id": r["short_id"] or "",
                "BSE Code": str(r["bse_code"] or ""),
                "NSE Code": r["nse_code"] or "",
                "ISIN": r["isin"] or "",
                "Macro": r["macro"] or "",
                "Sector": r["sector"] or "",
                "Industry": r["industry"] or "",
                "BasicIndustry": r["basic_industry"] or "",
                "CompanyFullName": r["company_full_name"] or "",
                "Market Capitalization": r["mcap"] or 0,
            })
        return result
    except Exception as e:
        print("Warning: could not load from DB: %s" % e)
        return []
    finally:
        conn.close()


def _load_from_csv():
    """Fallback: load from master CSV."""
    if not os.path.exists(MASTER_CSV):
        raise FileNotFoundError("Master CSV not found: %s" % MASTER_CSV)

    result = []
    with open(MASTER_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entry = {}
            for col in _KEEP_COLS:
                entry[col] = row.get(col, "")
            result.append(entry)
    return result


def load_companies(force=False):
    """Load company master data into module cache. Returns list of dicts."""
    global _COMPANIES, _NAME_INDEX, _TICKER_INDEX, _SECTOR_INDEX

    if _COMPANIES and not force:
        return _COMPANIES

    # Try DB first, fall back to CSV
    companies = _load_from_db()
    if not companies:
        companies = _load_from_csv()

    # Build indexes
    name_idx = {}
    ticker_idx = {}
    sector_idx = {}

    for c in companies:
        # Name index (lowercase)
        name = (c.get("Company Name") or "").strip().lower()
        if name:
            name_idx.setdefault(name, []).append(c)

        full_name = (c.get("CompanyFullName") or "").strip().lower()
        if full_name and full_name != name:
            name_idx.setdefault(full_name, []).append(c)

        # Ticker index: short_id, nse_code, bse_code all point to same record
        for field in ("Short_id", "NSE Code", "BSE Code"):
            val = (c.get(field) or "").strip().lower()
            if val:
                ticker_idx[val] = c

        # Sector index
        for field in ("Sector", "Industry", "BasicIndustry"):
            val = (c.get(field) or "").strip().lower()
            if val:
                sector_idx.setdefault(val, []).append(c)

    _COMPANIES = companies
    _NAME_INDEX = name_idx
    _TICKER_INDEX = ticker_idx
    _SECTOR_INDEX = sector_idx

    print("Loaded %d companies (%d name keys, %d ticker keys, %d sector keys)"
          % (len(companies), len(name_idx), len(ticker_idx), len(sector_idx)))
    return companies


# ---------------------------------------------------------------------------
# Matching functions
# ---------------------------------------------------------------------------

def _normalize(text):
    """Normalize a company name for matching."""
    if not text:
        return ""
    t = text.lower().strip()
    # Remove common suffixes
    for suffix in (" ltd", " ltd.", " limited", " pvt", " pvt.",
                   " private", " inc", " inc.", " corp", " corp.",
                   " corporation", " company", " co.", " co"):
        t = t.replace(suffix, "")
    # Remove punctuation and extra spaces
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _simple_ratio(s1, s2):
    """Simple similarity ratio without external dependencies.
    Uses longest common subsequence approach."""
    if not s1 or not s2:
        return 0
    if s1 == s2:
        return 100

    # Token-based overlap (works well for company names)
    tokens1 = set(s1.split())
    tokens2 = set(s2.split())

    if not tokens1 or not tokens2:
        return 0

    intersection = tokens1 & tokens2
    union = tokens1 | tokens2

    # Jaccard with length weighting
    jaccard = len(intersection) / len(union) * 100

    # Bonus if one is substring of other
    if s1 in s2 or s2 in s1:
        jaccard = max(jaccard, 80)

    return jaccard


try:
    from rapidfuzz import fuzz as _fuzz
    def _fuzzy_score(s1, s2):
        """Use rapidfuzz for high-quality fuzzy matching.
        Penalizes large length differences to avoid short substrings
        matching long strings (e.g. 'iti' in 'utilities').
        """
        base = max(
            _fuzz.ratio(s1, s2),
            _fuzz.token_sort_ratio(s1, s2),
        )
        # Only use partial_ratio when lengths are reasonably similar
        # This prevents "iti" matching "utilities capital cycle"
        len_ratio = min(len(s1), len(s2)) / max(len(s1), len(s2), 1)
        if len_ratio > 0.3:
            base = max(base, _fuzz.partial_ratio(s1, s2))
        return base
except ImportError:
    def _fuzzy_score(s1, s2):
        """Fallback to simple token matching."""
        return _simple_ratio(s1, s2)


def match_by_ticker(ticker):
    """Exact match by NSE Code, BSE Code, or Short_id. Returns company dict or None."""
    load_companies()
    return _TICKER_INDEX.get(ticker.strip().lower())


def match_by_name(name, min_score=70, max_results=5):
    """Fuzzy match by company name. Returns list of (company_dict, score, method)."""
    load_companies()
    query = _normalize(name)
    if not query:
        return []

    # 1. Exact name match
    exact = _NAME_INDEX.get(name.strip().lower())
    if exact:
        return [(c, 100.0, "exact_name") for c in exact[:max_results]]

    # 2. Exact normalized match
    for key, companies in _NAME_INDEX.items():
        if _normalize(key) == query:
            return [(c, 98.0, "exact_normalized") for c in companies[:max_results]]

    # 3. Fuzzy match against all company names
    # Guard: very short queries (<=3 chars) need near-exact match to avoid
    # false positives (e.g. "iti" from "Utilities" matching ITI Ltd)
    effective_min = min_score if len(query) > 4 else max(min_score, 98)

    scored = []
    seen_ids = set()
    for c in _COMPANIES:
        cname = _normalize(c.get("Company Name", ""))
        cfull = _normalize(c.get("CompanyFullName", ""))

        score = max(
            _fuzzy_score(query, cname) if cname else 0,
            _fuzzy_score(query, cfull) if cfull else 0,
        )

        cid = c.get("DataCompanyID")
        if score >= effective_min and cid not in seen_ids:
            scored.append((c, score, "fuzzy_name"))
            seen_ids.add(cid)

    # Sort by score desc, then by market cap desc
    scored.sort(key=lambda x: (
        -x[1],
        -(float(x[0].get("Market Capitalization") or 0))
    ))

    return scored[:max_results]


def match_by_sector(sector_text, min_score=60, max_results=20):
    """Match sector text against taxonomy. Returns list of (sector_key, companies, score)."""
    load_companies()
    query = sector_text.strip().lower()
    if not query:
        return []

    results = []
    seen = set()
    for key, companies in _SECTOR_INDEX.items():
        score = _fuzzy_score(query, key)
        if score >= min_score and key not in seen:
            results.append((key, companies, score))
            seen.add(key)

    results.sort(key=lambda x: -x[2])
    return results[:max_results]


def get_company_by_id(data_company_id):
    """Look up a company by DataCompanyID."""
    load_companies()
    for c in _COMPANIES:
        if str(c.get("DataCompanyID")) == str(data_company_id):
            return c
    return None
