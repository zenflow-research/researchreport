"""Level 1: Extract broker name and company name from PDF filenames.

This is the cheapest mapping step - no file I/O or LLM calls needed.
Handles multiple naming conventions from the Telegram research channel.
"""

import os
import re
import yaml


def _load_config():
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_CFG = None
_BROKERS = None
_BROKER_PATTERN = None


def _init():
    global _CFG, _BROKERS, _BROKER_PATTERN
    if _CFG is not None:
        return
    _CFG = _load_config()
    _BROKERS = _CFG.get("brokers", [])
    # Sort by length desc so "ICICI Securities" matches before "ICICI"
    _BROKERS.sort(key=len, reverse=True)
    # Build alternation pattern (escape special chars)
    escaped = [re.escape(b) for b in _BROKERS]
    _BROKER_PATTERN = re.compile(
        r"^(%s)[\s_\-]+" % "|".join(escaped), re.IGNORECASE
    )


def _clean_company_name(raw):
    """Clean extracted company name for matching."""
    if not raw:
        return ""
    name = raw.strip()
    # Replace underscores with spaces
    name = name.replace("_", " ")
    # Remove trailing numeric IDs (e.g., "_29285")
    name = re.sub(r"\s+\d{4,}$", "", name)
    # Remove common suffixes that aren't part of company name
    for pat in [
        r"\s+(?:Growth|Recovery|Earnings|Revenue|Strong|Weak|Inline|Update|"
        r"Result|Preview|Review|Note|Report|Analysis|Coverage|Buy|Sell|Hold|"
        r"Outperform|Underperform|Neutral|Overweight|Underweight|Target|"
        r"Improvement|Clarity|Steady|Nothing|Product|Regional|"
        r"Rating|Upgrade|Downgrade|Maintain|Initiat)\b.*$",
    ]:
        name = re.sub(pat, "", name, flags=re.IGNORECASE)
    name = name.strip(" -_,.")
    return name


def _classify_report_type(filename):
    """Guess report type from filename patterns."""
    fl = filename.lower()

    if any(kw in fl for kw in ["ipo note", "ipo_note", "ipo analysis"]):
        return "ipo_note"
    if any(kw in fl for kw in ["earnings preview", "earnings_preview",
                                 "q1fy", "q2fy", "q3fy", "q4fy",
                                 "q1 fy", "q2 fy", "q3 fy", "q4 fy"]):
        return "earnings_preview"
    if any(kw in fl for kw in ["sector", "sectoral", "industry"]):
        return "sector_report"
    if any(kw in fl for kw in ["daily", "morning", "front page",
                                 "the front page"]):
        return "daily_update"
    if any(kw in fl for kw in ["market", "outlook", "macro", "economy",
                                 "handbook", "resilience", "recession"]):
        return "market_overview"
    if any(kw in fl for kw in ["book -", "book_", "series"]):
        return "book"
    if any(kw in fl for kw in ["initiating coverage", "initiating_coverage"]):
        return "company_specific"
    if any(kw in fl for kw in ["upside", "downside", "target price",
                                 "target_price"]):
        return "company_specific"
    return None  # undetermined at filename level


def parse_filename(filename):
    """Parse a research report filename to extract broker, company, report type.

    Returns dict with keys:
        broker:       str or None
        company_raw:  str or None (raw extracted text)
        company:      str or None (cleaned for matching)
        report_type:  str or None
        confidence:   float (0.0 - 1.0)
        pattern_used: str (which pattern matched)
    """
    _init()
    result = {
        "broker": None,
        "company_raw": None,
        "company": None,
        "report_type": None,
        "confidence": 0.0,
        "pattern_used": None,
    }

    # Strip extension
    base = os.path.splitext(filename)[0]

    # Skip hash-named files (no useful info)
    if re.match(r"^[0-9a-f]{20,}", base):
        result["report_type"] = "unknown"
        return result

    # Classify report type from full filename
    result["report_type"] = _classify_report_type(filename)

    # --- Pattern 1: Broker_sees_X%_UPSIDE_in_Company_Description_ID ---
    m = re.match(
        r"^(.+?)_sees_\d+%_(?:UPSIDE|DOWNSIDE)_in_(.+)$",
        base
    )
    if m:
        result["broker"] = m.group(1).replace("_", " ")
        raw = m.group(2).replace("_", " ")
        result["company_raw"] = raw
        result["company"] = _clean_company_name(raw)
        result["confidence"] = 0.90
        result["pattern_used"] = "upside_pattern"
        result["report_type"] = result["report_type"] or "company_specific"
        return result

    # --- Pattern 2: "Broker sees good upside in Company" (space or underscore) ---
    m = re.match(
        r"^(.+?)[\s_]+[Ss]ees[\s_]+.*?(?:[Uu]pside|[Dd]ownside)[\s_]+in[\s_]+(.+)$",
        base
    )
    if m:
        result["broker"] = m.group(1).strip()
        raw = m.group(2).strip()
        result["company_raw"] = raw
        result["company"] = _clean_company_name(raw)
        result["confidence"] = 0.85
        result["pattern_used"] = "sees_upside_natural"
        result["report_type"] = result["report_type"] or "company_specific"
        return result

    # --- Pattern 3: "Broker Initiating Coverage on Company" (space or underscore) ---
    m = re.match(
        r"^(.+?)[\s_]+(?:Initiating|initiating|INITIATING|Initiates|initiates)[\s_]+(?:Coverage|coverage|COVERAGE)[\s_]+(?:on|ON)[\s_]+(.+)$",
        base, re.IGNORECASE
    )
    if m:
        result["broker"] = m.group(1).strip()
        raw = m.group(2).strip()
        result["company_raw"] = raw
        result["company"] = _clean_company_name(raw)
        result["confidence"] = 0.90
        result["pattern_used"] = "initiating_coverage"
        result["report_type"] = "company_specific"
        return result

    # --- Pattern 4: "Company - IC - Date - Broker" ---
    m = re.match(
        r"^(.+?)\s*-\s*IC\s*-\s*[\d-]+\s*-\s*(.+)$",
        base
    )
    if m:
        raw = m.group(1).strip()
        result["broker"] = m.group(2).strip()
        result["company_raw"] = raw
        result["company"] = _clean_company_name(raw)
        result["confidence"] = 0.90
        result["pattern_used"] = "company_ic_broker"
        result["report_type"] = "company_specific"
        return result

    # --- Pattern 5: "Broker on Company" ---
    m = re.match(
        r"^(.+?)\s+on\s+(.+)$",
        base, re.IGNORECASE
    )
    if m:
        broker_candidate = m.group(1).strip()
        company_candidate = m.group(2).strip()
        # Validate: broker_candidate should match a known broker
        for b in _BROKERS:
            if broker_candidate.lower().startswith(b.lower()):
                result["broker"] = b
                raw = company_candidate
                result["company_raw"] = raw
                result["company"] = _clean_company_name(raw)
                result["confidence"] = 0.80
                result["pattern_used"] = "broker_on_company"
                result["report_type"] = result["report_type"] or "company_specific"
                return result
        # "on" might be part of company name - skip

    # --- Pattern 6: Known broker prefix + remainder ---
    m = _BROKER_PATTERN.match(base)
    if m:
        result["broker"] = m.group(1).strip()
        remainder = base[m.end():].strip(" _-")

        # Skip daily/periodic reports - not company-specific
        if result["report_type"] in ("daily_update", "market_overview"):
            result["confidence"] = 0.40
            result["pattern_used"] = "broker_prefix_periodic"
            return result

        # Check if remainder looks like a company name (not too long)
        if remainder and len(remainder.split()) <= 8:
            result["company_raw"] = remainder.replace("_", " ")
            result["company"] = _clean_company_name(result["company_raw"])
            result["confidence"] = 0.60
            result["pattern_used"] = "broker_prefix"
            return result
        else:
            result["confidence"] = 0.30
            result["pattern_used"] = "broker_prefix_no_company"
            return result

    # --- No pattern matched ---
    result["confidence"] = 0.10
    result["pattern_used"] = "none"
    return result


def parse_batch(filenames):
    """Parse a list of filenames. Returns list of result dicts."""
    return [parse_filename(f) for f in filenames]


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_names = [
        "Motilal_Oswal_sees_18%_UPSIDE_in_Indostar_Capital_Finance_Improvement_29285.pdf",
        "HDFC Securities on Chennai Petro.pdf",
        "Kotak Initiating Coverage on LIC.pdf",
        "Hindustan Foods - IC - 22-04-2025 - Systematix.pdf",
        "Ashika on KPIT Technologies.pdf",
        "ICICI_Securities_sees_42%_UPSIDE_in_Honasa_Consumer_Clarity_80567.pdf",
        "Dolat_Capital_Q3FY23_Earnings_Preview_AlcoBev_Hotels_Media_Retail.pdf",
        "Antique Daily 07-Nov-25.pdf",
        "9418ff20aed446ae9d62ed553ea87371-04012023.pdf",
        "SBI Securities IPO Note on Amagi Media Labs Ltd.pdf",
        "Morgan_Stanley_Sees_Good_Upside_in_Bank_of_Baroda_and_Bank_of_India.pdf",
    ]

    for name in test_names:
        r = parse_filename(name)
        print("%-70s -> broker=%-20s company=%-30s type=%-18s conf=%.2f  pat=%s"
              % (name[:70], r["broker"], r["company"], r["report_type"],
                 r["confidence"], r["pattern_used"]))
