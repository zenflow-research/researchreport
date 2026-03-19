"""Level 2: Extract company/sector info from PDF first pages using Ollama.

Reads the first 1-2 pages of a PDF and uses a local LLM (Ollama)
to extract structured company identification data.
"""

import json
import os
import urllib.request

# ---------------------------------------------------------------------------
# PDF text extraction (PyMuPDF)
# ---------------------------------------------------------------------------

def extract_first_pages(pdf_path, max_pages=2):
    """Extract text from first N pages of a PDF using PyMuPDF."""
    try:
        import fitz  # PyMuPDF
        # Suppress MuPDF C-level warnings (broken font tables etc)
        fitz.TOOLS.mupdf_warnings(reset=True)
    except ImportError:
        raise ImportError("PyMuPDF required: pip install PyMuPDF")

    text_parts = []
    try:
        doc = fitz.open(pdf_path)
        for i in range(min(max_pages, len(doc))):
            page = doc[i]
            text_parts.append(page.get_text("text"))
        doc.close()
    except Exception as e:
        return "", str(e)

    return "\n---PAGE BREAK---\n".join(text_parts), None


# ---------------------------------------------------------------------------
# Ollama extraction
# ---------------------------------------------------------------------------

OLLAMA_PROMPT = """You are a financial research report analyzer. Extract the following from this research report page(s):

1. **company_name**: The primary company being analyzed (NOT the broker/publisher)
2. **ticker**: NSE or BSE ticker symbol if visible (e.g., "RELIANCE", "TCS", "500325")
3. **isin**: ISIN code if visible (e.g., "INE002A01018")
4. **sector**: Industry sector of the company (e.g., "IT Services", "Banking", "Pharma")
5. **broker**: The research firm that published the report
6. **report_type**: One of: company_specific, sector_report, market_overview, earnings_preview, thematic, daily_update, ipo_note, book
7. **report_date**: The publication date of the report in YYYY-MM-DD format. Look for dates in headers, footers, or title pages.
8. **multiple_companies**: true if the report covers multiple companies, false if single company
9. **other_companies**: List of other company names mentioned as subjects (not just references)

Respond ONLY with valid JSON. No explanation.

Example output:
{"company_name": "Tata Consultancy Services", "ticker": "TCS", "isin": null, "sector": "IT Services", "broker": "Motilal Oswal", "report_type": "company_specific", "report_date": "2025-01-15", "multiple_companies": false, "other_companies": []}

---
REPORT TEXT:
%s
"""


def _call_ollama(prompt, model="gemma2:latest",
                  base_url="http://localhost:11434", timeout=120):
    """Call Ollama API and return response text."""
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 500,
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        "%s/api/generate" % base_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )

    with urllib.request.urlopen(req, timeout=timeout) as resp:
        result = json.loads(resp.read().decode("utf-8"))
    return result.get("response", "").strip()


def _parse_json_response(text):
    """Extract JSON from LLM response (may be wrapped in markdown fences)."""
    # Strip markdown code fences
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last fence lines
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object in text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
    return None


def scan_pdf(pdf_path, model="gemma2:latest",
             base_url="http://localhost:11434", max_pages=2):
    """Scan a PDF's first pages with Ollama to extract company info.

    Returns dict with keys:
        company_name, ticker, isin, sector, broker, report_type,
        multiple_companies, other_companies, error
    """
    empty_result = {
        "company_name": None, "ticker": None, "isin": None,
        "sector": None, "broker": None, "report_type": None,
        "report_date": None,
        "multiple_companies": False, "other_companies": [],
        "error": None,
    }

    # Extract text
    text, err = extract_first_pages(pdf_path, max_pages)
    if err:
        empty_result["error"] = "PDF read error: %s" % err
        return empty_result
    if not text or len(text.strip()) < 50:
        empty_result["error"] = "Too little text extracted"
        return empty_result

    # Truncate to avoid token limits (approx 3000 chars = ~800 tokens)
    text_truncated = text[:4000]

    # Call Ollama
    prompt = OLLAMA_PROMPT % text_truncated
    try:
        response = _call_ollama(prompt, model=model, base_url=base_url)
    except Exception as e:
        empty_result["error"] = "Ollama error: %s" % str(e)
        return empty_result

    # Parse response
    parsed = _parse_json_response(response)
    if not parsed:
        empty_result["error"] = "Could not parse Ollama JSON response"
        return empty_result

    return {
        "company_name": parsed.get("company_name"),
        "ticker": parsed.get("ticker"),
        "isin": parsed.get("isin"),
        "sector": parsed.get("sector"),
        "broker": parsed.get("broker"),
        "report_type": parsed.get("report_type"),
        "report_date": parsed.get("report_date"),
        "multiple_companies": parsed.get("multiple_companies", False),
        "other_companies": parsed.get("other_companies", []),
        "error": None,
    }


# ---------------------------------------------------------------------------
# Claude CLI fallback (Level 3)
# ---------------------------------------------------------------------------

CLAUDE_PROMPT = """You are analyzing research report PDFs to identify which company they are about.

Given the text from a research report, extract:
1. company_name: Primary company (NOT the broker)
2. ticker: NSE or BSE ticker
3. sector: Industry sector
4. broker: Publishing firm
5. report_type: company_specific|sector_report|market_overview|earnings_preview|thematic|daily_update|ipo_note|book
6. other_companies: Other companies that are subjects (not just mentioned)

Respond ONLY with valid JSON array. One object per report.

---
REPORTS:
%s
"""


def scan_pdf_claude(pdf_paths, timeout=300):
    """Scan multiple PDFs using Claude CLI (Level 3 - expensive).

    Args:
        pdf_paths: list of PDF file paths
        timeout: subprocess timeout in seconds

    Returns list of dicts (same structure as scan_pdf).
    """
    import subprocess

    texts = []
    for i, path in enumerate(pdf_paths):
        text, err = extract_first_pages(path, max_pages=2)
        if err or not text:
            texts.append("REPORT %d [%s]: (could not read)" % (i, os.path.basename(path)))
        else:
            texts.append("REPORT %d [%s]:\n%s" % (i, os.path.basename(path), text[:3000]))

    combined = "\n\n===NEXT REPORT===\n\n".join(texts)
    prompt = CLAUDE_PROMPT % combined

    # Call Claude CLI via subprocess (same pattern as Annual_report_extract)
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)

    try:
        result = subprocess.run(
            ["claude", "-p"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            env=env,
        )

        if result.returncode != 0:
            return [{"error": "Claude CLI failed: %s" % result.stderr[:300]}]

        parsed = _parse_json_response(result.stdout)
        if isinstance(parsed, list):
            return parsed
        elif isinstance(parsed, dict):
            return [parsed]
        else:
            return [{"error": "Could not parse Claude response"}]

    except subprocess.TimeoutExpired:
        return [{"error": "Claude CLI timed out after %ds" % timeout}]
    except FileNotFoundError:
        return [{"error": "Claude CLI not found - install Claude Code"}]
