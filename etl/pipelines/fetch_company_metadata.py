"""
Auto-discovery Company Metadata ETL (Moneycontrol / yfinance)
- Reads company symbols from DB (companies table). If DB unavailable, reads /mnt/data/companies.csv as fallback.
- For each symbol (Yahoo format e.g. RELIANCE.NS):
    1) Try yfinance.Ticker(symbol).info -> longName, sector, industry, isin (if present)
    2) If yfinance misses fields, try Moneycontrol scraping (best-effort)
- Update companies table: name, sector, industry, isin, listing_date (if found)
- Safe / idempotent: only updates NULL columns by default (can be changed)
- Usage: run from project root:
    python etl/pipelines/fetch_company_metadata.py
"""

import time
import logging
from pathlib import Path
from typing import Optional
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import yfinance as yf
import pandas as pd
import datetime


try:
    from etl.utils.logger import get_logger
    from etl.utils.db import get_session
    from database.Models import Company
except Exception:
    # if running as script by path, try to add project root to sys.path (safe fallback)
    import sys, os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
    from etl.utils.logger import get_logger
    from etl.utils.db import get_session
    from database.Models import Company

logger = get_logger("etl.fetch_company_metadata")

# Fallback CSV (developer-provided)
FALLBACK_COMPANIES_CSV = Path("/mnt/data/companies.csv")

# Moneycontrol constants (best-effort)
MONEYCONTROL_BASE = "https://www.moneycontrol.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    "Referer": MONEYCONTROL_BASE,
}

# How many seconds to wait between requests (politeness)
REQUEST_PAUSE = 0.8

# Retry config for network ops
retry_network = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10),
                      retry=retry_if_exception_type((requests.exceptions.RequestException,)))


def read_symbols_from_db(session):
    """
    Return list of (id, symbol, name, sector, industry) for companies table
    """
    rows = session.query(Company).all()
    result = []
    for r in rows:
        result.append({
            "id": r.id,
            "symbol": r.symbol,
            "name": r.name,
            "sector": r.sector,
            "industry": r.industry,
        })
    return result


def read_symbols_from_csv(path=FALLBACK_COMPANIES_CSV):
    """
    Fallback: read uploaded CSV (expects column 'symbol')
    """
    if not path.exists():
        logger.error("Fallback CSV not found: %s", path)
        return []
    df = pd.read_csv(path)
    if "symbol" not in df.columns:
        logger.error("Fallback CSV missing 'symbol' column")
        return []
    symbols = []
    for _, row in df.iterrows():
        symbols.append({
            "id": None,
            "symbol": str(row["symbol"]).strip(),
            "name": row.get("name"),
            "sector": row.get("sector"),
            "industry": row.get("industry"),
        })
    logger.info("Loaded %d symbols from fallback CSV", len(symbols))
    return symbols


def safe_get_ticker_info(symbol: str) -> dict:
    """
    Use yfinance to get info. Returns dict possibly containing:
    longName, sector, industry, isin, website
    """
    try:
        t = yf.Ticker(symbol)
        info = t.info or {}
        # Keep only relevant keys
        return {
            "name": info.get("longName") or info.get("shortName") or None,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "isin": info.get("isin") or info.get("ISIN"),
            "listing_date": None,
            "source": "yfinance",
        }
    except Exception as e:
        logger.debug("yfinance failed for %s: %s", symbol, e)
        return {}


@retry_network
def moneycontrol_search(symbol_no_ns: str) -> Optional[str]:
    """
    Best-effort: use Moneycontrol search to find a profile URL.
    Moneycontrol does not publish a stable public API; this function tries to query
    the search page and parse the first plausible profile link.
    Input: symbol without .NS (e.g. RELIANCE)
    Returns full URL or None
    """
    q = symbol_no_ns
    search_url = f"{MONEYCONTROL_BASE}/g/search?q={q}"
    # Note: Moneycontrol uses dynamic content; this endpoint returns HTML with links we can parse.
    resp = requests.get(search_url, headers=HEADERS, timeout=12)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    # find first <a> that looks like a company profile (heuristic)
    anchors = soup.find_all("a", href=True)
    for a in anchors:
        href = a["href"]
        if "/company" in href or "/india/stockpricequote" in href or "/stocks/companyinfo" in href:
            # normalize
            if href.startswith("http"):
                return href
            else:
                return MONEYCONTROL_BASE + href
    return None


@retry_network
def moneycontrol_scrape_profile(url: str) -> dict:
    """
    Scrape profile page for name, sector, industry, isin, listing date (best-effort).
    Returns dict with keys similar to yfinance fallback.
    """
    logger.debug("Scraping Moneycontrol profile: %s", url)
    resp = requests.get(url, headers=HEADERS, timeout=12)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # heuristics:
    result = {"name": None, "sector": None, "industry": None, "isin": None, "listing_date": None, "source": "moneycontrol"}

    # Company name: look for <h1> or meta og:title
    h1 = soup.find("h1")
    if h1 and h1.text.strip():
        result["name"] = h1.text.strip()
    else:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            result["name"] = og["content"].strip()

    # Sector/industry: many pages have a table or breadcrumbs
    # Find key: "Sector" or "Industry" labels in page content
    text = soup.get_text(separator="|", strip=True)
    # quick heuristics
    for token in ["Sector:", "Sector", "Industry:", "Industry"]:
        if token in text:
            # crude attempt to extract
            try:
                parts = text.split(token, 1)[1]
                # take few words
                candidate = parts.split("|", 1)[0].strip()
                if token.startswith("Sector") and not result["sector"]:
                    result["sector"] = candidate
                elif token.startswith("Industry") and not result["industry"]:
                    result["industry"] = candidate
            except Exception:
                pass

    # ISIN: look for 'ISIN' substring
    if "ISIN" in text:
        try:
            after = text.split("ISIN", 1)[1]
            candidate = after.split("|", 1)[0]
            candidate = candidate.strip().strip(":").strip()
            # basic filter
            if len(candidate) >= 8:
                result["isin"] = candidate
        except Exception:
            pass

    # Listing date: look for 'Listing Date' or 'Date of Listing'
    for label in ["Listing Date", "Date of Listing", "Listed On"]:
        if label in text and not result["listing_date"]:
            try:
                after = text.split(label, 1)[1]
                candidate = after.split("|", 1)[0].strip()
                # try parse
                try:
                    dt = pd.to_datetime(candidate, errors="coerce")
                    if pd.notna(dt):
                        result["listing_date"] = dt.date().isoformat()
                except Exception:
                    pass
            except Exception:
                pass

    return result


def merge_metadata(current: dict, new: dict) -> dict:
    """
    Only overwrite null fields in current with values from new.
    current keys: name, sector, industry, isin, listing_date
    """
    out = current.copy()
    for k in ["name", "sector", "industry", "isin", "listing_date"]:
        if (out.get(k) is None or str(out.get(k)).strip() == "") and new.get(k):
            out[k] = new.get(k)
    return out


def update_company_in_db(session, company_id: int, updates: dict):
    """
    Update only specified columns for a given company id.
    """
    if not updates:
        return False
    try:
        company = session.query(Company).get(company_id)
        if not company:
            return False
        changed = False
        for k, v in updates.items():
            if v is None:
                continue
            # only update if current is null or empty
            cur = getattr(company, k, None)
            if cur is None or (isinstance(cur, str) and cur.strip() == ""):
                setattr(company, k, v)
                changed = True
        if changed:
            session.add(company)
            session.commit()
            logger.info("Updated company id=%s with fields: %s", company_id, list(updates.keys()))
        return changed
    except Exception as e:
        session.rollback()
        logger.exception("DB update error for company %s: %s", company_id, e)
        return False


def process_single(session, record):
    """
    record: dict with keys id, symbol, name, sector, industry
    """
    symbol = record["symbol"]
    company_id = record.get("id")  # may be None if from CSV fallback

    # normalize symbol to yfinance form (if not already)
    symbol = symbol.strip()
    # try yfinance
    info = safe_get_ticker_info(symbol)
    time.sleep(REQUEST_PAUSE)
    # if yfinance lacks sector/name -> try moneycontrol
    if not info.get("name") or not info.get("sector") or not info.get("industry"):
        # prepare moneycontrol token: strip .NS if present
        token = symbol.replace(".NS", "").replace(".BO", "").replace(".BSE", "")
        try:
            url = moneycontrol_search(token)
            if url:
                mc = moneycontrol_scrape_profile(url)
                # merge mc into info without overwriting good data
                info = merge_metadata(info, mc)
        except Exception as e:
            logger.debug("Moneycontrol attempt failed for %s: %s", symbol, e)

    # Prepare update dict
    updates = {
        "name": info.get("name"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "isin": info.get("isin"),
        "listing_date": info.get("listing_date"),
    }

    # filter None values
    updates = {k: v for k, v in updates.items() if v is not None and v != ""}

    if company_id:
        updated = update_company_in_db(session, company_id, updates)
        return updated
    else:
        # no company id (CSV fallback) - write to CSV (or log)
        logger.info("No DB id for %s — would update (simulated): %s", symbol, updates)
        return False


def main():
    started = datetime.datetime.utcnow() if (datetime := None) else None

    session = None
    symbols = []
    try:
        session = get_session()
        symbols = read_symbols_from_db(session)
        if not symbols:
            logger.warning("No rows read from DB companies table")
            # fallback to CSV
            symbols = read_symbols_from_csv()
    except Exception as e:
        logger.exception("Failed reading DB companies table, falling back to CSV: %s", e)
        symbols = read_symbols_from_csv()

    if not symbols:
        logger.error("No symbols to process. Exit.")
        return

    logger.info("Starting metadata fetch for %d symbols", len(symbols))

    # iterate and process
    processed = 0
    for rec in symbols:
        try:
            process_single(session, rec)
            processed += 1
        except Exception as e:
            logger.exception("Failed processing %s: %s", rec.get("symbol"), e)
        time.sleep(0.5)  # small pause between companies

    logger.info("Metadata ETL complete: processed %d symbols", processed)
    try:
        if session:
            session.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
