"""
Fill listing_date field using NSE official EQUITY_L.csv file.

Run:
    python etl/pipelines/fill_listing_date_nse.py
"""

import sys
import time
import logging
import datetime
import requests
import pandas as pd
from pathlib import Path
from sqlalchemy import text

# Ensure project root
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from etl.utils.db import get_session


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------

LOG = logging.getLogger("etl.fill_listing_date_nse_equity_l")
LOG.setLevel(logging.INFO)
h = logging.StreamHandler()
h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
LOG.addHandler(h)

REPORT_DIR = Path("data/etl_reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------------------------
# NSE EQUITY_L.csv source
# ------------------------------------------------------------------------------

EQUITY_L_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com"
}


def download_equity_l():
    """Download and load EQUITY_L.csv from NSE archives."""
    LOG.info("Downloading EQUITY_L.csv ...")

    r = requests.get(EQUITY_L_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()

    from io import StringIO
    df = pd.read_csv(StringIO(r.text))

    LOG.info("Downloaded EQUITY_L.csv with %d rows", len(df))
    return df


def normalize_symbol(sym: str) -> str:
    """Reformat DB symbol to match NSE format (remove .NS, .BSE)."""
    sym = sym.upper().strip()
    if "." in sym:
        sym = sym.split(".")[0]
    return sym


# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------

def get_all_companies(session_db):
    q = text("""
        SELECT id, symbol, listing_date
        FROM companies
    """)
    return session_db.execute(q).fetchall()


def update_listing_date(session_db, cid, listing_date):
    q = text("""
        UPDATE companies
        SET listing_date = :ld
        WHERE id = :cid
    """)
    session_db.execute(q, {"ld": listing_date, "cid": cid})


# ------------------------------------------------------------------------------
# Main Process
# ------------------------------------------------------------------------------

def run_fill():

    session_db = get_session()

    # STEP 1 — load all companies
    companies = get_all_companies(session_db)
    LOG.info("Loaded %d companies from DB", len(companies))

    # STEP 2 — download NSE listing data
    df = download_equity_l()

    # Normalize columns
    df.columns = [c.strip().upper() for c in df.columns]

    if "SYMBOL" not in df.columns or "DATE OF LISTING" not in df.columns:
        raise RuntimeError("EQUITY_L.csv format changed!")

    # Prepare mapping: symbol → date
    nse_map = {}

    for _, row in df.iterrows():
        sym = str(row["SYMBOL"]).strip().upper()
        ld_raw = str(row["DATE OF LISTING"]).strip()

        try:
            ld = datetime.datetime.strptime(ld_raw, "%d-%b-%Y").date()
            nse_map[sym] = ld.isoformat()
        except:
            continue

    LOG.info("Prepared %d listing-date mappings", len(nse_map))

    # Process each company
    report = []

    for cid, sym, ld in companies:
        sym_clean = normalize_symbol(sym)

        if ld not in (None, "", "0000-00-00"):
            # listing date already present
            continue

        if sym_clean in nse_map:
            new_date = nse_map[sym_clean]
            update_listing_date(session_db, cid, new_date)

            LOG.info("Updated %s → %s", sym_clean, new_date)

            report.append({
                "company_id": cid,
                "symbol": sym,
                "listing_date": new_date,
                "status": "UPDATED"
            })
        else:
            LOG.warning("Listing date not found for symbol %s", sym_clean)
            report.append({
                "company_id": cid,
                "symbol": sym,
                "listing_date": None,
                "status": "NOT_FOUND"
            })

        time.sleep(0.1)

    # Commit final changes
    try:
        session_db.commit()
    except:
        session_db.rollback()
        raise

    # Save report
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fp = REPORT_DIR / f"listing_date_nse_equity_l_{ts}.csv"
    pd.DataFrame(report).to_csv(fp, index=False)

    LOG.info("Saved report → %s", fp)
    LOG.info("Completed listing-date ETL.")


if __name__ == "__main__":
    run_fill()
