"""
Fix zero-volume rows in price_history using NSE bhavcopy (accurate method).

Usage:
    python etl/pipelines/fix_zero_volume_nse.py
"""

import os
import io
import sys
import time
import zipfile
import logging
import datetime
import requests
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Ensure project root
if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.append(str(root))

# Imports
from etl.utils.db import get_session
from database.Models import Company, PriceHistory

# Logging
LOG = logging.getLogger("etl.fix_zero_volume")
LOG.setLevel(logging.INFO)
h = logging.StreamHandler()
h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
LOG.addHandler(h)

# Fallback CSV
FALLBACK_CSV = Path("/mnt/data/price_history.csv")

REPORT_DIR = Path("data/etl_reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

NSE_BASE = "https://archives.nseindia.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com"
}
REQUEST_TIMEOUT = 20
REQUEST_PAUSE = 0.8

retry_network = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=1, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException,))
)


# ---------------------------- URL Builder ----------------------------

def nse_bhav_url_for_date(dt: datetime.date):
    mon = dt.strftime("%b").upper()
    dd = dt.strftime("%d")
    yyyy = dt.strftime("%Y")
    zip_name = f"cm{dd}{mon}{yyyy}bhav.csv.zip"
    inner = f"cm{dd}{mon}{yyyy}bhav.csv"
    url = f"{NSE_BASE}/content/historical/EQUITIES/{yyyy}/{mon}/{zip_name}"
    return url, inner


# ---------------------------- Downloader ----------------------------

@retry_network
def download_bhavzip(session: requests.Session, url: str) -> bytes:
    LOG.info("Downloading %s", url)
    r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
    r.raise_for_status()
    return r.content


def parse_bhav_csv_from_zipbytes(zip_bytes: bytes, inner_csv_name: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        names = z.namelist()
        target = None
        for n in names:
            if inner_csv_name.lower() in n.lower():
                target = n
                break
        if not target:
            for n in names:
                if n.lower().endswith(".csv"):
                    target = n
                    break
        if not target:
            raise RuntimeError("No CSV inside ZIP")
        LOG.info("Reading CSV: %s", target)
        with z.open(target) as f:
            df = pd.read_csv(f, dtype=str)
    return df


def build_symbol_volume_map(df: pd.DataFrame) -> dict:
    # Locate symbol column
    sym_col = None
    for c in df.columns:
        if c.strip().upper() in ("SYMBOL", "SC_NAME", "SC_CODE"):
            sym_col = c
            break
    if not sym_col:
        raise RuntimeError("SYMBOL column missing in bhav")

    # Locate volume column
    vol_col = None
    for target in ("TOTTRDQTY", "TOT_TRD_QTY"):
        for c in df.columns:
            if c.strip().upper() == target:
                vol_col = c
                break
        if vol_col:
            break
    if not vol_col:
        for c in df.columns:
            if "QTY" in c.upper() or "VOLUME" in c.upper():
                vol_col = c
                break
    if not vol_col:
        raise RuntimeError("No volume column found")

    mapping = {}
    for _, row in df.iterrows():
        sym = str(row[sym_col]).strip().upper()
        raw = row.get(vol_col, "")
        try:
            vol = int(float(str(raw).replace(",", "").strip()))
        except:
            vol = 0
        mapping[sym] = vol
    return mapping


# ---------------------------- DB Helpers ----------------------------

def get_zero_volume_dates(session_db):
    """Return list of trade_date values where volume=0 exists."""
    q = text("""
        SELECT DISTINCT trade_date
        FROM price_history
        WHERE volume = 0
        ORDER BY trade_date;
    """)
    rows = session_db.execute(q).fetchall()

    out = []
    for r in rows:
        d = r[0]
        if isinstance(d, datetime.date):
            out.append(d)
        else:
            out.append(datetime.datetime.fromisoformat(str(d)).date())
    return out


def get_company_symbol_map(session_db):
    q = text("SELECT id, symbol FROM companies;")
    rows = session_db.execute(q).fetchall()

    id_by_sym = {}
    sym_by_id = {}
    for r in rows:
        cid, sym = r
        if not sym:
            continue
        s = sym.strip().upper()
        base = s.split(".")[0]
        id_by_sym[base] = cid
        sym_by_id[cid] = s
    return id_by_sym, sym_by_id


def update_volumes_for_date(session_db, dt: datetime.date, bhav_map: dict):
    """Fix all price_history rows for one trade_date."""

    q = text("""
        SELECT ph.id, ph.company_id, c.symbol
        FROM price_history ph
        JOIN companies c ON ph.company_id = c.id
        WHERE ph.trade_date = :d AND ph.volume = 0;
    """)

    rows = session_db.execute(q, {"d": dt.isoformat()}).fetchall()

    updated_rows = []
    for ph_id, cid, sym in rows:
        base = sym.split(".")[0].upper()
        vol = bhav_map.get(base)

        if vol is None:
            vol = bhav_map.get(sym.upper().replace(".NS", ""))

        if vol is None:
            continue

        upd = text("UPDATE price_history SET volume = :v WHERE id = :id")
        session_db.execute(upd, {"v": int(vol), "id": ph_id})

        updated_rows.append({
            "price_history_id": ph_id,
            "company_id": cid,
            "symbol": sym,
            "trade_date": dt.isoformat(),
            "new_volume": int(vol)
        })

    session_db.commit()
    return updated_rows


# ---------------------------- Main ----------------------------

def run_fix():
    session_web = requests.Session()
    session_web.get("https://www.nseindia.com", headers=HEADERS)

    session_db = get_session()

    id_by_sym, sym_by_id = get_company_symbol_map(session_db)
    LOG.info("Loaded %d companies from DB", len(id_by_sym))

    zero_dates = get_zero_volume_dates(session_db)
    LOG.info("Found %d trade_dates with zero-volume rows", len(zero_dates))

    report = []

    for dt in zero_dates:
        url, inner = nse_bhav_url_for_date(dt)
        try:
            zip_bytes = download_bhavzip(session_web, url)
            df = parse_bhav_csv_from_zipbytes(zip_bytes, inner)
            bhav_map = build_symbol_volume_map(df)
            updated = update_volumes_for_date(session_db, dt, bhav_map)
            LOG.info("Date %s — updated %d rows", dt, len(updated))
            report.extend(updated)

        except requests.exceptions.HTTPError:
            LOG.warning("No bhavcopy for %s (holiday?)", dt)
        except Exception as e:
            LOG.exception("Error processing %s: %s", dt, e)

        time.sleep(REQUEST_PAUSE)

    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = REPORT_DIR / f"volume_fix_report_{ts}.csv"
    if report:
        pd.DataFrame(report).to_csv(out, index=False)
        LOG.info("Saved report: %s", out)
    else:
        LOG.info("No updates performed.")

    session_db.close()
    LOG.info("Completed.")


if __name__ == "__main__":
    run_fix()
