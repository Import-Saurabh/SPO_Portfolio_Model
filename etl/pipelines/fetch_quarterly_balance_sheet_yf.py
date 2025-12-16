"""
etl/pipelines/fetch_quarterly_balance_sheet_yf.py

Fetch quarterly balance sheet from Yahoo Finance (via yfinance) and insert into:
  financials_balance_sheet

Usage:
    cd <project_root>
    python etl/pipelines/fetch_quarterly_balance_sheet_yf.py

Notes:
- Requires: yfinance, pandas, sqlalchemy (and your project's etl.utils.db.get_session)
- Optional offline/company CSV path (default uses DB companies table):
    /mnt/data/companies.csv   <-- uploaded file in your project workspace
"""

import sys
import time
import logging
import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
import yfinance as yf
from sqlalchemy import text

# Ensure project root is on path when executed directly
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from etl.utils.db import get_session

LOG = logging.getLogger("etl.qbs_yf")
LOG.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
LOG.addHandler(ch)

# Optional path to uploaded companies CSV (developer-provided file)
# Developer note: uploaded file path available in conversation: /mnt/data/companies.csv
FALLBACK_COMPANIES_CSV = Path("/mnt/data/companies.csv")

# Map Yahoo-finance row names -> our DB columns
YF_TO_DB = {
    # Yahoo row label (index) : target column in financials_balance_sheet
    "Total Assets": "total_assets",
    "Total Liab": "total_liabilities",
    "Total Liabilities Net Minority Interest": "total_liabilities",
    "Total Stockholder Equity": "shareholder_equity",
    "Shareholder Equity": "shareholder_equity",
    "Total Equity": "shareholder_equity",
    "Total Current Assets": "current_assets",
    "Total Current Liabilities": "current_liabilities",
    "Cash": "cash_and_equivalents",
    "Cash And Cash Equivalents": "cash_and_equivalents",
    "Inventory": "inventory",
    "Net Receivables": "receivables",
    "Receivables": "receivables",
    "Long Term Debt": "long_term_debt",
    "Short Long Term Debt": "short_term_debt",
    "Short Term Debt": "short_term_debt",
    "Retained Earnings": "retained_earnings",
    # Add more mappings if needed
}

# SQL templates
SELECT_COMPANIES_SQL = """
    SELECT id, symbol
    FROM companies
    WHERE symbol IS NOT NULL AND symbol != ''
"""

INSERT_SQL = """
INSERT INTO financials_balance_sheet
  (company_id, fiscal_year, fiscal_quarter, report_date,
   total_assets, total_liabilities, shareholder_equity,
   current_assets, current_liabilities, cash_and_equivalents,
   inventory, receivables, long_term_debt, short_term_debt,
   retained_earnings, created_at)
VALUES
  (:company_id, :fiscal_year, :fiscal_quarter, :report_date,
   :total_assets, :total_liabilities, :shareholder_equity,
   :current_assets, :current_liabilities, :cash_and_equivalents,
   :inventory, :receivables, :long_term_debt, :short_term_debt,
   :retained_earnings, NOW())
"""

CHECK_EXIST_SQL = """
SELECT id FROM financials_balance_sheet
WHERE company_id = :company_id AND report_date = :report_date
LIMIT 1
"""

def to_decimal_safe(val) -> Optional[Decimal]:
    """Convert numeric from yfinance (int/float/np) to Decimal or None."""
    if val is None or (isinstance(val, float) and (pd.isna(val))):
        return None
    try:
        # yfinance values are usually integers (raw amount)
        return Decimal(str(int(val)))
    except (InvalidOperation, ValueError, TypeError):
        try:
            return Decimal(str(val))
        except Exception:
            return None

def quarter_from_date(dt: datetime.date) -> int:
    m = dt.month
    return (m - 1) // 3 + 1

def load_companies_from_db(session_db) -> List[tuple]:
    rows = session_db.execute(text(SELECT_COMPANIES_SQL)).fetchall()
    return [(r[0], r[1]) for r in rows]

def load_companies_from_csv(csv_path: Path) -> List[tuple]:
    if not csv_path.exists():
        return []
    df = pd.read_csv(csv_path, dtype=str)
    out = []
    # Expect columns id and symbol at least; try multiple column name options
    for idx, row in df.iterrows():
        try:
            cid = int(row.get("id") or row.get("company_id") or idx)
            sym = str(row.get("symbol") or row.get("ticker") or "").strip()
            if sym:
                out.append((cid, sym))
        except Exception:
            continue
    return out

def normalize_symbol_for_yf(sym: str) -> str:
    """
    Normalize symbol to Yahoo Finance style. For NSE tickers your repo probably uses 'RELIANCE.NS'.
    yfinance understands 'RELIANCE.NS' — keep as-is. If your symbol lacks exchange, optionally add '.NS'
    """
    s = sym.strip().upper()
    if "." not in s:
        # assume NSE if no suffix (be cautious)
        s = s + ".NS"
    return s

def extract_balance_rows(bal_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Input: yfinance Ticker.quarterly_balance_sheet (DataFrame)
      - index: labels (e.g. 'Total Assets', 'Total Liab', ...)
      - columns: datetimes (timestamps)
    Return mapping: report_date_iso -> dict of column values
    """
    out = {}
    if bal_df is None or bal_df.shape[0] == 0:
        return out

    # ensure index strings
    bal_df.index = [str(i).strip() for i in bal_df.index]

    # iterate columns (each column is a reporting date)
    for col in bal_df.columns:
        try:
            # yfinance column is a Timestamp
            if isinstance(col, (pd.Timestamp, datetime.datetime)):
                dt = pd.to_datetime(col).date()
            else:
                # Try parsing
                dt = pd.to_datetime(str(col)).date()
        except Exception:
            continue

        rowvals = {}
        for yf_label, db_col in YF_TO_DB.items():
            # find best matching index row (case-insensitive substring)
            matched = None
            for idx_label in bal_df.index:
                if yf_label.lower() == idx_label.lower():
                    matched = idx_label
                    break
            if matched is None:
                # try fuzzy contains
                for idx_label in bal_df.index:
                    if yf_label.lower() in idx_label.lower():
                        matched = idx_label
                        break

            if matched:
                raw = bal_df.at[matched, col]
                rowvals[db_col] = to_decimal_safe(raw)
            else:
                # leave missing as None
                rowvals[db_col] = None

        out[dt] = rowvals
    return out

def insert_balance_for_company(session_db, company_id: int, report_date: datetime.date, vals: dict):
    # Skip if already exists
    exists = session_db.execute(text(CHECK_EXIST_SQL), {"company_id": company_id, "report_date": report_date.isoformat()}).fetchone()
    if exists:
        LOG.info("Row exists for company=%s date=%s — skipping", company_id, report_date.isoformat())
        return False

    fiscal_year = report_date.year
    fiscal_quarter = quarter_from_date(report_date)

    params = {
        "company_id": company_id,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "report_date": report_date.isoformat(),
        "total_assets": vals.get("total_assets"),
        "total_liabilities": vals.get("total_liabilities"),
        "shareholder_equity": vals.get("shareholder_equity"),
        "current_assets": vals.get("current_assets"),
        "current_liabilities": vals.get("current_liabilities"),
        "cash_and_equivalents": vals.get("cash_and_equivalents"),
        "inventory": vals.get("inventory"),
        "receivables": vals.get("receivables"),
        "long_term_debt": vals.get("long_term_debt"),
        "short_term_debt": vals.get("short_term_debt"),
        "retained_earnings": vals.get("retained_earnings"),
    }

    # Convert Decimal -> numeric (string) for SQL param handling
    for k, v in list(params.items()):
        if isinstance(v, Decimal):
            params[k] = str(int(v))  # store integer string (DB DECIMAL will accept)
        elif v is None:
            params[k] = None

    session_db.execute(text(INSERT_SQL), params)
    return True

def run():
    session_db = get_session()
    LOG.info("DB session acquired")

    # Prefer DB companies table, but if you want to test offline use the uploaded CSV:
    companies = load_companies_from_db(session_db)
    if not companies and FALLBACK_COMPANIES_CSV.exists():
        LOG.info("No companies loaded from DB; using fallback CSV: %s", FALLBACK_COMPANIES_CSV)
        companies = load_companies_from_csv(FALLBACK_COMPANIES_CSV)

    LOG.info("Found %d companies to process", len(companies))
    report = []

    for cid, sym in companies:
        if not sym:
            continue
        yf_sym = normalize_symbol_for_yf(sym)
        LOG.info("Processing company_id=%s symbol=%s (yf=%s)", cid, sym, yf_sym)

        try:
            tk = yf.Ticker(yf_sym)
            bal = tk.quarterly_balance_sheet
            if bal is None or bal.empty:
                LOG.warning("No quarterly balance sheet for %s", yf_sym)
                report.append({"company_id": cid, "symbol": sym, "status": "NO_DATA"})
                time.sleep(0.5)
                continue

            # extract dictionary: date -> mapped values
            mapping = extract_balance_rows(bal)
            inserted_count = 0
            for report_dt, vals in mapping.items():
                try:
                    ok = insert_balance_for_company(session_db, cid, report_dt, vals)
                    if ok:
                        inserted_count += 1
                except Exception as e:
                    LOG.exception("Insert failed for %s %s: %s", cid, report_dt, e)
            # commit per company to reduce transaction size
            try:
                session_db.commit()
            except Exception as e:
                LOG.exception("Commit failed after inserts for %s: %s", cid, e)
                session_db.rollback()

            LOG.info("Inserted %d rows for %s", inserted_count, yf_sym)
            report.append({"company_id": cid, "symbol": sym, "inserted": inserted_count, "status": "OK"})
        except Exception as e:
            LOG.exception("Failed processing %s: %s", yf_sym, e)
            report.append({"company_id": cid, "symbol": sym, "status": f"ERROR: {e}"})

        # gentle pause to avoid throttling
        time.sleep(0.8)

    # final commit & close
    try:
        session_db.commit()
    except Exception:
        session_db.rollback()
    session_db.close()

    # Save report CSV in data/etl_reports
    try:
        out_dir = Path("data/etl_reports")
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out_file = out_dir / f"balance_sheet_yf_report_{ts}.csv"
        pd.DataFrame(report).to_csv(out_file, index=False)
        LOG.info("Saved report: %s", out_file)
    except Exception as e:
        LOG.warning("Failed to save report CSV: %s", e)

if __name__ == "__main__":
    run()
