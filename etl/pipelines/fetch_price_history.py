# etl/pipelines/fetch_price_history.py
"""
Production-grade ETL to fetch historical OHLCV for a universe (NIFTY 500).
- Reads universe from data/universe/nifty500.txt (one ticker per line, yfinance format)
- Downloads 5 years of daily data via yfinance
- Writes raw Parquet to data/bronze/prices/{symbol}.parquet
- Writes cleaned Parquet to data/silver/prices/{symbol}.parquet
- Inserts missing rows into price_history table (idempotent)
- Logs ETL run in etl_runs
"""
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


from pathlib import Path
import datetime
import json
import time
from typing import List
import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.exc import SQLAlchemyError

from etl.utils.logger import get_logger
from etl.utils.validation import validate_ohlcv
from etl.utils.db import get_session, ensure_companies, get_existing_dates, log_etl_run
from database.Models import PriceHistory

# config
ROOT = Path(".")
UNIVERSE_FILE = ROOT / "Data" / "universe" / "nifty500.txt"
BRONZE_DIR = ROOT / "Data" / "Bronze" / "prices"
SILVER_DIR = ROOT / "Data" / "Silver" / "prices"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)

logger = get_logger("etl.fetch_price_history")

YEARS = 5
END_DATE = datetime.date.today()
START_DATE = END_DATE - datetime.timedelta(days=365 * YEARS)
BATCH_SIZE = 25   # chunk tickers to avoid rate limits


def read_universe() -> List[str]:
    if not UNIVERSE_FILE.exists():
        raise FileNotFoundError(
            f"Universe file not found: {UNIVERSE_FILE}\n"
            "Create it with one yfinance ticker per line (e.g. RELIANCE.NS) or run a symbol grabber."
        )
    with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
        ticks = [l.strip() for l in f.readlines() if l.strip() and not l.startswith("#")]
    return ticks


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30), retry=retry_if_exception_type(Exception))
def download_yf(tickers: List[str], start: datetime.date, end: datetime.date) -> dict:
    """
    Uses yfinance.download in batch mode. Returns dict symbol -> DataFrame
    Retries automatically on network errors.
    """
    logger.info("Downloading tickers batch: %s", ", ".join(tickers))
    # yfinance accepts a space-separated list
    joined = " ".join(tickers)
    df = yf.download(joined, start=start.isoformat(), end=(end + datetime.timedelta(days=1)).isoformat(), group_by="ticker", threads=True, auto_adjust=False, progress=False)
    results = {}
    if isinstance(df.columns, pd.MultiIndex):
        # multi-ticker
        for t in tickers:
            try:
                sub = df[t].copy()
                # ensure standard column names
                sub.columns = [c if isinstance(c, str) else str(c) for c in sub.columns]
                results[t] = sub
            except Exception as e:
                logger.warning("Failed to parse %s from batch: %s", t, e)
                results[t] = pd.DataFrame()
    else:
        # single ticker
        results[tickers[0]] = df.copy()
    return results


def df_to_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    # store with pyarrow for efficiency
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=True)
    logger.debug("Wrote parquet %s (%d rows)", path, len(df))


def insert_into_db(session, company_id: int, df: pd.DataFrame) -> int:
    """
    Inserts rows from df (index = date, columns: Open, High, Low, Close, Adj Close, Volume)
    Only inserts rows not already present; returns number of inserted rows
    """
    existing = get_existing_dates(session, company_id)
    inserted = 0
    to_insert = []
    for dt, row in df.iterrows():
        dstr = dt.date().isoformat()
        if dstr in existing:
            continue
        ph = PriceHistory(
            company_id=company_id,
            trade_date=dt.date(),
            open=float(row.get("Open") if "Open" in row else row.get("open", None)),
            high=float(row.get("High") if "High" in row else row.get("high", None)),
            low=float(row.get("Low") if "Low" in row else row.get("low", None)),
            close=float(row.get("Close") if "Close" in row else row.get("close", None)),
            adj_close=float(row.get("Adj Close") if "Adj Close" in row else row.get("adj_close", None)),
            volume=int(row.get("Volume") if "Volume" in row else row.get("volume", 0)),
        )
        to_insert.append(ph)
        inserted += 1

    if to_insert:
        session.add_all(to_insert)
        session.commit()
    return inserted


def process_batch(session, tickers: List[str], mapping: dict):
    results = download_yf(tickers, START_DATE, END_DATE)
    total_inserted = 0
    for t in tickers:
        df = results.get(t, pd.DataFrame())
        if df is None or df.empty:
            logger.warning("No data for %s", t)
            continue

        # Standardize column names (yfinance can return different caps)
        df = df.rename(columns=lambda c: c if isinstance(c, str) else str(c))
        # keep known columns
        keep_cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in df.columns]
        df = df[keep_cols].copy()

        # validate & clean
        df_clean = validate_ohlcv(df)
        if df_clean.empty:
            logger.warning("After validation data empty for %s", t)
            continue

        # Write bronze
        bronze_path = BRONZE_DIR / f"{t}.parquet"
        df_to_parquet(df, bronze_path)

        # Write silver (cleaned)
        silver_path = SILVER_DIR / f"{t}.parquet"
        df_to_parquet(df_clean, silver_path)

        # Insert into DB using mapping
        company_id = mapping.get(t)
        if not company_id:
            logger.error("No company id for %s", t)
            continue

        try:
            inserted = insert_into_db(session, company_id, df_clean)
            logger.info("Inserted %d rows for %s into DB", inserted, t)
            total_inserted += inserted
        except SQLAlchemyError as e:
            session.rollback()
            logger.exception("DB insert error for %s: %s", t, e)

    return total_inserted


def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def main():
    started_at = datetime.datetime.utcnow()
    session = get_session()
    logger.info("Starting fetch_price_history ETL. Universe file: %s", UNIVERSE_FILE)
    try:
        tickers = read_universe()
        logger.info("Read %d tickers from universe file", len(tickers))

        # Prepare companies (ensure rows exist)
        symbols_payload = [{"symbol": t, "exchange": "NSE"} for t in tickers]
        mapping = ensure_companies(session, symbols_payload)
        logger.info("Company mapping created for %d companies", len(mapping))

        total_rows = 0
        for batch in chunk_list(tickers, BATCH_SIZE):
            logger.info("Processing batch of %d tickers", len(batch))
            inserted = process_batch(session, batch, mapping)
            total_rows += inserted
            # polite pause
            time.sleep(2)

        log_etl_run(session, pipeline_name="fetch_price_history", status="SUCCESS", rows_processed=total_rows, started_at=started_at, ended_at=datetime.datetime.utcnow())
        logger.info("ETL finished. Total rows inserted: %d", total_rows)
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        try:
            log_etl_run(session, pipeline_name="fetch_price_history", status="FAILED", error_message=str(e), started_at=started_at, ended_at=datetime.datetime.utcnow())
        except Exception:
            logger.exception("Failed to write etl_runs record")
    finally:
        session.close()


if __name__ == "__main__":
    main()
