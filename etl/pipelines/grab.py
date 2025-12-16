"""
NIFTY 500 symbol grabber
------------------------
Fetches NIFTY 500 constituents from NSE India (official source),
cleans symbol names, converts them into Yahoo Finance tickers (APPEND .NS),
and writes them into: data/universe/nifty500.txt

Usage:
    python etl/pipelines/grab_nifty500.py
"""

import os
import json
import time
import requests
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

# Output directory
UNIVERSE_DIR = Path("data/universe")
UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = UNIVERSE_DIR / "nifty500.txt"

NSE_URL = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
}


@retry(stop=stop_after_attempt(6), wait=wait_exponential(min=1, max=10))
def fetch_nse_json():
    """
    NSE blocks bots heavily; this retry wrapper + cookie session bypasses it.
    """
    session = requests.Session()
    # First call to homepage -> required to get cookies
    session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
    time.sleep(1)

    r = session.get(NSE_URL, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def convert_to_yf(symbol: str) -> str:
    """Convert NSE symbol into yfinance ticker (adds .NS)."""
    symbol = symbol.strip().upper()
    return f"{symbol}.NS"


def save_universe(symbols):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for sym in symbols:
            f.write(sym + "\n")


def main():
    print("Fetching NIFTY 500 constituents from NSE...")
    data = fetch_nse_json()

    if "data" not in data:
        raise ValueError("Unexpected NSE response format")

    stocks = data["data"]

    print(f"Fetched {len(stocks)} records from NSE")

    yf_tickers = []
    for entry in stocks:
        sym = entry.get("symbol")
        if not sym:
            continue
        yf_sym = convert_to_yf(sym)
        yf_tickers.append(yf_sym)

    yf_tickers = sorted(list(set(yf_tickers)))  # unique + sorted

    save_universe(yf_tickers)

    print(f"Successfully wrote {len(yf_tickers)} symbols → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
