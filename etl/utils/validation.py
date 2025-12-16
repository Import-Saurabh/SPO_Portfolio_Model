import pandas as pd
import numpy as np

def validate_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic validation and cleaning for raw OHLCV dataframe from yfinance.
    - Drop rows with all NaNs
    - Remove negative or zero prices
    - Fill small gaps via forward-fill for prices, volume set to 0 if missing
    Returns cleaned df
    """
    df = df.copy()
    df = df.dropna(how="all")
    # ensure index is date
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()]

    # Remove non-positive prices
    price_cols = ["Open", "High", "Low", "Close", "Adj Close"]
    for c in price_cols:
        if c in df.columns:
            df.loc[df[c] <= 0, c] = pd.NA

    # forward fill small missing values
    df[["Open", "High", "Low", "Close", "Adj Close"]] = df[["Open", "High", "Low", "Close", "Adj Close"]].ffill()
    # volume -> fill 0
    if "Volume" in df.columns:
        df["Volume"] = df["Volume"].fillna(0).astype("int64")
    # drop remaining rows missing Close
    df = df.dropna(subset=["Close", "Adj Close"], how="any")
    return df
