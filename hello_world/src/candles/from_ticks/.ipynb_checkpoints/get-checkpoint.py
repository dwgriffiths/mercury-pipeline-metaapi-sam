from datetime import datetime, timedelta, date, time
import numpy as np
import pandas as pd
import pytz

from src.config import *

# Helper functions
def idxmin(s):
    if len(s) == 0:
        return 0
    return np.argmin(s)

def idxmax(s):
    if len(s) == 0:
        return 0
    return np.argmax(s)

def get_ticks_to_candles(
    df_ticks: pd.DataFrame,
    frequency: str
):
    # Resample
    d_agg = {
        "ask": ["first", "last", "min", "max", idxmin, idxmax, "count"],
        "bid": ["first", "last", "min", "max", idxmin, idxmax],
    }
    df_candles = (
        df_ticks
        .resample(frequency, on="timestamp_utc")
        .agg(d_agg)
    )

    # So we know whether the low or the high came first
    for col in ["ask", "bid"]:
        df_candles[(col, "idx_hi_lo")] = "eq"
        mask = df_candles[(col, "idxmin")] < df_candles[(col, "idxmax")]
        df_candles.loc[mask, (col, "idx_hi_lo")] = "lo"
        mask = df_candles[(col, "idxmin")] > df_candles[(col, "idxmax")]
        df_candles.loc[mask, (col, "idx_hi_lo")] = "hi"
        df_candles = df_candles.drop(columns=[(col, "idxmin"), (col, "idxmax")])

    # Rename columns
    df_candles.columns = ['.'.join(col) if isinstance(col, tuple) else col for col in df_candles.columns.values]
    df_candles = (
        df_candles
        .rename(columns={c:c.replace("first", "open") for c in df_candles.columns})
        .rename(columns={c:c.replace("last", "close") for c in df_candles.columns})
        .rename(columns={c:c.replace("min", "low") for c in df_candles.columns})
        .rename(columns={c:c.replace("max", "high") for c in df_candles.columns})
        .reset_index()
        .rename(columns={"timestamp_utc": "timestamp_utc.open"})
        .rename(columns={"ask.count": "n_ticks"})
    )

    # Get time variables
    df_candles["timestamp_utc.close"] = df_candles["timestamp_utc.open"] + timedelta(seconds=FREQUENCIES[frequency]["seconds"])
    for c in ["open", "close"]:
        df_candles[f"timestamp_uk.{c}"] = (
            df_candles[f"timestamp_utc.{c}"]
            .dt.tz_localize("UTC")
            .dt.tz_convert("Europe/London")
            .dt.tz_localize(None)
        )

    return df_candles