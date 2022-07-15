from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import pytz

from src.config import *
from src.io import *
from src.batch import process_batch

def save_missing_candles_from_ticks(
    dynamo_table: str,
    batch_id: str,
):
    return process_batch(
        save_candles_from_ticks,
        dynamo_table,
        batch_id,
        is_async=False
    )

def save_candles_from_ticks(
    prefix_clean_ticks: str,
    frequencies: list,
    *args,
    **kwargs
):
    for frequency in frequencies:

        # Load clean ticks
        df_clean_ticks = wr.s3.read_parquet(
            path=prefix_clean_ticks,
            dataset=True,
            path_suffix=".parquet",
        )

        # Candlise ticks
        df_candles = get_candles_from_ticks(df_clean_ticks, frequency)
        
        # Make sure the partition_cols are in there
        filters = get_parameters_from_key(prefix_clean_ticks)
        for k, v in filters.items():
            df_candles[k] = v
        df_candles["frequency"] = frequency
            
        path = "/".join([
            "s3:/",
            BUCKET,
            DIR_CANDLES_ROOT,
            "from_ticks",
            ""
        ])
            
        #Save as parquet
        wr.s3.to_parquet(
            df_candles,
            path=path,
            dataset=True,
            partition_cols=[
                "symbol",
                "frequency",
                "year",
                "month",
                "date",
                "hour",
            ],
            mode="overwrite_partitions"
        )
    return True

# Helper functions
def idxmin(s):
    if len(s) == 0:
        return 0
    return np.argmin(s)

def idxmax(s):
    if len(s) == 0:
        return 0
    return np.argmax(s)

def get_candles_from_ticks(
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