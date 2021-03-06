from datetime import datetime
import pandas as pd
import pytz
from typing import Union

from src.config import *
from src.io import *
from src.batch import process_batch

def save_missing_clean_ticks(
    dynamo_table: str,
    batch_id: str,
):
    return process_batch(
        save_clean_ticks,
        dynamo_table,
        batch_id,
        is_async=False
    )

def save_clean_ticks(
    key_raw_ticks: str,
    *args,
    **kwargs
):
    # Load raw ticks
    data = load_json_data(key_raw_ticks)
    
    # Convert raw ticks to clean ticks
    df_clean_ticks = get_clean_ticks(data)
    
    path = "/".join([
        "s3:/",
        BUCKET,
        DIR_CLEAN_TICKS,
        ""
    ])
    
    #Save as parquet
    wr.s3.to_parquet(
        df_clean_ticks,
        dataset=True,
        path=path,
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

columns = [
    "symbol",
    "frequency",
    "year",
    "month",
    "date",
    "hour",
    "timestamp_utc",
    "timestamp_uk",
    "timestamp_broker",
    "bid",
    "ask",
    "last",
    "volume",
]

def get_clean_ticks(
    data: Union[dict, list]
):
    if isinstance(data, list):
        df_ticks = pd.DataFrame(data)
        df_ticks["timestamp_utc"] = df_ticks["time"]
        df_ticks["timestamp_broker"] = df_ticks["brokerTime"]
        df_ticks["timestamp_uk"] = df_ticks["timestamp_utc"].dt.tz_convert("Europe/London").dt.tz_localize(None)
        df_ticks["timestamp_utc"] = df_ticks["timestamp_utc"].dt.tz_localize(None)
    else:
        df_ticks = pd.DataFrame(data["ticks"])
        df_ticks["timestamp_utc"] = pd.to_datetime(df_ticks["time"].str[:17], format="%Y%m%d%H%M%S%f")
        df_ticks["timestamp_broker"] = pd.to_datetime(df_ticks["brokerTime"], format="%Y-%m-%d %H:%M:%S.%f")
        df_ticks["timestamp_uk"] = df_ticks["timestamp_utc"].dt.tz_localize("UTC").dt.tz_convert("Europe/London").dt.tz_localize(None)
    df_ticks[["ask", "bid"]] = df_ticks[["ask", "bid"]].replace(0.0, pd.NA)
    df_ticks["hour"] = df_ticks["timestamp_utc"].dt.strftime("%H")
    df_ticks["date"] = df_ticks["timestamp_utc"].dt.strftime("%Y%m%d")
    df_ticks["month"] = df_ticks["date"].str[4:6]
    df_ticks["year"] = df_ticks["date"].str[:4]
    df_ticks = df_ticks.drop(columns=["time", "brokerTime"])
    df_ticks["frequency"] = "tick"
    for col in ["last", "volume"]:
        if col not in df_ticks.columns:
            df_ticks[col] = pd.NA
    return df_ticks[columns]