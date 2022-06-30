from datetime import datetime, timedelta, date

from src.config import *
from src.io import *
from src.utils import * 

def get_matching_keys_candles(
    prefix: str
):
    return list(get_matching_s3_keys(
        bucket=BUCKET,
        prefix=f"{DIR_CANDLES_ROOT}/{prefix}",
        suffix=".parquet"
    ))

def get_matching_prefixes_candles_by_frequency(name_dataset: str):
    prefixes = {}
    for symbol in SYMBOLS:
        prefixes[symbol] = {}
        for frequency in FREQUENCIES:
            prefix = f"{DIR_CANDLES_ROOT}/{name_dataset}/symbol={symbol}/frequency={frequency}/"
            prefixes[symbol][frequency] = sorted(
                get_prefixes_from_keys(
                    list(
                        get_matching_s3_keys(
                            bucket=BUCKET,
                            prefix=prefix,
                            suffix=".parquet"
                        )
                    )
                )
            )
    return prefixes

def convert_prefix_clean_ticks_to_prefix_candles(
    prefix_clean_ticks: str,
    frequency: str,
    name_dataset: str
):
    return (
        prefix_clean_ticks
        .replace("/ticks/", f"/candles/{name_dataset}/")
        .replace("/frequency=tick/", f"/frequency={frequency}/")
    )