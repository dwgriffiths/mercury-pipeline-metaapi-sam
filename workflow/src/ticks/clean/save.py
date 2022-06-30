import awswrangler as wr
from datetime import datetime, timedelta, date

from src.config import *
from src.io import *
from src.utils import * 
from src.ticks.raw.io import *
from src.ticks.clean.io import *
from src.ticks.clean.get import get_clean_ticks

def setup_save_clean_ticks(
    overwrite: bool = None,
    batch_size: int = None,
    prev: list = None,
    **kwargs
):
    overwrite = False if overwrite is None else overwrite
    batch_size = 48 if batch_size is None else max(min(batch_size, 48), 1)
    overwrite = overwrite if prev is None else False
    
    keys_raw_ticks = get_matching_keys_raw_ticks()
    keys_clean_ticks_saved = get_matching_keys_clean_ticks()
    
    if overwrite:
        wr.s3.delete_objects(BUCKET, keys_clean_ticks_saved)
        keys_clean_ticks_saved = []
    prefixes_clean_ticks_saved = set(get_prefixes_from_keys(keys_clean_ticks_saved))

    items = []
    for key_raw_ticks in keys_raw_ticks:
        if key_raw_ticks.endswith("_0.json"):
            continue
        prefix_clean_ticks = convert_key_raw_ticks_to_clean_ticks(
            key_raw_ticks
        )
        if prefix_clean_ticks in prefixes_clean_ticks_saved:
            continue
        items.append({
            "key_raw_ticks": key_raw_ticks,
            "datetimestr": key_raw_ticks.split("_")[-2]
        })
    items = sorted(items, key=lambda x: x.get("datetimestr"), reverse=True)
    return batch_items(items, batch_size)

def save_clean_ticks(
    key_raw_ticks: str,
    **kwargs
):
    # Delete any objects which are already there.
    prefix_clean_ticks = convert_key_raw_ticks_to_clean_ticks(
        key_raw_ticks
    )
    wr.s3.delete_objects(
        get_matching_keys_clean_ticks(prefix_clean_ticks)
    )

    # Load raw ticks
    data = load_json_data(key_raw_ticks)
    
    # Convert raw ticks to clean ticks
    df_clean_ticks = get_clean_ticks(data)
    
    #Save as parquet
    wr.s3.to_parquet(
        df_clean_ticks,
        dataset=True,
        path=f"s3://{BUCKET}/{DIR_CLEAN_TICKS}/",
        partition_cols=[
            "symbol",
            "frequency",
            "year",
            "month",
            "date",
            "hour",
        ]
    )
    return True