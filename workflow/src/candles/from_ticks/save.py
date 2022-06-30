from datetime import datetime, timedelta, date
import pandas as pd

from src.config import *
from src.io import *
from src.utils import * 
from src.ticks.clean.io import *
from src.candles.io import *
from src.candles.from_ticks.get import get_ticks_to_candles
    
def setup_save_ticks_to_candles(
    overwrite: bool,
    batch_size: int
):
    overwrite = False if overwrite is None else overwrite
    batch_size = 48 if batch_size is None else max(min(batch_size, 48), 1)
    name_dataset = "from_ticks"
    
    keys_clean_ticks = get_matching_keys_clean_ticks()
    keys_candles_saved = get_matching_keys_candles(name_dataset)
    
    if overwrite:
        delete_objects(BUCKET, keys_candles_saved)
        keys_candles_saved = []
    prefixes_clean_ticks = get_prefixes_from_keys(keys_clean_ticks)
    prefixes_candles_saved = set(get_prefixes_from_keys(keys_candles_saved))
        
    items = []
    for prefix_clean_ticks in prefixes_clean_ticks:
        parameters = get_parameters_from_key(prefix_clean_ticks)
        item = {
            "prefix_clean_ticks": prefix_clean_ticks,
            "frequencies": [],
            "name_dataset": name_dataset,
            "datetimestr": parameters["date"] + parameters["hour"]
        }
        for frequency in FREQUENCIES:
            prefix_candles = convert_prefix_clean_ticks_to_prefix_candles(
                prefix_clean_ticks,
                frequency,
                name_dataset
            )
            if prefix_candles in prefixes_candles_saved:
                continue
            item["frequencies"].append(frequency)
        if item["frequencies"]:
            items.append(item)
    items = sorted(items, key=lambda x: x.get("datetimestr"), reverse=True)
    return batch_items(items, batch_size)

def save_ticks_to_candles(
    prefix_clean_ticks: str,
    frequencies: list,
    name_dataset: str
):
    for frequency in frequencies:
        
        # Delete any objects which are already there.
        prefix_candles = convert_prefix_clean_ticks_to_prefix_candles(
            prefix_clean_ticks
        )
        delete_objects(
            get_matching_keys_clean_ticks(prefix_candles)
        )
    
        # Load clean ticks
        df_clean_ticks = pd.read_parquet(
            f"s3://{BUCKET}/{prefix_clean_ticks}",
        )

        # Candlise ticks
        df_candles = get_ticks_to_candles(df_clean_ticks, frequency)
        
        # Make sure the partition_cols are in there
        df_candles["frequency"] = frequency
        filters = get_parameters_from_key(prefix_clean_ticks)
        for k, v in filters.items():
            df_candles[k] = v
            
        #Save as parquet
        df_candles.to_parquet(
            f"s3://{BUCKET}/{DIR_CANDLES_ROOT}/{name_dataset}/",
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
