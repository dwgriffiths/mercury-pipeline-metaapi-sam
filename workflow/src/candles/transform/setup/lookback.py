from datetime import datetime, timedelta, date

from src.config import *
from src.io import *
from src.utils import * 
from src.candles.io import *

def setup_candles_lookback(
    name_dataset_in: str,
    name_dataset_out: str,
    overwrite: bool,
    batch_size: int,
    lookback_rows: int
):
    overwrite = False if overwrite is None else overwrite
    batch_size = 48 if batch_size is None else max(min(batch_size, 48), 1)
    lookback_rows = 60 if lookback_rows is None else max(min(lookback_rows, 60), 1)
    
    prefixes_in = get_matching_prefixes_candles_by_frequency(name_dataset_in)
    keys_out_saved = get_matching_keys_candles(name_dataset_out)
    
    if overwrite:
        delete_objects(BUCKET, keys_out_saved)
        keys_out_saved = []
    prefixes_out = set(get_prefixes_from_keys(keys_out_saved))
        
    items = []
    for symbol in SYMBOLS:
        for frequency in FREQUENCIES:
            lookback_seconds = lookback_rows*FREQUENCIES[frequency]["seconds"]
            lookback_files = int(lookback_seconds / 3600) + int(lookback_seconds % 3600 != 0)

            prefixes = prefixes_in[symbol][frequency]
            for prefix, prefix_lookback in zip(
                prefixes[lookback_files:],
                prefixes[:-lookback_files]
            ):
                if prefix in prefixes_out:
                    continue
                parameters = get_parameters_from_key(prefix)
                items.append({
                    "prefix_in": prefix,
                    "prefix_lookback": prefix_lookback,
                    "name_dataset_out":name_dataset_out,
                    "datetimestr": parameters["date"] + parameters["hour"]
                })
    items = sorted(items, key=lambda x: x.get("datetimestr"), reverse=True)
    return batch_items(items, batch_size)