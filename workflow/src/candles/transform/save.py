import awswrangler as wr
from datetime import datetime, timedelta, date
import pandas as pd

from src.config import *
from src.io import *
from src.utils import * 
from src.candles.io import *

def ffill(
    df_candles: pd.DataFrame
):
    df_out = df_candles.copy()
    cols = ["ask.close", "bid.close"]
    df_out[cols] = (
        df_out[cols]
        .replace(0., pd.NA)
        .fillna(method="ffill")
    )
    for stem in ["ask", "bid"]:
        cols = [f"{stem}.{col}" for col in ["open", "close", "high", "low"]]
        df_out[cols] = (
            df_out[cols]
            .fillna(method="ffill", axis=1)
            .fillna(method="bfill", axis=1)
        )
    return df_out

FUNCTIONS = {
    "ffill": {
        "function": ffill,
    },
}

def setup_save_candles_transformed(
    name_dataset_in: str,
    name_dataset_out: str,
    function_names: list,
    overwrite: bool = None,
    batch_size: int = None,
    SaveCandlesTransformed: dict = None,
    lookback_rows: int = None,
    **kwargs
):
    overwrite = False if overwrite is None else overwrite
    batch_size = 48 if batch_size is None else max(min(batch_size, 48), 1)
    overwrite = overwrite if SaveCandlesTransformed is None or SaveCandlesTransformed.get(name_dataset_out) is None else False
    lookback_rows = 60 if lookback_rows is None else max(min(lookback_rows, 60), 1)
    assert all([f in FUNCTIONS for f in function_names])
    
    prefixes_in = get_matching_prefixes_candles_by_frequency(name_dataset_in)
    keys_out_saved = get_matching_keys_candles(name_dataset_out)
    
    if overwrite:
        wr.s3.delete_objects(keys_out_saved)
        keys_out_saved = []
    prefixes_out_saved = set(get_prefixes_from_keys(keys_out_saved))

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
                prefix_out = prefix.replace(name_dataset_in, name_dataset_out)
                if prefix_out in prefixes_out_saved:
                    continue
                parameters = get_parameters_from_key(prefix)
                items.append({
                    "prefix_in": prefix,
                    "prefix_lookback": prefix_lookback,
                    "name_dataset_out":name_dataset_out,
                    "function_names":function_names,
                    "datetimestr": parameters["date"] + parameters["hour"]
                })
    items = sorted(items, key=lambda x: x.get("datetimestr"), reverse=True)
    return batch_items(items, batch_size)

def save_candles_transformed(
    prefix_in: str,
    prefix_lookback: str,
    name_dataset_out: str,
    function_names: list = [],
    **kwargs
):
    name_dataset_in = prefix_in.split("/candles/")[-1].split("/")[0]
    prefix_out = prefix_in.replace(name_dataset_in, name_dataset_out)
    assert all([f in FUNCTIONS for f in function_names])
    
    # Delete any objects which are already there.
    wr.s3.delete_objects(
        get_matching_keys_candles(prefix_out)
    )

    # Load candles
    parameters_in = get_parameters_from_key(prefix_in)
    parameters_lookback = get_parameters_from_key(prefix_lookback)
    parameters_filter = {}
    for key in parameters_in:
        value_in, value_lookback = parameters_in[key], parameters_lookback[key]
        parameters_filter[f"{key}_min"] = min(value_in, value_lookback)
        parameters_filter[f"{key}_max"] = max(value_in, value_lookback)

    f_filter = lambda x: True if all([
        all([
            x[key] >= parameters_filter[f"{key}_min"],
            x[key] <= parameters_filter[f"{key}_max"],
        ]) for key in parameters_in
    ]) else False
                             
    # Load dataset
    df_in = wr.s3.read_parquet(
        path=f"s3://{BUCKET}/{DIR_CANDLES_ROOT}/{name_dataset_in}/",
        dataset=True,
        path_suffix=".parquet",
        partition_filter=f_filter
    )
                             
    # Transform candles
    df_out = df_in.copy()
    for function_name in function_names:
        function = FUNCTIONS[function_name]["function"]        
        df_out = function(df_out)
        
    # Filter transformed df to only get new rows
    mask = True
    for key, value in parameters_in.items():
        mask = mask & (df_out[key] == value)
    df_out = df_out[mask].copy()

    #Save as parquet
    wr.s3.to_parquet(
        df_out,
        path=f"s3://{BUCKET}/{DIR_CANDLES_ROOT}/{name_dataset_out}/",
        dataset=True,
        partition_cols=[
            "symbol",
            "frequency",
            "year",
            "month",
            "date",
            "hour",
        ]
    )
    