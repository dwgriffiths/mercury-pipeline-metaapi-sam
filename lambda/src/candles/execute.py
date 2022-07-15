from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import pytz

from src.config import *
from src.io import *
from src.batch import process_batch
import src.candles.transformations as transformations

def save_missing_candles(
    dynamo_table: str,
    batch_id: str,
):
    return process_batch(
        save_candles,
        dynamo_table,
        batch_id,
        is_async=False
    )

def save_candles(
    prefix_in: str,
    prefix_lookback: str,
    name_dataset_out: str,
    transformations: list = [],
    *args,
    **kwargs
):
    name_dataset_in = prefix_in.split("/candles/")[-1].split("/")[0]
    prefix_out = prefix_in.replace(name_dataset_in, name_dataset_out)

    # Load candles
    parameters_in = get_parameters_from_key(prefix_in)
    parameters_lookback = get_parameters_from_key(prefix_lookback)
    datetime_in = datetime.strptime(
        parameters_in["date"] + parameters_in["hour"], 
        "%Y%m%d%H"
    )
    datetime_lookback = datetime.strptime(
        parameters_lookback["date"] + parameters_lookback["hour"], 
        "%Y%m%d%H"
    )
    n_hours = int((datetime_in - datetime_lookback).total_seconds() / 3600) + 1
    hours = [datetime_lookback + timedelta(hours=i) for i in range(n_hours)]
    
    keys = [
        "/".join([
            "s3:/",
            BUCKET,
            DIR_CANDLES_ROOT,
            name_dataset_in,
            f"symbol={parameters_in['symbol']}",
            f"frequency={parameters_in['frequency']}",
            f"year={dt.strftime('%Y')}",
            f"month={dt.strftime('%m')}",
            f"date={dt.strftime('%Y%m%d')}",
            f"hour={dt.strftime('%H')}",
            ""
        ]) for dt in hours
    ]
           
    # Load dataset
    l = []
    for key in keys:
        if wr.s3.list_objects(key) != []:
            l.append(wr.s3.read_parquet(key, dataset=True))
    df_in = pd.concat(l)
                             
    # Transform candles
    df_out = df_in.copy()
    for transformation in transformations:
        function = getattr(transformations, transformation)     
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
        ],
        mode="overwrite_partitions"
    )
    return True
