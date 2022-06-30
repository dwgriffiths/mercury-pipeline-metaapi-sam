import awswrangler as wr
from datetime import datetime

from src.config import *
from src.io import *
from src.utils import *

def get_prefix_raw_ticks(
    symbol: str,
    datetimestr_from: str
):
    return f"s3://{BUCKET}/{DIR_RAW_TICKS}/{symbol}/ticks_{datetimestr_from}_"

def get_matching_keys_raw_ticks():
    path = f"s3://{BUCKET}/{DIR_RAW_TICKS}"
    return wr.s3.list_objects(path)

def get_matching_prefixes_raw_ticks():
    keys_raw_ticks = get_matching_keys_raw_ticks()
    return [
        "_".join(x.split("_")[:-1]) + "_" for x in keys_raw_ticks
    ]

def save_raw_ticks_to_json(data: dict):
    n_ticks = data["n"]
    key = f"{DIR_RAW_TICKS}/{symbol}/ticks_{datetimestr_from}_{n_ticks}.json"
    save_json_data(BUCKET, key, data)
