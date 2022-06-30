import awswrangler as wr
from datetime import datetime, timedelta, date

from src.config import *
from src.io import *
from src.utils import *

def get_matching_keys_clean_ticks(key: str = DIR_CLEAN_TICKS):
    path = f"s3://{BUCKET}/{key}"
    return wr.s3.list_objects(path)

def convert_key_raw_ticks_to_clean_ticks(
    key_raw_ticks: str
):
    symbol = key_raw_ticks.split("/ticks_")[0].split("/")[-1]
    datetimestr = key_raw_ticks.split("ticks_")[-1].split("_")[0]
    datetimeutc = datetime.strptime(datetimestr, "%Y%m%d%H%M%S%f")
    year = datetimeutc.strftime("%Y")
    month = datetimeutc.strftime("%m")
    date = datetimeutc.strftime("%Y%m%d")
    hour = datetimeutc.strftime("%H")
    return "/".join([
        "s3:/",
        BUCKET,
        DIR_CLEAN_TICKS,
        f"symbol={symbol}",
        f"frequency=tick",
        f"year={year}",
        f"month={month}",
        f"date={date}",
        f"hour={hour}",
        ""
    ])
