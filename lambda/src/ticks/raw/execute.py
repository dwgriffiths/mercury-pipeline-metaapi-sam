from datetime import datetime
from metaapi_cloud_sdk import MetaApi
import pytz

from src.config import *
from src.io import *
from src.access import get_demo_account
from src.batch import process_batch

def save_missing_raw_ticks(
    dynamo_table: str,
    batch_id: str,
):
    return process_batch(
        save_raw_ticks,
        dynamo_table,
        batch_id,
        is_async=True
    )

async def save_raw_ticks(
    symbol: str,
    datetimestr_from: str,
    datetimestr_to: str,
    *args,
    **kwargs
):
    prefix = get_prefix_raw_ticks(symbol, datetimestr_from)
    if wr.s3.does_object_exist(f"s3://{BUCKET}/{prefix}"):
        return False
    
    # Get ticks
    data = await get_raw_ticks(
        symbol,
        datetimestr_from,
        datetimestr_to
    )
    
    # Save as json
    save_raw_ticks_to_json(data)
    
    # Delete 
    return True

async def get_raw_ticks(
    symbol: str,
    datetimestr_from: str,
    datetimestr_to: str
):
    # Get demo account
    account = await get_demo_account()
    
    # Convert datetimes from string to datetime
    datetimeutc_from = pytz.timezone("UTC").localize(datetime.strptime(datetimestr_from, "%Y%m%d%H%M%S%f"))
    datetimeutc_to = pytz.timezone("UTC").localize(datetime.strptime(datetimestr_to, "%Y%m%d%H%M%S%f"))
    
    # Get the ticks
    l_ticks = await get_raw_ticks_between_datetimes(
        account,
        symbol,
        datetimeutc_from,
        datetimeutc_to
    )
    
    data = {
        "symbol":symbol,
        "datetimeutc_from": datetimeutc_from.strftime("%Y%m%d%H%M%S%f"),
        "datetimeutc_to": datetimeutc_to.strftime("%Y%m%d%H%M%S%f"),
        "datetimeutc_first": l_ticks[0]["time"].strftime("%Y%m%d%H%M%S%f") if l_ticks else None,
        "datetimeutc_last": l_ticks[-1]["time"].strftime("%Y%m%d%H%M%S%f") if l_ticks else None,
        "n":len(l_ticks),
        "ticks":l_ticks
    }
    
    return data

async def get_raw_ticks_between_datetimes(
    account: object,
    symbol: str,
    datetimeutc_from: datetime,
    datetimeutc_to: datetime
):
    offset = 0
    l_ticks = []
    while True:
        l_ticks_i = await account.get_historical_ticks(
            symbol,
            start_time=datetimeutc_from,
            limit=1000,
            offset=offset,
        )
        if len(l_ticks_i) == 0:
            break
        datetimeutc_last = l_ticks_i[-1]["time"]
        l_ticks.extend(l_ticks_i)
        offset += len(l_ticks_i)
        if datetimeutc_last >= datetimeutc_to:
            break
        
    i = len(l_ticks) - 1
    while i >= 0 and l_ticks[i]["time"] >= datetimeutc_to:
        i -= 1
    l_ticks = l_ticks[:i+1]
    return l_ticks
