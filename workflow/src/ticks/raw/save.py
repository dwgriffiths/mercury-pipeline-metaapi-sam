import awswrangler as wr
from datetime import datetime, timedelta, date

from src.config import *
from src.io import *
from src.utils import *
from src.ticks.raw.io import *
from src.ticks.raw.get import get_raw_ticks

def setup_save_raw_ticks(
    stop: bool = None,
    batch_size: int = None,
    **kwargs
):
    stop = True if stop is None else stop
    batch_size = 12 if batch_size is None else max(min(batch_size, 12), 1)

    item_ids = (
        wr.dynamodb
        .get_table(TABLE_PIPELINE)
        .scan(AttributesToGet=['id',])["Items"]
    )
    wr.dynamodb.delete_items(item_ids, TABLE_PIPELINE)

    datetimesutc_from, datetimesutc_to = get_datetime_boundaries(
        DATETIMEUTC_FROM,
        DATETIMEUTC_TO
    )
    prefixes_raw_ticks_saved = get_matching_prefixes_raw_ticks()
    
    items = []
    for symbol in SYMBOLS:
        for d_from, d_to in zip(
            datetimesutc_from,
            datetimesutc_to
        ):
            dstr_from = d_from.strftime(DATETIME_FORMAT)
            dstr_to = d_to.strftime(DATETIME_FORMAT)
            prefix_raw_ticks = get_prefix_raw_ticks(symbol, dstr_from)
            if prefix_raw_ticks in prefixes_raw_ticks_saved and stop:
                break
            if prefix_raw_ticks in prefixes_raw_ticks_saved:
                continue
            items.append({
                "symbol": symbol,
                "datetimestr_from": dstr_from,
                "datetimestr_to": dstr_to,
            })
    items = sorted(items, key=lambda x: x.get("datetimestr_from"), reverse=True)
    batches = batch_items(items, batch_size)
    wr.dynamodb.put_items(batches, TABLE_PIPELINE)
    batch_ids = [x["id"] for x in batches]
    return batch_ids
    
async def save_raw_ticks(
    symbol: str,
    datetimestr_from: str,
    datetimestr_to: str,
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
    
    