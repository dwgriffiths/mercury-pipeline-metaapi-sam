import awswrangler as wr
from datetime import datetime, timedelta

from src.config import *
from src.io import *
from src.batch import *
from src.time import get_datetime_boundaries_in_month

def find_raw_ticks_to_get(
    month: str,
    prefixes_saved: list = []
):
    (
        datetimesutc_from,
        datetimesutc_to
    ) = get_datetime_boundaries_in_month(month)
    items = []
    for symbol in SYMBOLS:
        for d_from, d_to in zip(
            datetimesutc_from,
            datetimesutc_to
        ):
            dstr_from = d_from.strftime(DATETIME_FORMAT)
            dstr_to = d_to.strftime(DATETIME_FORMAT)
            prefix = get_prefix_raw_ticks(symbol, dstr_from)
            if prefix in prefixes_saved:
                continue
            items.append({
                "symbol": symbol,
                "datetimestr_from": dstr_from,
                "datetimestr_to": dstr_to,
            })
    items = sorted(items, key=lambda x: x.get("datetimestr_from"), reverse=True)
    return items
    
def find_saved_raw_ticks(
    month: str,
    prefix: bool=True
):
    items = []
    for symbol in SYMBOLS:
        path = f"s3://{BUCKET}/{DIR_RAW_TICKS}/{symbol}/ticks_{month}"
        keys_symbol = wr.s3.list_objects(path, suffix=".json")
        if prefix:
            prefixes_symbol = [
                "_".join(x.split("_")[:-1]) + "_" for x in keys_symbol
            ]
            items.extend(prefixes_symbol)
        else:
            items.extend(keys_symbol)
    return items

def find_missing_raw_ticks(
    month: str,
    dynamo_table: str,
    batch_size: int = 12,
):
    prefixes_saved_ticks = find_saved_raw_ticks(
        month
    )
    prefixes_ticks_to_get = find_raw_ticks_to_get(
        month,
        prefixes_saved_ticks
    )
    batches = batch_items(
        prefixes_ticks_to_get,
        batch_size,
        month
    )

    # Clear DynamoDB table
    item_ids = (
        wr.dynamodb
        .get_table(dynamo_table)
        .scan(AttributesToGet=["id",])["Items"]
    )
    item_ids = [i for i in item_ids if i["id"].startswith(month)]
    wr.dynamodb.delete_items(item_ids, dynamo_table)
    
    # Add batches to DynamoDB table
    wr.dynamodb.put_items(batches, dynamo_table)
    
    batch_ids = [x["id"] for x in batches]
    return [batch_ids]