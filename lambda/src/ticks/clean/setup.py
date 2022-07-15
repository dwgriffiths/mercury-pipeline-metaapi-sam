import awswrangler as wr
from datetime import datetime, timedelta

from src.config import *
from src.io import *
from src.batch import *
from src.ticks.raw.setup import find_saved_raw_ticks

def find_clean_ticks_to_get(
    month: str,
    keys_raw_ticks: list,
    prefixes_saved: list = []
):
    items = []
    for key_raw in keys_raw_ticks:
        if key_raw.endswith("_0.json"):
            continue
        prefix_clean = convert_key_raw_ticks_to_prefix_clean_ticks(
            key_raw
        )
        if prefix_clean in prefixes_saved:
            continue
        items.append({
            "key_raw_ticks": key_raw,
            "datetimestr": key_raw.split("_")[-2]
        })
    items = sorted(items, key=lambda x: x.get("datetimestr"), reverse=True)
    return items
    
def find_saved_clean_ticks(
    month: str,
):
    items = []
    for symbol in SYMBOLS:
        path = "/".join([
            "s3:/",
            BUCKET,
            DIR_CLEAN_TICKS,
            f"symbol={symbol}",
            f"frequency=tick",
            f"year={month[:4]}",
            f"month={month[4:]}",
            ""
        ])
        keys_symbol = wr.s3.list_objects(path, suffix=".parquet")
        prefixes_symbol = get_prefixes_from_keys(keys_symbol)
        items.extend(prefixes_symbol)
    return items
    
def find_missing_clean_ticks(
    month: str,
    dynamo_table: str,
    batch_size: int = 50,
):
    keys_raw_ticks = find_saved_raw_ticks(month, prefix=False)
    prefixes_saved_ticks = find_saved_clean_ticks(
        month
    )
    
    prefixes_ticks_to_get = find_clean_ticks_to_get(
        month,
        keys_raw_ticks,
        prefixes_saved_ticks,
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
    