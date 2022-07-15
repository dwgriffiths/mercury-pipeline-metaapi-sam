import awswrangler as wr
from datetime import datetime, timedelta

from src.config import *
from src.io import *
from src.batch import *
from src.ticks.clean.setup import find_saved_clean_ticks
from src.candles.setup import find_saved_candles

def find_candles_to_get_from_ticks(
    month: str,
    prefixes_ticks: list,
    prefixes_saved: list = []
):
    items = []
    for prefix_ticks in prefixes_ticks:
        parameters = get_parameters_from_key(prefix_ticks)
        item = {
            "prefix_clean_ticks": prefix_ticks,
            "frequencies": [],
            "name_dataset": "from_ticks",
            "datetimestr": parameters["date"] + parameters["hour"]
        }
        for frequency in FREQUENCIES:
            prefix_candles = convert_prefix_clean_ticks_to_prefix_candles(
                prefix_ticks,
                frequency
            )
            if prefix_candles in prefixes_saved:
                continue
            item["frequencies"].append(frequency)
        if item["frequencies"]:
            items.append(item)
    items = sorted(items, key=lambda x: x.get("datetimestr"), reverse=True)
    return items

def find_missing_candles_from_ticks(
    month: str,
    dynamo_table: str,
    batch_size: int = 50,
):
    prefixes_ticks = find_saved_clean_ticks(month)
    prefixes_candles = find_saved_candles(month, dataset="from_ticks")
    
    prefixes_candles_to_get = find_candles_to_get_from_ticks(
        month,
        prefixes_ticks,
        prefixes_candles,
    )
    batches = batch_items(
        prefixes_candles_to_get,
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
    