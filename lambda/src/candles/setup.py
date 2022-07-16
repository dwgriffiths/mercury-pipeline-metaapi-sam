import awswrangler as wr
from datetime import datetime, timedelta

from src.config import *
from src.io import *
from src.batch import *

def find_candles_to_get(
    prefixes_in: list,
    name_dataset_in: str,
    name_dataset_out: str,
    transformations: list,
    lookback_rows: int,
    prefixes_saved: list = []
):
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
                if prefix_out in prefixes_saved:
                    continue
                parameters = get_parameters_from_key(prefix)
                items.append({
                    "prefix_in": prefix,
                    "prefix_lookback": prefix_lookback,
                    "name_dataset_out":name_dataset_out,
                    "transformations":transformations,
                    "datetimestr": parameters["date"] + parameters["hour"]
                })
    items = sorted(items, key=lambda x: x.get("datetimestr"), reverse=True)
    return items

def find_saved_candles(
    month: str,
    dataset: str,
    as_dict: bool = False 
):
    items = []
    for symbol in SYMBOLS:
        for frequency in FREQUENCIES:
            path = "/".join([
                "s3:/",
                BUCKET,
                DIR_CANDLES_ROOT,
                dataset,
                f"symbol={symbol}",
                f"frequency={frequency}",
                f"year={month[:4]}",
                f"month={month[4:]}",
                ""
            ])
            keys = wr.s3.list_objects(path, suffix=".parquet")
            prefixes = get_prefixes_from_keys(keys)
            items.extend(prefixes)
    if not as_dict:
        return items
    d_items = {}
    for symbol in SYMBOLS:
        d_items[symbol] = {}
        for frequency in FREQUENCIES:
            d_items[symbol][frequency] = sorted(
                [
                    i for i in items if all([
                        f"symbol={symbol}" in i,
                        f"frequency={frequency}" in i,
                    ])
                ]
            )
    return d_items

def find_missing_candles(
    month: str,
    dynamo_table: str,
    name_dataset_in: str,
    name_dataset_out: str,
    transformations: list,
    batch_size: int = 50,
    lookback_rows: int = 60,
    *args,
    **kwargs
):
    prefixes_in = find_saved_candles(
        month,
        name_dataset_in,
        as_dict=True
    )
    prefixes_out = find_saved_candles(
        month,
        name_dataset_out,
        as_dict=False
    )
    
    prefixes_candles_to_get = find_candles_to_get(
        prefixes_in,
        name_dataset_in,
        name_dataset_out,
        transformations,
        lookback_rows,
        prefixes_saved=prefixes_out
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
    