import asyncio
import awswrangler as wr
from datetime import datetime, timedelta, date

from src.config import *
from src.io import *

def batch_items(
    items: list,
    batch_size: int
):
    N = len(items)
    n_batches = int(N / batch_size) + int(N % batch_size != 0)
    batches = [[] for i in range(n_batches)]
    for i in range(N):
        j = i % n_batches
        batches[j].append(items[i])
    batches = [{"id":i, "batch":batch} for i, batch in enumerate(batches)]
    return batches

async def process_batch_async(
    func,
    batch_id: int = None,
):
    results = []
    table = wr.dynamodb.get_table(TABLE_PIPELINE)
    batch = table.get_item(Key={"id":batch_id})["Item"]
    for item in batch["batch"]:
        result = await func(**item)
        results.append(result)
    return all(results)

def process_batch(
    func,
    batch_id: int = None,
    is_async: bool = False
):
    if is_async:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            process_batch_async(func, batch_id)
        )
    results = []
    table = wr.dynamodb.get_table(TABLE_PIPELINE)
    batch = table.get_item(Key={"id":batch_id})["Item"]
    for item in batch["batch"]:
        results.append(func(**item))
    wr.dynamodb.delete_items([{"id":batch_id}], TABLE_PIPELINE)
    return all(results)

def get_datetime_boundaries(
    datetimeutc_from: datetime,
    datetimeutc_to: datetime,
    window_size: int = 3600, # in seconds
    reverse: bool= True
):
    """
    Get the time windows between two datetimes.
    """
    n_steps = int((datetimeutc_to - datetimeutc_from).total_seconds() / window_size)
    datetimesutc_from = [datetimeutc_from + timedelta(seconds=i*window_size) for i in range(n_steps)]
    datetimesutc_to = [x + timedelta(seconds=window_size) for x in datetimesutc_from]
    if reverse:
        return [x for x in reversed(datetimesutc_from)], [y for y in reversed(datetimesutc_to)]
    return datetimesutc_from, datetimesutc_to

def get_parameters_from_key(key: str):
    return {
        x.split("=")[0]:x.split("=")[1] for x in key.split("/") if "=" in x
    }

def get_prefixes_from_keys(keys: list):
    return [
        "/".join(key.split("/")[:-1]) + "/" for key in keys
    ]
