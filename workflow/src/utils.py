import asyncio
from datetime import datetime, timedelta, date

from src.config import *
from src.io import *

def batch_items(
    items: list,
    batch_size: int
):
    n_items = len(items)
    n_batches_total = int(n_items / batch_size) + int(n_items % batch_size != 0)
    N = min(n_items, MAX_ITERATIONS)
    n_batches = int(N / batch_size) + int(N % batch_size != 0)
    batches = [[] for i in range(n_batches)]
    for i in range(N):
        j = i % n_batches
        batches[j].append(items[i])
    return {
        "n_items": n_items,
        "n_batches_total": n_batches_total,
        "n_batches": n_batches,
        "batches": batches
    }

async def job_batch_async(
    func,
    batch: list
):
    results = []
    for i, item in enumerate(batch):
        result = await func(**item)
        results.append(result)
    return all(results)

def job_batch(
    func,
    batch: list = [],
    is_async: bool = False
):
    if is_async:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            job_batch_async(func, batch)
        )
    results = []
    for item in batch:
        results.append(func(**item))
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
