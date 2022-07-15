import asyncio
import awswrangler as wr

def batch_items(
    items: list,
    batch_size: int,
    id_prefix: str = ""
):
    N = len(items)
    n_batches = int(N / batch_size) + int(N % batch_size != 0)
    batches = [[] for i in range(n_batches)]
    for i in range(N):
        j = i % n_batches
        batches[j].append(items[i])
    batches = [{"id":f"{id_prefix}_{i}", "batch":batch} for i, batch in enumerate(batches)]
    return batches

async def process_batch_async(
    func,
    dynamo_table: str,
    batch_id: str,
):
    results = []
    table = wr.dynamodb.get_table(dynamo_table)
    batch = table.get_item(Key={"id":batch_id})["Item"]
    for item in batch["batch"]:
        result = await func(**item)
        results.append(result)
    return all(results)

def process_batch(
    func,
    dynamo_table: str,
    batch_id: str,
    is_async: bool = False
):
    if is_async:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            process_batch_async(func, dynamo_table, batch_id)
        )
    results = []
    table = wr.dynamodb.get_table(dynamo_table)
    batch = table.get_item(Key={"id":batch_id})["Item"]
    for item in batch["batch"]:
        results.append(func(**item))
    wr.dynamodb.delete_items([{"id":batch_id}], dynamo_table)
    return all(results)