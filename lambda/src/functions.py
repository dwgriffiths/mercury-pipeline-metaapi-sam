from src.config import *

def get_datetime_boundaries(
    datetimeutc_from: datetime,
    datetimeutc_to: datetime,
    window_size: int = 3600, # in seconds
    reverse: bool = True
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

def get_datetime_boundaries_in_month(
    month: str,
    window_size: int = 3600, # in seconds
    reverse: bool = True
):
    """
    Get the time windows between two datetimes within a month.
    """
    datetimeutc_from = datetime.strptime(month, "%Y%m")
    datetimeutc_to = min(
        (datetime.strptime(month, "%Y%m") + timedelta(days=40)).replace(day=1),
        DATETIMEUTC_TO
    )
    return get_datetime_boundaries(
        datetimeutc_from,
        datetimeutc_to,
        window_size,
        reverse
    )

def get_months(
    datetimestr_from: str = DATETIMESTR_FROM,
    datetimestr_to: str = DATETIMESTR_TO,
    reverse: bool = True
):
    """
    Get the time windows between two datetimes.
    """
    datetimeutc_from = datetime.strptime(
        datetimestr_from,
        DATETIME_FORMAT
    )
    datetimeutc_to = datetime.strptime(
        datetimestr_to,
        DATETIME_FORMAT
    )
    datetimesutc_from, datetimesutc_to = get_datetime_boundaries(
        datetimeutc_from,
        datetimeutc_to,
        reverse=False
    )
    months = [dt.strftime("%Y%m") for dt in datetimesutc_from]
    months.extend([dt.strftime("%Y%m") for dt in datetimesutc_to])
    months = sorted(list(set(months)))
    if reverse:
        return list(reversed(months))
    return list(months)

def get_prefix_raw_ticks(
    symbol: str,
    datetimestr_from: str
):
    return f"s3://{BUCKET}/{DIR_RAW_TICKS}/{symbol}/ticks_{datetimestr_from}_"

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
):
    items = []
    for symbol in SYMBOLS:
        path = f"s3://{BUCKET}/{DIR_RAW_TICKS}/{symbol}/ticks_{month}"
        keys_symbol = wr.s3.list_objects(path, suffix=".json")
        prefixes_symbol = [
            "_".join(x.split("_")[:-1]) + "_" for x in keys_symbol
        ]
        items.extend(prefixes_symbol)
    return items

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
    batches = batch_items(prefixes_ticks_to_get, batch_size)
    
    # Clear DynamoDB table
    item_ids = (
        wr.dynamodb
        .get_table(dynamo_table)
        .scan(AttributesToGet=['id',])["Items"]
    )
    wr.dynamodb.delete_items(item_ids, dynamo_table)
    
    # Add batches to DynamoDB table
    wr.dynamodb.put_items(batches, dynamo_table)
    
    batch_ids = [x["id"] for x in batches]
    return [batch_ids]
