import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import json

from src.config import *

s3 = boto3.client("s3")

def json_serialise_datetime(obj):
    """JSON serializer for datetimes which are not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.strftime("%Y%m%d%H%M%S%f")
    raise TypeError ("Type %s not serializable" % type(obj))
    
def save_json_data(bucket: str, key: str, data: dict):
    s3.put_object(
         Body=json.dumps(data, default=json_serialise_datetime),
         Bucket=bucket,
         Key=key
    )
    
def load_json_data(path: str):
    key = path.replace(f"s3://{BUCKET}/", "") 
    response = s3.get_object(
        Bucket=BUCKET, Key=key
    )
    body = response['Body'].read()
    text = body.decode()
    data = json.loads(text)
    return data

def save_raw_ticks_to_json(data: dict):
    symbol = data["symbol"]
    datetimestr_from = data["datetimeutc_from"]
    n_ticks = data["n"]
    key = f"{DIR_RAW_TICKS}/{symbol}/ticks_{datetimestr_from}_{n_ticks}.json"
    save_json_data(BUCKET, key, data)
    
def get_prefix_raw_ticks(
    symbol: str,
    datetimestr_from: str
):
    return f"s3://{BUCKET}/{DIR_RAW_TICKS}/{symbol}/ticks_{datetimestr_from}_"

def get_prefixes_from_keys(keys: list):
    return [
        "/".join(key.split("/")[:-1]) + "/" for key in keys
    ]

def get_parameters_from_key(key: str):
    return {
        x.split("=")[0]:x.split("=")[1] for x in key.split("/") if "=" in x
    }

def convert_key_raw_ticks_to_prefix_clean_ticks(
    key_raw_ticks: str
):
    symbol = key_raw_ticks.split("/ticks_")[0].split("/")[-1]
    datetimestr = key_raw_ticks.split("ticks_")[-1].split("_")[0]
    datetimeutc = datetime.strptime(datetimestr, "%Y%m%d%H%M%S%f")
    year = datetimeutc.strftime("%Y")
    month = datetimeutc.strftime("%m")
    date = datetimeutc.strftime("%Y%m%d")
    hour = datetimeutc.strftime("%H")
    return "/".join([
        "s3:/",
        BUCKET,
        DIR_CLEAN_TICKS,
        f"symbol={symbol}",
        f"frequency=tick",
        f"year={year}",
        f"month={month}",
        f"date={date}",
        f"hour={hour}",
        ""
    ])

def convert_prefix_clean_ticks_to_prefix_candles(
    prefix_clean_ticks: str,
    frequency: str,
):
    return (
        prefix_clean_ticks
        .replace("/ticks/", f"/candles/from_ticks/")
        .replace("/frequency=tick/", f"/frequency={frequency}/")
    )

