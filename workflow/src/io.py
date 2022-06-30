import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import json

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
    
def load_json_data(bucket: str, key: str):
    s3 = boto3.client("s3")
    response = s3.get_object(
        Bucket=bucket, Key=key
    )
    body = response['Body'].read()
    text = body.decode()
    data = json.loads(text)
    return data
