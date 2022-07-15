import awswrangler as wr
import botocore
from collections import OrderedDict
from datetime import datetime, timedelta, date

wr.config.botocore_config = botocore.config.Config(
    retries={"max_attempts": 10},
    connect_timeout=20,
    max_pool_connections=20
)

SYMBOLS = ["GBPUSD", "EURUSD"]


DATETIME_FORMAT = "%Y%m%d%H%M%S%f"
DATETIMEUTC_FROM = datetime(2020, 12, 1, 0, 0, 0)
DATETIMEUTC_TO = (datetime.utcnow()).replace(hour=0, minute=0, second=0, microsecond=0)

DATETIMESTR_FROM = DATETIMEUTC_FROM.strftime(DATETIME_FORMAT)
DATETIMESTR_TO = DATETIMEUTC_TO.strftime(DATETIME_FORMAT)


N_DAYS_TOTAL = (DATETIMEUTC_TO - DATETIMEUTC_FROM).days


BUCKET = "datalake.dgriffiths.io"
DIR_RAW_TICKS = "projects/mercury/data/raw/ticks"
DIR_CLEAN_TICKS = "projects/mercury/data/clean/ticks"
DIR_CANDLES_ROOT = "projects/mercury/data/clean/candles"

TABLE_PIPELINE = "mercury-pipeline-metaapi-PipelineTable-13PQ0AEV2S7YW"

MAX_BATCH_SIZE = 50

# Frequency order
FREQUENCIES = OrderedDict()
FREQUENCIES["15S"] = {"seconds":15}
FREQUENCIES["1T"] = {"seconds":60}
FREQUENCIES["5T"] = {"seconds":300}
FREQUENCIES["15T"] = {"seconds":900}
FREQUENCIES["1H"] = {"seconds":3600}