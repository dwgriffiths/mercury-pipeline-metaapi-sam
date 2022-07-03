from collections import OrderedDict
from datetime import datetime, timedelta, date

SYMBOLS = ["GBPUSD", "EURUSD"]
DATETIMEUTC_FROM = datetime(2021, 12, 27, 0, 0, 0)
DATETIMEUTC_TO = (datetime.utcnow()).replace(hour=0, minute=0, second=0, microsecond=0)

DATETIME_FORMAT = "%Y%m%d%H%M%S%f"

BUCKET = "datalake.dgriffiths.io"
DIR_RAW_TICKS = "projects/mercury/data/raw/ticks"
DIR_CLEAN_TICKS = "projects/mercury/data/clean/ticks"
DIR_CANDLES_ROOT = "projects/mercury/data/clean/candles"

TABLE_PIPELINE = "mercury-pipeline-metaapi-PipelineTable-13PQ0AEV2S7YW"

# Frequency order
FREQUENCIES = OrderedDict()
FREQUENCIES["15S"] = {"seconds":15}
FREQUENCIES["1T"] = {"seconds":60}
FREQUENCIES["5T"] = {"seconds":300}
FREQUENCIES["15T"] = {"seconds":900}
FREQUENCIES["1H"] = {"seconds":3600}