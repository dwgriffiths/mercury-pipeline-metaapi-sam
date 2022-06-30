import json

from src.config import *
from src.io import *
from src.utils import *
from src.ticks.raw.save import setup_save_raw_ticks, save_raw_ticks
from src.ticks.clean.save import setup_save_clean_ticks, save_clean_ticks
from src.candles.from_ticks.save import setup_save_ticks_to_candles, save_ticks_to_candles
from src.candles.transform.setup.lookback import setup_candles_lookback

def lambda_setup_save_raw_ticks(event, context):
    stop = event.get("stop")
    batch_size = event.get("batch_size")
    result = setup_save_raw_ticks(stop, batch_size)
    return result

def lambda_save_raw_ticks(event, context):
    batch = event.get("batch")
    result = job_batch(
        save_raw_ticks,
        batch,
        is_async=True
    )
    return result

def lambda_setup_save_clean_ticks(event, context):
    overwrite = event.get("overwrite")
    batch_size = event.get("batch_size")
    prev = event.get("ResultSaveCleanTicks")
    overwrite = overwrite if prev is None else False
    
    result = setup_save_clean_ticks(overwrite, batch_size)
    return result

def lambda_save_clean_ticks(event, context):
    batch = event.get("batch")
    result = job_batch(
        save_clean_ticks,
        batch,
    )
    return result