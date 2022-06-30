import json

from src.config import *
from src.io import *
from src.utils import *
from src.ticks.raw.save import setup_save_raw_ticks, save_raw_ticks
from src.ticks.clean.save import setup_save_clean_ticks, save_clean_ticks
from src.candles.from_ticks.save import setup_save_candles_from_ticks, save_candles_from_ticks
from src.candles.transform.setup.lookback import setup_candles_lookback

functions = {
    "setup_save_raw_ticks": {
        "function": setup_save_raw_ticks,
        "batch": False,
        "is_async": False,
    },
    "save_raw_ticks": {
        "function": save_raw_ticks,
        "batch": True,
        "is_async": True,
    },
    "setup_save_clean_ticks": {
        "function": setup_save_clean_ticks,
        "batch": False,
        "is_async": False,
    },
    "save_clean_ticks": {
        "function": save_clean_ticks,
        "batch": True,
        "is_async": False,
    },
    "setup_save_candles_from_ticks": {
        "function": setup_save_candles_from_ticks,
        "batch": False,
        "is_async": False,
    },
    "save_candles_from_ticks": {
        "function": save_candles_from_ticks,
        "batch": True,
        "is_async": False,
    }
}
def lambda_handler(event, context):
    function_name = event.get("function")
    kwargs = event.get("kwargs")
    assert function_name
    assert function_name in functions
    kwargs = kwargs if kwargs else {}
    function = functions[function_name]["function"]
    batch = functions[function_name]["batch"]
    is_async = functions[function_name]["is_async"]
    if batch:
        return job_batch(
            function,
            kwargs,
            is_async
        )
    return function(**kwargs)