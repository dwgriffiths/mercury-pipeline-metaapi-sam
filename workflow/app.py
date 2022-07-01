import json

from src.config import *
from src.io import *
from src.utils import *
from src.ticks.raw.save import setup_save_raw_ticks, save_raw_ticks
from src.ticks.clean.save import setup_save_clean_ticks, save_clean_ticks
from src.candles.from_ticks.save import setup_save_candles_from_ticks, save_candles_from_ticks
from src.candles.transform.save import setup_save_candles_transformed, save_candles_transformed

functions = {
    "setup_save_raw_ticks": {
        "function": setup_save_raw_ticks,
        "is_batch": False,
        "is_async": False,
    },
    "save_raw_ticks": {
        "function": save_raw_ticks,
        "is_batch": True,
        "is_async": True,
    },
    "setup_save_clean_ticks": {
        "function": setup_save_clean_ticks,
        "is_batch": False,
        "is_async": False,
    },
    "save_clean_ticks": {
        "function": save_clean_ticks,
        "is_batch": True,
        "is_async": False,
    },
    "setup_save_candles_from_ticks": {
        "function": setup_save_candles_from_ticks,
        "is_batch": False,
        "is_async": False,
    },
    "save_candles_from_ticks": {
        "function": save_candles_from_ticks,
        "is_batch": True,
        "is_async": False,
    },
    "setup_save_candles_transformed": {
        "function": setup_save_candles_transformed,
        "is_batch": False,
        "is_async": False,
    },
    "save_candles_transformed": {
        "function": save_candles_transformed,
        "is_batch": True,
        "is_async": False,
    },
}
def lambda_handler(event, context):
    function_name = event.get("function")
    kwargs = event.get("kwargs")
    assert function_name
    assert function_name in functions
    
    kwargs_extra = {k:v for k, v in event.items() if k not in ["function", "kwargs"]}
    if not kwargs:
        kwargs = kwargs_extra
    else:
        kwargs.update(kwargs_extra)
        
    function = functions[function_name]["function"]
    is_batch = functions[function_name]["is_batch"]
    is_async = functions[function_name]["is_async"]
    if is_batch:
        return job_batch(
            function,
            is_async=is_async,
            **kwargs,
        )
    return function(**kwargs)