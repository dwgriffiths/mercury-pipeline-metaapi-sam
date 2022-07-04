import json

from src.config import *
from src.io import *
from src.utils import *
from src.ticks.raw.save import setup_save_raw_ticks, save_raw_ticks
from src.ticks.clean.save import setup_save_clean_ticks, save_clean_ticks
from src.candles.from_ticks.save import setup_save_candles_from_ticks, save_candles_from_ticks
from src.candles.transform.save import setup_save_candles_transformed, save_candles_transformed

functions_setup = {
    "setup_save_raw_ticks": {
        "function": setup_save_raw_ticks,
        "is_async": False,
    },
    "setup_save_clean_ticks": {
        "function": setup_save_clean_ticks,
        "is_async": False,
    },
    "setup_save_candles_from_ticks": {
        "function": setup_save_candles_from_ticks,
        "is_async": False,
    },
    "setup_save_candles_transformed": {
        "function": setup_save_candles_transformed,
        "is_async": False,
    },
}

functions_process = {
    "save_raw_ticks": {
        "function": save_raw_ticks,
        "is_async": True,
    },
    "save_clean_ticks": {
        "function": save_clean_ticks,
        "is_async": False,
    },
    "save_candles_from_ticks": {
        "function": save_candles_from_ticks,
        "is_async": False,
    },
    "save_candles_transformed": {
        "function": save_candles_transformed,
        "is_async": False,
    },
}

def lambda_handler(event, context):
    function_name = event.get("function")
    assert function_name
    assert function_name in functions_setup | functions_process
    kwargs = {} if event.get("kwargs") is None else event.get("kwargs")
    kwargs.update({k:v for k, v in event.items() if k not in ["function", "kwargs"]})
    
    if function_name in functions_setup:
        function = functions_setup[function_name]["function"]
        kwargs["is_async"] = functions_setup[function_name]["is_async"]
        return function(**kwargs)

    function = functions_process[function_name]["function"]
    kwargs["is_async"] = functions_process[function_name]["is_async"]
    return process_batch(
        function,
        **kwargs,
    )