from src.config import *
from src.io import *
from src.time import get_months
from src.ticks.raw.setup import find_missing_raw_ticks
from src.ticks.raw.execute import save_missing_raw_ticks
from src.ticks.clean.setup import find_missing_clean_ticks
from src.ticks.clean.execute import save_missing_clean_ticks
from src.candles.from_ticks.setup import find_missing_candles_from_ticks
from src.candles.from_ticks.execute import save_missing_candles_from_ticks
from src.candles.setup import find_missing_candles
from src.candles.execute import save_missing_candles


def wrapper_get_months(*args, **kwargs):
    return get_months(*args, **kwargs)

def wrapper_find_missing_raw_ticks(*args, **kwargs):
    return find_missing_raw_ticks(*args, **kwargs)

def wrapper_save_missing_raw_ticks(*args, **kwargs):
    return save_missing_raw_ticks(*args, **kwargs)

def wrapper_find_missing_clean_ticks(*args, **kwargs):
    return find_missing_clean_ticks(*args, **kwargs)

def wrapper_save_missing_clean_ticks(*args, **kwargs):
    return save_missing_clean_ticks(*args, **kwargs)

def wrapper_find_missing_candles_from_ticks(*args, **kwargs):
    return find_missing_candles_from_ticks(*args, **kwargs)

def wrapper_save_missing_candles_from_ticks(*args, **kwargs):
    return save_missing_candles_from_ticks(*args, **kwargs)

def wrapper_find_missing_candles(*args, **kwargs):
    return find_missing_candles(*args, **kwargs)

def wrapper_save_missing_candles(*args, **kwargs):
    return save_missing_candles(*args, **kwargs)