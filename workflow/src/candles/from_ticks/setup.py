from datetime import datetime, timedelta, date

from src.config import *
from src.io import *
from src.utils import * 
from src.ticks.clean.io import *
from src.candles.io import *

# def setup_save_candles_without_na(
#     overwrite: bool,
#     batch_size: int,
#     lookback_rows: int
# ):
#     overwrite = False if overwrite is None else overwrite
#     batch_size = 48 if batch_size is None else max(min(batch_size, 48), 1)
#     lookback_rows = 60 if lookback_rows is None else max(min(lookback_rows, 60), 1)
    
#     dir_candles_in = f"{DIR_CANDLES_ROOT}/with_na"
#     dir_candles_out = f"{DIR_CANDLES_ROOT}/without_na"
    
#     return setup_candles_lookback(
#         dir_candles_in,
#         dir_candles_out,
#         overwrite,
#         batch_size,
#         lookback_rows,
#     )