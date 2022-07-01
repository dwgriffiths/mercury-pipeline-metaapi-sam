from datetime import datetime, timedelta, date
import pandas as pd

from src.config import *
from src.io import *
from src.utils import * 
from src.candles.io import *

def setup_save_candles_transformed(
    name_dataset_in: str,
    name_dataset_out: str,
    overwrite: bool = None,
    batch_size: int = None,
    SaveCandlesTransformed: dict = None,
    **kwargs
):
    overwrite = False if overwrite is None else overwrite
    batch_size = 48 if batch_size is None else max(min(batch_size, 48), 1)
    overwrite = overwrite if SaveCandlesTransformed is None or SaveCandlesTransformed.get(name_dataset_out) is None else False
    
    
    

# def ffill_candles(
#     df_candles: pd.DataFrame
# ):
#     cols = ["ask.close", "bid.close"]
#     df_candles[cols] = (
#         df_candles[cols]
#         .replace(0., pd.NA)
#         .fillna(method="ffill")
#     )
#     for stem in ["ask", "bid"]:
#         cols = [f"{stem}.{col}" for col in ["open", "high", "low"]]
#         df_candles[cols] = (
#             df_candles[cols]
#             .fillna(method="ffill", axis=1)
#             .fillna(method="bfill", axis=1)
#         )

# def save_transformed_candles(
#     prefix_in: str,
#     prefix_lookback: str,
#     name_dataset_out: str,
#     functions: list = [],
#     **kwargs
# ):
#     name_dataset_in = prefix_in.split("/candles/")[-1].split("/")[0]
#     prefix_out = prefix_in.replace(name_dataset_in, name_dataset_out)
    
#     # Delete any objects which are already there.
#     delete_objects(
#         BUCKET,
#         get_matching_keys_candles(prefix_out)
#     )

#     # Load candles
#     parameters_in = get_parameters_from_key(prefix_in)
#     parameters_lookback = get_parameters_from_key(prefix_lookback)
#     filters_from, filters_to = [], []
#     for key in ["year", "month", "date", "hour"]:
#         value_in, value_lookback = parameters_in[key], parameters_lookback[key]
#         if value_in == value_lookback:
#             filters_from.append((key, "==", value_in))
#             filters_to.append((key, "==", value_in))
#         else:
#             filters_from.append((key, "==", value_lookback))
#             filters_to.append((key, "==", value_in))

#     print(prefix_in)
#     print([filters_from, filters_to])
#     return [filters_from, filters_to]
#     df_in = pd.read_parquet(
#         f"s3://{BUCKET}/{DIR_CANDLES_ROOT}/{name_dataset_in}/",
#         filters = [filters_from, filters_to]
#     )
    
#     return df_in
    
#     # Transform candles
#     functions = [{"function":ffill_candles, "kwargs":{}}]
#     df_out = df_in.copy()
#     for function in functions:
#         df_out = function["function"](
#             df_out,
#             **function["kwargs"]
#         )
        
#     # Make sure the partition_cols are in there
#     parameters = get_parameters_from_key(prefix_out)
#     for k, v in parameters.items():
#         df_out[k] = v

#     # #Save as parquet
#     # df_out.to_parquet(
#     #     f"s3://{BUCKET}/{DIR_CANDLES_ROOT}/{name_dataset_out
#     }/",
#     #     partition_cols=[
#     #         "symbol",
#     #         "frequency",
#     #         "year",
#     #         "month",
#     #         "date",
#     #         "hour",
#     #     ]
#     # )
#     # return True
#     return df_out
    