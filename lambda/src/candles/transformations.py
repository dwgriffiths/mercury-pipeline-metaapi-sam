import numpy as np
import pandas as pd

def ffill(
    df_candles: pd.DataFrame
):
    df_out = df_candles.copy()
    cols = ["ask.close", "bid.close"]
    df_out[cols] = (
        df_out[cols]
        .replace(0., pd.NA)
        .fillna(method="ffill")
    )
    for stem in ["ask", "bid"]:
        cols = [f"{stem}.{col}" for col in ["open", "close", "high", "low"]]
        df_out[cols] = (
            df_out[cols]
            .fillna(method="ffill", axis=1)
            .fillna(method="bfill", axis=1)
        )
    return df_out