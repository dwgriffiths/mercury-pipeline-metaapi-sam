import numpy as np
import pandas as pd
import random
import string


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

def apply_id(
    df: pd.DataFrame,
    label: str
) -> pd.DataFrame:
    mask = df[label] != 0.0
    df[label] = np.nan
    df.loc[mask, label] = "".join(random.choices(string.ascii_uppercase, k=20))
    return df

def ema(
    df: pd.DataFrame
) -> pd.DataFrame:
    
    for av in [1, 10, 21, 50]:
        df[f"ema_{av}"] = df["ask.close"].ewm(span=av, adjust=False).mean()
        
    # up and downtrends
    df["ema_up"] = (df["ema_10"] > df["ema_21"]) & (df["ema_10"] > df["ema_50"])
    df["ema_dn"] = (df["ema_10"] < df["ema_21"]) & (df["ema_10"] < df["ema_50"])
    
    df["ema_up_start"] = df["ema_up"] > df["ema_up"].shift()
    df["ema_dn_start"] = df["ema_dn"] > df["ema_dn"].shift()
    df["ema_up_stop"] = df["ema_up"] < df["ema_up"].shift()
    df["ema_dn_stop"] = df["ema_dn"] < df["ema_dn"].shift()
    
    df["ema_up"] = df["ema_up"] | df["ema_up_stop"]
    df["ema_dn"] = df["ema_dn"] | df["ema_dn_stop"]

    # Get a unique index for each individual trend
    df["ema_up_i"] = df["ema_up_start"].cumsum() * df["ema_up"]
    df["ema_dn_i"] = df["ema_dn_start"].cumsum() * df["ema_dn"]

    df = df.groupby("ema_up_i").apply(apply_id, label="ema_up_i")
    df = df.groupby("ema_dn_i").apply(apply_id, label="ema_dn_i")

    return df

def trend_properties_micro(
    df: pd.DataFrame,
    updn: str
) -> pd.DataFrame:
    # Make sure that points are in time order (they should be anyway but hey ho).
    df = (
        df
        .sort_values(by="timestamp_utc.open", ascending=True)
        .reset_index(drop=True)
        .reset_index(drop=False)
        .rename(columns={"index": f"ema_{updn}_idx"})
    )
    
    if len(df) < 2:
        return df[[
            "timestamp_utc.open",
            f"ema_{updn}_i",
            f"ema_{updn}_idx",
        ]]

    i_peak = {
        "up": df.iloc[1:]["bid.high"].idxmax(),
        "dn": df.iloc[1:]["ask.low"].idxmin()
    }[updn]
    
    df[f"ema_{updn}_exit"] = df[f"ema_{updn}_idx"] == i_peak
    
    df[f"ema_{updn}_profit"] = np.nan
    mask = df[f"ema_{updn}_idx"] > 0
    mask = mask & (df[f"ema_{updn}_idx"] <= i_peak)

    df.loc[mask, f"ema_{updn}_profit"] = {
        "up": df.iloc[i_peak]["bid.high"] - df.loc[mask]["ask.open"],
        "dn": df.loc[mask]["bid.open"] - df.iloc[i_peak]["ask.low"],
    }[updn]
    
    i_entry = df[f"ema_{updn}_profit"].idxmax(skipna=True)
    df[f"ema_{updn}_entry"] = df[f"ema_{updn}_idx"] == i_entry
    
    return df[[
        "timestamp_utc.open",
        f"ema_{updn}_i",
        f"ema_{updn}_idx",
        f"ema_{updn}_entry",
        f"ema_{updn}_exit",
        f"ema_{updn}_profit",
    ]]

def trend_micro(
    df: pd.DataFrame
) -> pd.DataFrame:
    
    n_start = len(df)
    
    mask = df["ema_up"] == 1
    if len(df[mask]) > 0:
        dfi = (
            df[mask].groupby("ema_up_i")
            .apply(trend_properties_micro, updn="up")
            .reset_index(drop=True)
        )
        df = pd.merge(
            df,
            dfi,
            how="left",
            on=["timestamp_utc.open", "ema_up_i"]
        )

    mask = df["ema_dn"] == 1
    if len(df[mask]) > 0:
        dfi = (
            df[mask].groupby("ema_dn_i")
            .apply(trend_properties_micro, updn="dn")
            .reset_index(drop=True)
        )
        df = pd.merge(
            df,
            dfi,
            how="left",
            on=["timestamp_utc.open", "ema_dn_i"]
        )
        
    to_drop = [c for c in df.columns if "level_1" in c]
    df = (
        df.sort_values(by="timestamp_utc.open")
        .drop(columns=to_drop)
    )
    assert len(df) == n_start
    return df