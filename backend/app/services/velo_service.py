import pandas as pd

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df["hour"] = df["datetime"].dt.hour
    df["weekday"] = df["datetime"].dt.weekday
    df["is_weekend"] = df["weekday"].isin([5,6]).astype(int)
    return df

def compute_rolling_lags(df: pd.DataFrame) -> pd.DataFrame:
    df["rolling_3h"] = df.groupby("counter_id")["intensity"].transform(lambda x: x.rolling(3, min_periods=1).mean())
    df["lag_1h"] = df.groupby("counter_id")["intensity"].shift(1)
    df["lag_24h"] = df.groupby("counter_id")["intensity"].shift(24)
    return df

def categorize_counters(df: pd.DataFrame) -> pd.DataFrame:
    duration = df.groupby("counter_id")["datetime"].agg(["min", "max"])
    duration["days"] = (duration["max"] - duration["min"]).dt.days + 1
    
    def categorize(days):
        if days >= 200:
            return "plus de 200 jours"
        elif days >= 162:
            return "162 jours ou plus"
        elif days < 100:
            return "moins de 100 jours"
        else:
            return "autres"
    
    duration["category"] = duration["days"].apply(categorize)
    return duration