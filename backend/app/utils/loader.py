import pandas as pd
from datetime import datetime, timedelta
import os

def load_existing_csv(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        df = pd.read_csv(path)
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df
    return pd.DataFrame()

def update_csv(df_old: pd.DataFrame, df_new: pd.DataFrame, path: str):
    if not df_new.empty:
        df_final = pd.concat([df_old, df_new])
        df_final = df_final.drop_duplicates(subset=["datetime", "counter_id"])
        df_final.to_csv(path, index=False)
        print(f"CSV mis à jour : {len(df_new)} nouvelles lignes")
    else:
        print("Aucune nouvelle donnée à ajouter")