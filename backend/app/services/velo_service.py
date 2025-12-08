#velo_service: pipeline de préparation des données.
# services/velo_service.py

import pandas as pd
import numpy as np
import os

class VeloPipeline:
    def __init__(self, df_velo: pd.DataFrame, df_meteo: pd.DataFrame = None):
        self.df_velo = df_velo.copy()
        self.df_meteo = df_meteo.copy() if df_meteo is not None else None

        self.df_clean = None
        self.df_final = None


    # ---------------------------------------------------------
    # 1️ Nettoyage + tri + clipping des valeurs
    # ---------------------------------------------------------
    def clean_data(self):
        df = self.df_velo.copy()

        df = df.drop_duplicates()
        df["datetime"] = pd.to_datetime(df["datetime"])

        # Trier pour les lags
        df = df.sort_values(["counter_id", "datetime"])

        # Nettoyer intensity
        df["intensity"] = pd.to_numeric(df["intensity"], errors="coerce")
        df["intensity"] = df["intensity"].clip(lower=0, upper=300)

        # Colonnes temporelles
        df["hour"] = df["datetime"].dt.hour
        df["day_of_week"] = df["datetime"].dt.dayofweek
        df["month"] = df["datetime"].dt.month
        df["year"] = df["datetime"].dt.year
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

        self.df_clean = df
        return df


    # ---------------------------------------------------------
    # 2️ Features temporelles avancées
    #    - cycles sin/cos
    #    - lags horaires
    #    - rolling 96h
    # ---------------------------------------------------------
    def add_features(self, df: pd.DataFrame):
        df = df.copy()

        # Cycles sin/cos
        df["hour_sin"] = np.sin(2*np.pi*df["hour"]/24)
        df["hour_cos"] = np.cos(2*np.pi*df["hour"]/24)
        df["dow_sin"] = np.sin(2*np.pi*df["day_of_week"]/7)
        df["dow_cos"] = np.cos(2*np.pi*df["day_of_week"]/7)
        df["month_sin"] = np.sin(2*np.pi*df["month"]/12)
        df["month_cos"] = np.cos(2*np.pi*df["month"]/12)

        # Lags horaires
        for lag in [24, 48, 168]:
            df[f"lag_{lag}h"] = df.groupby("counter_id")["intensity"].shift(lag)

        # Rolling 96h (= 4 jours)
        df["mean_last_4_days"] = df.groupby("counter_id")["intensity"].transform(
            lambda x: x.shift(1).rolling(96).mean()
        )

        return df


    # ---------------------------------------------------------
    # 3️ Fusion vélo + météo
    # ---------------------------------------------------------
    def merge_with_weather(self, df: pd.DataFrame):
        if self.df_meteo is None:
            return df

        df_m = self.df_meteo.copy()

        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)
        df_m["datetime"] = pd.to_datetime(df_m["datetime"]).dt.tz_localize(None)

        return df.merge(df_m, on="datetime", how="left")
    # ---------------------------------------------------------
    # 4️ Pipeline complet
    # ---------------------------------------------------------
    def run_pipeline(self):
        print("Nettoyage...")
        df = self.clean_data()

        print("Ajout des features...")
        df = self.add_features(df)

        print("Fusion météo...")
        df = self.merge_with_weather(df)

        # Supprimer les lignes avec NaN créés par les lags
        df = df.dropna().reset_index(drop=True)

        self.df_final = df

        print("Pipeline terminé.")
        return df


    # ---------------------------------------------------------
    # 5️ Export CSV final
    # ---------------------------------------------------------
    def export(self, path="../data/processed/velo_data.csv"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.df_final.to_csv(path, index=False)
        print(f"Fichier exporté : {path}")


# ---------------------------------------------------------
# Bloc test
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        df_velo = pd.read_csv("../../data/raw/velo_data.csv")
        print(f"Données vélo chargées : {len(df_velo)} lignes")
    except FileNotFoundError:
        print("⚠ Fichier velo_data.csv non trouvé !")
        df_velo = pd.DataFrame()

    try:
        df_meteo = pd.read_csv("../../data/raw/ma_meteo.csv")
        print(f"Données météo chargées : {len(df_meteo)} lignes")
    except FileNotFoundError:
        print("⚠ Fichier meteo_data.csv non trouvé !")
        df_meteo = None
    
    pipeline = VeloPipeline(df_velo, df_meteo)
    df_final = pipeline.run_pipeline()
    pipeline.export()
    print(df_final.head())