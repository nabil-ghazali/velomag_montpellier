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
# if __name__ == "__main__":
#     try:
#         df_velo = pd.read_csv("../../data/raw/velo_data.csv")
#         print(f"Vélo chargé : {len(df_velo)} lignes")
#     except:
#         print("⚠ Fichier vélo introuvable")
#         df_velo = pd.DataFrame()

#     try:
#         df_meteo = pd.read_csv("../../data/raw/ma_meteo.csv")
#         print(f"Météo chargée : {len(df_meteo)} lignes")
#     except:
#         print("⚠ Fichier météo introuvable")
#         df_meteo = None

#     pipeline = VeloPipeline(df_velo, df_meteo)
#     df_final = pipeline.run_pipeline()
#     pipeline.export()
#     print(df_final.head())
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







# import pandas as pd
# import numpy as np
# import os

# class VeloPipeline:
#     def __init__(self, df_velo: pd.DataFrame, df_meteo: pd.DataFrame = None):
#         self.df_velo = df_velo.copy()
#         self.df_meteo = df_meteo.copy() if df_meteo is not None else None
#         self.duration = None
#         self.df_clean = None
#         self.df_365 = None
#         self.df_200 = None
#         self.df_100 = None

#     # ---------------------------------------------------------
#     # 1️ Nettoyage des données
#     # ---------------------------------------------------------
#     def clean_data(self):
#         df = self.df_velo.copy()

#         df = df.drop_duplicates()
#         df["datetime"] = pd.to_datetime(df["datetime"])

#         # Trier pour les lags
#         df = df.sort_values(["counter_id", "datetime"])

#         # Nettoyer intensity
#         df["intensity"] = pd.to_numeric(df["intensity"], errors="coerce")
#         df["intensity"] = df["intensity"].clip(lower=0, upper=300)

#         # Colonnes temporelles
#         df["hour"] = df["datetime"].dt.hour
#         df["day_of_week"] = df["datetime"].dt.dayofweek
#         df["month"] = df["datetime"].dt.month
#         df["year"] = df["datetime"].dt.year
#         df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

#         self.df_clean = df
#         return df
#     # ---------------------------------------------------------
#     # 2️ Catégorisation des compteurs
#     # ---------------------------------------------------------
#     def categorize_counters(self):
#         duration = self.df_clean.groupby("counter_id")["datetime"].agg(["min","max"])
#         duration["days"] = (duration["max"] - duration["min"]).dt.days + 1

#         def categorize(days):
#             if days >= 365:
#                 return "plus de 365 jours"
#             elif days >= 200:
#                 return "200 jours ou plus"
#             elif days < 100:
#                 return "moins de 100 jours"
#             else:
#                 return "autres"

#         duration["category"] = duration["days"].apply(categorize)
#         self.duration = duration
#         return duration

#     # ---------------------------------------------------------
#     # 3️ Feature engineering
#     # ---------------------------------------------------------
#     def add_features(self, df: pd.DataFrame):
#         df = df.copy()

#         # Cycles sin/cos
#         df["hour_sin"] = np.sin(2*np.pi*df["hour"]/24)
#         df["hour_cos"] = np.cos(2*np.pi*df["hour"]/24)
#         df["dow_sin"] = np.sin(2*np.pi*df["day_of_week"]/7)
#         df["dow_cos"] = np.cos(2*np.pi*df["day_of_week"]/7)
#         df["month_sin"] = np.sin(2*np.pi*df["month"]/12)
#         df["month_cos"] = np.cos(2*np.pi*df["month"]/12)

#         # Lags horaires
#         for lag in [24, 48, 168]:
#             df[f"lag_{lag}h"] = df.groupby("counter_id")["intensity"].shift(lag)

#         # Rolling 96h (= 4 jours)
#         df["mean_last_4_days"] = df.groupby("counter_id")["intensity"].transform(
#             lambda x: x.shift(1).rolling(96).mean()
#         )

#         return df
#     # ---------------------------------------------------------
#     # 4️ Fusion avec météo
#     # ---------------------------------------------------------
#     def merge_with_weather(self, df: pd.DataFrame):
#         if self.df_meteo is None:
#             return df
#         df['datetime'] = df['datetime'].dt.tz_convert(None)
#         df_m = self.df_meteo.copy()
#         df_m['datetime'] = pd.to_datetime(df_m['datetime']).dt.tz_localize(None)
#         df_merged = df.merge(df_m, on='datetime', how='left')
#         return df_merged

#     # ---------------------------------------------------------
#     # 5️ Export des datasets
#     # ---------------------------------------------------------
#     def export_data(self,
#                     path_clean="../data/processed/data_clean.csv",
#                     path_features="../data/processed/features.csv"):

#         # Créer les dossiers si inexistants
#         os.makedirs(os.path.dirname(path_clean), exist_ok=True)
#         os.makedirs(os.path.dirname(path_features), exist_ok=True)

#         df_clean_copy = self.df_clean.copy()
#         features_copy = df_clean_copy[['counter_id', 'datetime', 'intensity',
#                                 'lag_1d', 'lag_7d']].copy()


#         df_clean_copy.to_csv(path_clean, index=False)
#         features_copy.to_csv(path_features, index=False)

#         print(f"Datasets exportés : {path_clean}, {path_features}")

#     # ---------------------------------------------------------
#     # 6️ Pipeline complet
#     # ---------------------------------------------------------
#     def run_pipeline(self):
#         print("Nettoyage...")
#         df = self.clean_data()

#         print("Ajout des features...")
#         df = self.add_features(df)

#         print("Fusion météo...")
#         df = self.merge_with_weather(df)

#         # Supprimer les lignes avec NaN créés par les lags
#         df = df.dropna().reset_index(drop=True)

#         self.df_final = df

#         print("Pipeline terminé.")
#         return df

#     # def run_pipeline(self):
#     #     self.clean_data()
#     #     self.categorize_counters()

#     #     # Filtrage compteurs par durée
#     #     self.df_365 = self.df_clean[self.df_clean['counter_id'].isin(
#     #         self.duration[self.duration['category'] == "plus de 365 jours"].index
#     #     )]
#     #     self.df_200 = self.df_clean[self.df_clean['counter_id'].isin(
#     #         self.duration[self.duration['category'] == "200 jours ou plus"].index
#     #     )]
#     #     self.df_100 = self.df_clean[self.df_clean['counter_id'].isin(
#     #         self.duration[self.duration['category'] == "moins de 100 jours"].index
#     #     )]

#     #     # Feature engineering
#     #     self.df_clean = self.add_features(self.df_clean)
#     #     self.df_365 = self.add_features(self.df_365)
#     #     self.df_200 = self.add_features(self.df_200)
#     #     self.df_100 = self.add_features(self.df_100)

#     #     # Fusion météo
#     #     self.df_clean = self.merge_with_weather(self.df_clean)
#     #     self.df_365 = self.merge_with_weather(self.df_365)
#     #     self.df_200 = self.merge_with_weather(self.df_200)
#     #     self.df_100 = self.merge_with_weather(self.df_100)

#     #     # Export
#     #     self.export_data()

#     #     return {
#     #         'df_clean': self.df_clean,
#     #         'df_365': self.df_365,
#     #         'df_200': self.df_200,
#     #         'df_100': self.df_100,
#     #         'duration': self.duration
#     #     }

# # ---------------------------------------------------------
# # Bloc test
# # ---------------------------------------------------------
# if __name__ == "__main__":
#     try:
#         df_velo = pd.read_csv("../../data/raw/velo_data.csv")
#         print(f"Données vélo chargées : {len(df_velo)} lignes")
#     except FileNotFoundError:
#         print("⚠ Fichier velo_data.csv non trouvé !")
#         df_velo = pd.DataFrame()

#     try:
#         df_meteo = pd.read_csv("../../data/raw/ma_meteo.csv")
#         print(f"Données météo chargées : {len(df_meteo)} lignes")
#     except FileNotFoundError:
#         print("⚠ Fichier meteo_data.csv non trouvé !")
#         df_meteo = None

#     if not df_velo.empty:
#         pipeline = VeloPipeline(df_velo, df_meteo)
#         results = pipeline.run_pipeline()
#         print("Pipeline terminé !")
#         print("Exemple df_clean :")
#         print(results['df_clean'].head())