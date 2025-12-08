#velo_service: pipeline de préparation des données.
# services/velo_service.py

import pandas as pd
import numpy as np
import os

class VeloPipeline:
    def __init__(self, df_velo: pd.DataFrame, df_meteo: pd.DataFrame = None):
        self.df_velo = df_velo.copy()
        self.df_meteo = df_meteo.copy() if df_meteo is not None else None
        self.duration = None
        self.df_clean = None
        self.df_365 = None
        self.df_200 = None
        self.df_100 = None

    # ---------------------------------------------------------
    # 1️ Nettoyage des données
    # ---------------------------------------------------------
    def clean_data(self):
        df = self.df_velo.drop_duplicates().copy()
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['intensity'] = pd.to_numeric(df['intensity'], errors='coerce')
        df['hour'] = df['datetime'].dt.hour
        df['weekday'] = df['datetime'].dt.weekday
        df['is_weekend'] = df['weekday'].isin([5, 6]).astype(int)

        df['intensity'] = df.groupby('counter_id')['intensity'].transform(
            lambda x: np.where(x > 300, np.nanmedian(x), x)
        )
        self.df_clean = df
        return df

    # ---------------------------------------------------------
    # 2️ Catégorisation des compteurs
    # ---------------------------------------------------------
    def categorize_counters(self):
        duration = self.df_clean.groupby("counter_id")["datetime"].agg(["min","max"])
        duration["days"] = (duration["max"] - duration["min"]).dt.days + 1

        def categorize(days):
            if days >= 365:
                return "plus de 365 jours"
            elif days >= 200:
                return "200 jours ou plus"
            elif days < 100:
                return "moins de 100 jours"
            else:
                return "autres"

        duration["category"] = duration["days"].apply(categorize)
        self.duration = duration
        return duration

    # ---------------------------------------------------------
    # 3️ Feature engineering
    # ---------------------------------------------------------
    def add_features(self, df: pd.DataFrame):
        df = df.copy()  # force une copie indépendante pour éviter le warning
        df['rolling_3h'] = df.groupby('counter_id')['intensity'].transform(
            lambda x: x.rolling(3, min_periods=1).mean()
        )
        df['lag_1h'] = df.groupby('counter_id')['intensity'].shift(1)
        df['lag_24h'] = df.groupby('counter_id')['intensity'].shift(24)
        df['pct_change_1h'] = df.groupby('counter_id')['intensity'].pct_change()
        return df

    # ---------------------------------------------------------
    # 4️ Fusion avec météo
    # ---------------------------------------------------------
    def merge_with_weather(self, df: pd.DataFrame):
        if self.df_meteo is None:
            return df
        df['datetime'] = df['datetime'].dt.tz_convert(None)
        df_m = self.df_meteo.copy()
        df_m['datetime'] = pd.to_datetime(df_m['datetime']).dt.tz_localize(None)
        df_merged = df.merge(df_m, on='datetime', how='left')
        return df_merged

    # ---------------------------------------------------------
    # 5️ Export des datasets
    # ---------------------------------------------------------
    def export_data(self,
                    path_clean="data_files/processed/data_clean.csv",
                    path_features="data_files/processed/features.csv"):

        # Créer les dossiers si inexistants
        os.makedirs(os.path.dirname(path_clean), exist_ok=True)
        os.makedirs(os.path.dirname(path_features), exist_ok=True)

        df_clean_copy = self.df_clean.copy()
        features_copy = df_clean_copy[['counter_id', 'datetime', 'intensity',
                                    'rolling_3h', 'lag_1h', 'lag_24h', 'pct_change_1h']].copy()

        df_clean_copy.to_csv(path_clean, index=False)
        features_copy.to_csv(path_features, index=False)

        print(f"Datasets exportés : {path_clean}, {path_features}")

    # ---------------------------------------------------------
    # 6️ Pipeline complet
    # ---------------------------------------------------------
    def run_pipeline(self):
        self.clean_data()
        self.categorize_counters()
    
        # Filtrage compteurs par durée
        self.df_365 = self.df_clean[self.df_clean['counter_id'].isin(
            self.duration[self.duration['category'] == "plus de 365 jours"].index
        )]
        self.df_200 = self.df_clean[self.df_clean['counter_id'].isin(
            self.duration[self.duration['category'] == "200 jours ou plus"].index
        )]
        self.df_100 = self.df_clean[self.df_clean['counter_id'].isin(
            self.duration[self.duration['category'] == "moins de 100 jours"].index
        )]

        # Feature engineering
        self.df_clean = self.add_features(self.df_clean)
        self.df_365 = self.add_features(self.df_365)
        self.df_200 = self.add_features(self.df_200)
        self.df_100 = self.add_features(self.df_100)

        # Fusion météo
        self.df_clean = self.merge_with_weather(self.df_clean)
        self.df_365 = self.merge_with_weather(self.df_365)
        self.df_200 = self.merge_with_weather(self.df_200)
        self.df_100 = self.merge_with_weather(self.df_100)

        # Export
        self.export_data()

        return {
            'df_clean': self.df_clean,
            'df_365': self.df_365,
            'df_200': self.df_200,
            'df_100': self.df_100,
            'duration': self.duration
        }

# ---------------------------------------------------------
# Bloc test CORRIGÉ
# ---------------------------------------------------------
if __name__ == "__main__":

    # --- 1. CHARGEMENT VÉLO ---
    try:
        print("Chargement vélo...")
        # CRUCIAL : sep=';'
        df_velo = pd.read_csv("data_files/velo_data.csv", sep=';')
        
        # Nettoyage Date Vélo : On gère le '+00:00' (UTC) et on le retire pour avoir une date naïve
        # df_velo['datetime'] = pd.to_datetime(df_velo['datetime'], utc=True).dt.tz_localize(None)
        
        print(f"✅ Vélo chargé : {len(df_velo)} lignes")
        # Debug pour être sûr
        # print(df_velo.head(1)) 

    except FileNotFoundError:
        print("⚠ Fichier velo_data.csv non trouvé !")
        df_velo = pd.DataFrame()

    # --- 2. CHARGEMENT MÉTÉO ---
    try:
        print("Chargement météo...")
        # CRUCIAL : sep=';' ET decimal=',' (sinon '5,8' devient du texte)
        df_meteo = pd.read_csv("data_files/meteo_mtp.csv", sep=';', decimal=',')
        
        # Nettoyage Date Météo : Format standard YYYY-MM-DD HH:MM:SS
        # df_meteo['datetime'] = pd.to_datetime(df_meteo['datetime'])
        
        # NOTE : Si vos données météo sont en heure locale (Paris), décommentez les lignes suivantes
        # pour les convertir en UTC comme le vélo. Sinon, on suppose qu'elles sont déjà synchro.
        # df_meteo['datetime'] = df_meteo['datetime'].dt.tz_localize('Europe/Paris', ambiguous=True).dt.tz_convert('UTC').dt.tz_localize(None)

        print(f"✅ Météo chargée : {len(df_meteo)} lignes")

    except FileNotFoundError:
        print("⚠ Fichier meteo_mtp.csv non trouvé !")
        df_meteo = None

    # --- 3. LANCEMENT PIPELINE ---
    if not df_velo.empty:
        # On instancie la classe avec les DataFrames propres
        pipeline = VeloPipeline(df_velo, df_meteo)
        
        # On lance le traitement
        results = pipeline.run_pipeline()
        
        print("\n🎉 Pipeline terminé avec succès !")
        print("Aperçu df_clean :")
        print(results['df_clean'].head())