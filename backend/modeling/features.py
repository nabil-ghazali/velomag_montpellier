import os
import pandas as pd
import numpy as np
import holidays
from dotenv import load_dotenv
from database.schemas import Database

load_dotenv()

class FeatureEngineering:
    def __init__(self):
        # Configuration de la DB
        self.user = os.getenv("user")
        self.password = os.getenv("password")
        self.host = os.getenv("host")
        self.port = os.getenv("port")
        self.dbname = os.getenv("dbname")
        
        self.database_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}?sslmode=require"
        self.db = Database(self.database_url)

    def create_dataset(self) -> pd.DataFrame:
        """
        Orchestre tout le processus :
        1. Récupération (DB)
        2. Fusion (Merge)
        3. Feature Engineering avancé (Pipeline)
        """
        print("1️⃣  Chargement des données depuis la DB...")
        df_velo = self.db.pull_data("velo_clean")
        df_meteo = self.db.pull_data("meteo_clean")

        # Conversion et Nettoyage des Timezones
        df_velo['datetime'] = pd.to_datetime(df_velo['datetime'], utc=True).dt.tz_localize(None)
        df_meteo['datetime'] = pd.to_datetime(df_meteo['datetime'], utc=True).dt.tz_localize(None)

        # Fusion (Merge)
        print("2️⃣  Fusion des données Vélos & Météo...")
        df_merged = pd.merge(
            df_velo,
            df_meteo,
            on='datetime',
            how='left'
        )

        # On lance la pipeline de transformation
        print("3️⃣  Feature Engineering (Création des Lags & Cycles)...")
        df_final = self._pipeline_feature_engineering_finale(df_merged)

        print(f" Dataset final prêt : {df_final.shape[0]} lignes, {df_final.shape[1]} colonnes.")
        return df_final

    def _pipeline_feature_engineering_finale(self, df_input):
        """
        Prépare les données pour une prédiction à J+1.
        Gère le ré-échantillonnage, les features cycliques et les lags.
        """
        # 1. Copie de sécurité
        df = df_input.copy()
        
        # ---------------------------------------------------------
        # ÉTAPE 0 : STANDARDISATION DES NOMS 
        # ---------------------------------------------------------
        """
        La standardisation des noms permettra d'utiliser cette pipeline avec le model prophet
        """
        rename_dict = {
            'datetime': 'ds', 
            'intensity': 'count'
        }
        # On ne renomme que si les colonnes existent (pour éviter les erreurs)
        df = df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns})
        df['ds'] = pd.to_datetime(df['ds'])
        
        # ---------------------------------------------------------
        # ÉTAPE 1 : NETTOYAGE & RESAMPLING (Robustesse)
        # ---------------------------------------------------------
        df_list = []
        weather_cols = ['temperature_2m', 'wind_speed_10m', 'precipitation']
        
        # On travaille compteur par compteur pour ne pas mélanger les données
        unique_counters = df['counter_id'].unique()
        
        for counter in unique_counters:
            # Extraction des données du compteur spécifique
            temp = df[df['counter_id'] == counter].set_index('ds')
            
            # On supprime les doublons d'index s'il y en a
            temp = temp[~temp.index.duplicated(keep='first')]

            # On force la grille horaire (remplit les trous temporels)
            temp = temp.resample('h').asfreq()
            
            # Remplissage intelligent :
            # 1. Le vélo -> 0 si manquant
            temp['count'] = temp['count'].fillna(0)
            
            # 2. La météo -> On propage la dernière valeur connue (Forward Fill)
            existing_weather = [c for c in weather_cols if c in temp.columns]
            if existing_weather:
                temp[existing_weather] = temp[existing_weather].ffill().fillna(0)
            
            # 3. L'ID du compteur
            temp['counter_id'] = counter
            
            df_list.append(temp.reset_index())
            
        df = pd.concat(df_list, ignore_index=True)

        # ---------------------------------------------------------
        # ÉTAPE 2 : FEATURES TEMPORELLES & CYCLIQUES
        # ---------------------------------------------------------
        df['hour'] = df['ds'].dt.hour
        df['day_of_week'] = df['ds'].dt.dayofweek
        df['month'] = df['ds'].dt.month
        df['year'] = df['ds'].dt.year 
        
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

        # Encodage Cyclique (Sinus/Cosinus) pour que 23h soit proche de 00h
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
        df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

        # ---------------------------------------------------------
        # ÉTAPE 3 : CALENDRIER (Jours Fériés France)
        # ---------------------------------------------------------
        fr_holidays = holidays.France()
        df['is_holiday'] = df['ds'].apply(lambda x: 1 if x in fr_holidays else 0)

        # ---------------------------------------------------------
        # ÉTAPE 4 : LAGS (La Mémoire du Modèle)
        # ---------------------------------------------------------
        df = df.sort_values(['counter_id', 'ds'])
        
        # Lag 24h (Trafic d'hier à la même heure)
        df['lag_24h'] = df.groupby('counter_id')['count'].shift(24)
        
        # Lag 48h (Trafic d'avant-hier)
        df['lag_48h'] = df.groupby('counter_id')['count'].shift(48)
        
        # Lag 1 semaine (Trafic semaine dernière)
        df['lag_168h'] = df.groupby('counter_id')['count'].shift(168)
        
        # Moyenne glissante sur les 4 derniers jours à la même heure
        # (Sert à lisser les pics inhabituels)
        grouped = df.groupby('counter_id')['count']
        df['mean_last_4_days'] = grouped.shift(24).rolling(window=4).mean()

        # ---------------------------------------------------------
        # ÉTAPE 5 : NETTOYAGE FINAL
        # ---------------------------------------------------------
        # Encodage ID pour XGBoost (Transforme les strings en nombres)
        df['counter_id_encoded'] = df['counter_id'].astype('category').cat.codes
        
        # Suppression des NaN générés par les lags (les 7 premiers jours de l'historique sont vides)
        df = df.dropna()
        
        return df

# --- Bloc de test ---
if __name__ == "__main__":
    fe = FeatureEngineering()
    try:
        df_final = fe.create_dataset()
        print(df_final.head())
        print(df_final.columns)
        
        # Sauvegarde pour vérification
        df_final.to_csv("train_data_final.csv", index=False)
        print(" Fichier 'train_data_final.csv' généré pour vérification.")
        
    except Exception as e:
        print(f" Erreur : {e}")