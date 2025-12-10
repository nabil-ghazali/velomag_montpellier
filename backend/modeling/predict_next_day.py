import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import requests
import holidays
from datetime import datetime, timedelta
from backend.data.schemas import Database
from backend.modeling.features import FeatureEngineering
from pathlib import Path

class Predictor:
    def __init__(self):
        # Chemin absolu dynamique
        current_dir = Path(__file__).resolve().parent
        self.model_path = current_dir.parent / "model" / "model_velo.pkl"
        print(f" Chemin du modèle défini sur : {self.model_path}")
        
        # Jusqu'à quand prédire ? (Demain réel)
        self.real_tomorrow = datetime.now().date() + timedelta(days=1)
        
    def get_weather_data(self, date_target) -> pd.DataFrame:
        """Récupère météo Archive (Passé) ou Forecast (Futur)"""
        is_past = date_target.date() < datetime.now().date()
        
        url = "https://archive-api.open-meteo.com/v1/era5" if is_past else "https://api.open-meteo.com/v1/forecast"
        
        params = {
            "latitude": 43.6108, "longitude": 3.8767,
            "hourly": "temperature_2m,wind_speed_10m,precipitation",
            "start_date": date_target.strftime("%Y-%m-%d"),
            "end_date": date_target.strftime("%Y-%m-%d")
        }
        if not is_past: params["timezone"] = "UTC"

        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            df = pd.DataFrame(r.json()['hourly'])
            df['ds'] = pd.to_datetime(df['time'])
            return df
        except:
            dates = pd.date_range(start=date_target, periods=24, freq='h')
            return pd.DataFrame({'ds': dates, 'temperature_2m': 12, 'wind_speed_10m': 10, 'precipitation': 0})

    def run_recursive_prediction(self):
        print(f" Démarrage du mode RÉCURSIF HYBRIDE...")
        print(f" Objectif : Atteindre le {self.real_tomorrow}")

        # 1. Chargement Historique
        fe = FeatureEngineering()
        df_history = fe.create_dataset()
        df_history = df_history.sort_values(['counter_id', 'ds'])

        # --- AJOUT : Préparation des coordonnées ---
        # On crée un petit dictionnaire/dataframe des coordonnées pour chaque compteur
        # On prend la dernière position connue pour chaque ID
        coords_ref = df_history[['counter_id', 'lat', 'lon']].drop_duplicates(subset=['counter_id'], keep='last')
        # -------------------------------------------

        # Mémoire
        memory = df_history.set_index(['counter_id', 'ds'])['count'].to_dict()

        # 2. Initialisation Boucle
        last_known_date = df_history['ds'].max()
        current_target_date = last_known_date + timedelta(days=1)
        current_target_date = current_target_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # 3. Chargement Modèle
        try:
            model = joblib.load(self.model_path)
            model_cols = model.get_booster().feature_names
        except Exception as e:
            print(f" Erreur Modèle : {e}"); return

        # === BOUCLE ===
        while current_target_date.date() <= self.real_tomorrow:
            print(f" Calcul pour le : {current_target_date.date()} ...")
            
            df_weather = self.get_weather_data(current_target_date)
            
            counters = df_history['counter_id'].unique()
            unique_encoded = df_history[['counter_id', 'counter_id_encoded']].drop_duplicates()
            
            day_rows = []
            
            for counter in counters:
                if unique_encoded[unique_encoded['counter_id'] == counter].empty: continue
                encoded_id = unique_encoded[unique_encoded['counter_id'] == counter]['counter_id_encoded'].values[0]
                
                temp_df = df_weather.copy()
                temp_df['counter_id'] = counter
                temp_df['counter_id_encoded'] = encoded_id
                
                # Récupération Lags
                def get_lag(row, hours):
                    target = row['ds'] - timedelta(hours=hours)
                    return memory.get((counter, target), 0)

                temp_df['lag_24h'] = temp_df.apply(lambda r: get_lag(r, 24), axis=1)
                temp_df['lag_48h'] = temp_df.apply(lambda r: get_lag(r, 48), axis=1)
                temp_df['lag_168h'] = temp_df.apply(lambda r: get_lag(r, 168), axis=1)
                temp_df['lag_336h'] = temp_df.apply(lambda r: get_lag(r, 336), axis=1)
                temp_df['lag_504h'] = temp_df.apply(lambda r: get_lag(r, 504), axis=1)
                temp_df['mean_last_4_days'] = (temp_df['lag_24h'] + temp_df['lag_48h']) / 2

                day_rows.append(temp_df)
            
            if not day_rows:
                current_target_date += timedelta(days=1)
                continue

            df_day = pd.concat(day_rows, ignore_index=True)
            
            # Features
            df_day['hour'] = df_day['ds'].dt.hour
            df_day['day_of_week'] = df_day['ds'].dt.dayofweek
            df_day['month'] = df_day['ds'].dt.month
            df_day['is_weekend'] = df_day['day_of_week'].isin([5, 6]).astype(int)
            fr_holidays = holidays.France()
            df_day['is_holiday'] = df_day['ds'].apply(lambda x: 1 if x in fr_holidays else 0)
            
            df_day['hour_sin'] = np.sin(2 * np.pi * df_day['hour'] / 24)
            df_day['hour_cos'] = np.cos(2 * np.pi * df_day['hour'] / 24)
            df_day['month_sin'] = np.sin(2 * np.pi * df_day['month'] / 12)
            df_day['month_cos'] = np.cos(2 * np.pi * df_day['month'] / 12)
            df_day['dow_sin'] = np.sin(2 * np.pi * df_day['day_of_week'] / 7)
            df_day['dow_cos'] = np.cos(2 * np.pi * df_day['day_of_week'] / 7)

            # Prédiction
            for col in model_cols:
                if col not in df_day.columns: df_day[col] = 0
            
            preds = model.predict(df_day[model_cols])
            preds = [max(0, int(x)) for x in preds]
            
            # Mise à jour Mémoire
            df_day['predicted_values'] = preds
            for _, row in df_day.iterrows():
                memory[(row['counter_id'], row['ds'])] = row['predicted_values']
            
            # --- Sauvegarde BDD CORRIGÉE ---
            df_export = df_day[['ds', 'counter_id', 'predicted_values']].rename(columns={'ds': 'datetime'})
            
            #  FUSION AVEC LES COORDONNÉES 
            df_export = df_export.merge(coords_ref, on='counter_id', how='left')
            
            try:
                # On pousse datetime, counter_id, predicted_values, lat, lon
                fe.db.push_data(df_export, "model_data")
            except Exception as e:
                print(f" Erreur BDD : {e}")

            current_target_date += timedelta(days=1)

        print(" Terminé ! Prédictions (avec GPS) envoyées.")

if __name__ == "__main__":
    p = Predictor()
    p.run_recursive_prediction()